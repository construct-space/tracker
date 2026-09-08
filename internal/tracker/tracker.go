package tracker

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"construct/tracker/internal/config"
)

// isWebSocketUpgrade reports whether the request is actually trying to
// open a WebSocket. RFC 6455 requires both `Connection: upgrade` and
// `Upgrade: websocket` (case-insensitive). Plain HTTP scanners send
// neither, so we can short-circuit them cheaply.
func isWebSocketUpgrade(r *http.Request) bool {
	conn := strings.ToLower(r.Header.Get("Connection"))
	if !strings.Contains(conn, "upgrade") {
		return false
	}
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// Trystero / WebTorrent WSS tracker protocol.
//
// Wire format is a single JSON object per WebSocket message. We only
// implement `action:"announce"` — the protocol's full BEP set isn't
// needed for WebRTC-only swarms.
//
// Announce from a client carries either:
//   - an offer batch (the peer wants to be matched with others):
//       { action, info_hash, peer_id, numwant, offers:[{offer_id, offer:{type:"offer", sdp}}] }
//   - an answer to a specific peer (replying to an offer we forwarded):
//       { action, info_hash, peer_id, to_peer_id, offer_id, answer:{type:"answer", sdp} }
//   - a lifecycle event (we honor "stopped"; others are ignored):
//       { action, info_hash, peer_id, event:"stopped" }
//
// What we send back:
//   - an interval ack to the announcer (so they know we're alive):
//       { action:"announce", info_hash, interval, complete, incomplete }
//   - forwarded offers to up-to-numwant other peers in the same swarm:
//       { action:"announce", info_hash, peer_id:<offerer>, offer:{...}, offer_id }
//   - forwarded answers to the original offerer:
//       { action:"announce", info_hash, peer_id:<answerer>, answer:{...}, offer_id }
//
// Identity: peer_id is a 20-char string the client picks. We trust it
// within a single connection; cross-connection collisions would only
// confuse the colliders, so no global uniqueness check.

type message struct {
	Action    string          `json:"action,omitempty"`
	InfoHash  string          `json:"info_hash,omitempty"`
	PeerID    string          `json:"peer_id,omitempty"`
	ToPeerID  string          `json:"to_peer_id,omitempty"`
	OfferID   string          `json:"offer_id,omitempty"`
	Numwant   int             `json:"numwant,omitempty"`
	Offers    []offerEntry    `json:"offers,omitempty"`
	Offer     json.RawMessage `json:"offer,omitempty"`
	Answer    json.RawMessage `json:"answer,omitempty"`
	Event     string          `json:"event,omitempty"`
	Interval  int             `json:"interval,omitempty"`
	Complete  int             `json:"complete,omitempty"`
	Incompl   int             `json:"incomplete,omitempty"`
	Failure   string          `json:"failure reason,omitempty"`
	Warning   string          `json:"warning message,omitempty"`
}

type offerEntry struct {
	OfferID string          `json:"offer_id"`
	Offer   json.RawMessage `json:"offer"`
}

// peer is a connected client. Writes are serialized through send so the
// reader goroutine of one connection can hand work to the writer of
// another without locking the socket.
type peer struct {
	id     string
	swarms map[string]struct{} // info_hashes this peer has announced on
	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	conn   *websocket.Conn
	last   time.Time
}

// Swarm tracks all peers for a single info_hash.
type swarm struct {
	mu    sync.RWMutex
	peers map[string]*peer
}

func (s *swarm) add(p *peer)        { s.mu.Lock(); s.peers[p.id] = p; s.mu.Unlock() }
func (s *swarm) remove(id string)   { s.mu.Lock(); delete(s.peers, id); s.mu.Unlock() }
func (s *swarm) size() int          { s.mu.RLock(); defer s.mu.RUnlock(); return len(s.peers) }
func (s *swarm) get(id string) *peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peers[id]
}

// pick returns up to n peers other than `exclude`, in random order. This
// is the matchmaking step — every announcer gets a different slice each
// time, so a busy swarm meshes over many announces.
func (s *swarm) pick(exclude string, n int) []*peer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n <= 0 || len(s.peers) <= 1 {
		return nil
	}
	out := make([]*peer, 0, n)
	// Reservoir-ish: walk all peers (Go's map iteration is randomized),
	// take up to n that aren't the excluded one.
	for _, p := range s.peers {
		if p.id == exclude {
			continue
		}
		out = append(out, p)
		if len(out) >= n {
			break
		}
	}
	return out
}

// Tracker is the in-memory swarm registry. There's deliberately no
// persistence — peers come and go in seconds, and the whole point of
// the tracker is to be a rendezvous, not state of record.
type Tracker struct {
	cfg    *config.Config
	mu     sync.Mutex
	swarms map[string]*swarm
}

func New(cfg *config.Config) *Tracker {
	t := &Tracker{cfg: cfg, swarms: map[string]*swarm{}}
	go t.reapLoop()
	return t
}

// reapLoop periodically prunes empty swarms so the top-level map doesn't
// grow without bound when callers create one-off rooms.
func (t *Tracker) reapLoop() {
	tick := time.NewTicker(60 * time.Second)
	defer tick.Stop()
	for range tick.C {
		t.mu.Lock()
		for k, s := range t.swarms {
			if s.size() == 0 {
				delete(t.swarms, k)
			}
		}
		t.mu.Unlock()
	}
}

// Handle upgrades the request to WebSocket and runs one peer session.
// Connection lifecycle: reader goroutine parses messages and forwards
// to internal handlers; writer goroutine drains the send channel. When
// either ends, ctx is cancelled and the peer is removed from all swarms.
//
// Non-WebSocket GETs (browsers, healthcheckers, internet scanners
// probing for /telescope, /info.php, etc.) get a plain 200 with a short
// banner instead of a noisy upgrade-failed log line.
func (t *Tracker) Handle(w http.ResponseWriter, r *http.Request) {
	if !isWebSocketUpgrade(r) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("construct tracker — WebSocket only\n"))
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // origin already checked upstream
		Subprotocols:       []string{},
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		// Don't log — real clients don't fail here; the noise comes
		// from misbehaving scanners that already passed isWebSocketUpgrade
		// but sent a malformed handshake.
		return
	}
	defer conn.CloseNow()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	p := &peer{
		swarms: map[string]struct{}{},
		send:   make(chan []byte, 64),
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		last:   time.Now(),
	}

	// Writer pump. Closes the socket on send-channel close.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-p.send:
				if !ok {
					return
				}
				wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
				err := conn.Write(wctx, websocket.MessageText, msg)
				wcancel()
				if err != nil {
					return
				}
			}
		}
	}()

	// Reader loop.
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		p.last = time.Now()
		var msg message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Action != "announce" {
			continue
		}
		if msg.PeerID == "" || msg.InfoHash == "" {
			continue
		}
		// First announce on this connection sets the peer id.
		if p.id == "" {
			p.id = msg.PeerID
		} else if p.id != msg.PeerID {
			// Peers can talk on one connection only. Drop mismatches.
			continue
		}
		t.handleAnnounce(p, &msg)
	}

	// Cleanup: yank this peer from every swarm it joined.
	for ih := range p.swarms {
		t.mu.Lock()
		if s, ok := t.swarms[ih]; ok {
			s.remove(p.id)
		}
		t.mu.Unlock()
	}
	cancel()
}

func (t *Tracker) swarmFor(infoHash string) *swarm {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.swarms[infoHash]
	if !ok {
		s = &swarm{peers: map[string]*peer{}}
		t.swarms[infoHash] = s
	}
	return s
}

func (t *Tracker) handleAnnounce(p *peer, m *message) {
	s := t.swarmFor(m.InfoHash)

	if m.Event == "stopped" {
		s.remove(p.id)
		delete(p.swarms, m.InfoHash)
		return
	}

	if _, joined := p.swarms[m.InfoHash]; !joined {
		if s.size() >= t.cfg.MaxPeersPerSwarm {
			send(p, message{
				Action:   "announce",
				InfoHash: m.InfoHash,
				Failure:  "swarm full",
			})
			return
		}
		s.add(p)
		p.swarms[m.InfoHash] = struct{}{}
	}

	// Ack the announcer with interval + swarm size.
	send(p, message{
		Action:   "announce",
		InfoHash: m.InfoHash,
		Interval: t.cfg.AnnounceIntervalSeconds,
		Complete: s.size(),
		Incompl:  0,
	})

	// Case 1: announce carries offers — match with other peers.
	if len(m.Offers) > 0 {
		numwant := m.Numwant
		if numwant <= 0 || numwant > t.cfg.MaxOffersPerAnnounce {
			numwant = t.cfg.MaxOffersPerAnnounce
		}
		offers := m.Offers
		if numwant > len(offers) {
			numwant = len(offers)
		}
		// Shuffle the offers so successive announcers don't all send the
		// same offer to the same recipient.
		rand.Shuffle(len(offers), func(i, j int) { offers[i], offers[j] = offers[j], offers[i] })
		picks := s.pick(p.id, numwant)
		for i, recipient := range picks {
			if i >= len(offers) {
				break
			}
			send(recipient, message{
				Action:   "announce",
				InfoHash: m.InfoHash,
				PeerID:   p.id,
				OfferID:  offers[i].OfferID,
				Offer:    offers[i].Offer,
			})
		}
		return
	}

	// Case 2: announce is an answer to a previously forwarded offer.
	if len(m.Answer) > 0 && m.ToPeerID != "" {
		target := s.get(m.ToPeerID)
		if target == nil {
			return
		}
		send(target, message{
			Action:   "announce",
			InfoHash: m.InfoHash,
			PeerID:   p.id,
			OfferID:  m.OfferID,
			Answer:   m.Answer,
		})
		return
	}
	// Plain keep-alive announce: nothing else to do; the ack above is
	// already on its way.
}

func send(p *peer, m message) {
	data, err := json.Marshal(m)
	if err != nil {
		return
	}
	select {
	case p.send <- data:
	case <-p.ctx.Done():
	default:
		// Backpressure: the writer can't keep up. Drop and disconnect
		// rather than block the sender's goroutine.
		p.cancel()
	}
}

// Stats snapshots active swarms / peers for /health responses.
func (t *Tracker) Stats() (swarms, peers int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	swarms = len(t.swarms)
	seen := map[string]struct{}{}
	for _, s := range t.swarms {
		s.mu.RLock()
		for id := range s.peers {
			seen[id] = struct{}{}
		}
		s.mu.RUnlock()
	}
	peers = len(seen)
	return
}

