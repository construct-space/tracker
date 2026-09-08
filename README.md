# tracker

Self-hosted WebTorrent / Trystero WebRTC signaling tracker. Lets spaces
that use WebRTC mesh (currently `space-meet`) bootstrap peer connections
without depending on public BitTorrent trackers (which periodically
break, expire TLS certs, or get rate-limited).

## Protocol

Implements the WebSocket variant of the BitTorrent `announce` action
that Trystero (`@trystero-p2p/torrent`) and `bittorrent-tracker` speak.
One JSON message per WebSocket frame. We only handle `action:"announce"`;
the rest of BEP is unused on WebRTC swarms.

Three message shapes a client sends:

```json
// 1. offer batch — "match me with up to numwant other peers"
{
  "action": "announce", "info_hash": "...", "peer_id": "...",
  "numwant": 3,
  "offers": [{ "offer_id": "...", "offer": { "type": "offer", "sdp": "..." } }]
}

// 2. answer — reply to an offer the tracker forwarded
{
  "action": "announce", "info_hash": "...", "peer_id": "...",
  "to_peer_id": "<offerer>", "offer_id": "...",
  "answer": { "type": "answer", "sdp": "..." }
}

// 3. lifecycle
{ "action": "announce", "info_hash": "...", "peer_id": "...", "event": "stopped" }
```

Three message shapes the tracker sends back:

```json
// ack to the announcer (so clients know we're alive + how often to re-announce)
{ "action": "announce", "info_hash": "...", "interval": 30, "complete": 4, "incomplete": 0 }

// forwarded offer to a chosen recipient
{ "action": "announce", "info_hash": "...", "peer_id": "<offerer>",
  "offer_id": "...", "offer": { "type": "offer", "sdp": "..." } }

// forwarded answer to the original offerer
{ "action": "announce", "info_hash": "...", "peer_id": "<answerer>",
  "offer_id": "...", "answer": { "type": "answer", "sdp": "..." } }
```

No persistence. Swarms are in-memory; peers are dropped on disconnect.
Reap loop prunes empty swarms every 60s.

## Run locally

```bash
go run .
# wss://localhost:8080  (no TLS in dev — wire wscat or trystero against ws://)
curl http://localhost:8080/health
```

## Config (env)

| Var | Default | Notes |
|---|---|---|
| `PORT` | `8080` (`80` in container) | Listen port |
| `ALLOWED_ORIGINS` | `*` | CSV of allowed `Origin` headers. Use `*` to allow all. |
| `ANNOUNCE_INTERVAL_SECONDS` | `30` | Tells clients how often to re-announce |
| `MAX_OFFERS_PER_ANNOUNCE` | `20` | Cap on offers a single announce can carry |
| `MAX_PEERS_PER_SWARM` | `500` | Soft cap before a swarm refuses new joiners |
| `IDLE_TIMEOUT_SECONDS` | `120` | Currently unused (peers dropped on socket close) |

## Deploy (CapRover)

Same shape as `storage` / `delivery`: `captain-definition` + `Dockerfile`.
Point `tracker.lisaos.dev` at the CapRover app, enable HTTPS, done.

Browsers require `wss://`, so TLS is mandatory in any non-localhost
deployment — CapRover's automatic Let's Encrypt handles this.

## Wire from a space

In `space-meet/src/composables/useCall.ts`, pass `relayUrls` to Trystero:

```ts
room = joinRoom(
  {
    appId: options.appId || 'construct.meet',
    rtcConfig: ice,
    relayUrls: ['wss://tracker.lisaos.dev'],
  },
  name,
)
```

Drop the public trackers entirely once this is in production, or keep
one as a fallback in the same array.
