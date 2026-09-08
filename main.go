package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"construct/tracker/internal/config"
	"construct/tracker/internal/tracker"
)

func main() {
	cfg := config.Load()
	tr := tracker.New(cfg)

	mux := http.NewServeMux()

	// WebSocket endpoint clients connect to. Trystero / WebTorrent use
	// the root path with no announce suffix; we accept both for
	// compatibility with the wider ecosystem.
	wsHandler := originGuard(cfg, http.HandlerFunc(tr.Handle))
	mux.Handle("/", wsHandler)
	mux.Handle("/announce", wsHandler)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		swarms, peers := tr.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     true,
			"swarms": swarms,
			"peers":  peers,
		})
	})

	log.Printf("tracker listening on :%s (origins=%v, interval=%ds)", cfg.Port, cfg.AllowedOrigins, cfg.AnnounceIntervalSeconds)
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           withLogger(mux),
		ReadHeaderTimeout: 10 * time.Second,
		// No ReadTimeout/WriteTimeout: a WebSocket connection is meant
		// to be long-lived; the websocket library manages its own
		// per-message deadlines.
		IdleTimeout: 0,
	}
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// originGuard rejects WebSocket handshakes from origins not on the
// allowlist. The websocket library does its own same-origin check; this
// is an explicit additional gate so AllowedOrigins is the single source
// of truth.
func originGuard(cfg *config.Config, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !cfg.OriginAllowed(origin) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		// Deliberately silent. A public tracker gets constant scanner
		// probes (telescope, actuator, info.php, .well-known/*) plus
		// legitimate WS upgrades whose interesting lifecycle is logged
		// by the tracker itself if we ever need it. Use /health for
		// liveness instead of access logs.
	})
}
