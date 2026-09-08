package config

import (
	"bufio"
	"os"
	"strings"
)

// Config is the tracker runtime config. We expose AllowedOrigins so the
// WebSocket upgrade can refuse non-allowlisted browsers (Origin check),
// MaxOffersPerAnnounce to cap fan-out, and AnnounceIntervalSeconds to
// tell clients how often to re-announce.
type Config struct {
	Port           string
	AllowedOrigins []string

	// AnnounceIntervalSeconds — how often clients should re-announce.
	// 30s matches Trystero's defaultAnnounceMs (clients clamp anyway).
	AnnounceIntervalSeconds int

	// MaxOffersPerAnnounce caps how many SDP offers a single announce can
	// carry. Trystero uses offerPoolSize=3; a misbehaving client could
	// flood. Default 20.
	MaxOffersPerAnnounce int

	// MaxPeersPerSwarm — soft cap before we start refusing new joiners
	// for a single info_hash. Prevents one swarm from eating the box.
	MaxPeersPerSwarm int

	// IdleTimeoutSeconds — peer is dropped if no announce in this window.
	IdleTimeoutSeconds int
}

func Load() *Config {
	loadEnvFile(".env")
	origins := splitCSV(env("ALLOWED_ORIGINS", "*"))
	return &Config{
		Port:                    env("PORT", "8080"),
		AllowedOrigins:          origins,
		AnnounceIntervalSeconds: envInt("ANNOUNCE_INTERVAL_SECONDS", 30),
		MaxOffersPerAnnounce:    envInt("MAX_OFFERS_PER_ANNOUNCE", 20),
		MaxPeersPerSwarm:        envInt("MAX_PEERS_PER_SWARM", 500),
		IdleTimeoutSeconds:      envInt("IDLE_TIMEOUT_SECONDS", 120),
	}
}

// OriginAllowed reports whether a browser Origin header is allowed to
// open a WebSocket. "*" in the allowlist means allow-all.
func (c *Config) OriginAllowed(origin string) bool {
	for _, o := range c.AllowedOrigins {
		if o == "*" || o == origin {
			return true
		}
	}
	return false
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := env(key, "")
	if v == "" {
		return fallback
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return fallback
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if os.Getenv(key) == "" {
			_ = os.Setenv(key, val)
		}
	}
}
