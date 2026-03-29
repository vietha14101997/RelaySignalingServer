package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port            int
	TLSCertFile     string
	TLSKeyFile      string
	MaxMessageSize  int64
	RoomIdleTimeout int // seconds

	// Database (empty = dev mode, no DB required)
	DatabaseURL string

	// JWT
	JWTSecret       string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration

	// TURN/STUN (coturn)
	TURNSecret string // shared secret with coturn (use-auth-secret)
	TURNDomain string // public domain/IP for STUN/TURN URLs
	STUNPort   int    // default 3478
	TURNSPort  int    // default 5349 (TLS)
	TURNCredTTL time.Duration // credential TTL, default 24h
}

func Load() *Config {
	cfg := &Config{
		Port:            8443,
		MaxMessageSize:  64 << 20, // 64MB (speed test sends large binary frames)
		RoomIdleTimeout: 300,     // 5 minutes
		DatabaseURL:     "", // empty = dev mode (no DB)
		JWTSecret:       "change-me-in-production",
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 30 * 24 * time.Hour, // 30 days
		TURNSecret:      "",
		TURNDomain:      "",
		STUNPort:        3478,
		TURNSPort:       5349,
		TURNCredTTL:     24 * time.Hour,
	}

	if p := os.Getenv("RELAY_PORT"); p != "" {
		if v, err := strconv.Atoi(p); err == nil {
			cfg.Port = v
		}
	}
	if f := os.Getenv("TLS_CERT_FILE"); f != "" {
		cfg.TLSCertFile = f
	}
	if f := os.Getenv("TLS_KEY_FILE"); f != "" {
		cfg.TLSKeyFile = f
	}
	if d := os.Getenv("DATABASE_URL"); d != "" {
		cfg.DatabaseURL = d
	}
	if s := os.Getenv("JWT_SECRET"); s != "" {
		cfg.JWTSecret = s
	}
	if s := os.Getenv("TURN_SECRET"); s != "" {
		cfg.TURNSecret = s
	}
	if d := os.Getenv("TURN_DOMAIN"); d != "" {
		cfg.TURNDomain = d
	}

	return cfg
}

func (c *Config) HasTLS() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}
