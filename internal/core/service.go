// Package core holds source-controller's behavior: operator requests, the
// Source HTTP edge, webhook intake, polling, the outbox and the consumers. It
// depends only on the Store and broker.Broker interfaces.
package core

import (
	"context"
	"crypto/ed25519"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Publisher is the publisher name of every message source-controller emits.
const Publisher = "source-controller"

// SigningKey is one Ed25519 poll-signing key.
type SigningKey struct {
	ID      string
	Private ed25519.PrivateKey
}

// CursorKey is one cursor HMAC key; the first one signs new cursors.
type CursorKey struct {
	ID     int64
	Secret []byte
}

// Config configures the core.
type Config struct {
	PublicBaseURL      string
	PollAllowedOrigins []string
	PollSigningKeys    []SigningKey
	PollClient         *http.Client
	CursorKeys         []CursorKey
	// RequestTimeout bounds team-registry requests (default 5 s).
	RequestTimeout time.Duration
	// PublishTimeout bounds every JetStream publish acknowledgement (5 s).
	PublishTimeout time.Duration
	// Poll windows relative to leased_at (defaults 5 s, 35 s, +60 s).
	PollStartWindow time.Duration
	PollCallWindow  time.Duration
	PollExpiryGrace time.Duration
	// Now is the service clock for poll call timing; database times come
	// from Tx.Now.
	Now    func() time.Time
	Logger *slog.Logger
	// ReadyChecks run on /v1/readyz besides the database and broker checks.
	ReadyChecks []func(context.Context) error
}

// Service is the source-controller core.
type Service struct {
	cfg    Config
	store  Store
	broker broker.Broker
	log    *slog.Logger
	kick   chan struct{}
}

// ErrNotImplemented marks behavior not written yet.
var ErrNotImplemented = errors.New("not implemented")

// New builds the core.
func New(cfg Config, store Store, b broker.Broker) (*Service, error) {
	if store == nil || b == nil {
		return nil, errors.New("store and broker are required")
	}
	if len(cfg.PollSigningKeys) == 0 || len(cfg.CursorKeys) == 0 {
		return nil, errors.New("poll signing keys and cursor keys are required")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.PublishTimeout == 0 {
		cfg.PublishTimeout = 5 * time.Second
	}
	if cfg.PollStartWindow == 0 {
		cfg.PollStartWindow = 5 * time.Second
	}
	if cfg.PollCallWindow == 0 {
		cfg.PollCallWindow = 35 * time.Second
	}
	if cfg.PollExpiryGrace == 0 {
		cfg.PollExpiryGrace = 60 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.PollClient == nil {
		cfg.PollClient = &http.Client{}
	}
	return &Service{cfg: cfg, store: store, broker: b, log: cfg.Logger, kick: make(chan struct{}, 1)}, nil
}
