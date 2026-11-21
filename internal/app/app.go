// Package app wires source-controller: configuration, PostgreSQL, NATS, the
// HTTP edge and the background loops.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/pandops/flowai-source-controller/internal/config"
	"github.com/pandops/flowai-source-controller/internal/core"
	"github.com/pandops/flowai-source-controller/internal/natsbroker"
	"github.com/pandops/flowai-source-controller/internal/postgres"
)

const (
	stream        = "ALL_EVENT"
	consumerName  = "source-controller"
	pollInterval  = time.Second
	flushInterval = 2 * time.Second
)

// Run starts the service and blocks until ctx ends.
func Run(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	if err := cfg.CheckFiles(); err != nil {
		return err
	}
	signing, err := cfg.SigningKeys()
	if err != nil {
		return err
	}
	cursors, err := cfg.DecodedCursorKeys()
	if err != nil {
		return err
	}
	pollClient, err := pollHTTPClient(cfg.Poll.CAFile)
	if err != nil {
		return err
	}

	store, err := postgres.Open(ctx, cfg.DSN(), cfg.DB.Schema)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	opts, err := natsbroker.Options(natsbroker.Settings{URL: cfg.NATS.URL, CAFile: cfg.NATS.CAFile, CredentialsFile: cfg.NATS.CredentialsFile,
		InboxPrefix: cfg.NATS.InboxPrefix}, log)
	if err != nil {
		return err
	}
	nc, err := nats.Connect(cfg.NATS.URL, opts...)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}
	defer nc.Close()
	b, err := natsbroker.New(nc)
	if err != nil {
		return err
	}

	keys := make([]core.SigningKey, len(signing))
	for i, k := range signing {
		keys[i] = core.SigningKey{ID: k.ID, Private: k.Private}
	}
	cks := make([]core.CursorKey, len(cursors))
	for i, k := range cursors {
		cks[i] = core.CursorKey{ID: k.ID, Secret: k.Secret}
	}
	svc, consumer, err := Wire(core.Config{
		PublicBaseURL:      cfg.PublicBaseURL,
		PollAllowedOrigins: cfg.Poll.AllowedOrigins,
		PollSigningKeys:    keys,
		PollClient:         pollClient,
		CursorKeys:         cks,
		RequestTimeout:     cfg.NATS.RequestTimeout,
		Logger:             log,
	}, store, b)
	if err != nil {
		return err
	}

	subs, err := b.ServeRequests(svc, cfg.NATS.RequestTimeout)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer func() {
		for _, s := range subs {
			_ = s.Drain()
		}
	}()
	Start(ctx, svc, consumer, log)

	server := &http.Server{Addr: cfg.HTTP.Listen, Handler: svc.HTTPHandler(), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- server.ListenAndServe() }()
	log.Info("source-controller started", "listen", cfg.HTTP.Listen)
	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return server.Shutdown(shutdown)
}

// Wire builds the core over the broker as production runs it: it binds the
// declared durable consumer of ALL_EVENT and adds that consumer to the
// readiness checks, next to the broker connection and the database.
func Wire(c core.Config, store core.Store, b *natsbroker.Broker) (*core.Service, *natsbroker.Consumer, error) {
	consumer := b.NewConsumer(stream, consumerName)
	c.ReadyChecks = append(append([]func(context.Context) error(nil), c.ReadyChecks...), consumer.Ready)
	svc, err := core.New(c, store, b)
	if err != nil {
		return nil, nil, err
	}
	return svc, consumer, nil
}

// Start runs the background work until ctx ends: the durable consumer, the
// poll cycle and the outbox relay, each of which keeps retrying while the
// broker or the database is unavailable.
func Start(ctx context.Context, svc *core.Service, consumer *natsbroker.Consumer, log *slog.Logger) {
	go consumer.Run(ctx, svc, log)
	go loop(ctx, pollInterval, nil, func() {
		if err := svc.RunPollCycle(ctx); err != nil {
			log.Error("poll cycle failed")
		}
	})
	go loop(ctx, flushInterval, svc.Kick(), func() {
		if _, err := svc.FlushOutbox(ctx); err != nil && ctx.Err() == nil {
			log.Warn("outbox not fully published; will retry")
		}
	})
}

// loop runs fn every interval and whenever kick fires, until ctx ends.
func loop(ctx context.Context, interval time.Duration, kick <-chan struct{}, fn func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-kick:
		}
		fn()
	}
}

// pollHTTPClient trusts only the configured poll CA.
func pollHTTPClient(caFile string) (*http.Client, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, errors.New("poll CA file is missing")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("poll CA file holds no certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	transport.Proxy = nil
	return &http.Client{Transport: transport}, nil
}
