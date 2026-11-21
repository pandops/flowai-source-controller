// Package natsbroker binds broker.Broker to NATS JetStream with nats.go.
package natsbroker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Settings are the NATS connection settings.
type Settings struct {
	URL             string
	CAFile          string
	CredentialsFile string
	InboxPrefix     string
}

// Options returns the nats.go options for the production connection: TLS
// with the stand CA, the service's user credentials and its inbox prefix.
// Missing or unreadable trust or credentials fail closed.
func Options(s Settings, log *slog.Logger) ([]nats.Option, error) {
	if !strings.HasPrefix(s.URL, "tls://") {
		return nil, errors.New("nats url must use tls://")
	}
	if s.CredentialsFile == "" {
		return nil, errors.New("nats credentials file is required")
	}
	creds, err := os.ReadFile(s.CredentialsFile)
	if err != nil || !strings.Contains(string(creds), "BEGIN NATS USER JWT") || !strings.Contains(string(creds), "BEGIN USER NKEY SEED") {
		return nil, errors.New("nats credentials file is missing or invalid")
	}
	pem, err := os.ReadFile(s.CAFile)
	if err != nil {
		return nil, errors.New("nats CA file is missing")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("nats CA file holds no certificate")
	}
	if s.InboxPrefix == "" {
		return nil, errors.New("nats inbox prefix is required")
	}
	return []nats.Option{
		nats.Name("source-controller"),
		nats.UserCredentials(s.CredentialsFile),
		nats.Secure(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}),
		nats.CustomInboxPrefix(s.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Warn("nats disconnected") }),
		nats.ReconnectHandler(func(_ *nats.Conn) { log.Info("nats reconnected") }),
	}, nil
}

// Broker is the NATS implementation of broker.Broker.
type Broker struct {
	nc *nats.Conn
	js jetstream.JetStream
}

// New wraps a connection.
func New(nc *nats.Conn) (*Broker, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return &Broker{nc: nc, js: js}, nil
}

func header(h broker.Header) nats.Header {
	out := nats.Header{}
	for k, v := range h {
		out.Set(k, v)
	}
	return out
}

func flat(h nats.Header) broker.Header {
	out := broker.Header{}
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

// Publish implements broker.Broker with a JetStream publish that waits for
// the acknowledgement.
func (b *Broker) Publish(ctx context.Context, m broker.Message) error {
	if !b.nc.IsConnected() {
		return broker.ErrDisconnected
	}
	msg := &nats.Msg{Subject: m.Subject, Header: header(m.Header), Data: m.Data}
	opts := []jetstream.PublishOpt{}
	if id := m.Header["Nats-Msg-Id"]; id != "" {
		opts = append(opts, jetstream.WithMsgID(id))
	}
	if _, err := b.js.PublishMsg(ctx, msg, opts...); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
			return broker.ErrTimeout
		case errors.Is(err, nats.ErrNoResponders), errors.Is(err, jetstream.ErrNoStreamResponse):
			return fmt.Errorf("%w: %s", broker.ErrRejected, err.Error())
		case !b.nc.IsConnected():
			return broker.ErrDisconnected
		}
		return fmt.Errorf("%w: %s", broker.ErrRejected, err.Error())
	}
	return nil
}

// Request implements broker.Broker.
func (b *Broker) Request(ctx context.Context, r broker.Request) (broker.Reply, error) {
	if !b.nc.IsConnected() {
		return broker.Reply{}, broker.ErrDisconnected
	}
	msg, err := b.nc.RequestMsgWithContext(ctx, &nats.Msg{Subject: r.Subject, Header: header(r.Header), Data: r.Data})
	switch {
	case err == nil:
		return broker.Reply{Header: flat(msg.Header), Data: msg.Data}, nil
	case errors.Is(err, nats.ErrNoResponders):
		return broker.Reply{}, broker.ErrNoResponders
	case !b.nc.IsConnected():
		return broker.Reply{}, broker.ErrDisconnected
	}
	return broker.Reply{}, broker.ErrTimeout
}

// Connected implements broker.Broker.
func (b *Broker) Connected() bool { return b.nc.IsConnected() }

// Handler answers requests and applies deliveries.
type Handler interface {
	HandleRequest(ctx context.Context, r broker.Request) broker.Reply
	HandleEvent(ctx context.Context, d broker.Delivery) broker.Disposition
}

// ServeRequests subscribes the ui and service request subjects in the
// source-controller queue group.
func (b *Broker) ServeRequests(h Handler, timeout time.Duration) ([]*nats.Subscription, error) {
	var subs []*nats.Subscription
	for _, subject := range []string{"source-controller.ui.*.sub", "source-controller.service.*.sub"} {
		sub, err := b.nc.QueueSubscribe(subject, "source-controller", func(m *nats.Msg) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			rep := h.HandleRequest(ctx, broker.Request{Subject: m.Subject, Header: flat(m.Header), Data: m.Data})
			out := nats.NewMsg(m.Reply)
			out.Header = header(rep.Header)
			out.Data = rep.Data
			_ = m.RespondMsg(out)
		})
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

// Consumer binds the declared durable consumer; it never creates it.
type Consumer struct {
	b      *Broker
	stream string
	name   string
	mu     sync.Mutex
	bound  jetstream.Consumer
}

// NewConsumer returns a binder for one declared durable consumer.
func (b *Broker) NewConsumer(stream, name string) *Consumer {
	return &Consumer{b: b, stream: stream, name: name}
}

// Ready reports whether the durable consumer exists.
func (c *Consumer) Ready(ctx context.Context) error {
	_, err := c.b.js.Consumer(ctx, c.stream, c.name)
	return err
}

// Run fetches and applies deliveries until ctx ends.
func (c *Consumer) Run(ctx context.Context, h Handler, log *slog.Logger) {
	for ctx.Err() == nil {
		c.mu.Lock()
		if c.bound == nil {
			cons, err := c.b.js.Consumer(ctx, c.stream, c.name)
			if err != nil {
				c.mu.Unlock()
				sleep(ctx, time.Second)
				continue
			}
			c.bound = cons
		}
		cons := c.bound
		c.mu.Unlock()
		batch, err := cons.Fetch(32, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			c.mu.Lock()
			c.bound = nil
			c.mu.Unlock()
			sleep(ctx, time.Second)
			continue
		}
		for msg := range batch.Messages() {
			d := broker.Delivery{Subject: msg.Subject(), Header: flat(msg.Headers()), Data: msg.Data()}
			switch h.HandleEvent(ctx, d) {
			case broker.Ack:
				_ = msg.Ack()
			case broker.Term:
				_ = msg.Term()
			default:
				_ = msg.Nak()
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
