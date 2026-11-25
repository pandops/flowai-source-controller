package mocks

import (
	"context"
	"errors"
	"sync"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Broker is a fake broker.Broker. Publish stores the message (the JetStream
// ack) unless PublishHook returns an error; Request dispatches to Responders.
type Broker struct {
	mu        sync.Mutex
	published []broker.Message
	requests  []broker.Request
	down      bool

	// PublishHook runs before a publish is acknowledged; a non-nil error
	// fails the publish and the message is not stored.
	PublishHook func(m broker.Message) error
	// BlockPublish, when it returns true for a message, makes the publish
	// wait for its context like an acknowledgement that never arrives.
	BlockPublish func(m broker.Message) bool
	// Responders answer requests by subject.
	Responders map[string]func(broker.Request) (broker.Reply, error)
}

// NewBroker returns a connected fake broker.
func NewBroker() *Broker {
	return &Broker{Responders: map[string]func(broker.Request) (broker.Reply, error){}}
}

// SetDown disconnects or reconnects the fake broker.
func (b *Broker) SetDown(down bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.down = down
}

// Publish implements broker.Broker.
func (b *Broker) Publish(ctx context.Context, m broker.Message) error {
	b.mu.Lock()
	down, hook, block := b.down, b.PublishHook, b.BlockPublish
	b.mu.Unlock()
	if down {
		return broker.ErrDisconnected
	}
	if block != nil && block(m) {
		<-ctx.Done()
		return broker.ErrTimeout
	}
	if hook != nil {
		if err := hook(m); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return broker.ErrTimeout
	}
	header := broker.Header{}
	for k, v := range m.Header {
		header[k] = v
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, broker.Message{Subject: m.Subject, Header: header, Data: append([]byte(nil), m.Data...)})
	return nil
}

// Request implements broker.Broker.
func (b *Broker) Request(ctx context.Context, r broker.Request) (broker.Reply, error) {
	b.mu.Lock()
	down := b.down
	responder := b.Responders[r.Subject]
	b.requests = append(b.requests, r)
	b.mu.Unlock()
	if down {
		return broker.Reply{}, broker.ErrDisconnected
	}
	if responder == nil {
		return broker.Reply{}, broker.ErrNoResponders
	}
	return responder(r)
}

// Connected implements broker.Broker.
func (b *Broker) Connected() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.down
}

// Published returns the acknowledged messages in publish order.
func (b *Broker) Published() []broker.Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]broker.Message(nil), b.published...)
}

// Requests returns every request sent.
func (b *Broker) Requests() []broker.Request {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]broker.Request(nil), b.requests...)
}

// Reset forgets recorded messages and requests.
func (b *Broker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published, b.requests = nil, nil
}

// ErrRejected is a ready-made publish failure.
var ErrRejected = errors.New("fake broker: publish rejected")
