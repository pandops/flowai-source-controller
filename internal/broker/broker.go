// Package broker is source-controller's messaging boundary. The core logic
// depends only on the Broker interface; nats.go binds it to NATS JetStream.
package broker

import (
	"context"
	"errors"
)

// Header carries NATS message headers (single values).
type Header map[string]string

// Message is one JetStream stream publication.
type Message struct {
	Subject string
	Header  Header
	Data    []byte
}

// Request is one core NATS request received or sent.
type Request struct {
	Subject string
	Header  Header
	Data    []byte
}

// Reply is a core NATS reply. Header "status" carries the HTTP-equivalent
// status (rule 9).
type Reply struct {
	Header Header
	Data   []byte
}

// Delivery is one message delivered by a durable consumer.
type Delivery struct {
	Subject string
	Header  Header
	Data    []byte
}

// Disposition tells the consumer adapter how to settle a delivery.
type Disposition int

const (
	// Ack settles the message after its effect committed (or it is ignored).
	Ack Disposition = iota
	// Nak asks for redelivery (temporary failure).
	Nak
	// Term terminates the message: it is never redelivered.
	Term
)

var (
	// ErrNoResponders: no service subscribed to the request subject.
	ErrNoResponders = errors.New("broker: no responders")
	// ErrTimeout: no reply or publish acknowledgement within the timeout.
	ErrTimeout = errors.New("broker: timeout")
	// ErrDisconnected: the service's own broker connection is down.
	ErrDisconnected = errors.New("broker: disconnected")
	// ErrRejected: JetStream refused the publish.
	ErrRejected = errors.New("broker: publish rejected")
)

// Broker is everything the core needs from NATS.
type Broker interface {
	// Publish sends a stream message and returns only after the JetStream
	// acknowledgement (or an error).
	Publish(ctx context.Context, m Message) error
	// Request sends a core NATS request and waits for its reply.
	Request(ctx context.Context, r Request) (Reply, error)
	// Connected reports whether the broker connection is up.
	Connected() bool
}
