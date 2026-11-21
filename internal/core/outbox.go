package core

import (
	"context"
	"fmt"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/schemas"
)

const outboxBatch = 100

// enqueue validates a stream message against its schema and stores it in
// the outbox of tx under seq.
func enqueue(tx Tx, seq, teamID int64, subject string, data map[string]any, requestID int64) error {
	raw := mustJSON(data)
	name, err := schemas.ForSubject(subject)
	if err != nil {
		return err
	}
	if err := schemas.Validate(name, raw); err != nil {
		return fmt.Errorf("message for %s violates its schema: %w", subject, err)
	}
	header := map[string]string{"team": itoa(teamID), "Nats-Msg-Id": fmt.Sprintf("%s-%d", Publisher, seq)}
	if requestID > 0 {
		header["request_id"] = itoa(requestID)
	}
	return tx.Enqueue(OutboxMessage{Seq: seq, Subject: subject, Header: header, Data: raw})
}

// event enqueues one ALL_EVENT message of kind about a Source.
func (s *Service) event(tx Tx, teamID, sourceID int64, kind string, fields map[string]any) error {
	seq, err := tx.NextPublisherSeq()
	if err != nil {
		return err
	}
	now, err := tx.Now()
	if err != nil {
		return err
	}
	data := map[string]any{"publisher": Publisher, "publisher_seq": seq, "team_id": teamID, "occurred_at": ts(now), "kind": kind, "source_id": sourceID}
	for k, v := range fields {
		data[k] = v
	}
	s.notify()
	return enqueue(tx, seq, teamID, "source-controller.event."+kind+".pub", data, 0)
}

// audit enqueues one operator audit message.
func (s *Service) audit(tx Tx, rc *reqCtx, action string, resourceID *int64, outcome, reason string, changes map[string]any) error {
	seq, err := tx.NextPublisherSeq()
	if err != nil {
		return err
	}
	now, err := tx.Now()
	if err != nil {
		return err
	}
	var resource any
	if resourceID != nil {
		resource = *resourceID
	}
	data := map[string]any{"publisher": Publisher, "publisher_seq": seq, "team_id": rc.team, "occurred_at": ts(now),
		"source_service": Publisher, "actor_type": "operator", "actor_id": rc.operator, "action": action, "resource_type": "source",
		"resource_id": resource, "request_id": rc.requestID, "outcome": outcome}
	if reason != "" {
		data["reason"] = reason
	}
	if len(changes) > 0 {
		data["changes"] = changes
	}
	s.notify()
	return enqueue(tx, seq, rc.team, "source-controller.audit."+action+".pub", data, rc.requestID)
}

// auditRejected records a rejected operator action in its own transaction.
func (s *Service) auditRejected(ctx context.Context, rc *reqCtx, action string, resourceID *int64, reason string) {
	if err := s.store.Tx(ctx, func(tx Tx) error { return s.audit(tx, rc, action, resourceID, "rejected", reason, nil) }); err != nil {
		s.log.Error("rejected action audit not stored", "action", action)
	}
}

// notify wakes the background outbox publisher.
func (s *Service) notify() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Kick returns the channel that signals new outbox rows.
func (s *Service) Kick() <-chan struct{} { return s.kick }

// FlushOutbox publishes pending outbox messages in publisher_seq order and
// deletes each after its JetStream ack. It stops at the first unacknowledged
// publish and reports it.
func (s *Service) FlushOutbox(ctx context.Context) (int, error) {
	total := 0
	for {
		var n, rows int
		var pubErr error
		err := s.store.Tx(ctx, func(tx Tx) error {
			n, rows, pubErr = 0, 0, nil
			pending, err := tx.Outbox(outboxBatch)
			if err != nil {
				return err
			}
			rows = len(pending)
			for _, m := range pending {
				name, err := schemas.ForSubject(m.Subject)
				if err == nil {
					err = schemas.Validate(name, m.Data)
				}
				if err != nil {
					s.log.Error("outbox row violates its schema; dropped", "publisher_seq", m.Seq, "subject", m.Subject)
					if err := tx.DeleteOutbox(m.Seq); err != nil {
						return err
					}
					continue
				}
				pctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
				err = s.broker.Publish(pctx, broker.Message{Subject: m.Subject, Header: broker.Header(m.Header), Data: m.Data})
				cancel()
				if err != nil {
					// Commit the rows already published; report the error after.
					pubErr = err
					break
				}
				if err := tx.DeleteOutbox(m.Seq); err != nil {
					return err
				}
				n++
			}
			return nil
		})
		if err != nil {
			return total, err
		}
		total += n
		if pubErr != nil {
			return total, pubErr
		}
		if rows < outboxBatch {
			return total, nil
		}
	}
}
