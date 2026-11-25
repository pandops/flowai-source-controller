//go:build integration

package test

// 1.7 row: outbox publisher_seq comes from source-controller's own sequence
// in the committing transaction; a rolled-back transaction publishes nothing
// and a restart continues the sequence.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/core"
	"github.com/pandops/flowai-source-controller/internal/postgres"
)

func TestV0048PublisherSeqIntegration(t *testing.T) {
	ctx := context.Background()
	admin, dbName := freshDB(t)
	role := serviceRole(t, admin, dbName, defaultSchema, true, false)
	st := openStore(t, role, defaultSchema)
	enqueue := func(s *postgres.Store, fail bool) (int64, error) {
		var seq int64
		err := s.Tx(ctx, func(tx core.Tx) error {
			var err error
			if seq, err = tx.NextPublisherSeq(); err != nil {
				return err
			}
			data := fmt.Sprintf(`{"publisher":"source-controller","publisher_seq":%d,"team_id":3,"occurred_at":"2026-11-07T12:00:00.000Z","kind":"schema_registered","source_id":1,"schema_revision":1,"implementation_version":"1.0.0","settings_status":"complete","revision":2,"field_count":0}`, seq)
			if err := tx.Enqueue(core.OutboxMessage{Seq: seq, Subject: "source-controller.event.schema_registered.pub",
				Header: broker.Header{"team": "3", "Nats-Msg-Id": fmt.Sprintf("source-controller-%d", seq)}, Data: []byte(data)}); err != nil {
				return err
			}
			if fail {
				return errors.New("rollback")
			}
			return nil
		})
		return seq, err
	}
	first, err := enqueue(st, false)
	if err != nil {
		t.Fatal(err)
	}
	lost, _ := enqueue(st, true)
	second, err := enqueue(st, false)
	if err != nil {
		t.Fatal(err)
	}
	if first >= lost || lost >= second {
		t.Fatalf("sequence %d %d %d is not from one increasing sequence", first, lost, second)
	}

	t.Run("a rolled-back transaction publishes nothing", func(t *testing.T) {
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.outbox WHERE publisher_seq = $1`, lost); n != 0 {
			t.Fatal("rolled-back row in the outbox")
		}
		e := newPGEnv(t, st, nil, nil)
		e.flush()
		for _, m := range e.broker.Published() {
			if m.Header["Nats-Msg-Id"] == fmt.Sprintf("source-controller-%d", lost) {
				t.Fatal("rolled-back message published")
			}
		}
		if len(e.broker.Published()) != 2 {
			t.Fatalf("published %d, want 2", len(e.broker.Published()))
		}
	})

	t.Run("a restart continues the sequence", func(t *testing.T) {
		st.Close()
		st2 := openStore(t, role, defaultSchema)
		third, err := enqueue(st2, false)
		if err != nil {
			t.Fatal(err)
		}
		if third <= second {
			t.Fatalf("after restart %d <= %d", third, second)
		}
		// The autocommit allocation of the webhook path draws from the same
		// sequence and never repeats a value.
		next, err := st2.NextPublisherSeq(ctx)
		if err != nil || next <= third {
			t.Fatalf("NextPublisherSeq %d (%v) after %d", next, err, third)
		}
	})
}
