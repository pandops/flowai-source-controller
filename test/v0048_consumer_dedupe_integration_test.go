//go:build integration

package test

// Consumer dedupe decided by the database (critique 1, finding 2): a
// redelivered (publisher, publisher_seq) changes counts and the team
// projection exactly once, also when two replicas apply it concurrently.

import (
	"sync"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/mocks"
)

func TestV0048ConsumerDedupeIntegration(t *testing.T) {
	admin, role, e := freshEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.webhookSource(3, "jira-main")
	// A second replica over the same database.
	r2 := newPGEnv(t, openStore(t, role, defaultSchema), e.clock, mocks.NewBroker())

	t.Run("redelivered intake_processed is counted once", func(t *testing.T) {
		ev := intakeProcessed(700, 3, id, "deduplicated")
		for i := 0; i < 3; i++ {
			if d := e.deliver("task-manager.event.intake_processed.sub", 3, ev); d != broker.Ack {
				t.Fatalf("delivery %d: %v", i, d)
			}
		}
		if c := e.view(3, id)["intake_counts"].(map[string]any); asInt(c["deduplicated"]) != 1 {
			t.Fatalf("counts %v", c)
		}
	})

	t.Run("two replicas apply one outcome once", func(t *testing.T) {
		ev := intakeProcessed(701, 3, id, "accepted")
		var wg sync.WaitGroup
		got := make([]broker.Disposition, 8)
		for i := range got {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				replica := e
				if i%2 == 1 {
					replica = r2
				}
				got[i] = replica.deliver("task-manager.event.intake_processed.sub", 3, ev)
			}(i)
		}
		wg.Wait()
		// A delivery that lost the race may be redelivered; redeliver those.
		for i, d := range got {
			if d == broker.Nak {
				got[i] = e.deliver("task-manager.event.intake_processed.sub", 3, ev)
			}
			if got[i] != broker.Ack {
				t.Fatalf("delivery %d: %v", i, got[i])
			}
		}
		if c := e.view(3, id)["intake_counts"].(map[string]any); asInt(c["accepted"]) != 1 {
			t.Fatalf("counts %v", c)
		}
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.applied_messages WHERE publisher = 'task-manager' AND publisher_seq = 701`); n != 1 {
			t.Fatalf("applied rows %d", n)
		}
	})

	t.Run("a replayed team event changes the projection once", func(t *testing.T) {
		// Team names never change, so archived_at tells the events apart.
		update := teamEvent(800, "team_updated", 3, "acme", false)
		if d := e.deliver("team-registry.event.team_updated.sub", 3, update); d != broker.Ack {
			t.Fatal(d)
		}
		if d := e.deliver("team-registry.event.team_archived.sub", 3, teamEvent(801, "team_archived", 3, "acme", true)); d != broker.Ack {
			t.Fatal(d)
		}
		var wg sync.WaitGroup
		for _, replica := range []*pgEnv{e, r2, e, r2} {
			wg.Add(1)
			go func(r *pgEnv) {
				defer wg.Done()
				_ = r.deliver("team-registry.event.team_updated.sub", 3, update)
			}(replica)
		}
		wg.Wait()
		var name string
		var archived bool
		if err := admin.QueryRow(`SELECT team_name, archived_at IS NOT NULL FROM `+defaultSchema+`.teams WHERE team_id = 3`).Scan(&name, &archived); err != nil || name != "acme" || !archived {
			t.Fatalf("projection %q archived=%v (%v)", name, archived, err)
		}
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.applied_messages WHERE publisher = 'team-registry' AND publisher_seq = 800`); n != 1 {
			t.Fatalf("applied rows %d", n)
		}
	})
}
