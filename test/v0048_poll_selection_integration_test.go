//go:build integration

package test

// Poll selection decided by SQL and row locks (critique 1, finding 4):
// disabled, incomplete, blocked, archived-team and already-unfinished
// pollers get no poll; two replicas starting due polls create one poll.

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/core"
	"github.com/pandops/flowai-source-controller/internal/mocks"
)

func TestV0048PollSelectionIntegration(t *testing.T) {
	ctx := context.Background()
	admin, role, e := freshEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	var calls atomic.Int64
	e.setPoll(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
	})

	blocked := e.pollerSource(3, "blocked")
	for i := 0; i < 5; i++ {
		polls, err := e.svc.StartDuePolls(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range polls {
			_ = e.svc.ExecutePoll(ctx, p)
		}
		e.clock.Advance(11 * time.Minute)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id = $1 AND polling_blocked_at IS NOT NULL`, blocked); n != 1 {
		t.Fatal("the failing poller is not blocked")
	}
	// Created after the blocking loop, so only their own filters can keep
	// them out of the selection below.
	disabled := e.pollerSource(3, "disabled")
	e.ui("sources-disable", 3, map[string]any{"id": disabled}, nil)

	id, key, err := e.createSource(3, "incomplete", "poller")
	if err != nil {
		t.Fatal(err)
	}
	e.registerSchema(key, `[{"name":"project","label":"Project","type":"string","required":true,"default":null}]`)
	if st, body, _ := e.request("ui", "sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 5},
		broker.Header{"If-Match": e.etagOf(3, id)}); st != 200 || body["settings_status"] != "incomplete" {
		t.Fatalf("incomplete poller settings: %d %v", st, body["settings_status"])
	}

	e.pollerSource(5, "archived")
	if d := e.deliver("team-registry.event.team_archived.sub", 5, teamEvent(900, "team_archived", 5, "globex", true)); d != broker.Ack {
		t.Fatal(d)
	}
	unfinished := e.pollerSource(3, "unfinished")
	first, err := e.svc.StartDuePolls(ctx)
	if err != nil || len(first) != 1 || first[0].SourceID != unfinished {
		t.Fatalf("first start %v %v", first, err)
	}

	t.Run("ineligible pollers get no poll", func(t *testing.T) {
		e.clock.Advance(11 * time.Minute)
		before := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls`)
		polls, err := e.svc.StartDuePolls(ctx)
		if err != nil || len(polls) != 0 {
			t.Fatalf("started %v (%v)", polls, err)
		}
		if after := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls`); after != before {
			t.Fatalf("poll rows %d -> %d", before, after)
		}
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND source_id IN ($1, $2)`, disabled, id); n != 0 {
			t.Fatalf("disabled or incomplete pollers have %d polls", n)
		}
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE source_id = $1 AND team_id = 3`, unfinished); n != 1 {
			t.Fatalf("unfinished Source has %d polls", n)
		}
	})

	t.Run("two replicas start one poll for a due Source", func(t *testing.T) {
		calls.Store(0)
		e.setPoll(nil)
		due := e.pollerSource(3, "due")
		r2 := newPGEnv(t, openStore(t, role, defaultSchema), e.clock, mocks.NewBroker())
		r2.setPoll(nil)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var started []core.Poll
		for _, r := range []*pgEnv{e, r2, e, r2} {
			wg.Add(1)
			go func(r *pgEnv) {
				defer wg.Done()
				polls, err := r.svc.StartDuePolls(ctx)
				if err != nil {
					t.Errorf("start: %v", err)
				}
				for _, p := range polls {
					if p.SourceID == due {
						mu.Lock()
						started = append(started, p)
						mu.Unlock()
						_ = r.svc.ExecutePoll(ctx, p)
					}
				}
			}(r)
		}
		wg.Wait()
		if len(started) != 1 {
			t.Fatalf("started %d polls", len(started))
		}
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND source_id = $1`, due); n != 1 {
			t.Fatalf("poll rows %d", n)
		}
	})
}
