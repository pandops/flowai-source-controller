//go:build integration

package test

// 1.7 rows: an outbox message is published after a crash between commit and
// publish, exactly once per consumer (one Nats-Msg-Id per row, never two
// publishes of one row); developer-tooling "Poll crash boundary recovers
// once".

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/mocks"
)

func TestV0048OutboxSurvivesACrashBetweenCommitAndPublish(t *testing.T) {
	ctx := context.Background()
	admin, dbName := freshDB(t)
	role := serviceRole(t, admin, dbName, defaultSchema, true, false)
	st := openStore(t, role, defaultSchema)
	clock := &mocks.Clock{}
	b := mocks.NewBroker()
	e := newPGEnv(t, st, clock, b)
	e.addTeam(3, "acme")
	b.SetDown(true) // the instance dies after commit, before publishing
	for i := 0; i < 5; i++ {
		if _, _, err := e.createSource(3, fmt.Sprintf("s-%d", i), "webhook"); err != nil {
			t.Fatal(err)
		}
	}
	committed := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.outbox`)
	if committed < 5 || len(b.Published()) != 0 {
		t.Fatalf("outbox %d published %d", committed, len(b.Published()))
	}
	st.Close()
	b.SetDown(false)

	// Restart: two instances over the same database flush concurrently.
	r1 := newPGEnv(t, openStore(t, role, defaultSchema), clock, b)
	r2 := newPGEnv(t, openStore(t, role, defaultSchema), clock, b)
	var wg sync.WaitGroup
	for _, r := range []*pgEnv{r1, r2} {
		wg.Add(1)
		go func(r *pgEnv) {
			defer wg.Done()
			if _, err := r.svc.FlushOutbox(ctx); err != nil {
				t.Errorf("flush: %v", err)
			}
		}(r)
	}
	wg.Wait()
	r1.flush()
	msgs := r1.published("")
	if len(msgs) != committed {
		t.Fatalf("published %d of %d committed rows", len(msgs), committed)
	}
	ids := map[string]bool{}
	for _, m := range msgs {
		id := m.Header["Nats-Msg-Id"]
		if ids[id] {
			t.Fatalf("row %s published twice", id)
		}
		ids[id] = true
		if want := fmt.Sprintf("source-controller-%d", asInt(decodeMap(t, m.Data)["publisher_seq"])); id != want {
			t.Fatalf("Nats-Msg-Id %s, want %s", id, want)
		}
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.outbox`); n != 0 {
		t.Fatalf("%d rows left after the flush", n)
	}
}

// waitForLockedBackend waits until a backend of role waits on a lock and
// returns its pid.
func waitForLockedBackend(t *testing.T, admin *sql.DB, role string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		err := admin.QueryRowContext(context.Background(), `SELECT pid FROM pg_stat_activity WHERE usename = $1 AND wait_event_type = 'Lock' LIMIT 1`, role).Scan(&pid)
		if err == nil {
			return pid
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the poll transaction never reached the outbox")
	return 0
}

func TestV0048PollCrashBoundaryRecoversOnce(t *testing.T) {
	for _, boundary := range []struct {
		name  string
		tasks int
	}{
		{"paused before its intake publication", 3},
		{"paused before its outcome publication", 0},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			ctx := context.Background()
			admin, dbName := freshDB(t)
			role := serviceRole(t, admin, dbName, defaultSchema, true, false)
			st := openStore(t, role, defaultSchema)
			clock := &mocks.Clock{}
			b := mocks.NewBroker()
			e := newPGEnv(t, st, clock, b)
			e.addTeam(3, "acme")
			source := e.pollerSource(3, "poller")
			e.flush()
			e.setPoll(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				tasks := "["
				for i := 1; i <= boundary.tasks; i++ {
					if i > 1 {
						tasks += ","
					}
					tasks += fmt.Sprintf(`{"external_id":"P-%d","payload":{"n":%d}}`, i, i)
				}
				_, _ = w.Write([]byte(`{"tasks":` + tasks + `],"next_cursor":null}`))
			})
			polls, err := e.svc.StartDuePolls(ctx)
			if err != nil || len(polls) != 1 {
				t.Fatalf("start: %v %v", polls, err)
			}
			e.flush()
			before := len(b.Published())

			// Hold the outbox so the poll's transaction pauses at its first
			// outbox write, then terminate that session.
			lock, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := lock.ExecContext(ctx, `LOCK TABLE `+defaultSchema+`.outbox IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = e.svc.ExecutePoll(ctx, polls[0])
			}()
			pid := waitForLockedBackend(t, admin, role.name)
			mustExec(t, admin, `SELECT pg_terminate_backend($1)`, pid)
			_ = lock.Rollback()
			<-done
			st.Close() // the instance stops

			// Startup recovery after the lease expired, by a new instance.
			clock.Advance(3 * time.Minute)
			r := newPGEnv(t, openStore(t, role, defaultSchema), clock, b)
			for i := 0; i < 2; i++ {
				if err := r.svc.RunPollCycle(ctx); err != nil {
					t.Fatalf("recovery cycle %d: %v", i, err)
				}
				r.flush()
			}
			finalized := 0
			intake := map[string]int{}
			for _, m := range r.published("")[before:] {
				body := decodeMap(t, m.Data)
				switch {
				case m.Subject == "source-controller.event.poll_finalized.pub" && asInt(body["poll_id"]) == polls[0].ID:
					finalized++
				case m.Subject == "source-controller.task.3.pub":
					intake[fmt.Sprint(body["external_id"])]++
				}
			}
			if finalized != 1 {
				t.Fatalf("interrupted poll finalized %d times", finalized)
			}
			for id, n := range intake {
				if n != 1 {
					t.Fatalf("intake %s published %d times", id, n)
				}
			}
			if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND source_id = $1 AND finished_at IS NULL AND poll_id = $2`, source, polls[0].ID); n != 0 {
				t.Fatal("interrupted poll still unfinished")
			}
		})
	}
}
