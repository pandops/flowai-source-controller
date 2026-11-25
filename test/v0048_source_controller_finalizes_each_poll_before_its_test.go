//go:build !integration

package test

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// Coverage: source-controller finalizes each poll before its successor.

func TestV0048SettingsChangeInvalidatesAnInFlightPollCursor(t *testing.T) {
	for _, change := range []string{"poll_url", "away_and_back"} {
		t.Run(change, func(t *testing.T) {
			e := newEnv(t)
			e.addTeam(3, "acme")
			id, _ := e.pollerSource(3, "poller-main")
			e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
				// The operator edits settings while the page is in flight.
				switch change {
				case "poll_url":
					e.saveSettings(3, id, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/moved"})
				default:
					e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "OTHER"}})
					e.saveSettings(3, id, map[string]any{"values": map[string]any{}})
				}
				writeJSON(w, 200, map[string]any{"tasks": []any{task("A"), task("B")}, "next_cursor": "resume-1"})
			})
			s := e.store.Source(3, id)
			old := "old-cursor"
			s.Cursor = &old
			e.store.PutSource(*s)
			e.pollOnce()
			s = e.store.Source(3, id)
			if len(e.intake()) != 2 {
				t.Fatalf("published %d", len(e.intake()))
			}
			if s.Cursor != nil {
				t.Fatalf("poll cursor overwrote the cleared cursor: %v", *s.Cursor)
			}
			e.setPoll(nil)
			e.makeDue(3, id)
			e.pollOnce()
			calls := e.calls()
			if calls[len(calls)-1].Body["cursor"] != nil {
				t.Fatalf("next poll cursor %v", calls[len(calls)-1].Body["cursor"])
			}
		})
	}
}

func TestV0048PollExpiresBeforeFinalization(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("A", 3, nil)))
	polls, _ := e.svc.StartDuePolls(context.Background())
	if len(polls) != 1 {
		t.Fatal("no poll")
	}
	a := polls[0]
	// Page 1 is published; the replica stops before finalizing.
	s := e.store.Source(3, id)
	pre := "pre-poll"
	s.Cursor = &pre
	e.store.PutSource(*s)
	e.clock.Advance(96 * time.Second)
	// While A is unfinished no successor starts, even when due.
	e.makeDue(3, id)
	if again, _ := e.svc.StartDuePolls(context.Background()); len(again) != 0 {
		t.Fatal("successor started while A is unfinished")
	}
	// A poll cycle first finalizes A, then starts the successor.
	if err := e.svc.RunPollCycle(context.Background()); err != nil {
		t.Fatal(err)
	}
	pa := e.store.Poll(3, a.ID)
	if pa == nil || !pa.Finished {
		t.Fatalf("A not finalized: %+v", pa)
	}
	finalized := e.messages("source-controller.event.poll_finalized.pub")
	if len(finalized) < 1 {
		t.Fatal("no poll_finalized")
	}
	first := decode(t, finalized[0].Data)
	if num(first["poll_id"]) != a.ID || first["error_code"] != "lease_expired" || first["outcome"] != nil {
		t.Fatalf("first finalization %v", first)
	}
	calls := e.calls()
	if len(calls) != 1 || num(calls[0].Body["poll_id"]) == a.ID || calls[0].Body["cursor"] != "pre-poll" {
		t.Fatalf("successor calls %v", calls)
	}
}
