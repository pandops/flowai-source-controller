//go:build !integration

package test

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Coverage: source-controller calls pollers safely and never retries a failed
// poll.

func TestV0048PollerReturnsSeveralPages(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("P1", 100, "c1"), page("P2", 30, nil)))
	polls := e.pollOnce()
	calls := e.calls()
	if len(polls) != 1 || len(calls) != 2 {
		t.Fatalf("polls %d calls %d", len(polls), len(calls))
	}
	for i, c := range calls {
		if !e.verifySignature(c) {
			t.Fatalf("call %d signature does not verify", i+1)
		}
		if num(c.Body["poll_id"]) != polls[0].ID || num(c.Body["page"]) != int64(i+1) || num(c.Body["source_id"]) != id || c.Body["implementation_version"] != "1.0.0" {
			t.Fatalf("call %d body %v", i+1, c.Body)
		}
	}
	if calls[0].Body["cursor"] != nil || calls[1].Body["cursor"] != "c1" {
		t.Fatalf("cursors %v %v", calls[0].Body["cursor"], calls[1].Body["cursor"])
	}
	if len(e.intake()) != 130 {
		t.Fatalf("published %d", len(e.intake()))
	}
	s := e.store.Source(3, id)
	if s.Cursor != nil || s.LastPoll.Status != "succeeded" || s.LastPoll.PagesRead != 2 {
		t.Fatalf("cursor %v last poll %+v", s.Cursor, s.LastPoll)
	}
}

func TestV0048PollStopsAtThePageLimit(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	var script []func(int) (int, any)
	for i := 1; i <= 10; i++ {
		script = append(script, page(fmt.Sprintf("P%d", i), 100, fmt.Sprintf("c%d", i)))
	}
	e.setPoll(pages(t, script...))
	e.pollOnce()
	if len(e.calls()) != 10 || len(e.intake()) != 1000 {
		t.Fatalf("calls %d published %d", len(e.calls()), len(e.intake()))
	}
	s := e.store.Source(3, id)
	if s.Cursor == nil || *s.Cursor != "c10" {
		t.Fatalf("stored cursor %v", s.Cursor)
	}
	e.setPoll(pages(t, page("N", 0, nil)))
	e.makeDue(3, id)
	e.pollOnce()
	calls := e.calls()
	if last := calls[len(calls)-1]; last.Body["cursor"] != "c10" || num(last.Body["page"]) != 1 {
		t.Fatalf("next poll page 1 body %v", last.Body)
	}
}

func TestV0048PollURLRedirects(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	followed := false
	e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
		if r.URL.Path == "/elsewhere" {
			followed = true
			writeJSON(w, 200, map[string]any{"tasks": []any{task("X")}, "next_cursor": nil})
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
	})
	e.pollOnce()
	s := e.store.Source(3, id)
	if followed || len(e.intake()) != 0 || s.LastPoll == nil || *s.LastPoll.Outcome != "http_error" || s.LastPoll.Status != "failed" {
		t.Fatalf("followed %v published %d last %+v", followed, len(e.intake()), s.LastPoll)
	}
}

func TestV0048PollerReturnsTooManyTasks(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("P1", 20, "c1"), page("P2", 101, nil)))
	e.pollOnce()
	s := e.store.Source(3, id)
	if len(e.intake()) != 20 || *s.LastPoll.Outcome != "invalid_response" || s.LastPoll.Status != "failed" {
		t.Fatalf("published %d last %+v", len(e.intake()), s.LastPoll)
	}
	// An invalid task schema is invalid_response too.
	e.setPoll(pages(t, func(int) (int, any) {
		return 200, map[string]any{"tasks": []any{map[string]any{"external_id": "A", "payload": map[string]any{}, "flow_id": 3}}, "next_cursor": nil}
	}))
	e.makeDue(3, id)
	e.pollOnce()
	if s := e.store.Source(3, id); *s.LastPoll.Outcome != "invalid_response" || len(e.intake()) != 20 {
		t.Fatalf("invalid task: %+v", s.LastPoll)
	}
}

func TestV0048PollerIsUnavailable(t *testing.T) {
	e := newEnv(t, withCallWindow(300*time.Millisecond))
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) { writeJSON(w, 503, map[string]any{}) })
	e.pollOnce()
	if s := e.store.Source(3, id); *s.LastPoll.Outcome != "http_error" || len(e.calls()) != 1 {
		t.Fatalf("5xx: %+v calls %d", s.LastPoll, len(e.calls()))
	}
	// A timeout: the page is aborted at call_deadline and not retried.
	e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	})
	e.makeDue(3, id)
	e.pollOnce()
	s := e.store.Source(3, id)
	if *s.LastPoll.Outcome != "timeout" || len(e.calls()) != 2 || s.ConsecutiveFailures != 2 {
		t.Fatalf("timeout: %+v calls %d failures %d", s.LastPoll, len(e.calls()), s.ConsecutiveFailures)
	}
	// No new poll before the next due time.
	if polls := e.pollOnce(); len(polls) != 0 {
		t.Fatal("a failed poll was retried before its due time")
	}
}
