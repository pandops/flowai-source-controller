//go:build !integration

package test

import (
	"net/http"
	"testing"
	"time"
)

// Coverage: The platform triggers every poll.

func TestV0048DuePollerReturnsTasks(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("T", 2, nil)))
	e.pollOnce()
	s := e.store.Source(3, id)
	if len(e.intake()) != 2 || s.LastPoll.Status != "succeeded" || s.LastPoll.PublishedTasks != 2 {
		t.Fatalf("published %d last %+v", len(e.intake()), s.LastPoll)
	}
	c := e.calls()[0].Body
	if num(c["settings_revision"]) < 1 || c["settings"].(map[string]any)["project"] != "OPS" {
		t.Fatalf("callback %v", c)
	}
}

func TestV0048TSIPollerReturnsSeveralPages(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("A", 100, "c"), page("B", 30, nil)))
	e.pollOnce()
	if len(e.intake()) != 130 || e.store.Source(3, id).Cursor != nil {
		t.Fatalf("published %d", len(e.intake()))
	}
}

func TestV0048TSIPollStopsAtThePageLimit(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	var script []func(int) (int, any)
	for i := 0; i < 10; i++ {
		script = append(script, page("P"+itoa(int64(i)), 100, "c"+itoa(int64(i))))
	}
	e.setPoll(pages(t, script...))
	e.pollOnce()
	if s := e.store.Source(3, id); len(e.intake()) != 1000 || s.Cursor == nil || *s.Cursor != "c9" || s.LastPoll.Status != "succeeded" {
		t.Fatalf("published %d cursor %v", len(e.intake()), s.Cursor)
	}
}

func TestV0048TSIPollerIsUnavailable(t *testing.T) {
	e := newEnv(t, withCallWindow(300*time.Millisecond))
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	n := 0
	e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
		n++
		if n == 1 {
			writeJSON(w, 200, map[string]any{"tasks": []any{task("A")}, "next_cursor": "c"})
			return
		}
		writeJSON(w, 502, map[string]any{})
	})
	e.pollOnce()
	if s := e.store.Source(3, id); len(e.intake()) != 1 || len(e.calls()) != 2 || s.LastPoll.Status != "failed" || s.UnfinishedPollID != nil {
		t.Fatalf("published %d calls %d last %+v", len(e.intake()), len(e.calls()), s.LastPoll)
	}
}

func TestV0048TSIPollerIsDisabledOrIncomplete(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.pollerSource(3, "poller-main")
	e.registerSchema(key, "1.0.0", field("project", "string", true, nil), field("extra", "string", true, nil))
	e.makeDue(3, id)
	if polls := e.pollOnce(); len(polls) != 0 || len(e.calls()) != 0 {
		t.Fatal("incomplete poller polled")
	}
}
