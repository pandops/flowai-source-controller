//go:build !integration

package test

import (
	"net/http"
	"testing"
	"time"
)

// Coverage: source-controller blocks polling after five consecutive failures.

func failingPoller(w http.ResponseWriter, r *http.Request, call pollCall) {
	writeJSON(w, 500, map[string]any{})
}

func failPolls(e *env, id int64, n int) {
	e.setPoll(failingPoller)
	for i := 0; i < n; i++ {
		e.makeDue(3, id)
		e.pollOnce()
	}
}

func TestV0048FifthConsecutiveFailureBlocksPolling(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	rev := e.store.Source(3, id).Revision
	failPolls(e, id, 5)
	if len(e.calls()) != 5 {
		t.Fatalf("calls %d (a failed call was retried?)", len(e.calls()))
	}
	s := e.store.Source(3, id)
	if s.ConsecutiveFailures != 5 || s.PollingBlockedAt == nil || s.Revision != rev+1 {
		t.Fatalf("failures %d blocked %v revision %d", s.ConsecutiveFailures, s.PollingBlockedAt, s.Revision)
	}
	blocked := e.messages("source-controller.event.polling_blocked.pub")
	if len(blocked) != 1 {
		t.Fatalf("polling_blocked events %d", len(blocked))
	}
	view := sourceView(t, e, 3, id)
	polling := view["polling"].(map[string]any)
	last := view["last_poll"].(map[string]any)
	if num(polling["consecutive_failures"]) != 5 || polling["polling_blocked_at"] == nil || last["error_code"] != "http_error" {
		t.Fatalf("view polling %v last %v", polling, last)
	}
	failPolls(e, id, 1)
	if len(e.calls()) != 5 {
		t.Fatal("a blocked poller was polled")
	}
}

func TestV0048SuccessfulPollBreaksTheFailureStreak(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	failPolls(e, id, 4)
	e.setPoll(nil) // an empty successful poll
	e.makeDue(3, id)
	e.pollOnce()
	if s := e.store.Source(3, id); s.ConsecutiveFailures != 0 || s.LastPoll.Status != "succeeded" {
		t.Fatalf("after success %d %+v", s.ConsecutiveFailures, s.LastPoll)
	}
	failPolls(e, id, 1)
	if s := e.store.Source(3, id); s.ConsecutiveFailures != 1 || s.PollingBlockedAt != nil {
		t.Fatalf("next failure counted %d", s.ConsecutiveFailures)
	}
}

func TestV0048OperatorExplicitlyResumesPolling(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	failPolls(e, id, 5)
	calls := len(e.calls())
	s := e.store.Source(3, id)
	etag := `"rev-` + itoa(int64(s.Revision)) + `"`
	if r := e.ui("sources-unblock-polling", 3, map[string]any{"id": id}, nil); r.Status != 400 {
		t.Fatalf("missing If-Match: %d", r.Status)
	}
	r := e.uiIfMatch("sources-unblock-polling", 3, map[string]any{"id": id}, nil, etag)
	if r.Status != 200 || r.Header["ETag"] != `"rev-`+itoa(int64(s.Revision+1))+`"` {
		t.Fatalf("unblock %d %v %s", r.Status, r.Header, r.Raw)
	}
	after := e.store.Source(3, id)
	now := e.clock.Now()
	if after.PollingBlockedAt != nil || after.ConsecutiveFailures != 0 || after.NextPollAt == nil ||
		after.NextPollAt.Before(now.Add(9*time.Minute)) || after.NextPollAt.After(now.Add(11*time.Minute)) || after.LastPoll == nil {
		t.Fatalf("after unblock %+v", after)
	}
	e.pollOnce()
	if len(e.calls()) != calls {
		t.Fatal("unblock made an immediate call")
	}
	if r := e.uiIfMatch("sources-unblock-polling", 3, map[string]any{"id": id}, nil, r.Header["ETag"]); r.Status != 409 || r.code() != "polling_not_blocked" {
		t.Fatalf("not blocked: %d %s", r.Status, r.Raw)
	}
}

func TestV0048AnObsoleteUnblockRequestIsReplayed(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	failPolls(e, id, 5)
	old := `"rev-` + itoa(int64(e.store.Source(3, id).Revision)) + `"`
	if r := e.uiIfMatch("sources-unblock-polling", 3, map[string]any{"id": id}, nil, old); r.Status != 200 {
		t.Fatalf("first unblock %d", r.Status)
	}
	failPolls(e, id, 5)
	if e.store.Source(3, id).PollingBlockedAt == nil {
		t.Fatal("not blocked again")
	}
	r := e.uiIfMatch("sources-unblock-polling", 3, map[string]any{"id": id}, nil, old)
	if r.Status != 412 || r.code() != "stale_revision" || e.store.Source(3, id).PollingBlockedAt == nil {
		t.Fatalf("replay %d %s", r.Status, r.Raw)
	}
}
