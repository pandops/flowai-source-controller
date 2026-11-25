//go:build !integration

package test

import (
	"strings"
	"testing"
	"time"
)

// Coverage: service-platform service binds opaque cursors to their read.

func listCursor(t *testing.T, e *env, team int64) string {
	t.Helper()
	r := e.ui("sources-list", team, map[string]any{"limit": 1}, nil)
	c, _ := r.Body["next_cursor"].(string)
	if c == "" {
		t.Fatalf("no cursor: %s", r.Raw)
	}
	return c
}

func seedSources(e *env, team int64, n int) {
	for i := 0; i < n; i++ {
		e.createSource(team, "s"+itoa(int64(i)), "webhook")
		e.clock.Advance(time.Millisecond)
	}
}

func TestV0048ForwardPageAdvances(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	seedSources(e, 3, 3)
	r := e.ui("sources-list", 3, map[string]any{"limit": 1, "cursor": listCursor(t, e, 3)}, nil)
	if num(r.Body["items"].([]any)[0].(map[string]any)["id"]) != 2 {
		t.Fatalf("%s", r.Raw)
	}
}

func TestV0048TamperedCursor(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	seedSources(e, 3, 3)
	c := listCursor(t, e, 3)
	flipped := []byte(c)
	flipped[len(flipped)/2] ^= 1
	for _, bad := range []string{string(flipped), c + "x", "garbage", strings.Repeat("A", 40)} {
		if r := e.ui("sources-list", 3, map[string]any{"cursor": bad}, nil); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%q: %d", bad, r.Status)
		}
	}
}

func TestV0048CursorReusedForAnotherOperationOrTeam(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	seedSources(e, 3, 3)
	seedSources(e, 5, 3)
	c := listCursor(t, e, 3)
	if r := e.ui("sources-list", 5, map[string]any{"cursor": c}, nil); r.Status != 400 {
		t.Fatalf("foreign team: %d", r.Status)
	}
	_, key := e.webhookSource(3, "keys")
	if rec := e.httpDo("GET", "/v1/sources/poll-signing-keys?cursor="+c, key, "", ""); rec.Code != 400 {
		t.Fatalf("another operation: %d", rec.Code)
	}
}

func TestV0048ChangedFilter(t *testing.T) {
	// sources-list has no filter; an unknown parameter is rejected.
	e := newEnv(t)
	e.addTeam(3, "acme")
	seedSources(e, 3, 2)
	if r := e.ui("sources-list", 3, map[string]any{"cursor": listCursor(t, e, 3), "mode": "poller"}, nil); r.Status != 400 {
		t.Fatalf("%d", r.Status)
	}
}

func TestV0048LimitOutOfRange(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for _, l := range []any{0, 201, -1, "50", 1.5} {
		if r := e.ui("sources-list", 3, map[string]any{"limit": l}, nil); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("limit %v: %d", l, r.Status)
		}
	}
}
