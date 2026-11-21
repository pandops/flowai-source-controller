//go:build !integration

package test

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Coverage: Every list in the source APIs is paginated or bounded.

func TestV0048TSIOperatorPagesThroughSources(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for i := 0; i < 120; i++ {
		e.createSource(3, fmt.Sprintf("s%03d", i), "webhook")
		e.clock.Advance(time.Millisecond)
	}
	total, cursor := 0, ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		r := e.ui("sources-list", 3, params, nil)
		total += len(r.Body["items"].([]any))
		next, _ := r.Body["next_cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if total != 120 {
		t.Fatalf("total %d", total)
	}
}

func TestV0048RequestExceedsAListMaximum(t *testing.T) {
	e := newEnv(t, withSigningKeys(3))
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	tasks := make([]any, 101)
	for i := range tasks {
		tasks[i] = task(fmt.Sprintf("T%d", i))
	}
	if rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": tasks}), true); rec.Code != 400 {
		t.Fatalf("101 tasks: %d", rec.Code)
	}
	for _, limit := range []any{500, 0, 201} {
		if r := e.ui("sources-list", 3, map[string]any{"limit": limit}, nil); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("limit %v: %d", limit, r.Status)
		}
	}
	if rec := e.httpDo(http.MethodGet, "/v1/sources/poll-signing-keys?limit=500", key, "", ""); rec.Code != 400 {
		t.Fatalf("keys limit: %d", rec.Code)
	}
	rec := e.httpDo(http.MethodGet, "/v1/sources/poll-signing-keys?limit=2", key, "", "")
	b := decode(t, rec.Body.Bytes())
	if rec.Code != 200 || len(b["keys"].([]any)) != 2 || b["next_cursor"] == nil {
		t.Fatalf("keys page: %d %s", rec.Code, rec.Body.String())
	}
	rec = e.httpDo(http.MethodGet, "/v1/sources/poll-signing-keys?cursor="+b["next_cursor"].(string), key, "", "")
	if b2 := decode(t, rec.Body.Bytes()); rec.Code != 200 || len(b2["keys"].([]any)) != 1 || b2["next_cursor"] != nil {
		t.Fatalf("keys page 2: %d %s", rec.Code, rec.Body.String())
	}
	if len(e.intake()) != 0 {
		t.Fatal("published")
	}
}
