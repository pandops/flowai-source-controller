//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
)

// Coverage: source-controller issues, rotates and revokes Source keys.

func TestV0048OperatorRotatesASourceKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, old := e.webhookSource(3, "jira-main")
	before := sourceView(t, e, 3, id)
	r := e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	if r.Status != 200 || r.Header["Cache-Control"] != "no-store" {
		t.Fatalf("rotate %d %v", r.Status, r.Header)
	}
	assertSchema(t, "ui.sources-rotate-key.reply.schema.json", r.Raw)
	fresh := r.Body["api_key"].(string)
	if fresh == old {
		t.Fatal("rotation kept the key")
	}
	if rec := e.webhook("acme", "jira-main", old, mustJSON(task("A")), false); rec.Code != 401 {
		t.Fatalf("old key webhook %d", rec.Code)
	}
	if rec := e.httpDo(http.MethodGet, "/v1/sources/self", old, "", ""); rec.Code != 401 {
		t.Fatalf("old key self %d", rec.Code)
	}
	if rec := e.webhook("acme", "jira-main", fresh, mustJSON(task("A")), false); rec.Code != 202 {
		t.Fatalf("new key webhook %d", rec.Code)
	}
	after := r.Body["source"].(map[string]any)
	for _, k := range []string{"id", "name", "settings_status", "revision", "created_at"} {
		if before[k] != after[k] {
			t.Fatalf("%s changed: %v -> %v", k, before[k], after[k])
		}
	}
	if strings.Contains(string(sourceViewRaw(t, e, 3, id)), fresh) {
		t.Fatal("a later read shows the key")
	}
}

func TestV0048DisabledSourceUsesItsKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, wkey := e.webhookSource(3, "jira-main")
	pid, pkey := e.pollerSource(3, "poller-main")
	unknown := e.httpDo(http.MethodGet, "/v1/sources/self", "tsk_v1_"+strings.Repeat("Z", 43), "", "")
	for _, id := range []int64{1, pid} {
		if r := e.ui("sources-disable", 3, map[string]any{"id": id}, nil); r.Status != 200 || r.Body["enabled"] != false {
			t.Fatalf("disable %d: %d %s", id, r.Status, r.Raw)
		}
	}
	checks := map[string]func() int{
		"webhook":  func() int { return e.webhook("acme", "jira-main", wkey, mustJSON(task("A")), false).Code },
		"settings": func() int { return e.httpDo(http.MethodGet, "/v1/sources/self/settings", wkey, "", "").Code },
		"schema":   func() int { return e.registerSchema(pkey, "1.0.0").Code },
		"status": func() int {
			return e.httpDo(http.MethodPut, "/v1/sources/self/status", pkey, "application/json", mustJSON(statusBody("idle"))).Code
		},
	}
	for name, check := range checks {
		if code := check(); code != unknown.Code || code != 401 {
			t.Fatalf("%s with a disabled key: %d", name, code)
		}
	}
	e.makeDue(3, pid)
	e.pollOnce()
	if len(e.calls()) != 0 {
		t.Fatal("a disabled poller was polled")
	}
	if len(e.intake()) != 0 {
		t.Fatal("a disabled Source published")
	}
}
