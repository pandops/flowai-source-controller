//go:build !integration

package test

import "testing"

// Coverage: source-controller answers Source lookups from other services.

func TestV0048FlowRegistryValidatesABinding(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(3, "jira-main", "webhook")
	e.createSource(5, "first", "webhook")
	e.createSource(5, "second", "poller")
	r := e.service("sources-lookup", 3, map[string]any{"team_id": 3, "source_ids": []int64{1, 2}})
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	assertSchema(t, "service.sources-lookup.reply.schema.json", r.Raw)
	sources := r.Body["sources"].([]any)
	if len(sources) != 1 {
		t.Fatalf("sources %v", sources)
	}
	s := sources[0].(map[string]any)
	if num(s["source_id"]) != 1 || s["name"] != "jira-main" || s["mode"] != "webhook" || s["settings_status"] != "awaiting_schema" || s["enabled"] != true || s["polling_blocked"] != false {
		t.Fatalf("source %v", s)
	}
	if r := e.service("sources-lookup", 3, map[string]any{"team_id": 5, "source_ids": []int64{1}}); r.Status != 400 {
		t.Fatalf("header/body team mismatch: %d", r.Status)
	}
}
