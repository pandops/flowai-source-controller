//go:build !integration

package test

import "testing"

// Coverage: Source contracts distinguish source resources from upstream task
// identifiers.

func TestV0048SameSourceRetriesAnExternalTaskIdentifier(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	for i := 0; i < 2; i++ {
		if rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-5")), false); rec.Code != 202 {
			t.Fatalf("attempt %d: %d", i, rec.Code)
		}
	}
	msgs := e.intake()
	if len(msgs) != 2 {
		t.Fatalf("messages %d", len(msgs))
	}
	for _, m := range msgs {
		b := decode(t, m.Data)
		if num(b["team_id"]) != 3 || num(b["source_id"]) != id || b["tasks"].([]any)[0].(map[string]any)["external_id"] != "ISSUE-5" {
			t.Fatalf("message %v", b)
		}
	}
}

func TestV0048SourceRouteNamesANonIntegerID(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "jira-main", "webhook")
	for _, id := range []any{"jira-main", "3f2c9a1e-0000-4000-8000-000000000000"} {
		if r := e.ui("sources-get", 3, map[string]any{"id": id}, nil); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%v: %d %s", id, r.Status, r.Raw)
		}
	}
}
