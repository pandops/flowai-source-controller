//go:build integration

package test

// Team isolation decided by SQL (critique 1, finding 1): teams 3 and 5 both
// own Sources numbered 1 and 2 with the same names; team 3 never reads or
// changes team 5's rows. Rows: equal numbers grant nothing, foreign reads and
// writes, list predicate before limit and cursor, sources-lookup,
// binding-guard and intake_processed with a foreign Source.

import (
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

func TestV0048TeamIsolationIntegration(t *testing.T) {
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.webhookSource(3, "jira-main")
	for _, name := range []string{"jira-main", "gitlab-main", "third", "fourth"} {
		e.webhookSource(5, name)
	}
	// Team 5 has Sources 1..4; team 3 only Source 1.
	before := map[int64]int64{}
	for id := int64(1); id <= 4; id++ {
		before[id] = revisionOf(t, admin, 5, id)
	}
	unknownStatus, unknown, _ := e.ui("sources-get", 3, map[string]any{"id": 99}, nil)

	t.Run("foreign reads return the unknown-id answer", func(t *testing.T) {
		for _, op := range []string{"sources-get", "sources-settings-get"} {
			status, body, _ := e.ui(op, 3, map[string]any{"id": 2}, nil)
			if status != 404 || unknownStatus != 404 || body["code"] != unknown["code"] || body["message"] != unknown["message"] {
				t.Fatalf("%s: %d %v", op, status, body)
			}
		}
		if v := e.view(3, 1); v["name"] != "jira-main" || asInt(v["team_id"]) != 3 {
			t.Fatalf("team 3 Source 1 %v", v)
		}
	})

	t.Run("foreign writes change nothing", func(t *testing.T) {
		writes := []struct {
			op     string
			body   any
			header broker.Header
		}{
			{"sources-settings-put", map[string]any{"values": map[string]any{}}, broker.Header{"If-Match": `"rev-2"`}},
			{"sources-rotate-key", nil, nil},
			{"sources-disable", nil, nil},
			{"sources-update-implementation", map[string]any{"implementation_version": "2.0.0"}, broker.Header{"If-Match": `"rev-2"`}},
		}
		for _, w := range writes {
			status, body, _ := e.request("ui", w.op, 3, map[string]any{"id": 2}, w.body, w.header)
			if status != 404 || body["code"] != unknown["code"] {
				t.Fatalf("%s: %d %v", w.op, status, body)
			}
		}
		for id := int64(1); id <= 4; id++ {
			if rev := revisionOf(t, admin, 5, id); rev != before[id] {
				t.Fatalf("team 5 Source %d revision %d -> %d", id, before[id], rev)
			}
		}
		var enabled int
		admin.QueryRow(`SELECT count(*) FROM ` + defaultSchema + `.sources WHERE team_id = 5 AND enabled AND key_digest IS NOT NULL`).Scan(&enabled)
		if enabled != 4 {
			t.Fatalf("team 5 enabled Sources %d", enabled)
		}
	})

	t.Run("the list predicate applies before limit and cursor", func(t *testing.T) {
		status, body, _ := e.ui("sources-list", 3, map[string]any{"limit": 1}, nil)
		items, _ := body["items"].([]any)
		if status != 200 || len(items) != 1 || body["next_cursor"] != nil || asInt(items[0].(map[string]any)["team_id"]) != 3 {
			t.Fatalf("team 3 list: %d %v", status, body)
		}
		_, page, _ := e.ui("sources-list", 5, map[string]any{"limit": 2}, nil)
		cursor, _ := page["next_cursor"].(string)
		if cursor == "" {
			t.Fatal("team 5 has no second page")
		}
		if status, _, _ := e.ui("sources-list", 3, map[string]any{"cursor": cursor}, nil); status != 400 {
			t.Fatalf("team 5 cursor used by team 3: %d", status)
		}
	})

	t.Run("sources-lookup and binding-guard see only the caller's team", func(t *testing.T) {
		status, body, _ := e.request("service", "sources-lookup", 3, nil, map[string]any{"team_id": 3, "source_ids": []int64{1, 2, 3}}, nil)
		sources, _ := body["sources"].([]any)
		if status != 200 || len(sources) != 1 || asInt(sources[0].(map[string]any)["source_id"]) != 1 {
			t.Fatalf("lookup %d %v", status, body)
		}
		status, body, _ = e.request("service", "binding-guard", 3, nil, map[string]any{"team_id": 3,
			"sources": []any{map[string]any{"source_id": 2, "expected_revision": before[2]}}}, nil)
		if status != 409 || body["code"] != "source_revision_conflict" || revisionOf(t, admin, 5, 2) != before[2] {
			t.Fatalf("guard on a foreign Source: %d %v", status, body)
		}
		own := revisionOf(t, admin, 3, 1)
		status, _, _ = e.request("service", "binding-guard", 3, nil, map[string]any{"team_id": 3,
			"sources": []any{map[string]any{"source_id": 1, "expected_revision": own}}}, nil)
		if status != 200 || revisionOf(t, admin, 3, 1) != own+1 || revisionOf(t, admin, 5, 1) != before[1] {
			t.Fatal("guard advanced the wrong team's Source")
		}
	})

	t.Run("intake_processed naming a foreign Source changes no count", func(t *testing.T) {
		if d := e.deliver("task-manager.event.intake_processed.sub", 3, intakeProcessed(500, 3, 4, "accepted")); d != broker.Ack {
			t.Fatalf("disposition %v", d)
		}
		if d := e.deliver("task-manager.event.intake_processed.sub", 3, intakeProcessed(501, 5, 1, "accepted")); d == broker.Nak {
			t.Fatalf("mismatched header redelivered: %v", d)
		}
		for _, team := range []int64{3, 5} {
			counts := e.view(team, 1)["intake_counts"].(map[string]any)
			if asInt(counts["accepted"]) != 0 {
				t.Fatalf("team %d counts %v", team, counts)
			}
		}
		if asInt(e.view(5, 4)["intake_counts"].(map[string]any)["accepted"]) != 0 {
			t.Fatal("team 5 Source 4 counted a team 3 outcome")
		}
	})
}
