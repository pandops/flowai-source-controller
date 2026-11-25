//go:build !integration

package test

import "testing"

// Coverage: message-broker admits new teams, Executors, Flows and Sources
// without a restart (source-controller form).

func TestV0048TeamIsCreatedWithoutARestart(t *testing.T) {
	e := newEnv(t)
	e.registryTeam(12, "newco")
	e.applyTeamEvent("team_created", 12, "newco", false)
	_, key := e.webhookSource(12, "jira-main")
	if rec := e.webhook("newco", "jira-main", key, mustJSON(task("A")), false); rec.Code != 202 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if msgs := e.intake(); len(msgs) != 1 || msgs[0].Subject != "source-controller.task.12.pub" {
		t.Fatal("new team's intake subject")
	}
	// A team the projection has not seen yet is fetched from team-registry.
	e.registryTeam(13, "later")
	if r := e.ui("sources-create", 13, nil, createBody("x", "webhook")); r.Status != 201 {
		t.Fatalf("unknown team fetch: %d %s", r.Status, r.Raw)
	}
}

func TestV0048SourceIsCreatedWithoutARestart(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.webhookSource(3, "first")
	id, key := e.webhookSource(3, "second")
	if rec := e.webhook("acme", "second", key, mustJSON(task("A")), false); rec.Code != 202 {
		t.Fatalf("%d", rec.Code)
	}
	// The running service publishes the new Source's task on its team's
	// intake subject, carrying the new source_id (coverage 608).
	msgs := e.intake()
	if len(msgs) != 1 || msgs[0].Subject != "source-controller.task.3.pub" || msgs[0].Header["team"] != "3" {
		t.Fatalf("intake messages %v", msgs)
	}
	m := decode(t, msgs[0].Data)
	tasks, _ := m["tasks"].([]any)
	if num(m["team_id"]) != 3 || num(m["source_id"]) != id || id != 2 || len(tasks) != 1 || tasks[0].(map[string]any)["external_id"] != "A" {
		t.Fatalf("intake for Source %d: %s", id, msgs[0].Data)
	}
}
