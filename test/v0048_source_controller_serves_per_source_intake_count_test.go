//go:build !integration

package test

import (
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Coverage: source-controller serves per-Source intake counts from
// task-manager outcomes.

func counts(t *testing.T, e *env, team, id int64) map[string]any {
	return sourceView(t, e, team, id)["intake_counts"].(map[string]any)
}

func TestV0048DeduplicatedSourceTaskIsCounted(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "jira-main", "webhook")
	for _, outcome := range []string{"deduplicated", "accepted", "no_target", "rejected"} {
		if d := e.event("task-manager.event.intake_processed.sub", 3, e.intakeProcessed(3, id, outcome)); d != broker.Ack {
			t.Fatalf("%s: %v", outcome, d)
		}
	}
	c := counts(t, e, 3, id)
	if num(c["deduplicated"]) != 1 || num(c["accepted"]) != 1 || num(c["no_target"]) != 1 || num(c["failed"]) != 1 {
		t.Fatalf("counts %v", c)
	}
}

func TestV0048RedeliveredOutcomeIsCountedOnce(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "jira-main", "webhook")
	ev := e.intakeProcessed(3, id, "deduplicated")
	for i := 0; i < 3; i++ {
		if d := e.event("task-manager.event.intake_processed.sub", 3, ev); d != broker.Ack {
			t.Fatalf("delivery %d: %v", i, d)
		}
	}
	if c := counts(t, e, 3, id); num(c["deduplicated"]) != 1 {
		t.Fatalf("counts %v", c)
	}
}

func TestV0048OutcomeForAnUnknownSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	id, _ := e.createSource(5, "jira-main", "webhook")
	// Unknown Source in team 3, and a team-5 Source named under header team 3.
	for _, ev := range []map[string]any{e.intakeProcessed(3, 77, "accepted"), e.intakeProcessed(3, id, "accepted")} {
		if d := e.event("task-manager.event.intake_processed.sub", 3, ev); d != broker.Ack {
			t.Fatalf("not acknowledged: %v", d)
		}
	}
	if c := counts(t, e, 5, id); num(c["accepted"]) != 0 {
		t.Fatalf("counts %v", c)
	}
	// Header team differs from the payload team: changes nothing either.
	if d := e.event("task-manager.event.intake_processed.sub", 3, e.intakeProcessed(5, id, "accepted")); d == broker.Nak {
		t.Fatal("mismatched team header redelivered")
	}
	if c := counts(t, e, 5, id); num(c["accepted"]) != 0 {
		t.Fatalf("counts %v", c)
	}
}
