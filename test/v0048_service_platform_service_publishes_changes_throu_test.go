//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Coverage: service-platform service publishes changes through an outbox.

func TestV0048RolledBackChangePublishesNothing(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.flush()
	e.store.FailCommit = true
	if r := e.ui("sources-create", 3, nil, createBody("jira-main", "webhook")); r.Status != 503 && r.Status != 500 {
		t.Fatalf("failed commit replied %d", r.Status)
	}
	e.flush()
	for _, m := range e.messages("") {
		if strings.Contains(m.Subject, "source_created") && strings.Contains(string(m.Data), `"succeeded"`) {
			t.Fatal("a rolled-back creation was published")
		}
	}
	if e.store.Source(3, 1) != nil {
		t.Fatal("rolled back Source exists")
	}
}

func TestV0048SynchronousPublicationsWithoutAnOutbox(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.flush()
	before := len(e.store.Enqueued)
	e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	for _, m := range e.store.Enqueued[before:] {
		if strings.HasPrefix(m.Subject, "source-controller.task.") {
			t.Fatal("webhook intake used the outbox")
		}
	}
	if len(e.intake()) != 1 {
		t.Fatal("not published synchronously")
	}
}
