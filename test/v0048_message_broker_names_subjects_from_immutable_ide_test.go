//go:build !integration

package test

import (
	"regexp"
	"testing"
)

// Coverage: message-broker names subjects from immutable identifiers.

func TestV0048TeamAppearsAsASubjectTokenOnlyInTheTaskQueueAndSourceIntake(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	e.pollerSource(3, "poller-main")
	e.pollOnce()
	intake := regexp.MustCompile(`^source-controller\.task\.[1-9][0-9]*\.pub$`)
	other := regexp.MustCompile(`^source-controller\.(event|audit)\.[a-z_]+\.pub$`)
	msgs := e.messages("")
	if len(msgs) == 0 {
		t.Fatal("nothing published")
	}
	for _, m := range msgs {
		if !intake.MatchString(m.Subject) && !other.MatchString(m.Subject) {
			t.Fatalf("subject %s", m.Subject)
		}
		if intake.MatchString(m.Subject) && m.Subject != "source-controller.task.3.pub" {
			t.Fatalf("intake subject %s does not carry team_id", m.Subject)
		}
	}
}
