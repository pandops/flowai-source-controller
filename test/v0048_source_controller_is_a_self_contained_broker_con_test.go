//go:build !integration

package test

import (
	"testing"
)

// Coverage: source-controller is a self-contained broker-connected service.

func TestV0048PublishedMessagesMatchTheirSchemas(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.webhookSource(3, "hook")
	e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	e.pollerSource(3, "p")
	e.setPoll(pages(t, page("P", 1, nil)))
	e.pollOnce()
	seen := map[string]bool{}
	for _, m := range e.messages("") { // messages validates every one
		seen[m.Subject] = true
	}
	for _, want := range []string{"source-controller.task.3.pub", "source-controller.event.poll_finalized.pub", "source-controller.audit.source_key_rotated.pub"} {
		if !seen[want] {
			t.Fatalf("no %s published", want)
		}
	}
}
