//go:build !integration

package test

import "testing"

// Coverage: message-broker carries one intake message per webhook request or
// polled task.

func TestV0048WebhookBatchIsOneMessage(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	tasks := make([]any, 100)
	for i := range tasks {
		tasks[i] = task("T" + itoa(int64(i)))
	}
	if rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": tasks}), true); rec.Code != 202 {
		t.Fatalf("%d", rec.Code)
	}
	msgs := e.intake()
	if len(msgs) != 1 || len(decode(t, msgs[0].Data)["tasks"].([]any)) != 100 {
		t.Fatalf("messages %d", len(msgs))
	}
}

func TestV0048PolledTasksAreSeparateMessages(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, page("P", 3, nil)))
	e.pollOnce()
	msgs := e.intake()
	if len(msgs) != 3 {
		t.Fatalf("messages %d", len(msgs))
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		b := decode(t, m.Data)
		if len(b["tasks"].([]any)) != 1 || seen[m.Header["Nats-Msg-Id"]] {
			t.Fatalf("message %v", b)
		}
		seen[m.Header["Nats-Msg-Id"]] = true
	}
}
