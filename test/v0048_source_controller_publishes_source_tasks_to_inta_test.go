//go:build !integration

package test

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/core"
)

// Coverage: source-controller publishes source tasks to SOURCE_CONTROLLER_TASK
// without deduplication.

func TestV0048SameExternalIdentifierIsSubmittedTwice(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	first := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-1")), false)
	second := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-1")), false)
	if first.Code != 202 || second.Code != 202 {
		t.Fatalf("%d %d", first.Code, second.Code)
	}
	a, b := num(decode(t, first.Body.Bytes())["intake_id"]), num(decode(t, second.Body.Bytes())["intake_id"])
	if a == b {
		t.Fatalf("repeated external_id reused intake_id %d", a)
	}
	msgs := e.intake()
	if len(msgs) != 2 {
		t.Fatalf("want two intake messages, got %d", len(msgs))
	}
	if num(decode(t, msgs[1].Data)["tasks"].([]any)[0].(map[string]any)["intake_id"]) != b {
		t.Fatal("second message does not carry the second intake_id")
	}
}

func TestV0048IntakeIDsIdentifyTheTasksOfOneMessage(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.store.SetCounter(3, "intake", 40)
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": []any{task("A"), task("B")}}), true)
	if rec.Code != 202 {
		t.Fatalf("status %d", rec.Code)
	}
	msgs := e.intake()
	if len(msgs) != 1 {
		t.Fatalf("messages %d", len(msgs))
	}
	m := decode(t, msgs[0].Data)
	seq := num(m["publisher_seq"])
	if m["publisher"] != "source-controller" || seq < 1 || msgs[0].Header["Nats-Msg-Id"] != fmt.Sprintf("source-controller-%d", seq) {
		t.Fatalf("identity %v %v", m, msgs[0].Header)
	}
	tasks := m["tasks"].([]any)
	if num(tasks[0].(map[string]any)["intake_id"]) != 41 || num(tasks[1].(map[string]any)["intake_id"]) != 42 {
		t.Fatalf("tasks %v", tasks)
	}
}

func TestV0048RestartContinuesThePublisherSequence(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.store.SetPublisherSeq(900)
	// A new core instance over the same store is a restart.
	restarted, err := core.New(core.Config{PublicBaseURL: publicBase, PollSigningKeys: []core.SigningKey{{ID: "k1", Private: privateKey(1)}},
		CursorKeys: []core.CursorKey{{ID: 1, Secret: []byte("0123456789abcdef0123456789abcdef")}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: e.clock.Now}, e.store, e.broker)
	if err != nil {
		t.Fatal(err)
	}
	e.http = restarted.HTTPHandler()
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-1")), false)
	if rec.Code != 202 {
		t.Fatalf("status %d", rec.Code)
	}
	m := decode(t, e.intake()[0].Data)
	if num(m["publisher_seq"]) <= 900 {
		t.Fatalf("publisher_seq %v did not continue after 900", m["publisher_seq"])
	}
}

func TestV0048IntakeMessageCarriesDerivedOwnership(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "jira-main")
	e.setPoll(pages(t, func(int) (int, any) {
		return 200, map[string]any{"tasks": []any{task("ISSUE-7")}, "next_cursor": nil}
	}))
	polls := e.pollOnce()
	msgs := e.intake()
	if len(polls) != 1 || len(msgs) != 1 {
		t.Fatalf("polls %d messages %d", len(polls), len(msgs))
	}
	m := decode(t, msgs[0].Data)
	if msgs[0].Subject != "source-controller.task.3.pub" || msgs[0].Header["team"] != "3" || num(m["team_id"]) != 3 || num(m["source_id"]) != id ||
		m["origin"] != "poll" || num(m["poll_id"]) != polls[0].ID || num(m["page"]) != 1 {
		t.Fatalf("message %s %v %v", msgs[0].Subject, msgs[0].Header, m)
	}
	tasks := m["tasks"].([]any)
	if len(tasks) != 1 || tasks[0].(map[string]any)["external_id"] != "ISSUE-7" {
		t.Fatalf("tasks %v", tasks)
	}
	for _, forbidden := range []string{"flow", "tag", "image", "binding"} {
		if strings.Contains(string(msgs[0].Data), `"`+forbidden) {
			t.Fatalf("intake carries %s: %s", forbidden, msgs[0].Data)
		}
	}
}
