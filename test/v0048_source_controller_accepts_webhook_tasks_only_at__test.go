//go:build !integration

package test

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Coverage: source-controller accepts webhook tasks only at the Source's URL.

func TestV0048WebhookSourceSubmitsANewTask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-7")), false)
	if rec.Code != 202 {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec.Body.Bytes())
	if body["external_id"] != "ISSUE-7" || num(body["intake_id"]) != 1 || len(body) != 2 {
		t.Fatalf("receipt %v", body)
	}
	msgs := e.intake()
	if len(msgs) != 1 || msgs[0].Subject != "source-controller.task.3.pub" {
		t.Fatalf("intake messages %v", msgs)
	}
	m := decode(t, msgs[0].Data)
	if num(m["source_id"]) != id || m["origin"] != "webhook" || len(m["tasks"].([]any)) != 1 {
		t.Fatalf("intake %v", m)
	}
}

func TestV0048WebhookOversizedBody(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	big := mustJSON(map[string]any{"external_id": "ISSUE-1", "payload": map[string]any{"text": strings.Repeat("x", 1<<20)}})
	rec := e.webhook("acme", "jira-main", key, big, false)
	if rec.Code != 413 || decode(t, rec.Body.Bytes())["code"] != "payload_too_large" {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	if len(e.intake()) != 0 {
		t.Fatal("oversized body was published")
	}
}

func TestV0048WebhookBatchContainsOneInvalidTask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	cases := map[string]func([]any) []any{
		"empty external_id": func(tasks []any) []any {
			tasks[17] = map[string]any{"external_id": "", "payload": map[string]any{}}
			return tasks
		},
		"duplicate external_id": func(tasks []any) []any {
			tasks[30] = task("ISSUE-3")
			return tasks
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			tasks := make([]any, 50)
			for i := range tasks {
				tasks[i] = task(fmt.Sprintf("ISSUE-%d", i))
			}
			rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": mutate(tasks)}), true)
			body := decode(t, rec.Body.Bytes())
			if rec.Code != 400 || body["code"] != "invalid_request" {
				t.Fatalf("status %d %s", rec.Code, rec.Body.String())
			}
			fields := fmt.Sprint(body["fields"])
			if !strings.Contains(fields, "/tasks/") || !strings.Contains(fields, "/external_id") {
				t.Fatalf("fields %v", body["fields"])
			}
			if strings.Contains(rec.Body.String(), "Fix ISSUE") {
				t.Fatal("error discloses a value")
			}
			if len(e.intake()) != 0 {
				t.Fatal("a task of the batch was published")
			}
		})
	}
}

func TestV0048WebhookValidBatch(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": []any{task("A"), task("B"), task("C")}}), true)
	if rec.Code != 202 {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	receipts := decode(t, rec.Body.Bytes())["tasks"].([]any)
	msgs := e.intake()
	if len(msgs) != 1 {
		t.Fatalf("want one intake message, got %d", len(msgs))
	}
	entries := decode(t, msgs[0].Data)["tasks"].([]any)
	if len(receipts) != 3 || len(entries) != 3 {
		t.Fatalf("receipts %v entries %v", receipts, entries)
	}
	for i, want := range []string{"A", "B", "C"} {
		r, en := receipts[i].(map[string]any), entries[i].(map[string]any)
		if r["external_id"] != want || en["external_id"] != want || num(r["intake_id"]) != num(en["intake_id"]) || num(r["intake_id"]) != int64(i+1) {
			t.Fatalf("receipt %d %v entry %v", i, r, en)
		}
	}
}

func TestV0048WebhookKeyDoesNotMatchThePath(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	_, key := e.webhookSource(3, "jira-main")
	e.webhookSource(3, "gitlab-main")
	e.webhookSource(5, "jira-main")
	unknown := e.webhook("acme", "jira-main", "tsk_v1_"+strings.Repeat("A", 43), mustJSON(task("X")), false)
	for _, path := range [][2]string{{"acme", "gitlab-main"}, {"globex", "jira-main"}, {"nobody", "jira-main"}} {
		rec := e.webhook(path[0], path[1], key, mustJSON(task("X")), false)
		if rec.Code != 401 || decode(t, rec.Body.Bytes())["code"] != "invalid_source_key" {
			t.Fatalf("%v: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	if unknown.Code != 401 || decode(t, unknown.Body.Bytes())["code"] != "invalid_source_key" {
		t.Fatalf("unknown key %d %s", unknown.Code, unknown.Body.String())
	}
	if len(e.intake()) != 0 {
		t.Fatal("a mismatched key published")
	}
	// The path names compare ignoring case.
	if rec := e.webhook("ACME", "Jira-Main", key, mustJSON(task("X")), false); rec.Code != 202 {
		t.Fatalf("case-insensitive path: %d %s", rec.Code, rec.Body.String())
	}
}

func TestV0048WebhookLegacyOwnershipHeaderWithAWrongKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.webhookSource(3, "jira-main")
	for _, h := range []string{"X-FlowAI-Team-Id", "X-FlowAI-Source-System-Id", "X-FlowAI-Listener-Identity", "X-FlowAI-Plugin-Instance-Id"} {
		rec := e.webhook("acme", "jira-main", "tsk_v1_"+strings.Repeat("B", 43), mustJSON(task("X")), false, h, "3")
		if rec.Code != 400 || decode(t, rec.Body.Bytes())["code"] != "invalid_request" {
			t.Fatalf("%s: %d %s", h, rec.Code, rec.Body.String())
		}
	}
	// Order: media type before size before schema before key.
	rec := e.httpDo(http.MethodPost, "/webhook/acme/jira-main/task", "bad", "text/plain", mustJSON(task("X")))
	if rec.Code != 415 {
		t.Fatalf("media type: %d", rec.Code)
	}
	rec = e.webhook("acme", "jira-main", "bad", `{"external_id":"X"}`, false)
	if rec.Code != 400 {
		t.Fatalf("schema before key: %d", rec.Code)
	}
	if len(e.intake()) != 0 {
		t.Fatal("published")
	}
}

func TestV0048WebhookBrokerUnavailable(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	// A publish whose acknowledgement never arrives: the reply comes after
	// the 5 s confirmation limit, not later. (The broker-down step is not
	// covered: infrastructure, exercised by the E2E full flow; owner,
	// 2026-11-09.)
	e.broker.BlockPublish = func(m broker.Message) bool { return strings.HasPrefix(m.Subject, "source-controller.task.") }
	start := time.Now()
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("X")), false)
	waited := time.Since(start)
	if rec.Code != 503 || decode(t, rec.Body.Bytes())["code"] != "intake_unavailable" || waited < 4500*time.Millisecond || waited > 6*time.Second {
		t.Fatalf("unconfirmed: %d after %v %s", rec.Code, waited, rec.Body.String())
	}
	e.broker.BlockPublish = nil
	e.flush()
	if len(e.intake()) != 0 {
		t.Fatal("an intake message was published for a 503 request")
	}
}

func TestV0048WebhookBatchPublishFailsAsAWhole(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	attempts := 0
	e.broker.PublishHook = func(m broker.Message) error {
		if strings.HasPrefix(m.Subject, "source-controller.task.") {
			attempts++
			return broker.ErrRejected
		}
		return nil
	}
	tasks := make([]any, 40)
	for i := range tasks {
		tasks[i] = task(fmt.Sprintf("T-%d", i))
	}
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": tasks}), true)
	if rec.Code != 503 || decode(t, rec.Body.Bytes())["code"] != "intake_unavailable" || strings.Contains(rec.Body.String(), "intake_id") {
		t.Fatalf("status %d %s", rec.Code, rec.Body.String())
	}
	if attempts != 1 {
		t.Fatalf("want exactly one publish attempt, got %d", attempts)
	}
	e.broker.PublishHook = nil
	e.flush()
	if len(e.intake()) != 0 {
		t.Fatal("tasks of the failed batch were published")
	}
	counts := sourceView(t, e, 3, id)["intake_counts"].(map[string]any)
	if num(counts["published"]) != 0 || num(counts["rejected"]) != 40 {
		t.Fatalf("counts %v", counts)
	}
}

func TestV0048WebhookRetryAfterTheFirstTaskWasArchived(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	// The first publish is stored but its acknowledgement is lost.
	var stored []broker.Message
	e.broker.PublishHook = func(m broker.Message) error {
		stored = append(stored, m)
		return broker.ErrTimeout
	}
	if rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-9")), false); rec.Code != 503 {
		t.Fatalf("first: %d", rec.Code)
	}
	e.broker.PublishHook = nil
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-9")), false)
	if rec.Code != 202 {
		t.Fatalf("retry: %d %s", rec.Code, rec.Body.String())
	}
	first := decode(t, stored[0].Data)
	retry := decode(t, e.intake()[0].Data)
	firstID := num(first["tasks"].([]any)[0].(map[string]any)["intake_id"])
	retryID := num(decode(t, rec.Body.Bytes())["intake_id"])
	if retryID == firstID || num(retry["publisher_seq"]) == num(first["publisher_seq"]) {
		t.Fatalf("retry reused intake_id %d or publisher_seq %v", retryID, first["publisher_seq"])
	}
}

// The scenario's "writes no database row for it" is read as recorded in
// deviations.md D21 (coordinator decision for coverage 753): before the
// JetStream acknowledgement only the autocommitted publisher_seq and intake_id
// allocations happen and the validation read rolls back; after it exactly one
// transaction stores the Source's webhook counts and one webhook_accepted (or,
// on a failed publish, webhook_rejected) event outbox row; the intake message
// itself never goes through the outbox.
func TestV0048WebhookIntakeWritesNoOutboxRow(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	e.flush()
	before := *e.store.Source(3, id)
	commits := e.store.Commits
	outboxed := len(e.store.Enqueued)
	commitsAtPublish, outboxAtPublish := -1, -1
	var fail error
	e.broker.PublishHook = func(m broker.Message) error {
		if strings.HasPrefix(m.Subject, "source-controller.task.") {
			commitsAtPublish, outboxAtPublish = e.store.Commits, len(e.store.Enqueued)
			return fail
		}
		return nil
	}
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-1")), false)
	committed, rows := e.store.Commits-commits, e.store.Enqueued[outboxed:]
	if rec.Code != 202 {
		t.Fatalf("status %d", rec.Code)
	}
	if commitsAtPublish != commits || outboxAtPublish != outboxed {
		t.Fatalf("a transaction committed before the intake publish (%d -> %d commits, %d -> %d outbox rows)", commits, commitsAtPublish, outboxed, outboxAtPublish)
	}
	for _, m := range e.store.Enqueued[outboxed:] {
		if strings.HasPrefix(m.Subject, "source-controller.task.") {
			t.Fatal("the intake message went through the outbox")
		}
	}
	msgs := e.intake()
	if len(msgs) != 1 {
		t.Fatal("intake not published directly")
	}
	// After the acknowledgement: exactly one transaction, holding one
	// webhook_accepted event row and the Source's published count, nothing else.
	if committed != 1 {
		t.Fatalf("%d transactions committed for one accepted request, want 1", committed)
	}
	if len(rows) != 1 || rows[0].Subject != "source-controller.event.webhook_accepted.pub" {
		t.Fatalf("outbox rows after the request: %v", rows)
	}
	event, intake := decode(t, rows[0].Data), decode(t, msgs[0].Data)
	if num(event["source_id"]) != id || num(event["task_count"]) != 1 || num(event["intake_publisher_seq"]) != num(intake["publisher_seq"]) ||
		fmt.Sprint(event["intake_ids"]) != "[1]" {
		t.Fatalf("webhook_accepted %s for intake %s", rows[0].Data, msgs[0].Data)
	}
	after := *e.store.Source(3, id)
	if after.Counts.Published != before.Counts.Published+1 || after.Counts.Rejected != before.Counts.Rejected {
		t.Fatalf("counts %+v -> %+v", before.Counts, after.Counts)
	}
	after.Counts = before.Counts
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("the webhook changed more than the Source counts:\n%+v\n%+v", before, after)
	}

	// A publish JetStream does not confirm: still nothing before the publish,
	// then one transaction with the rejected count and one webhook_rejected
	// row; no published count, no webhook_accepted, no intake row.
	e.flush()
	before = *e.store.Source(3, id)
	commits, outboxed = e.store.Commits, len(e.store.Enqueued)
	fail = errors.New("not confirmed")
	rec = e.webhook("acme", "jira-main", key, mustJSON(task("ISSUE-2")), false)
	committed, rows = e.store.Commits-commits, e.store.Enqueued[outboxed:]
	if rec.Code != 503 || decode(t, rec.Body.Bytes())["code"] != "intake_unavailable" {
		t.Fatalf("failed publish: %d %s", rec.Code, rec.Body.String())
	}
	if commitsAtPublish != commits || outboxAtPublish != outboxed || committed != 1 {
		t.Fatalf("commits before the failed publish %d -> %d, after the request %d", commits, commitsAtPublish, committed)
	}
	if len(rows) != 1 || rows[0].Subject != "source-controller.event.webhook_rejected.pub" || decode(t, rows[0].Data)["error_code"] != "intake_unavailable" {
		t.Fatalf("outbox rows after the failed publish: %v", rows)
	}
	after = *e.store.Source(3, id)
	if after.Counts.Published != before.Counts.Published || after.Counts.Rejected != before.Counts.Rejected+1 {
		t.Fatalf("counts after the failed publish %+v -> %+v", before.Counts, after.Counts)
	}
}

func TestV0048WebhookIntakeIDsAreIntegersPerTeam(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	_, key3 := e.webhookSource(3, "jira-main")
	_, key5 := e.webhookSource(5, "jira-main")
	e.store.SetCounter(3, "intake", 40)
	rec := e.webhook("acme", "jira-main", key3, mustJSON(map[string]any{"tasks": []any{task("A"), task("B")}}), true)
	if rec.Code != 202 || !strings.Contains(rec.Body.String(), `"intake_id":41`) || !strings.Contains(rec.Body.String(), `"intake_id":42`) {
		t.Fatalf("team 3: %d %s", rec.Code, rec.Body.String())
	}
	rec = e.webhook("globex", "jira-main", key5, mustJSON(task("A")), false)
	if rec.Code != 202 || !strings.Contains(rec.Body.String(), `"intake_id":1`) {
		t.Fatalf("team 5: %d %s", rec.Code, rec.Body.String())
	}
}
