//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Coverage: Webhook tasks are accepted only at the source's URL.

func TestV0048TSIWebhookSourceSubmitsANewTask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	if rec := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false); rec.Code != 202 || len(e.intake()) != 1 {
		t.Fatalf("%d", rec.Code)
	}
	for _, path := range []string{"/webhook/acme/jira-main", "/webhook/acme/jira-main/task/extra", "/webhook/acme", "/internal/v1/webhook-tasks"} {
		if rec := e.httpDo(http.MethodPost, path, key, "application/json", mustJSON(task("A"))); rec.Code != 404 {
			t.Fatalf("%s: %d", path, rec.Code)
		}
	}
	if rec := e.httpDo(http.MethodGet, "/webhook/acme/jira-main/task", key, "", ""); rec.Code != 404 {
		t.Fatalf("GET: %d", rec.Code)
	}
	// Non-canonical spellings of the Source's URL (ledger M286): double
	// slashes, dot segments, a trailing slash and an encoded slash are "every
	// other path" of the delta spec, so each gets the plain 404 not_found
	// error (no redirect to a cleaned path) and nothing is published.
	for _, path := range []string{"/webhook/acme//jira-main/task", "/webhook//acme/jira-main/task", "//webhook/acme/jira-main/task",
		"/webhook/acme/jira-main//task", "/webhook/acme/./jira-main/task", "/webhook/acme/x/../jira-main/task",
		"/webhook/acme/jira-main/task/", "/webhook/acme/jira%2Fmain/task"} {
		rec := e.httpDo(http.MethodPost, path, key, "application/json", mustJSON(task("NONCANONICAL")))
		if rec.Code != 404 || rec.Header().Get("Location") != "" {
			t.Fatalf("%s: %d location %q, want 404 without a redirect", path, rec.Code, rec.Header().Get("Location"))
		}
		if body := decode(t, rec.Body.Bytes()); body["code"] != "not_found" || strings.Contains(rec.Body.String(), "NONCANONICAL") {
			t.Fatalf("%s: body %s", path, rec.Body.String())
		}
	}
	if len(e.intake()) != 1 {
		t.Fatal("a non-canonical path published a task")
	}
}

func TestV0048RequiredFieldIsMissing(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	for body, path := range map[string]string{`{"external_id":"A"}`: "/payload", `{"external_id":"","payload":{}}`: "/external_id"} {
		rec := e.webhook("acme", "jira-main", key, body, false)
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), `"`+path+`"`) {
			t.Fatalf("%s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	if len(e.intake()) != 0 {
		t.Fatal("published")
	}
}

func TestV0048WebhookSourceSubmitsAValidBatch(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": []any{task("A"), task("B"), task("C")}}), true)
	if rec.Code != 202 || len(decode(t, rec.Body.Bytes())["tasks"].([]any)) != 3 || len(e.intake()) != 1 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestV0048BatchPublishIsNotConfirmed(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.broker.BlockPublish = func(m broker.Message) bool { return strings.HasPrefix(m.Subject, "source-controller.task.") }
	start := time.Now()
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": []any{task("A"), task("B")}}), true)
	waited := time.Since(start)
	if rec.Code != 503 || decode(t, rec.Body.Bytes())["code"] != "intake_unavailable" || waited < 4500*time.Millisecond || waited > 6*time.Second {
		t.Fatalf("%d after %v", rec.Code, waited)
	}
	e.broker.BlockPublish = nil
	if len(e.intake()) != 0 {
		t.Fatal("published")
	}
}

func TestV0048IntakeIDTracesToTheCreatedTasks(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.store.SetCounter(3, "intake", 40)
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	m := e.intake()[0]
	b := decode(t, m.Data)
	if num(decode(t, rec.Body.Bytes())["intake_id"]) != 41 || num(b["tasks"].([]any)[0].(map[string]any)["intake_id"]) != 41 ||
		m.Header["Nats-Msg-Id"] != "source-controller-"+itoa(num(b["publisher_seq"])) {
		t.Fatalf("%s %v", rec.Body.String(), m.Header)
	}
}

func TestV0048ResendAfterTheEarlierTaskWasArchived(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	first := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	again := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	if again.Code != 202 || num(decode(t, again.Body.Bytes())["intake_id"]) == num(decode(t, first.Body.Bytes())["intake_id"]) {
		t.Fatalf("%d %s", again.Code, again.Body.String())
	}
}

func TestV0048TSIBatchContainsOneInvalidTask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	tasks := []any{task("A"), task("B"), task("A")}
	rec := e.webhook("acme", "jira-main", key, mustJSON(map[string]any{"tasks": tasks}), true)
	if rec.Code != 400 || !strings.Contains(rec.Body.String(), "/tasks/2/external_id") || len(e.intake()) != 0 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestV0048TSIKeyDoesNotMatchThePath(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	e.webhookSource(3, "gitlab-main")
	if rec := e.webhook("acme", "gitlab-main", key, mustJSON(task("A")), false); rec.Code != 401 || len(e.intake()) != 0 {
		t.Fatalf("%d", rec.Code)
	}
}
