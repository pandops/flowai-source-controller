//go:build !integration

package test

import (
	"fmt"
	"strings"
	"testing"
)

// Coverage: Each source API key alone determines task ownership.

func TestV0048PollerOrWebhookTaskIsPersisted(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	wid, key := e.webhookSource(3, "hook")
	pid, _ := e.pollerSource(3, "poll")
	e.webhook("acme", "hook", key, mustJSON(task("NEW-1")), false)
	e.setPoll(pages(t, page("P", 1, nil)))
	e.pollOnce()
	msgs := e.intake()
	if len(msgs) != 2 || num(decode(t, msgs[0].Data)["source_id"]) != wid || num(decode(t, msgs[1].Data)["source_id"]) != pid {
		t.Fatalf("messages %d", len(msgs))
	}
}

func TestV0048ClientForgesOwnership(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "hook")
	for _, f := range []string{"team_id", "source_id", "flow_id", "source_system_id", "team_name"} {
		body := task("A")
		body[f] = 9
		if rec := e.webhook("acme", "hook", key, mustJSON(body), false); rec.Code != 400 {
			t.Fatalf("%s: %d", f, rec.Code)
		}
	}
	// Tag and image fields are forged classes too (task-sources-interface
	// spec: "any ownership, flow, tag, or image field"; ledger M224, M394):
	// each is refused with 400 invalid_request naming its path, never its
	// value, single and in a batch.
	const canary = "forged-value-canary"
	for _, f := range []string{"required_tag", "tag", "tags", "executor_tag", "model_alias", "image", "default_image", "runtime_image",
		"flow_ids", "binding", "team", "owner"} {
		body := task("A")
		body[f] = canary
		rec := e.webhook("acme", "hook", key, mustJSON(body), false)
		got := decode(t, rec.Body.Bytes())
		if rec.Code != 400 || got["code"] != "invalid_request" || !strings.Contains(fmt.Sprint(got["fields"]), "/"+f) || strings.Contains(rec.Body.String(), canary) {
			t.Fatalf("%s: %d %s", f, rec.Code, rec.Body.String())
		}
		rec = e.webhook("acme", "hook", key, mustJSON(map[string]any{"tasks": []any{task("B"), body}}), true)
		got = decode(t, rec.Body.Bytes())
		if rec.Code != 400 || got["code"] != "invalid_request" || !strings.Contains(fmt.Sprint(got["fields"]), "/tasks/1/"+f) || strings.Contains(rec.Body.String(), canary) {
			t.Fatalf("batch %s: %d %s", f, rec.Code, rec.Body.String())
		}
	}
	// The same fields at the batch envelope level are refused as well.
	for _, f := range []string{"required_tag", "model_alias", "image", "team_id"} {
		rec := e.webhook("acme", "hook", key, mustJSON(map[string]any{"tasks": []any{task("C")}, f: canary}), true)
		got := decode(t, rec.Body.Bytes())
		if rec.Code != 400 || got["code"] != "invalid_request" || !strings.Contains(fmt.Sprint(got["fields"]), "/"+f) || strings.Contains(rec.Body.String(), canary) {
			t.Fatalf("batch envelope %s: %d %s", f, rec.Code, rec.Body.String())
		}
	}
	if rec := e.webhook("acme", "hook", key, mustJSON(task("A")), false, "X-FlowAI-Team-Id", "9"); rec.Code != 400 {
		t.Fatalf("header: %d", rec.Code)
	}
	if len(e.intake()) != 0 {
		t.Fatal("published a forged task")
	}
}

func TestV0048DifferentSourceKeysReuseASourceID(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	a, ka := e.webhookSource(3, "a")
	b, kb := e.webhookSource(3, "b")
	e.webhook("acme", "a", ka, mustJSON(task("SAME")), false)
	e.webhook("acme", "b", kb, mustJSON(task("SAME")), false)
	msgs := e.intake()
	if len(msgs) != 2 || num(decode(t, msgs[0].Data)["source_id"]) != a || num(decode(t, msgs[1].Data)["source_id"]) != b {
		t.Fatal("tasks not owned by their own Sources")
	}
}
