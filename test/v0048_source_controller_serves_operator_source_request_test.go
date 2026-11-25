//go:build !integration

package test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// Coverage: source-controller serves operator Source requests over the broker.

func TestV0048OperatorCreatesASourceThroughGateway(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("sources-create", 3, nil, createBody("jira-main", "webhook"))
	if r.Status != 201 || r.Header["Cache-Control"] != "no-store" {
		t.Fatalf("status %d header %v %s", r.Status, r.Header, r.Raw)
	}
	source := r.Body["source"].(map[string]any)
	if num(source["team_id"]) != 3 || num(source["id"]) != 1 || num(source["source_id"]) != 1 || source["name"] != "jira-main" ||
		source["settings_status"] != "awaiting_schema" || source["webhook_url"] != publicBase+"/webhook/acme/jira-main/task" {
		t.Fatalf("source %v", source)
	}
	assertSchema(t, "ui.sources-create.reply.schema.json", r.Raw)
}

func TestV0048OperatorReadsAForeignSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(5, "jira-main", "webhook")
	e.createSource(5, "gitlab-main", "webhook")
	foreign := e.ui("sources-get", 3, map[string]any{"id": 2}, nil)
	unknown := e.ui("sources-get", 3, map[string]any{"id": 99}, nil)
	if foreign.Status != 404 || unknown.Status != 404 || foreign.code() != unknown.code() || foreign.Body["message"] != unknown.Body["message"] {
		t.Fatalf("foreign %d %s unknown %d %s", foreign.Status, foreign.Raw, unknown.Status, unknown.Raw)
	}
	for _, op := range []string{"sources-settings-get", "sources-rotate-key", "sources-disable"} {
		if r := e.ui(op, 3, map[string]any{"id": 1}, nil); r.Status != 404 || r.code() != unknown.code() {
			t.Fatalf("%s foreign: %d %s", op, r.Status, r.Raw)
		}
	}
}

func TestV0048OperatorPagesThroughSources(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for i := 0; i < 120; i++ {
		e.createSource(3, fmt.Sprintf("src-%03d", i), "webhook")
		e.clock.Advance(time.Millisecond)
	}
	var sizes []int
	var ids []int64
	cursor := ""
	for page := 0; page < 4; page++ {
		params := map[string]any{"limit": 50}
		if cursor != "" {
			params["cursor"] = cursor
		}
		r := e.ui("sources-list", 3, params, nil)
		if r.Status != 200 {
			t.Fatalf("page %d: %d %s", page, r.Status, r.Raw)
		}
		assertSchema(t, "ui.sources-list.reply.schema.json", r.Raw)
		items := r.Body["items"].([]any)
		sizes = append(sizes, len(items))
		for _, item := range items {
			ids = append(ids, num(item.(map[string]any)["id"]))
		}
		next, _ := r.Body["next_cursor"].(string)
		if next == "" {
			if r.Body["next_cursor"] != nil {
				t.Fatalf("next_cursor %v", r.Body["next_cursor"])
			}
			break
		}
		cursor = next
	}
	if fmt.Sprint(sizes) != "[50 50 20]" {
		t.Fatalf("page sizes %v", sizes)
	}
	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("order broken at %d: %v", i, ids)
		}
	}
}

func TestV0048OperatorNamesASourceByANonIntegerID(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "jira-main", "poller")
	before := *e.store.Source(3, id)
	revisionsBefore := len(e.store.SourceRevisions(3, id))
	accesses, enqueued, seq := e.store.AccessCount(), len(e.store.Enqueued), e.store.LastPublisherSeq()
	for _, bad := range []any{"jira-main", "abc", "3f2c9a1e-0000-4000-8000-000000000000", "3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f", 0, -1, -4, 1.5, "1", "+1", "01",
		json.Number("9223372036854775808"), 9223372036854775807.0 * 4} {
		for _, op := range []string{"sources-get", "sources-settings-put"} {
			var body any // sources-get takes no body, so its refusal is the id's
			if op == "sources-settings-put" {
				body = map[string]any{"values": map[string]any{}}
			}
			r := e.uiIfMatch(op, 3, map[string]any{"id": bad}, body, `"rev-1"`)
			if r.Status != 400 || r.code() != "invalid_request" {
				t.Fatalf("%s id %v: %d %s", op, bad, r.Status, r.Raw)
			}
		}
	}
	// Refused before any read or write: no transaction, sequence or outbox row.
	if e.store.AccessCount() != accesses || len(e.store.Enqueued) != enqueued || e.store.LastPublisherSeq() != seq {
		t.Fatalf("an invalid ID touched the store (accesses %d -> %d)", accesses, e.store.AccessCount())
	}
	after := *e.store.Source(3, id)
	if after.Revision != before.Revision || len(e.store.SourceRevisions(3, id)) != revisionsBefore {
		t.Fatal("an invalid ID changed the Source")
	}
}
