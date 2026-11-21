//go:build !integration

package test

import (
	"strings"
	"testing"
	"time"
)

// Coverage: source-controller stores Source settings revisions.

func TestV0048OperatorReadsSourceSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.pollerSource(3, "poller-main")
	e.registerSchema(key, "1.0.0", field("project", "string", true, "OPS"), field("limit", "integer", false, 5))
	s := e.store.Source(3, id)
	if s.Revision != 4 {
		t.Fatalf("revision is %d, want 4", s.Revision)
	}
	r, etag := e.settingsGet(3, id)
	if r.Status != 200 || etag != `"rev-`+itoa(int64(s.Revision))+`"` {
		t.Fatalf("%d etag %q", r.Status, etag)
	}
	assertSchema(t, "ui.sources-settings-get.reply.schema.json", r.Raw)
	platform := r.Body["platform"].(map[string]any)
	if platform["poll_url"] != e.poller.URL+"/poll" || num(platform["poll_interval_minutes"]) != 10 || r.Body["settings_status"] != "complete" {
		t.Fatalf("settings %s", r.Raw)
	}
	if len(r.Body["fields"].([]any)) != 2 || strings.Contains(string(r.Raw), key) || strings.Contains(string(r.Raw), "api_key") {
		t.Fatalf("settings %s", r.Raw)
	}
}

func TestV0048SettingsNameAForeignSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	id, _ := e.createSource(5, "jira-main", "webhook")
	revisions := len(e.store.SourceRevisions(5, id))
	foreign := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}}, `"rev-1"`)
	unknown := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": 77}, map[string]any{"values": map[string]any{}}, `"rev-1"`)
	if foreign.Status != 404 || unknown.Status != 404 || foreign.code() != unknown.code() {
		t.Fatalf("foreign %d %s unknown %d", foreign.Status, foreign.Raw, unknown.Status)
	}
	if len(e.store.SourceRevisions(5, id)) != revisions || e.store.Source(5, id).Revision != 1 {
		t.Fatal("a foreign write appended a revision")
	}
}

func TestV0048StandaloneSourceSaveWithoutAnyFlow(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", true, nil))
	e.broker.Reset()
	r := e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "OPS"}})
	if r.Status != 200 || r.Body["settings_status"] != "complete" || r.Header["ETag"] != `"rev-3"` {
		t.Fatalf("%d %v %s", r.Status, r.Header, r.Raw)
	}
	assertSchema(t, "ui.sources-settings-put.reply.schema.json", r.Raw)
	for _, req := range e.broker.Requests() {
		t.Fatalf("settings save made a request to %s", req.Subject)
	}
}

func TestV0048IntervalOnlyEditKeepsTheCursor(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	s := e.store.Source(3, id)
	cursor := "page-7"
	leased := e.clock.Now().Add(-3 * time.Minute)
	s.Cursor, s.LastLeasedAt = &cursor, &leased
	e.store.PutSource(*s)
	before := *e.store.Source(3, id)
	r := e.saveSettings(3, id, map[string]any{"values": map[string]any{}, "poll_interval_minutes": 30})
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	after := e.store.Source(3, id)
	if after.Revision != before.Revision+1 || after.Generation != before.Generation || after.Cursor == nil || *after.Cursor != cursor {
		t.Fatalf("revision %d->%d generation %d->%d cursor %v", before.Revision, after.Revision, before.Generation, after.Generation, after.Cursor)
	}
	if after.NextPollAt == nil || !after.NextPollAt.Equal(leased.Add(30*time.Minute)) {
		t.Fatalf("next poll %v want %v", after.NextPollAt, leased.Add(30*time.Minute))
	}
	// A poll URL change advances the generation and clears the cursor.
	r = e.saveSettings(3, id, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/other"})
	after = e.store.Source(3, id)
	if r.Status != 200 || after.Generation != before.Generation+1 || after.Cursor != nil {
		t.Fatalf("url change: %d generation %d cursor %v", r.Status, after.Generation, after.Cursor)
	}
	// A no-op keeps the revision.
	rev := after.Revision
	e.saveSettings(3, id, map[string]any{"values": map[string]any{}})
	if e.store.Source(3, id).Revision != rev {
		t.Fatal("a no-op changed the revision")
	}
}
