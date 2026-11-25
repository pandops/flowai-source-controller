//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
)

// Coverage: source-controller authenticates Source self requests at its HTTP
// edge.

func TestV0048SourceRegistersFieldsAfterReceivingItsKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	rec := e.registerSchema(key, "1.0.0", field("project", "string", true, "OPS"), field("token_label", "string", true, nil))
	if rec.Code != 200 {
		t.Fatalf("register %d %s", rec.Code, rec.Body.String())
	}
	body := decode(t, rec.Body.Bytes())
	if body["settings_status"] != "incomplete" || num(body["schema_revision"]) != 1 {
		t.Fatalf("register reply %v", body)
	}
	settings, _ := e.settingsGet(3, id)
	values := map[string]any{}
	for _, f := range settings.Body["fields"].([]any) {
		m := f.(map[string]any)
		values[m["name"].(string)] = m["value"]
	}
	if values["project"] != "OPS" || values["token_label"] != nil {
		t.Fatalf("effective values %v", values)
	}
	self := e.httpDo(http.MethodGet, "/v1/sources/self", key, "", "")
	if self.Code != 200 || decode(t, self.Body.Bytes())["settings_status"] != "incomplete" {
		t.Fatalf("self %d %s", self.Code, self.Body.String())
	}
	if strings.Contains(self.Body.String(), key) {
		t.Fatal("self shows the key")
	}
}

func TestV0048SchemaChangesWhileAnOperatorEditsSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", false, nil))
	e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "A"}})
	_, loaded := e.settingsGet(3, id)
	if loaded != `"rev-3"` {
		t.Fatalf("loaded %s", loaded)
	}
	if rec := e.registerSchema(key, "1.0.0", field("project", "string", false, nil), field("label", "string", false, nil)); rec.Code != 200 {
		t.Fatalf("re-register %d", rec.Code)
	}
	if _, now := e.settingsGet(3, id); now != `"rev-4"` {
		t.Fatalf("after schema change %s", now)
	}
	r := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{"project": "B"}}, loaded)
	if r.Status != 412 || r.code() != "stale_revision" {
		t.Fatalf("stale save %d %s", r.Status, r.Raw)
	}
	if string(e.store.Source(3, id).Values["project"]) != `"A"` {
		t.Fatal("stale save changed a value")
	}
}

func TestV0048SchemaUsesAReservedName(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	for _, name := range []string{"flow_id", "flow_ids", "source_ids", "poll_url", "poll_interval_minutes", "webhook_url"} {
		rec := e.registerSchema(key, "1.0.0", field(name, "string", false, nil))
		if rec.Code != 400 || decode(t, rec.Body.Bytes())["code"] != "invalid_request" {
			t.Fatalf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	s := e.store.Source(3, id)
	if s.Schema != nil || s.Revision != 1 || len(e.store.SchemaRevisions(3, id)) != 0 {
		t.Fatal("a rejected schema changed the Source")
	}
}

func TestV0048PollerReadsWebhookSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.createSource(3, "poller-main", "poller")
	rec := e.httpDo(http.MethodGet, "/v1/sources/self/settings", key, "", "")
	if rec.Code != 403 || decode(t, rec.Body.Bytes())["code"] != "not_webhook_source" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}
