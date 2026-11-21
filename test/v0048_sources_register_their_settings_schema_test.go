//go:build !integration

package test

import "testing"

// Coverage: Sources register their settings schema.

func TestV0048TSISourceRegistersFieldsAfterReceivingItsKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "poller-main", "poller")
	if rec := e.registerSchema(key, "1.0.0", field("project", "string", true, "OPS"), field("label", "string", true, nil)); rec.Code != 200 {
		t.Fatalf("%d", rec.Code)
	}
	if e.store.Source(3, id).SettingsStatus != "incomplete" {
		t.Fatal("not incomplete")
	}
	r := e.saveSettings(3, id, map[string]any{"values": map[string]any{"label": "L"}, "poll_url": e.poller.URL + "/p", "poll_interval_minutes": 5})
	if r.Body["settings_status"] != "complete" {
		t.Fatalf("after filling: %s", r.Raw)
	}
	// Identical registration is idempotent.
	rev := e.store.Source(3, id).Revision
	e.registerSchema(key, "1.0.0", field("project", "string", true, "OPS"), field("label", "string", true, nil))
	if e.store.Source(3, id).Revision != rev || len(e.store.SchemaRevisions(3, id)) != 1 {
		t.Fatal("identical schema appended a revision")
	}
}

func TestV0048SourceChangesItsSchema(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	e.registerSchema(key, "1.0.0", field("keep", "string", false, nil), field("drop", "string", false, nil))
	e.saveSettings(3, id, map[string]any{"values": map[string]any{"keep": "K", "drop": "D"}})
	e.registerSchema(key, "1.0.0", field("keep", "string", false, nil), field("fresh", "integer", true, nil))
	s := e.store.Source(3, id)
	if string(s.Values["keep"]) != `"K"` || s.Values["drop"] != nil || s.SettingsStatus != "incomplete" {
		t.Fatalf("values %v status %s", s.Values, s.SettingsStatus)
	}
	if rec := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false); rec.Code != 409 || decode(t, rec.Body.Bytes())["code"] != "source_settings_incomplete" {
		t.Fatalf("incomplete webhook %d", rec.Code)
	}
}

func TestV0048TSISchemaChangesWhileAnOperatorEditsSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	_, loaded := e.settingsGet(3, id)
	e.registerSchema(key, "1.0.0", field("x", "string", false, nil))
	if r := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}}, loaded); r.Status != 412 {
		t.Fatalf("%d", r.Status)
	}
}

func TestV0048SchemaUsesAReservedNameOrInvalidDefault(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	bad := [][]map[string]any{
		{field("poll_url", "string", false, nil)},
		{field("count", "integer", false, "five")},
		{field("count", "integer", false, 1.5)},
		{field("flag", "boolean", false, "yes")},
		{field("Upper", "string", false, nil)},
		{field("dup", "string", false, nil), field("dup", "string", false, nil)},
	}
	many := make([]map[string]any, 51)
	for i := range many {
		many[i] = field("f"+itoa(int64(i)), "string", false, nil)
	}
	bad = append(bad, many)
	for i, fields := range bad {
		if rec := e.registerSchema(key, "1.0.0", fields...); rec.Code != 400 {
			t.Fatalf("case %d: %d", i, rec.Code)
		}
	}
	if rec := e.registerSchema(key, "9.9.9"); rec.Code != 400 {
		t.Fatalf("version mismatch: %d", rec.Code)
	}
	if s := e.store.Source(3, id); s.Schema != nil || s.Revision != 1 {
		t.Fatal("rejected schema changed the Source")
	}
}
