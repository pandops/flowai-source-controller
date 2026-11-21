//go:build !integration

package test

import (
	"sync"
	"testing"
)

// Coverage: Operators configure sources through flow settings (the Source
// editor part source-controller owns).

func TestV0048OperatorCompletesPollerSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "poller-main", "poller")
	e.registerSchema(key, "1.0.0", field("project", "string", true, nil))
	r := e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "OPS"}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 10})
	if r.Status != 200 || r.Body["settings_status"] != "complete" {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	e.makeDue(3, id)
	e.pollOnce()
	calls := e.calls()
	if len(calls) != 1 || calls[0].URL != e.poller.URL+"/poll" {
		t.Fatalf("calls %v", calls)
	}
	s := e.store.Source(3, id)
	if s.NextPollAt == nil || s.NextPollAt.Sub(*s.LastLeasedAt).Minutes() != 10 {
		t.Fatalf("next poll %v last leased %v", s.NextPollAt, s.LastLeasedAt)
	}
	// Poll fields on a webhook Source and Flow fields are rejected.
	wid, _ := e.createSource(3, "hook", "webhook")
	for _, body := range []map[string]any{
		{"values": map[string]any{}, "poll_url": e.poller.URL},
		{"values": map[string]any{}, "flow_ids": []any{1}},
		{"values": map[string]any{}, "source_settings": []any{}},
	} {
		if r := e.saveSettings(3, wid, body); r.Status != 400 {
			t.Fatalf("%v: %d", body, r.Status)
		}
	}
	// A poll URL outside the allowlist is rejected.
	if r := e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "OPS"}, "poll_url": "https://elsewhere.example/poll"}); r.Status != 400 {
		t.Fatalf("allowlist: %d", r.Status)
	}
}

func TestV0048OperatorViewsWebhookSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "gitlab-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", false, "OPS"))
	r, _ := e.settingsGet(3, id)
	if r.Body["platform"].(map[string]any)["webhook_url"] != publicBase+"/webhook/acme/gitlab-main/task" || len(r.Body["fields"].([]any)) != 1 {
		t.Fatalf("%s", r.Raw)
	}
}

func TestV0048TeamHasNoSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("sources-list", 3, nil, nil)
	if r.Status != 200 || len(r.Body["items"].([]any)) != 0 || r.Body["next_cursor"] != nil {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
}

func TestV0048TwoOperatorsEditSettingsConcurrently(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", false, nil))
	_, etag := e.settingsGet(3, id)
	statuses := make([]int, 2)
	var wg sync.WaitGroup
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i] = e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{"project": "P" + itoa(int64(i))}}, etag).Status
		}(i)
	}
	wg.Wait()
	if ok := statuses[0] == 200 && statuses[1] == 412 || statuses[0] == 412 && statuses[1] == 200; !ok {
		t.Fatalf("statuses %v", statuses)
	}
}

func TestV0048BindingChangeMakesAnOpenSourceEditorStale(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.webhookSource(3, "jira-main")
	setRevision(e, 3, id, 5)
	before := *e.store.Source(3, id)
	if r := e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{map[string]any{"source_id": id, "expected_revision": 5}}}); r.Status != 200 {
		t.Fatalf("guard %d", r.Status)
	}
	after := e.store.Source(3, id)
	if after.Revision != 6 || len(after.Values) != len(before.Values) {
		t.Fatal("guard changed values or did not advance")
	}
	if r := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}}, `"rev-5"`); r.Status != 412 {
		t.Fatalf("%d", r.Status)
	}
}
