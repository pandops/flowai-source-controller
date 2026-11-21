//go:build !integration

package test

import "testing"

// Coverage: source-controller guards Flow binding changes with the Source
// revision.

func setRevision(e *env, team, id int64, rev int) {
	s := e.store.Source(team, id)
	s.Revision = rev
	e.store.PutSource(*s)
}

func TestV0048BindingGuardMatchesAndAdvancesTheRevisions(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	a, _ := e.pollerSource(3, "a")
	b, _ := e.createSource(3, "b", "webhook")
	setRevision(e, 3, a, 3)
	setRevision(e, 3, b, 7)
	before := *e.store.Source(3, a)
	r := e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{
		map[string]any{"source_id": b, "expected_revision": 7}, map[string]any{"source_id": a, "expected_revision": 3}}})
	if r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	assertSchema(t, "service.binding-guard.reply.schema.json", r.Raw)
	got := map[int64]int64{}
	for _, s := range r.Body["sources"].([]any) {
		m := s.(map[string]any)
		got[num(m["source_id"])] = num(m["revision"])
	}
	if got[a] != 4 || got[b] != 8 {
		t.Fatalf("revisions %v", got)
	}
	after := e.store.Source(3, a)
	if after.Revision != 4 || after.Generation != before.Generation || string(after.Values["project"]) != string(before.Values["project"]) || after.SettingsStatus != before.SettingsStatus {
		t.Fatal("binding guard changed more than the revision")
	}
}

func TestV0048BindingGuardSeesAStaleRevision(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "a", "webhook")
	e.registerSchema(key, "1.0.0") // moves the revision to 2
	other, _ := e.createSource(3, "b", "webhook")
	r := e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{
		map[string]any{"source_id": other, "expected_revision": 1}, map[string]any{"source_id": id, "expected_revision": 1}}})
	if r.Status != 409 || r.code() != "source_revision_conflict" {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	if e.store.Source(3, id).Revision != 2 || e.store.Source(3, other).Revision != 1 {
		t.Fatal("a conflicting guard changed a revision")
	}
}

func TestV0048BindingGuardNamesAForeignSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	for i := 0; i < 4; i++ {
		e.createSource(5, "s"+itoa(int64(i)), "webhook")
	}
	stale := e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{map[string]any{"source_id": 4, "expected_revision": 1}}})
	if stale.Status != 409 || stale.code() != "source_revision_conflict" {
		t.Fatalf("%d %s", stale.Status, stale.Raw)
	}
	if e.store.Source(5, 4).Revision != 1 {
		t.Fatal("foreign Source changed")
	}
}

func TestV0048SourceEditRacesABindingChange(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "a")
	setRevision(e, 3, id, 5)
	before := *e.store.Source(3, id)
	if r := e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{map[string]any{"source_id": id, "expected_revision": 5}}}); r.Status != 200 {
		t.Fatalf("guard %d", r.Status)
	}
	r := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{"project": "NEW"}, "poll_url": e.poller.URL + "/x"}, `"rev-5"`)
	if r.Status != 412 || r.code() != "stale_revision" {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	after := e.store.Source(3, id)
	if after.Generation != before.Generation || *after.PollURL != *before.PollURL || after.Values["project"] != nil {
		t.Fatal("stale settings write changed state")
	}
}
