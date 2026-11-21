//go:build !integration

package test

import (
	"context"
	"net/http"
	"testing"
)

// Coverage: service-platform service stores team ownership in normalized form.

func TestV0048TeamOwnedParentKeepsItsTeam(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "a", "webhook")
	e.registerSchema(key, "1.0.0")
	rotated := e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	if s := e.store.Source(3, id); s.TeamID != 3 {
		t.Fatal("team changed")
	}
	// Writes that try to move the Source to team 5 are refused and change
	// nothing (coverage 713; the database-level refusal is
	// TestV0048TeamOwnedParentKeepsItsTeamIntegration).
	e.addTeam(5, "globex")
	_, etag := e.settingsGet(3, id)
	for name, r := range map[string]reply{
		"settings body team_id":  e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}, "team_id": 5}, etag),
		"settings param team_id": e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id, "team_id": 5}, map[string]any{"values": map[string]any{}}, etag),
		"implementation team_id": e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0", "team_id": 5}, etag),
		"create with team_id":    e.ui("sources-create", 3, nil, map[string]any{"name": "a", "mode": "webhook", "contract_version": "source.v1", "implementation_version": "1.0.0", "filter_summary": "x", "team_id": 5}),
	} {
		if r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%s: %d %s", name, r.Status, r.Raw)
		}
	}
	// Another team's context cannot reach or rewrite it.
	for _, op := range []string{"sources-rotate-key", "sources-disable"} {
		if r := e.ui(op, 5, map[string]any{"id": id}, nil); r.Status != 404 {
			t.Fatalf("%s by team 5: %d %s", op, r.Status, r.Raw)
		}
	}
	// Source self status cannot carry ownership.
	status := statusBody("idle")
	status["team_id"] = 5
	newKey, _ := rotated.Body["api_key"].(string)
	if rec := e.httpDo(http.MethodPut, "/v1/sources/self/status", newKey, "application/json", mustJSON(status)); rec.Code != 400 {
		t.Fatalf("self status with team_id: %d %s", rec.Code, rec.Body.String())
	}
	if s := e.store.Source(3, id); s == nil || s.TeamID != 3 || !s.Enabled || e.store.Source(5, id) != nil {
		t.Fatalf("Source after the team-changing writes: %+v, team 5 copy %v", s, e.store.Source(5, id))
	}
	if r := e.ui("sources-get", 3, map[string]any{"id": id}, nil); r.Status != 200 {
		t.Fatalf("team 3 view: %d %s", r.Status, r.Raw)
	}
}

func TestV0048ChildReferencesAForeignTeamParent(t *testing.T) {
	// A poll is stored under its Source's team; it cannot be finalized
	// through another team's numbers.
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.pollerSource(3, "p")
	polls, _ := e.svc.StartDuePolls(context.Background())
	if len(polls) != 1 || polls[0].TeamID != 3 || e.store.Poll(5, polls[0].ID) != nil {
		t.Fatalf("polls %v", polls)
	}
}

func TestV0048DisplayMetadataNeverAuthorizes(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	_, key := e.webhookSource(5, "hook")
	// The URL's team name is compared with the key's Source only.
	if rec := e.webhook("acme", "hook", key, mustJSON(task("A")), false); rec.Code != 401 {
		t.Fatalf("%d", rec.Code)
	}
}
