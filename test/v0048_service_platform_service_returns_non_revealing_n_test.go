//go:build !integration

package test

import "testing"

// Coverage: service-platform service returns non-revealing not-found for
// foreign resources (Source form of the task and environment scenarios).

func TestV0048OperatorReadsAForeignTask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(5, "theirs", "webhook")
	foreign, unknown := e.ui("sources-get", 3, map[string]any{"id": 1}, nil), e.ui("sources-get", 3, map[string]any{"id": 2}, nil)
	if foreign.Status != 404 || foreign.code() != unknown.code() || foreign.Body["message"] != unknown.Body["message"] {
		t.Fatalf("%s vs %s", foreign.Raw, unknown.Raw)
	}
}

func TestV0048OperatorWritesAForeignEnvironment(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(5, "theirs", "webhook")
	for _, op := range []string{"sources-rotate-key", "sources-disable"} {
		if r := e.ui(op, 3, map[string]any{"id": 1}, nil); r.Status != 404 {
			t.Fatalf("%s: %d", op, r.Status)
		}
	}
	if r := e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": 1}, map[string]any{"implementation_version": "2"}, `"rev-1"`); r.Status != 404 {
		t.Fatalf("implementation: %d", r.Status)
	}
	if s := e.store.Source(5, 1); !s.Enabled || s.ImplementationVersion != "1.0.0" {
		t.Fatal("foreign write changed the Source")
	}
}
