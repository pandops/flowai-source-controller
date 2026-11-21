//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
)

// Coverage: Each source API key is shown only once when created.

func TestV0048TSIOperatorRotatesASourceKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, old := e.webhookSource(3, "hook")
	r := e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	fresh, _ := r.Body["api_key"].(string)
	if r.Status != 200 || fresh == "" || fresh == old || r.Header["Cache-Control"] != "no-store" {
		t.Fatalf("%d %v", r.Status, r.Header)
	}
	if rec := e.webhook("acme", "hook", old, mustJSON(task("A")), false); rec.Code != 401 {
		t.Fatalf("old key %d", rec.Code)
	}
}

func TestV0048OperatorTriesToViewAPreviouslyCreatedKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "hook")
	reads := []reply{
		e.ui("sources-get", 3, map[string]any{"id": id}, nil),
		e.ui("sources-list", 3, nil, nil),
		e.ui("sources-settings-get", 3, map[string]any{"id": id}, nil),
		e.ui("sources-disable", 3, map[string]any{"id": id}, nil),
	}
	for i, r := range reads {
		if strings.Contains(string(r.Raw), key) || strings.Contains(string(r.Raw), "api_key") {
			t.Fatalf("read %d shows the key: %s", i, r.Raw)
		}
	}
	if rec := e.httpDo(http.MethodGet, "/v1/sources/self", key, "", ""); strings.Contains(rec.Body.String(), key) {
		t.Fatal("self shows the key")
	}
}

func TestV0048KeyIsRevokedOrSourceDisabled(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "hook")
	e.ui("sources-disable", 3, map[string]any{"id": id}, nil)
	for _, rec := range []int{
		e.webhook("acme", "hook", key, mustJSON(task("A")), false).Code,
		e.httpDo(http.MethodGet, "/v1/sources/self/settings", key, "", "").Code,
		e.registerSchema(key, "1.0.0").Code,
		e.httpDo(http.MethodPut, "/v1/sources/self/status", key, "application/json", mustJSON(statusBody("idle"))).Code,
	} {
		if rec != 401 {
			t.Fatalf("disabled key accepted: %d", rec)
		}
	}
	if s := e.store.Source(3, id); s.KeyDigest != nil || s.Enabled {
		t.Fatal("disable kept the key digest")
	}
}
