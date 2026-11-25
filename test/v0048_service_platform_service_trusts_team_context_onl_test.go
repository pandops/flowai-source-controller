//go:build !integration

package test

import (
	"fmt"
	"testing"
)

// Coverage: service-platform service trusts team context only from ui
// requests.

func TestV0048GatewayForwardsVerifiedOperatorContext(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "jira-main", "webhook")
	a := decode(t, e.messages("source-controller.audit.source_created.pub")[0].Data)
	if num(a["actor_id"]) != 501 || num(a["team_id"]) != 3 || num(a["resource_id"]) != id || num(a["request_id"]) < 7000 {
		t.Fatalf("audit %v", a)
	}
}

func TestV0048PayloadNamesAnotherTeam(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(5, "theirs", "webhook")
	r := e.ui("sources-list", 3, map[string]any{"team_id": 5}, nil)
	if r.Status != 400 {
		t.Fatalf("params with a team accepted: %d", r.Status)
	}
	// The display-only team_name header grants nothing.
	req := e.uiRequest("sources-get", 3, map[string]any{"id": 1}, nil)
	req.Header["team_name"] = "globex"
	if got := e.do(req); got.Status != 404 {
		t.Fatalf("team_name header selected another team: %d", got.Status)
	}
}

func TestV0048ContextIsMissing(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	// D12 owner decision 2026-11-09: no valid request_id, no request_id
	// field; every other error keeps it.
	for _, h := range []string{"team", "operator_id", "request_id"} {
		req := e.uiRequest("sources-create", 3, nil, createBody("x", "webhook"))
		want := req.Header["request_id"]
		delete(req.Header, h)
		r := e.do(req)
		if r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("without %s: %d", h, r.Status)
		}
		rid, has := r.Body["request_id"]
		if h == "request_id" && has {
			t.Fatalf("without request_id: %s, want no request_id field", r.Raw)
		}
		if h != "request_id" && fmt.Sprint(rid) != want {
			t.Fatalf("without %s: %s, want request_id %s", h, r.Raw, want)
		}
	}
	for _, bad := range []string{"0", "-1", "07", "r-1", "9223372036854775808"} {
		req := e.uiRequest("sources-create", 3, nil, createBody("x", "webhook"))
		req.Header["request_id"] = bad
		r := e.do(req)
		if _, has := r.Body["request_id"]; r.Status != 400 || r.code() != "invalid_request" || has {
			t.Fatalf("request_id %q: %d %s, want 400 invalid_request without request_id", bad, r.Status, r.Raw)
		}
	}
	unknown := e.uiRequest("sources-create", 3, nil, nil)
	unknown.Subject = "source-controller.ui.no-such-operation.sub"
	delete(unknown.Header, "request_id")
	if r := e.do(unknown); r.Status != 404 || hasRequestID(r) {
		t.Fatalf("unknown operation without request_id: %d %s, want 404 without request_id", r.Status, r.Raw)
	}
	req := e.uiRequest("sources-list", 3, nil, nil)
	req.Header["team"] = "admin"
	if r := e.do(req); r.Status != 400 {
		t.Fatalf("team admin: %d", r.Status)
	}
	if e.store.Source(3, 1) != nil {
		t.Fatal("created without context")
	}
}

func hasRequestID(r reply) bool {
	_, has := r.Body["request_id"]
	return has
}
