//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
)

// Coverage: message-broker allows HTTP only on the named edges
// (source-controller form; the Executor and journal edges are not
// applicable to source-controller).

func TestV0048ServiceOffersNoInternalHTTPAPI(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "jira-main", "webhook")
	for _, p := range []string{"/ui/v1/sources", "/internal/v1/poll-leases", "/internal/v1/webhook-tasks", "/v1/teams", "/healthz", "/debug/pprof/", "/admin/v1/teams"} {
		for _, m := range []string{http.MethodGet, http.MethodPost} {
			if rec := e.httpDo(m, p, "", "application/json", "{}"); rec.Code != 404 {
				t.Fatalf("%s %s: %d", m, p, rec.Code)
			}
		}
	}
}

func TestV0048SourceSystemReadsItsSettingsOnThePublicEdge(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "jira-main")
	if rec := e.httpDo(http.MethodGet, "/v1/sources/self/settings", key, "", ""); rec.Code != 200 {
		t.Fatalf("%d", rec.Code)
	}
	for _, p := range []string{"/v1/sources/self/settings", "/v1/sources/poll-signing-keys"} {
		for _, k := range []string{"", "tsk_v1_" + strings.Repeat("Q", 43)} {
			rec := e.httpDo(http.MethodGet, p, k, "", "")
			b := decode(t, rec.Body.Bytes())
			if rec.Code != 401 || b["code"] != "invalid_source_key" || b["values"] != nil || b["keys"] != nil || strings.Contains(rec.Body.String(), "jira-main") {
				t.Fatalf("%s without a valid key: %d %s", p, rec.Code, rec.Body.String())
			}
		}
	}
}

func TestV0048ExecutorEdgeErrorsAreDistinguishable(t *testing.T) {
	// Not applicable to source-controller: it has no Executor edge. Its edge
	// errors stay distinguishable by code.
	e := newEnv(t)
	e.addTeam(3, "acme")
	codes := map[string]bool{}
	codes[decode(t, e.httpDo(http.MethodGet, "/v1/sources/self", "", "", "").Body.Bytes())["code"].(string)] = true
	codes[decode(t, e.webhook("acme", "x", "k", "{", false).Body.Bytes())["code"].(string)] = true
	if !codes["invalid_source_key"] || !codes["invalid_request"] {
		t.Fatalf("codes %v", codes)
	}
}

func TestV0048JournalStreamEdgeRequiresTheGatewayToken(t *testing.T) {
	// Not applicable to source-controller: it serves no journal stream edge.
	e := newEnv(t)
	if rec := e.httpDo(http.MethodGet, "/v1/journal/stream", "", "", ""); rec.Code != 404 {
		t.Fatalf("%d", rec.Code)
	}
}
