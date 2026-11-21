//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Coverage: Teams register poller and webhook sources in the Sources interface.

func TestV0048TeamCreatesAPollerSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("sources-create", 3, nil, createBody("jira-main", "poller"))
	if r.Status != 201 || r.Body["api_key"] == nil {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	s := r.Body["source"].(map[string]any)
	if s["settings_status"] != "awaiting_schema" || s["webhook_url"] != nil || s["polling"] == nil {
		t.Fatalf("source %v", s)
	}
}

func TestV0048SourceNameIsAlreadyUsed(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "jira-main", "poller")
	r := e.ui("sources-create", 3, nil, createBody("jira-main", "webhook"))
	if r.Status != 409 || r.code() != "source_name_conflict" || strings.Contains(string(r.Raw), "tsk_v1_") {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
}

func TestV0048TeamCreatesAWebhookSource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("sources-create", 3, nil, createBody("gitlab-main", "webhook"))
	if r.Status != 201 || r.Body["source"].(map[string]any)["webhook_url"] != publicBase+"/webhook/acme/gitlab-main/task" {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
}

func TestV0048RegistrationAttemptsForeignOwnershipOrAFlow(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(5, "theirs", "webhook")
	body := createBody("mine", "webhook")
	body["team_id"] = 5
	r := e.ui("sources-create", 3, nil, body)
	if r.Status != 400 || strings.Contains(string(r.Raw), "theirs") || strings.Contains(string(r.Raw), "globex") {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	if len(e.ui("sources-list", 5, nil, nil).Body["items"].([]any)) != 1 || len(e.ui("sources-list", 3, nil, nil).Body["items"].([]any)) != 0 {
		t.Fatal("rejected registration mutated a team")
	}
}
