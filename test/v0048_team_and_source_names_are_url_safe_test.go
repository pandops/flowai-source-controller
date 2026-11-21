//go:build !integration

package test

import (
	"testing"
)

// Coverage: Team and source names are URL-safe. Source names: 1-63 RFC 3986
// unreserved characters, not "." or "..", unique per team ignoring case;
// source-controller enforces this. Team names: ^[a-z][a-z0-9]*(-[a-z0-9]+)*$,
// 1-10 characters, unique across the platform; team-registry enforces this
// and source-controller does not validate them. Both names appear verbatim in
// the webhook URL.

func TestV0048NameContainsACharacterOutsideTheURLSafeSet(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for _, name := range []string{"jira main", "jira/main", "jira%20", "jíra", ".", "..", "", "a1234567890123456789012345678901234567890123456789012345678901234"} {
		if r := e.ui("sources-create", 3, nil, createBody(name, "webhook")); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%q: %d %s", name, r.Status, r.Raw)
		}
	}
	if r := e.ui("sources-create", 3, nil, createBody("A-z0.9_~", "webhook")); r.Status != 201 {
		t.Fatalf("unreserved name: %d %s", r.Status, r.Raw)
	}
	if r := e.ui("sources-list", 3, nil, nil); len(r.Body["items"].([]any)) != 1 {
		t.Fatal("an invalid name created a Source")
	}
}

func TestV0048NameDiffersFromAnExistingOneOnlyByCase(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "jira-main", "webhook")
	if r := e.ui("sources-create", 3, nil, createBody("Jira-Main", "webhook")); r.Status != 409 || r.code() != "source_name_conflict" {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
}

// Every team name allowed by the team-name rule, including its 1- and
// 10-character bounds, is used verbatim as the first webhook path segment.
func TestV0048TeamNamesFollowTheTeamNameRuleInTheWebhookURL(t *testing.T) {
	e := newEnv(t)
	for i, name := range []string{"a", "ops", "team-2", "a1-b2-c3", "abcdefghij"} {
		team := int64(20 + i)
		e.addTeam(team, name)
		id, key := e.webhookSource(team, "jira-main")
		if url := sourceView(t, e, team, id)["webhook_url"]; url != publicBase+"/webhook/"+name+"/jira-main/task" {
			t.Fatalf("%q: webhook URL %v", name, url)
		}
		if rec := e.webhook(name, "jira-main", key, mustJSON(task("A")), false); rec.Code != 202 {
			t.Fatalf("%q: webhook %d %s", name, rec.Code, rec.Body.String())
		}
	}
	// source-controller SHALL NOT validate team names: a team created before
	// the rule, with a longer name, is stored and used verbatim.
	const legacy = "legacy-team-name"
	e.registryTeam(40, legacy)
	e.applyTeamEvent("team_created", 40, legacy, false)
	if team := e.store.Team(40); team == nil || team.TeamName != legacy {
		t.Fatalf("projection after team_created %+v", team)
	}
	id, key := e.webhookSource(40, "jira-main")
	if url := sourceView(t, e, 40, id)["webhook_url"]; url != publicBase+"/webhook/"+legacy+"/jira-main/task" {
		t.Fatalf("%q: webhook URL %v", legacy, url)
	}
	if rec := e.webhook(legacy, "jira-main", key, mustJSON(task("A")), false); rec.Code != 202 {
		t.Fatalf("%q: webhook %d %s", legacy, rec.Code, rec.Body.String())
	}
}
