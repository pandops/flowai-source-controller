//go:build !integration

package test

import (
	"testing"
)

// Coverage: source-controller follows team names and stops intake for archived
// teams.

func TestV0048ArchivedTeamsWebhookSourceSubmitsATask(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "jira-main")
	e.applyTeamEvent("team_archived", 3, "acme", true)
	rec := e.webhook("acme", "jira-main", key, mustJSON(task("A")), false)
	if rec.Code != 409 || decode(t, rec.Body.Bytes())["code"] != "team_archived" {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if len(e.intake()) != 0 {
		t.Fatal("published for an archived team")
	}
	if rejected := e.messages("source-controller.event.webhook_rejected.pub"); len(rejected) != 1 {
		t.Fatalf("webhook_rejected events %d", len(rejected))
	}
	// The Source stays readable; writes are rejected.
	if r := e.ui("sources-get", 3, map[string]any{"id": id}, nil); r.Status != 200 {
		t.Fatalf("read after archive %d", r.Status)
	}
	if r := e.saveSettings(3, id, map[string]any{"values": map[string]any{}}); r.Status != 404 {
		t.Fatalf("settings write after archive %d", r.Status)
	}
	if r := e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0"}, `"rev-2"`); r.Status != 409 || r.code() != "source_unavailable" {
		t.Fatalf("implementation after archive %d %s", r.Status, r.Raw)
	}
}

func TestV0048ArchivedTeamsPollerIsDue(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	e.applyTeamEvent("team_archived", 3, "acme", true)
	e.makeDue(3, id)
	if polls := e.pollOnce(); len(polls) != 0 || len(e.calls()) != 0 {
		t.Fatalf("polls %d calls %d", len(polls), len(e.calls()))
	}
}

// Scenario: Team update keeps the webhook URL. A team name never changes after
// create, so a team_updated event that carries only a new default_image leaves
// the team's webhook URLs unchanged.
func TestV0048TeamUpdateKeepsTheWebhookURL(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "ops")
	id, key := e.webhookSource(3, "jira-main")
	before := sourceView(t, e, 3, id)["webhook_url"]
	// teamEvent carries default_image "b…", the registry team "a…": only the
	// image changes, the name stays "ops".
	e.applyTeamEvent("team_updated", 3, "ops", false)
	if team := e.store.Team(3); team == nil || team.TeamName != "ops" {
		t.Fatalf("projection after team_updated %+v", team)
	}
	if rec := e.webhook("ops", "jira-main", key, mustJSON(task("A")), false); rec.Code != 202 {
		t.Fatalf("webhook after team_updated %d %s", rec.Code, rec.Body.String())
	}
	if len(e.intake()) != 1 {
		t.Fatalf("intake messages %d", len(e.intake()))
	}
	want := publicBase + "/webhook/ops/jira-main/task"
	if url := sourceView(t, e, 3, id)["webhook_url"]; url != want || before != want {
		t.Fatalf("webhook URL before %v after %v, want %s", before, url, want)
	}
}
