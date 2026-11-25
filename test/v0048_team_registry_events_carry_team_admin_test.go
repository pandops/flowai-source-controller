//go:build !integration

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Coverage: team-registry publishes team changes for downstream projections.
// Its events carry `team: admin` (the header of the administrator request
// that caused them) and the changed team's integer team_id in the payload;
// source-controller keys its projection by that payload team_id.

func (e *env) eventWithTeamHeader(subject, team string, payload map[string]any) broker.Disposition {
	e.t.Helper()
	raw, _ := json.Marshal(payload)
	header := broker.Header{"team": team, "Nats-Msg-Id": fmt.Sprint(payload["publisher"]) + "-" + fmt.Sprint(payload["publisher_seq"])}
	return e.svc.HandleEvent(context.Background(), broker.Delivery{Subject: subject, Header: header, Data: raw})
}

func TestV0048TeamRegistryEventsWithTeamAdminReachTheProjection(t *testing.T) {
	e := newEnv(t)
	if d := e.eventWithTeamHeader("team-registry.event.team_created.sub", "admin", e.teamEvent("team_created", 7, "newco", false)); d != broker.Ack {
		t.Fatalf("team_created with team: admin: %v", d)
	}
	if team := e.store.Team(7); team == nil || team.TeamName != "newco" || team.ArchivedAt != nil {
		t.Fatalf("projection after team_created %+v", team)
	}
	// A team name never changes: team_updated carries only a new default_image.
	update := e.teamEvent("team_updated", 7, "newco", false)
	update["default_image"] = "registry.example/agent@sha256:" + strings.Repeat("c", 64)
	if d := e.eventWithTeamHeader("team-registry.event.team_updated.sub", "admin", update); d != broker.Ack {
		t.Fatalf("team_updated with team: admin: %v", d)
	}
	if team := e.store.Team(7); team == nil || team.TeamName != "newco" || team.ArchivedAt != nil || team.LastSeq != update["publisher_seq"].(int64) {
		t.Fatalf("projection after team_updated %+v (want last_seq %v)", team, update["publisher_seq"])
	}
	if d := e.eventWithTeamHeader("team-registry.event.team_archived.sub", "admin", e.teamEvent("team_archived", 7, "newco", true)); d != broker.Ack {
		t.Fatalf("team_archived with team: admin: %v", d)
	}
	if team := e.store.Team(7); team == nil || team.ArchivedAt == nil {
		t.Fatalf("projection after team_archived %+v", team)
	}
	if team := e.store.Team(1); team != nil {
		t.Fatalf("an admin header created a projection row other than the payload team: %+v", team)
	}
}

func TestV0048TeamHeaderChecksStayStrict(t *testing.T) {
	e := newEnv(t)
	// A decimal header naming another team than the payload is rejected.
	if d := e.eventWithTeamHeader("team-registry.event.team_created.sub", "8", e.teamEvent("team_created", 7, "newco", false)); d != broker.Term {
		t.Fatalf("team_created with a mismatched decimal header: %v", d)
	}
	if team := e.store.Team(7); team != nil {
		t.Fatalf("mismatched header changed the projection: %+v", team)
	}
	// A missing or malformed header is rejected.
	for _, h := range []string{"", "seven", "Admin"} {
		if d := e.eventWithTeamHeader("team-registry.event.team_created.sub", h, e.teamEvent("team_created", 7, "newco", false)); d != broker.Term {
			t.Fatalf("team_created with header %q: %v", h, d)
		}
	}
	// `team: admin` is accepted only on team-registry's team events.
	e.addTeam(3, "acme")
	src, _ := e.webhookSource(3, "jira-main")
	if d := e.eventWithTeamHeader("task-manager.event.intake_processed.sub", "admin", e.intakeProcessed(3, src, "accepted")); d != broker.Term {
		t.Fatalf("intake_processed with team: admin: %v", d)
	}
	if d := e.eventWithTeamHeader("task-manager.event.intake_processed.sub", "4", e.intakeProcessed(3, src, "accepted")); d != broker.Term {
		t.Fatalf("intake_processed with a mismatched header: %v", d)
	}
	if d := e.eventWithTeamHeader("task-manager.event.intake_processed.sub", "3", e.intakeProcessed(3, src, "accepted")); d != broker.Ack {
		t.Fatalf("intake_processed with its own team header: %v", d)
	}
}
