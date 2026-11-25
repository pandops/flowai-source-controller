//go:build !integration

package test

import (
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

// Coverage: service-platform service gets foreign data only from the broker.

func TestV0048TeamArchiveReachesAProjection(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.applyTeamEvent("team_archived", 3, "acme", true)
	if team := e.store.Team(3); team == nil || team.ArchivedAt == nil {
		t.Fatalf("projection %+v", team)
	}
}

func TestV0048ProjectionIgnoresAReplayedEvent(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	// Team names never change, so archived_at tells the events apart.
	update := e.teamEvent("team_updated", 3, "acme", false)
	if d := e.event("team-registry.event.team_updated.sub", 3, update); d != broker.Ack {
		t.Fatal(d)
	}
	e.applyTeamEvent("team_archived", 3, "acme", true)
	if d := e.event("team-registry.event.team_updated.sub", 3, update); d != broker.Ack {
		t.Fatal(d)
	}
	if team := e.store.Team(3); team == nil || team.TeamName != "acme" || team.ArchivedAt == nil {
		t.Fatalf("replay changed the projection: %+v", team)
	}
	// A malformed event is terminated, not redelivered forever.
	if d := e.event("team-registry.event.team_updated.sub", 3, map[string]any{"publisher": "team-registry"}); d != broker.Term {
		t.Fatalf("malformed: %v", d)
	}
}
