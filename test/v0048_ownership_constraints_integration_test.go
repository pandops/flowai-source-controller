//go:build integration

package test

// Normalized ownership enforced by database constraints (critique 1,
// finding 6): a poll, settings revision or schema revision stored with team 3
// for a Source that exists only in team 5 is refused and creates no row.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/core"
)

func TestV0048OwnershipConstraintsIntegration(t *testing.T) {
	ctx := context.Background()
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	if _, _, err := e.createSource(5, "theirs", "poller"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	writes := map[string]func(core.Tx) error{
		"poll": func(tx core.Tx) error {
			return tx.InsertPoll(&core.Poll{TeamID: 3, ID: 1, SourceID: 1, URL: "https://poller.example/poll", Values: json.RawMessage(`{}`),
				SettingsRevision: 1, Generation: 1, LeasedAt: now, StartBy: now, CallDeadline: now, ExpiresAt: now, ScheduledAt: now})
		},
		"settings revision": func(tx core.Tx) error { return tx.AppendSourceRevision(3, 1, 2, json.RawMessage(`{}`)) },
		"schema revision":   func(tx core.Tx) error { return tx.AppendSchemaRevision(3, 1, 1, "1.0.0", []core.Field{}) },
	}
	for name, write := range writes {
		err := e.store.Tx(ctx, write)
		if sqlState(err) != "23503" {
			t.Fatalf("%s for a foreign-team Source: %v, want a foreign key violation", name, err)
		}
	}
	for _, table := range []string{"polls", "source_revisions", "source_schema_revisions"} {
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.`+table+` WHERE team_id = 3`); n != 0 {
			t.Fatalf("%s has %d team 3 rows", table, n)
		}
	}
}
