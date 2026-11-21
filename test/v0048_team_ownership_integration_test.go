//go:build integration

package test

// Coverage 713 (service-platform, Team-owned parent keeps its team): the
// database itself refuses every write that changes the team_id of a stored
// team-owned row, whether or not the row has children, and the service's own
// update path cannot move a Source to another team.

import (
	"context"
	"strings"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/core"
)

func TestV0048TeamOwnedParentKeepsItsTeamIntegration(t *testing.T) {
	ctx := context.Background()
	admin, role, e := freshEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	bare, _, err := e.createSource(3, "bare", "webhook") // a Source row with as few children as possible
	if err != nil {
		t.Fatal(err)
	}
	hook, _ := e.webhookSource(3, "hook") // has schema and settings revisions
	poller := e.pollerSource(3, "poller")
	polls, err := e.svc.StartDuePolls(ctx)
	if err != nil || len(polls) != 1 {
		t.Fatalf("start poll: %v %v", polls, err)
	}

	// The service's update path keys every write by the stored team: a Source
	// value carrying team 5 updates no row and creates none.
	_ = e.store.Tx(ctx, func(tx core.Tx) error {
		src, err := tx.Source(3, hook, true)
		if err != nil || src == nil {
			t.Fatalf("load Source: %v", err)
		}
		src.TeamID = 5
		_ = tx.UpdateSource(src)
		return nil
	})

	// The service role, writing SQL directly, cannot change team_id either.
	db := role.db
	// A Source row with no child rows at all (inserted directly): only an
	// immutability rule, not the child foreign keys, can refuse moving it.
	if _, err := db.ExecContext(ctx, `INSERT INTO `+defaultSchema+`.sources (team_id, source_id, name, mode, enabled, settings_status, created_at, doc)
		VALUES (3, 99, 'orphan', 'webhook', true, 'awaiting_schema', now(), '{}')`); err != nil {
		t.Fatalf("insert child-less Source: %v", err)
	}
	updates := map[string]string{
		"source with no child rows": `UPDATE ` + defaultSchema + `.sources SET team_id = 5 WHERE team_id = 3 AND source_id = 99`,
		"source with one revision":  `UPDATE ` + defaultSchema + `.sources SET team_id = 5 WHERE team_id = 3 AND source_id = ` + fmtInt(bare),
		"source with children":      `UPDATE ` + defaultSchema + `.sources SET team_id = 5 WHERE team_id = 3 AND source_id = ` + fmtInt(hook),
		"settings revision":         `UPDATE ` + defaultSchema + `.source_revisions SET team_id = 5 WHERE team_id = 3`,
		"schema revision":           `UPDATE ` + defaultSchema + `.source_schema_revisions SET team_id = 5 WHERE team_id = 3`,
		"poll":                      `UPDATE ` + defaultSchema + `.polls SET team_id = 5 WHERE team_id = 3`,
	}
	for name, q := range updates {
		_, err := db.ExecContext(ctx, q)
		if sqlState(err) != "23514" || !strings.Contains(err.Error(), "team_id is immutable") {
			t.Errorf("%s: %v, want check_violation \"team_id is immutable\"", name, err)
		}
	}
	// An update that leaves team_id unchanged still works.
	if _, err := db.ExecContext(ctx, `UPDATE `+defaultSchema+`.sources SET team_id = team_id, name = name WHERE team_id = 3`); err != nil {
		t.Fatalf("same-team update refused: %v", err)
	}

	for _, table := range []string{"sources", "source_revisions", "source_schema_revisions", "polls"} {
		if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.`+table+` WHERE team_id = 5`); n != 0 {
			t.Errorf("%s has %d team 5 rows", table, n)
		}
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id IN ($1, $2, $3, 99)`, bare, hook, poller); n != 4 {
		t.Fatalf("team 3 keeps %d of its 4 Sources", n)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3`); n != 1 {
		t.Fatalf("team 3 keeps %d polls, want 1", n)
	}
}
