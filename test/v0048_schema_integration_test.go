//go:build integration

package test

// 1.7 row: source-controller writes only source_controller_schema with its
// own role; another schema is refused. Also service-platform "Access to a
// foreign schema is denied" and rule 16 (schema pre-created by the
// deployment, no CREATE on the database).

import (
	"context"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/postgres"
)

func TestV0048SchemaIsolationIntegration(t *testing.T) {
	ctx := context.Background()
	admin, dbName := freshDB(t)
	mustExec(t, admin, `CREATE SCHEMA team_registry_schema`)
	mustExec(t, admin, `CREATE TABLE team_registry_schema.teams (team_id bigint, team_name text)`)
	mustExec(t, admin, `INSERT INTO team_registry_schema.teams VALUES (3, 'acme')`)
	role := serviceRole(t, admin, dbName, defaultSchema, true, false)
	st := openStore(t, role, defaultSchema)

	t.Run("pre-created schema migrates twice without CREATE on the database", func(t *testing.T) {
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("second migrate: %v", err)
		}
	})

	t.Run("migrations create objects only in source_controller_schema", func(t *testing.T) {
		rows, err := admin.QueryContext(ctx, `SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE pg_get_userbyid(c.relowner) = $1 AND n.nspname <> 'pg_toast'`, role.name)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var ns, rel string
			_ = rows.Scan(&ns, &rel)
			n++
			if ns != defaultSchema {
				t.Errorf("object %s.%s outside %s", ns, rel, defaultSchema)
			}
		}
		if n == 0 {
			t.Fatal("migration created no object")
		}
	})

	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		var n int
		if err := role.db.QueryRowContext(ctx, `SELECT count(*) FROM team_registry_schema.teams`).Scan(&n); sqlState(err) != "42501" {
			t.Fatalf("read of team_registry_schema = %v, want 42501", err)
		}
		if _, err := role.db.ExecContext(ctx, `INSERT INTO team_registry_schema.teams VALUES (4, 'x')`); sqlState(err) != "42501" {
			t.Fatalf("write to team_registry_schema = %v, want 42501", err)
		}
		if _, err := role.db.ExecContext(ctx, `CREATE TABLE public.sc_leak (x int)`); sqlState(err) != "42501" {
			t.Fatalf("create in public = %v, want 42501", err)
		}
	})

	t.Run("the service works as its role", func(t *testing.T) {
		e := newPGEnv(t, st, nil, nil)
		e.addTeam(3, "acme")
		if _, _, err := e.createSource(3, "jira-main", "webhook"); err != nil {
			t.Fatal(err)
		}
		e.flush()
		if len(e.published("source-controller.audit.source_created.pub")) != 1 {
			t.Fatal("no audit published")
		}
		if err := st.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Configured schema and host are used", func(t *testing.T) {
		other := serviceRole(t, admin, dbName, "sc_custom_schema", true, false)
		openStore(t, other, "sc_custom_schema")
		if n := count(t, admin, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'sc_custom_schema'`); n == 0 {
			t.Fatal("no tables in sc_custom_schema")
		}
		if _, err := other.db.ExecContext(ctx, `SELECT 1 FROM `+defaultSchema+`.outbox`); sqlState(err) != "42501" {
			t.Fatalf("second role reads the first schema: %v", err)
		}
	})

	t.Run("a missing schema is created only with the right to create it", func(t *testing.T) {
		noRight := serviceRole(t, admin, dbName, "sc_missing_schema", false, false)
		s, err := postgres.Open(ctx, noRight.dsn(), "sc_missing_schema")
		if err == nil {
			defer s.Close()
			err = s.Migrate(ctx)
		}
		if err == nil {
			t.Fatal("migration without the schema or CREATE succeeded")
		}
		withRight := serviceRole(t, admin, dbName, "sc_created_schema", false, true)
		openStore(t, withRight, "sc_created_schema")
		if n := count(t, admin, `SELECT count(*) FROM pg_namespace WHERE nspname = 'sc_created_schema'`); n != 1 {
			t.Fatal("schema not created")
		}
	})
}
