//go:build !integration

package test

import (
	"strings"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/postgres"
)

// Coverage: service-platform service owns one PostgreSQL schema and role.

func TestV0048AccessToAForeignSchemaIsDenied(t *testing.T) {
	// Unit form: every statement the store issues names only its own schema;
	// the integration test proves the role's grants.
	for _, stmt := range postgres.Statements() {
		lower := strings.ToLower(stmt)
		for _, foreign := range []string{"public.", "team_registry_schema", "task_manager_schema", "state_registry", "flow_registry_schema"} {
			if strings.Contains(lower, foreign) {
				t.Fatalf("statement names %s: %s", foreign, stmt)
			}
		}
	}
	if len(postgres.Statements()) == 0 {
		t.Fatal("no statements reported")
	}
}
