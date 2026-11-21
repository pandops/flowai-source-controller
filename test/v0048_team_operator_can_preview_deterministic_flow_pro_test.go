//go:build !integration

package test

import "testing"

// Coverage (rule 13 analog): Team operator can preview deterministic flow
// prompt rendering. flow-registry owns the preview; source-controller answers
// no preview operation and changes nothing for one.

func TestV0048OperatorPreviewsSourceInterpolation(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("flow-preview", 3, nil, map[string]any{"template": "Solve {{external_id}}", "external_id": "ISSUE-42"})
	if r.Status != 404 || len(e.messages("")) != 0 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
}

func TestV0048PreviewInputAttemptsAForeignTeamOrUnknownPlaceholder(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	r := e.ui("flow-preview", 3, nil, map[string]any{"template": "{{secret}}", "team_id": 5})
	if r.Status != 404 || len(e.ui("sources-list", 3, nil, nil).Body["items"].([]any)) != 0 {
		t.Fatalf("%d", r.Status)
	}
}
