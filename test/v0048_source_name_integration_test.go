//go:build integration

package test

// Source name uniqueness decided by the unique index (critique 1, finding 3):
// case-insensitive per team, free for another team, one winner of a race.

import (
	"sync"
	"testing"
)

func TestV0048SourceNameUniquenessIntegration(t *testing.T) {
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	if _, _, err := e.createSource(3, "jira-main", "webhook"); err != nil {
		t.Fatal(err)
	}
	body := func(name string) map[string]any {
		return map[string]any{"name": name, "mode": "webhook", "contract_version": "source.v1", "implementation_version": "1.0.0", "filter_summary": ""}
	}
	for _, name := range []string{"jira-main", "JIRA-Main"} {
		status, reply, _ := e.ui("sources-create", 3, nil, body(name))
		if status != 409 || reply["code"] != "source_name_conflict" || reply["api_key"] != nil {
			t.Fatalf("%s: %d %v", name, status, reply)
		}
	}
	if _, _, err := e.createSource(5, "JIRA-MAIN", "webhook"); err != nil {
		t.Fatalf("another team: %v", err)
	}
	statuses := make([]int, 8)
	var wg sync.WaitGroup
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i], _, _ = e.ui("sources-create", 3, nil, body("race"))
		}(i)
	}
	wg.Wait()
	wins := 0
	for _, s := range statuses {
		switch s {
		case 201:
			wins++
		case 409:
		default:
			t.Fatalf("status %d in %v", s, statuses)
		}
	}
	if wins != 1 {
		t.Fatalf("winners %d: %v", wins, statuses)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND lower(name) = 'race' AND key_digest IS NOT NULL`); n != 1 {
		t.Fatalf("stored race Sources with a key: %d", n)
	}
}
