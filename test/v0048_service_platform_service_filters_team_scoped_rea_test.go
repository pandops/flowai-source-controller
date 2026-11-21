//go:build !integration

package test

import (
	"sync"
	"testing"
)

// Coverage: service-platform service filters team-scoped reads before shaping.

func TestV0048TeamsShareFilterValues(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(3, "jira-main", "webhook")
	e.createSource(5, "jira-main", "webhook")
	e.createSource(5, "other", "webhook")
	r := e.ui("sources-list", 3, map[string]any{"limit": 1}, nil)
	if len(r.Body["items"].([]any)) != 1 || r.Body["next_cursor"] != nil {
		t.Fatalf("%s", r.Raw)
	}
}

func TestV0048ConcurrentTeamStreams(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	for i := 0; i < 5; i++ {
		e.createSource(3, "a"+itoa(int64(i)), "webhook")
		e.createSource(5, "b"+itoa(int64(i)), "webhook")
	}
	var wg sync.WaitGroup
	for _, team := range []int64{3, 5, 3, 5} {
		wg.Add(1)
		go func(team int64) {
			defer wg.Done()
			r := e.ui("sources-list", team, nil, nil)
			for _, it := range r.Body["items"].([]any) {
				if num(it.(map[string]any)["team_id"]) != team {
					t.Errorf("team %d saw %v", team, it)
				}
			}
		}(team)
	}
	wg.Wait()
}
