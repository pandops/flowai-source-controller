//go:build !integration

package test

import (
	"sync"
	"testing"
)

// Coverage: source-controller makes each Source its own source system.

func TestV0048TeamCreatesASource(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for i, mode := range []string{"poller", "webhook"} {
		r := e.ui("sources-create", 3, nil, createBody([]string{"jira-main", "gitlab-main"}[i], mode))
		if r.Status != 201 {
			t.Fatalf("%s: %d %s", mode, r.Status, r.Raw)
		}
		source := r.Body["source"].(map[string]any)
		if num(source["id"]) != int64(i+1) || num(source["source_id"]) != num(source["id"]) || source["settings_status"] != "awaiting_schema" || source["mode"] != mode {
			t.Fatalf("source %v", source)
		}
		key, _ := r.Body["api_key"].(string)
		if len(key) != 50 || key[:7] != "tsk_v1_" {
			t.Fatalf("key %q", key)
		}
		stored := e.store.Source(3, num(source["id"]))
		if stored == nil || len(stored.KeyDigest) != 32 || string(stored.KeyDigest) == key {
			t.Fatal("key digest not stored")
		}
	}
}

func TestV0048SourceIDsAreNumberedPerTeam(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	a, _ := e.createSource(3, "first", "webhook")
	b, _ := e.createSource(3, "second", "webhook")
	c, _ := e.createSource(5, "other", "webhook")
	if a != 1 || b != 2 || c != 1 {
		t.Fatalf("ids %d %d %d", a, b, c)
	}
	if got := sourceView(t, e, 3, 1); got["name"] != "first" || num(got["team_id"]) != 3 {
		t.Fatalf("team 3 Source 1 %v", got)
	}
}

func TestV0048SourceNameCollides(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(3, "jira-main", "webhook")
	for _, name := range []string{"jira-main", "JIRA-Main"} {
		if r := e.ui("sources-create", 3, nil, createBody(name, "poller")); r.Status != 409 || r.code() != "source_name_conflict" || r.Body["api_key"] != nil {
			t.Fatalf("%s: %d %s", name, r.Status, r.Raw)
		}
	}
	// A disabled Source keeps its name.
	e.ui("sources-disable", 3, map[string]any{"id": 1}, nil)
	if r := e.ui("sources-create", 3, nil, createBody("jira-main", "poller")); r.Status != 409 {
		t.Fatalf("disabled name reuse: %d", r.Status)
	}
	// Another team may use the name.
	if r := e.ui("sources-create", 5, nil, createBody("jira-main", "poller")); r.Status != 201 {
		t.Fatalf("other team: %d", r.Status)
	}
	// Concurrent creations: exactly one winner and one key.
	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses[i] = e.ui("sources-create", 3, nil, createBody("race", "webhook")).Status
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
			t.Fatalf("status %d", s)
		}
	}
	if wins != 1 {
		t.Fatalf("winners %d", wins)
	}
}

func TestV0048CreationNamesAnOwnershipOrFlowField(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	for _, extra := range []string{"team_id", "source_system_id", "api_key", "flow_id", "flow_ids", "source_id"} {
		body := createBody("jira-main", "webhook")
		body[extra] = 5
		if extra == "api_key" {
			body[extra] = "tsk_v1_chosen"
		}
		if r := e.ui("sources-create", 3, nil, body); r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%s: %d %s", extra, r.Status, r.Raw)
		}
	}
	if r := e.ui("sources-list", 3, nil, nil); len(r.Body["items"].([]any)) != 0 {
		t.Fatal("a rejected creation created a Source")
	}
}
