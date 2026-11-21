//go:build !integration

package test

import (
	"sort"
	"sync"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/core"
)

// Coverage: service-platform services use integer identifiers.

func TestV0048InvalidIdentifierIsRejected(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "a", "webhook")
	// The spec's values (abc, 0, -1, 1.5, 2^63, a UUID) and other
	// non-canonical forms.
	bad := []string{"abc", "0", "-1", "1.5", "9223372036854775808", "3f2c8a10-6b1e-4d2a-9c3e-5a7b8c9d0e1f", "+3", "03", "99999999999999999999"}
	// A refused request reads and writes nothing: no transaction, no
	// sequence allocation, no outbox row.
	untouched := func(what string, send func() reply) {
		t.Helper()
		accesses, enqueued, seq := e.store.AccessCount(), len(e.store.Enqueued), e.store.LastPublisherSeq()
		r := send()
		if r.Status != 400 || r.code() != "invalid_request" {
			t.Fatalf("%s: %d %s, want 400 invalid_request", what, r.Status, r.Raw)
		}
		if e.store.AccessCount() != accesses || len(e.store.Enqueued) != enqueued || e.store.LastPublisherSeq() != seq {
			t.Fatalf("%s: the refused request touched the store (accesses %d -> %d)", what, accesses, e.store.AccessCount())
		}
	}
	for _, team := range bad {
		for _, op := range []string{"sources-list", "sources-create"} {
			untouched("team "+team+" "+op, func() reply {
				var body any
				if op == "sources-create" {
					body = createBody("b", "webhook")
				}
				req := e.uiRequest(op, 3, nil, body)
				req.Header["team"] = team
				return e.do(req)
			})
		}
	}
	untouched("string source_id", func() reply {
		return e.service("binding-guard", 3, map[string]any{"team_id": 3, "sources": []any{map[string]any{"source_id": "1", "expected_revision": 1}}})
	})
	if r := e.ui("sources-list", 3, nil, nil); r.Status != 200 {
		t.Fatalf("valid team after the refusals: %d %s", r.Status, r.Raw)
	}
}

func TestV0048FirstRecordInEachTeamIs1(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	a, _ := e.pollerSource(3, "p")
	b, _ := e.pollerSource(5, "p")
	polls := e.pollOnce()
	if a != 1 || b != 1 || len(polls) != 2 || polls[0].ID != 1 || polls[1].ID != 1 {
		t.Fatalf("sources %d %d polls %v", a, b, polls)
	}
}

func TestV0048EqualNumbersInTwoTeamsGrantNothing(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	e.createSource(3, "mine", "webhook")
	e.createSource(5, "theirs", "webhook")
	if v := sourceView(t, e, 3, 1); v["name"] != "mine" {
		t.Fatalf("%v", v["name"])
	}
	e.ui("sources-disable", 3, map[string]any{"id": 1}, nil)
	if !e.store.Source(5, 1).Enabled {
		t.Fatal("team 3 disabled team 5's Source 1")
	}
}

func TestV0048ConcurrentCreatesGetUniqueIncreasingNumbers(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	ids := make([]int64, 20)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids[i], _ = e.createSource(3, "s"+itoa(int64(i)), "webhook")
		}(i)
	}
	wg.Wait()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, id := range ids {
		if id != int64(i+1) {
			t.Fatalf("ids %v", ids)
		}
	}
}

func TestV0048RolledBackTransactionNeitherPublishesNorReusesItsSequence(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "a", "webhook")
	before := e.store.LastPublisherSeq()
	e.store.FailCommit = true
	if r := e.ui("sources-create", 3, nil, createBody("b", "webhook")); r.Status == 201 {
		t.Fatal("a failed commit replied 201")
	}
	failed := e.store.LastPublisherSeq()
	if failed <= before {
		t.Fatalf("the failed transaction allocated no publisher_seq (%d -> %d)", before, failed)
	}
	e.createSource(3, "c", "webhook")
	for _, s := range []*core.Source{e.store.Source(3, 1), e.store.Source(3, 2), e.store.Source(3, 3)} {
		if s != nil && s.Name == "b" {
			t.Fatal("the rolled-back Source is stored")
		}
	}
	var after []int64
	for _, m := range e.messages("") {
		b := decode(t, m.Data)
		if changes, _ := b["changes"].(map[string]any); changes["name"] == "b" && b["outcome"] == "succeeded" {
			t.Fatal("the rolled-back creation was published")
		}
		if seq := num(b["publisher_seq"]); seq > before {
			after = append(after, seq)
		}
	}
	if len(after) == 0 {
		t.Fatal("nothing published after the failed commit")
	}
	for _, seq := range after {
		if seq <= failed {
			t.Fatalf("publisher_seq %d reuses a value the failed transaction allocated (<= %d)", seq, failed)
		}
	}
}

func TestV0048RedeliveredMessageIsDeduplicatedByPublisherSequence(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "a", "webhook")
	ev := e.intakeProcessed(3, id, "accepted")
	for i := 0; i < 2; i++ {
		if d := e.event("task-manager.event.intake_processed.sub", 3, ev); d != broker.Ack {
			t.Fatal(d)
		}
	}
	if c := counts(t, e, 3, id); num(c["accepted"]) != 1 {
		t.Fatalf("%v", c)
	}
}
