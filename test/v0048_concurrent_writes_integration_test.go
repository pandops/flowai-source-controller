//go:build integration

package test

// Concurrent Source writes serialized by row locks (critique 1, finding 5):
// two settings saves with one If-Match give one 200 and one 412; a binding
// guard racing a settings save on the same revision lets exactly one win.

import (
	"sort"
	"sync"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

func TestV0048ConcurrentWritesIntegration(t *testing.T) {
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	id, key, err := e.createSource(3, "jira-main", "webhook")
	if err != nil {
		t.Fatal(err)
	}
	e.registerSchema(key, `[{"name":"project","label":"Project","type":"string","required":false,"default":null}]`)

	revisions := func() int {
		return count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.source_revisions WHERE team_id = 3 AND source_id = $1`, id)
	}

	t.Run("two operators save with one revision", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			etag := e.etagOf(3, id)
			before := revisions()
			statuses := make([]int, 2)
			var wg sync.WaitGroup
			for i := range statuses {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					statuses[i], _, _ = e.request("ui", "sources-settings-put", 3, map[string]any{"id": id},
						map[string]any{"values": map[string]any{"project": "P" + itoa64(int64(round*2+i))}}, broker.Header{"If-Match": etag})
				}(i)
			}
			wg.Wait()
			sort.Ints(statuses)
			if statuses[0] != 200 || statuses[1] != 412 {
				t.Fatalf("round %d statuses %v", round, statuses)
			}
			if after := revisions(); after != before+1 {
				t.Fatalf("round %d appended %d revisions", round, after-before)
			}
		}
	})

	t.Run("a binding guard races a settings save", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			rev := revisionOf(t, admin, 3, id)
			etag := e.etagOf(3, id)
			var guard, save int
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				guard, _, _ = e.request("service", "binding-guard", 3, nil, map[string]any{"team_id": 3,
					"sources": []any{map[string]any{"source_id": id, "expected_revision": rev}}}, nil)
			}()
			go func() {
				defer wg.Done()
				save, _, _ = e.request("ui", "sources-settings-put", 3, map[string]any{"id": id},
					map[string]any{"values": map[string]any{"project": "G" + itoa64(int64(round))}}, broker.Header{"If-Match": etag})
			}()
			wg.Wait()
			ok := guard == 200 && save == 412 || guard == 409 && save == 200
			if !ok {
				t.Fatalf("round %d guard %d save %d", round, guard, save)
			}
			if after := revisionOf(t, admin, 3, id); after != rev+1 {
				t.Fatalf("round %d revision %d -> %d", round, rev, after)
			}
		}
	})
}

func itoa64(v int64) string { return fmtInt(v) }
