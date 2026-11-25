//go:build integration

package test

// 1.7 row: per-team counters under concurrent creates of one kind in one
// team give unique, increasing numbers; gaps after a rollback are allowed and
// no number is reused; the first record in a new team is 1.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/pandops/flowai-source-controller/internal/core"
)

func TestV0048PerTeamCounterIntegration(t *testing.T) {
	ctx := context.Background()
	admin, dbName := freshDB(t)
	role := serviceRole(t, admin, dbName, defaultSchema, true, false)
	st := openStore(t, role, defaultSchema)
	e := newPGEnv(t, st, nil, nil)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")

	t.Run("concurrent Source creates in one team get unique increasing numbers", func(t *testing.T) {
		const n = 16
		ids := make([]int64, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ids[i], _, errs[i] = e.createSource(3, fmt.Sprintf("s-%d", i), "webhook")
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for i := 1; i < n; i++ {
			if ids[i] <= ids[i-1] {
				t.Fatalf("duplicate or unordered ids %v", ids)
			}
		}
		if ids[0] != 1 {
			t.Fatalf("first Source in team 3 is %d", ids[0])
		}
		if c := count(t, admin, `SELECT count(DISTINCT source_id) FROM `+defaultSchema+`.sources WHERE team_id = 3`); c != n {
			t.Fatalf("%d distinct stored ids", c)
		}
	})

	t.Run("the first record in a new team is 1", func(t *testing.T) {
		id, _, err := e.createSource(5, "first", "webhook")
		if err != nil || id != 1 {
			t.Fatalf("first Source in team 5 is %d (%v)", id, err)
		}
	})

	t.Run("a rolled-back allocation reuses no committed number", func(t *testing.T) {
		var committed []int64
		for i := 0; i < 6; i++ {
			var got int64
			err := st.Tx(ctx, func(tx core.Tx) error {
				var err error
				got, err = tx.NextCounter(3, "poll")
				if err == nil && i%2 == 1 {
					return errors.New("rollback")
				}
				return err
			})
			if err == nil {
				committed = append(committed, got)
			}
		}
		for i := 1; i < len(committed); i++ {
			if committed[i] <= committed[i-1] {
				t.Fatalf("poll numbers %v", committed)
			}
		}
		if committed[0] != 1 {
			t.Fatalf("first poll number in team 3 is %d", committed[0])
		}
	})

	t.Run("concurrent poll numbers in one team are unique", func(t *testing.T) {
		const n = 12
		got := make([]int64, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_ = st.Tx(ctx, func(tx core.Tx) error {
					var err error
					got[i], err = tx.NextCounter(5, "poll")
					return err
				})
			}(i)
		}
		wg.Wait()
		seen := map[int64]bool{}
		for _, v := range got {
			if v < 1 || seen[v] {
				t.Fatalf("poll numbers %v", got)
			}
			seen[v] = true
		}
	})

	t.Run("concurrent webhook batches get distinct per-team intake IDs", func(t *testing.T) {
		_, k3 := e.webhookSource(3, "hook")
		_, k5 := e.webhookSource(5, "hook")
		const batches = 8
		got := make([][]int64, batches)
		var wg sync.WaitGroup
		for i := 0; i < batches; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				body := fmt.Sprintf(`{"tasks":[{"external_id":"A%d","payload":{}},{"external_id":"B%d","payload":{}},{"external_id":"C%d","payload":{}}]}`, i, i, i)
				rec := e.webhook("acme", "hook", k3, body, true)
				if rec.Code != 202 {
					t.Errorf("batch %d: %d %s", i, rec.Code, rec.Body.String())
					return
				}
				for _, r := range decodeMap(t, rec.Body.Bytes())["tasks"].([]any) {
					got[i] = append(got[i], asInt(r.(map[string]any)["intake_id"]))
				}
			}(i)
		}
		wg.Wait()
		seen := map[int64]bool{}
		for i, ids := range got {
			if len(ids) != 3 || ids[1] != ids[0]+1 || ids[2] != ids[1]+1 {
				t.Fatalf("batch %d ids %v are not consecutive", i, ids)
			}
			for _, id := range ids {
				if seen[id] {
					t.Fatalf("intake id %d issued twice", id)
				}
				seen[id] = true
			}
		}
		rec := e.webhook("globex", "hook", k5, `{"external_id":"X","payload":{}}`, false)
		if rec.Code != 202 || asInt(decodeMap(t, rec.Body.Bytes())["intake_id"]) != 1 {
			t.Fatalf("team 5 first intake: %d %s", rec.Code, rec.Body.String())
		}
	})
}
