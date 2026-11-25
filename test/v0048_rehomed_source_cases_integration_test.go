//go:build integration

package test

// Survivors of State Registry integration cases that v0048 deleted, on the
// real PostgreSQL store (decision ledger docs/testing/v0048-test-decisions.csv).

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/mocks"
)

// rawUI sends one ui request and returns the status, raw body and headers.
func (e *pgEnv) rawUI(op string, team int64, params map[string]any, body any, ifMatch string) (int, string, broker.Header) {
	e.t.Helper()
	extra := broker.Header{}
	if ifMatch != "" {
		extra["If-Match"] = ifMatch
	}
	if params == nil {
		params = map[string]any{}
	}
	envelope := map[string]any{"params": params}
	if body != nil {
		envelope["body"] = body
	}
	raw, _ := json.Marshal(envelope)
	e.mu.Lock()
	e.reqSeq++
	rid := 7000 + e.reqSeq
	e.mu.Unlock()
	h := broker.Header{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(rid, 10), "operator_id": "501"}
	for k, v := range extra {
		h[k] = v
	}
	rep := e.svc.HandleRequest(context.Background(), broker.Request{Subject: "source-controller.ui." + op + ".sub", Header: h, Data: raw})
	status, _ := strconv.Atoi(rep.Header["status"])
	return status, string(rep.Data), rep.Header
}

// saveRaw writes settings from a raw JSON body with the current ETag.
func (e *pgEnv) saveRaw(team, id int64, body string) {
	e.t.Helper()
	var parsed any
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		e.t.Fatal(err)
	}
	if status, raw, _ := e.rawUI("sources-settings-put", team, map[string]any{"id": id}, parsed, e.etagOf(team, id)); status != 200 {
		e.t.Fatalf("save %s: %d %s", body, status, raw)
	}
}

func settingsField(t *testing.T, raw, name string) map[string]any {
	t.Helper()
	body := decodeMap(t, []byte(raw))
	for _, f := range body["fields"].([]any) {
		if m := f.(map[string]any); m["name"] == name {
			return m
		}
	}
	t.Fatalf("no field %s in %s", name, raw)
	return nil
}

// M267: the latest stored Source revision records the committed settings
// status.
func TestV0048SourceRevisionRecordsCommittedStatusIntegration(t *testing.T) {
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	id, key, err := e.createSource(3, "history-source", "webhook")
	if err != nil {
		t.Fatal(err)
	}
	rec := e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key,
		`{"contract_version":"source.v1","implementation_version":"1.0.0","fields":[{"name":"project","label":"Project","type":"string","required":true,"default":null}]}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"settings_status":"incomplete"`) {
		t.Fatalf("register: %d %s", rec.Code, rec.Body.String())
	}
	latest := func() (int64, string) {
		var rev int64
		var status string
		if err := admin.QueryRowContext(context.Background(), `SELECT revision, snapshot->>'settings_status' FROM `+defaultSchema+`.source_revisions WHERE team_id = 3 AND source_id = $1 ORDER BY revision DESC LIMIT 1`, id).Scan(&rev, &status); err != nil {
			t.Fatal(err)
		}
		return rev, status
	}
	if rev, status := latest(); status != "incomplete" || rev != revisionOf(t, admin, 3, id) {
		t.Fatalf("latest revision %d status %q, want the current revision with incomplete", rev, status)
	}
	e.saveRaw(3, id, `{"values":{"project":"OPS"}}`)
	if rev, status := latest(); status != "complete" || rev != revisionOf(t, admin, 3, id) {
		t.Fatalf("latest revision %d status %q, want the current revision with complete", rev, status)
	}
}

// M273, M274, M275: a required empty-string value stays "" (not null) and
// keeps settings complete across an unrelated save, and a number or integer
// default 9007199254740993 survives registration, jsonb storage and an
// unrelated save byte-exact.
func TestV0048SettingsValuesKeepTheirExactStoredFormIntegration(t *testing.T) {
	_, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	t.Run("required empty string", func(t *testing.T) {
		id, key, err := e.createSource(3, "empty-string", "webhook")
		if err != nil {
			t.Fatal(err)
		}
		e.registerSchema(key, `[{"name":"token","label":"Token","type":"string","required":true,"default":""},{"name":"flag","label":"Flag","type":"boolean","required":false,"default":false}]`)
		for _, body := range []string{`{"values":{"flag":true}}`, `{"values":{"token":"","flag":true}}`} {
			e.saveRaw(3, id, body)
			status, raw, _ := e.rawUI("sources-settings-get", 3, map[string]any{"id": id}, nil, "")
			if status != 200 {
				t.Fatalf("settings: %d %s", status, raw)
			}
			if v, ok := settingsField(t, raw, "token")["value"]; !ok || v != "" {
				t.Fatalf("after %s token value %#v present %v, want \"\"", body, v, ok)
			}
			if !strings.Contains(raw, `"value":""`) {
				t.Fatalf("after %s settings do not carry the empty string: %s", body, raw)
			}
			if settingsField(t, raw, "flag")["value"] != true || decodeMap(t, []byte(raw))["settings_status"] != "complete" {
				t.Fatalf("after %s: %s", body, raw)
			}
		}
	})
	exact := regexp.MustCompile(`"value":9007199254740993[,}]`)
	for _, kind := range []string{"number", "integer"} {
		t.Run("large "+kind+" default", func(t *testing.T) {
			id, key, err := e.createSource(3, "large-"+kind, "webhook")
			if err != nil {
				t.Fatal(err)
			}
			e.registerSchema(key, `[{"name":"counter","label":"Counter","type":"`+kind+`","required":false,"default":9007199254740993},{"name":"label","label":"Label","type":"string","required":false,"default":"original"}]`)
			_, initial, _ := e.rawUI("sources-settings-get", 3, map[string]any{"id": id}, nil, "")
			if !exact.MatchString(initial) || !strings.Contains(initial, `"default":9007199254740993`) {
				t.Fatalf("schema default lost precision: %s", initial)
			}
			for _, body := range []string{`{"values":{"label":"changed"}}`, `{"values":{"counter":9007199254740993,"label":"changed"}}`} {
				e.saveRaw(3, id, body)
				_, raw, _ := e.rawUI("sources-settings-get", 3, map[string]any{"id": id}, nil, "")
				if !exact.MatchString(raw) {
					t.Fatalf("after %s counter is not exactly 9007199254740993: %s", body, raw)
				}
				if settingsField(t, raw, "label")["value"] != "changed" {
					t.Fatalf("after %s: %s", body, raw)
				}
			}
		})
	}
}

// pollCycle starts every due poll and executes it.
func (e *pgEnv) pollCycle(ctx context.Context) int {
	e.t.Helper()
	polls, err := e.svc.StartDuePolls(ctx)
	if err != nil {
		e.t.Fatalf("start polls: %v", err)
	}
	for _, p := range polls {
		if err := e.svc.ExecutePoll(ctx, p); err != nil {
			e.t.Fatalf("execute poll: %v", err)
		}
	}
	e.flush()
	return len(polls)
}

// M239 on PostgreSQL: five consecutive failures block polling durably; the
// block and counter survive a restart and an implementation update, a stale
// unblock is 412 and the current-revision unblock resets them and schedules
// the next poll one interval after the new schema completes the settings.
func TestV0048FiveFailureBlockIntegration(t *testing.T) {
	ctx := context.Background()
	admin, role, e := freshEnv(t)
	e.addTeam(3, "acme")
	calls := 0
	e.setPoll(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
	})
	id := e.pollerSource(3, "failing")
	revBefore := revisionOf(t, admin, 3, id)
	for i := 1; i <= 5; i++ {
		if n := e.pollCycle(ctx); n != 1 {
			t.Fatalf("cycle %d started %d polls", i, n)
		}
		failures := asInt(e.view(3, id)["polling"].(map[string]any)["consecutive_failures"])
		if failures != int64(i) {
			t.Fatalf("after failure %d the counter is %d", i, failures)
		}
		e.clock.Advance(11 * time.Minute)
	}
	if calls != 5 {
		t.Fatalf("calls %d, want 5 (no retry)", calls)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id = $1 AND polling_blocked_at IS NOT NULL`, id); n != 1 {
		t.Fatal("the polling_blocked_at column is not set")
	}
	if got := revisionOf(t, admin, 3, id); got != revBefore+1 {
		t.Fatalf("block revision %d, want %d", got, revBefore+1)
	}
	if n := len(e.published("source-controller.event.polling_blocked.pub")); n != 1 {
		t.Fatalf("polling_blocked events %d", n)
	}
	if n := e.pollCycle(ctx); n != 0 || calls != 5 {
		t.Fatalf("a blocked poller was polled: %d polls %d calls", n, calls)
	}
	blockedAt := e.view(3, id)["polling"].(map[string]any)["polling_blocked_at"]

	// Restart: a new instance over the same schema keeps the block.
	r2 := newPGEnv(t, openStore(t, role, defaultSchema), e.clock, mocks.NewBroker())
	view := r2.view(3, id)
	polling := view["polling"].(map[string]any)
	if asInt(polling["consecutive_failures"]) != 5 || polling["polling_blocked_at"] != blockedAt || view["last_poll"].(map[string]any)["error_code"] != "http_error" {
		t.Fatalf("after restart polling %v last %v", polling, view["last_poll"])
	}
	if n := r2.pollCycle(ctx); n != 0 {
		t.Fatalf("restarted instance polled a blocked Source: %d", n)
	}
	staleTag := r2.etagOf(3, id)

	// An implementation update keeps the block and counter.
	status, body, h := r2.request("ui", "sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0"}, broker.Header{"If-Match": staleTag})
	if status != 200 {
		t.Fatalf("implementation update: %d %v", status, body)
	}
	newKey := body["api_key"].(string)
	polling = r2.view(3, id)["polling"].(map[string]any)
	if asInt(polling["consecutive_failures"]) != 5 || polling["polling_blocked_at"] != blockedAt {
		t.Fatalf("implementation update cleared the block: %v", polling)
	}
	if st, b, _ := r2.request("ui", "sources-unblock-polling", 3, map[string]any{"id": id}, nil, broker.Header{"If-Match": staleTag}); st != 412 || b["code"] != "stale_revision" {
		t.Fatalf("stale unblock: %d %v", st, b)
	}
	current := h["ETag"]
	if st, b, uh := r2.request("ui", "sources-unblock-polling", 3, map[string]any{"id": id}, nil, broker.Header{"If-Match": current}); st != 200 || uh["ETag"] == current {
		t.Fatalf("current unblock: %d %v %v", st, b, uh)
	}
	polling = r2.view(3, id)["polling"].(map[string]any)
	if asInt(polling["consecutive_failures"]) != 0 || polling["polling_blocked_at"] != nil || polling["next_poll_at"] != nil {
		t.Fatalf("after unblock while awaiting the schema: %v", polling)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id = $1 AND polling_blocked_at IS NULL`, id); n != 1 {
		t.Fatal("the polling_blocked_at column is still set")
	}
	saved := r2.clock.Now()
	rec := r2.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", newKey, `{"contract_version":"source.v1","implementation_version":"2.0.0","fields":[]}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"settings_status":"complete"`) {
		t.Fatalf("new schema: %d %s", rec.Code, rec.Body.String())
	}
	next, err := time.Parse(time.RFC3339Nano, r2.view(3, id)["polling"].(map[string]any)["next_poll_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if d := next.Sub(saved); d < 10*time.Minute-time.Second || d > 10*time.Minute+time.Second {
		t.Fatalf("next poll %s after completion, want one ten-minute interval", d)
	}
	if n := r2.pollCycle(ctx); n != 0 || calls != 5 {
		t.Fatalf("unblock made an immediate call: %d polls %d calls", n, calls)
	}
}

// M250: 100 row-locked Sources whose polls expired first cannot starve the
// recovery of an independent expired poll or delay an independent due poll.
func TestV0048LockedExpiredPollsDoNotStarveRecoveryIntegration(t *testing.T) {
	ctx := context.Background()
	admin, _, e := freshEnv(t)
	e.addTeam(3, "acme")
	var ids []int64
	for i := 0; i < 101; i++ {
		ids = append(ids, e.pollerSource(3, "fair-source-"+strconv.Itoa(i)))
	}
	later := ids[100]
	started := 0
	for i := 0; i < 3 && started < 101; i++ {
		polls, err := e.svc.StartDuePolls(ctx)
		if err != nil {
			t.Fatal(err)
		}
		started += len(polls)
	}
	if started != 101 {
		t.Fatalf("started %d polls, want 101", started)
	}
	var laterPoll int64
	if err := admin.QueryRowContext(ctx, `SELECT poll_id FROM `+defaultSchema+`.polls WHERE team_id = 3 AND source_id = $1`, later).Scan(&laterPoll); err != nil {
		t.Fatal(err)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND poll_id < $1`, laterPoll); n != 100 {
		t.Fatalf("polls ordered before the later candidate %d, want 100", n)
	}
	lockConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close()
	lockTx, err := lockConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback() }()
	locked := lockRows(t, lockTx, ids[:100])
	if locked != 100 {
		t.Fatalf("locked %d Source rows, want 100", locked)
	}
	e.clock.Advance(2 * time.Minute) // past every expires_at

	recoverCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := e.svc.FinalizeExpiredPolls(recoverCtx); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND poll_id = $1 AND finished_at IS NOT NULL`, laterPoll); n != 1 {
		t.Fatal("the independent expired poll was not recovered while 100 predecessors were locked")
	}
	if n := count(t, admin, `SELECT count(*) FROM `+defaultSchema+`.polls WHERE team_id = 3 AND poll_id < $1 AND finished_at IS NULL`, laterPoll); n != 100 {
		t.Fatalf("locked predecessors still unfinished %d, want 100", n)
	}
	e.clock.Advance(11 * time.Minute)
	begin := time.Now()
	leaseCtx, cancelLease := context.WithTimeout(ctx, 3*time.Second)
	defer cancelLease()
	due, err := e.svc.StartDuePolls(leaseCtx)
	if err != nil {
		t.Fatalf("independent due poll: %v", err)
	}
	if elapsed := time.Since(begin); elapsed >= 2500*time.Millisecond {
		t.Fatalf("independent due poll took %s", elapsed)
	}
	if len(due) != 1 || due[0].SourceID != later {
		t.Fatalf("due polls %+v, want only Source %d", due, later)
	}
	var still int
	if err := lockTx.QueryRowContext(ctx, `SELECT count(*) FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id = ANY($1)`, int64s(ids[:100])).Scan(&still); err != nil || still != 100 {
		t.Fatalf("lock transaction ended early: %d %v", still, err)
	}
}

func lockRows(t *testing.T, tx *sql.Tx, ids []int64) int {
	t.Helper()
	rows, err := tx.QueryContext(context.Background(), `SELECT source_id FROM `+defaultSchema+`.sources WHERE team_id = 3 AND source_id = ANY($1) ORDER BY source_id FOR UPDATE`, int64s(ids))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	return n
}

// int64s renders ids as a PostgreSQL bigint array literal.
func int64s(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return "{" + strings.Join(parts, ",") + "}"
}
