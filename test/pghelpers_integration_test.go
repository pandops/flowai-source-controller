//go:build integration

package test

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/core"
	"github.com/pandops/flowai-source-controller/internal/mocks"
	"github.com/pandops/flowai-source-controller/internal/postgres"
	"github.com/pandops/flowai-source-controller/schemas"
)

const defaultSchema = "source_controller_schema"

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func sqlState(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

// pgRole is a login role created the way the deployment creates the
// service role: no superuser, no CREATE on the database.
type pgRole struct {
	name, password, dbName string
	db                     *sql.DB
}

func (r pgRole) dsn() string { return shared.roleDSN(r.name, r.password, r.dbName) }

// serviceRole creates a login role; with ownSchema the deployment's
// pre-created schema owned by the role is reproduced. With createOnDB the
// role may create schemas in the database.
func serviceRole(t *testing.T, admin *sql.DB, dbName, schema string, ownSchema, createOnDB bool) pgRole {
	t.Helper()
	suffix, _ := randomToken(4)
	r := pgRole{name: "sc_role_" + suffix, dbName: dbName}
	r.password, _ = randomToken(12)
	mustExec(t, admin, `REVOKE CREATE ON DATABASE "`+dbName+`" FROM PUBLIC`)
	mustExec(t, admin, `REVOKE ALL ON SCHEMA public FROM PUBLIC`)
	mustExec(t, admin, `CREATE ROLE `+r.name+` LOGIN PASSWORD '`+r.password+`' NOSUPERUSER NOCREATEDB NOCREATEROLE`)
	mustExec(t, admin, `GRANT CONNECT ON DATABASE "`+dbName+`" TO `+r.name)
	if createOnDB {
		mustExec(t, admin, `GRANT CREATE ON DATABASE "`+dbName+`" TO `+r.name)
	}
	if ownSchema {
		mustExec(t, admin, `CREATE SCHEMA `+schema+` AUTHORIZATION `+r.name)
	}
	var err error
	r.db, err = sql.Open("pgx", r.dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = r.db.Close()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.ExecContext(c, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1`, r.name)
		_, _ = admin.ExecContext(c, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		_, _ = admin.ExecContext(c, `DROP OWNED BY `+r.name+` CASCADE`)
		if _, err := admin.ExecContext(c, `DROP ROLE `+r.name); err != nil {
			t.Errorf("cleanup: drop role %s: %v", r.name, err)
		}
	})
	return r
}

// openStore opens and migrates source-controller's store as role.
func openStore(t *testing.T, r pgRole, schema string) *postgres.Store {
	t.Helper()
	s, err := postgres.Open(context.Background(), r.dsn(), schema)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate %s: %v", schema, err)
	}
	return s
}

// pgEnv is a core service over the real store with a fake broker, a fake
// clock and an HTTPS poller.
type pgEnv struct {
	t      *testing.T
	clock  *mocks.Clock
	store  *postgres.Store
	broker *mocks.Broker
	svc    *core.Service
	http   http.Handler
	poller *httptest.Server

	mu     sync.Mutex
	pollFn http.HandlerFunc
	reqSeq int64
	pubSeq int64
}

// newPGEnv builds a service over st. A shared clock and broker let a
// "restarted" instance continue the first one's timeline.
func newPGEnv(t *testing.T, st *postgres.Store, clock *mocks.Clock, b *mocks.Broker) *pgEnv {
	t.Helper()
	if clock == nil {
		clock = &mocks.Clock{}
	}
	if b == nil {
		b = mocks.NewBroker()
	}
	st.SetClock(clock.Now)
	e := &pgEnv{t: t, clock: clock, store: st, broker: b}
	e.poller = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		e.mu.Lock()
		fn := e.pollFn
		e.mu.Unlock()
		if fn == nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tasks":[],"next_cursor":null}`))
			return
		}
		fn(w, r)
	}))
	t.Cleanup(e.poller.Close)
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 1
	b.Responders["team-registry.service.get.pub"] = func(r broker.Request) (broker.Reply, error) {
		return broker.Reply{Header: broker.Header{"status": "404"}, Data: []byte(`{"code":"canonical_team_not_found","message":"not found","request_id":1}`)}, nil
	}
	svc, err := core.New(core.Config{
		PublicBaseURL:      "https://flowai.example",
		PollAllowedOrigins: []string{e.poller.URL},
		PollSigningKeys:    []core.SigningKey{{ID: "k1", Private: ed25519.NewKeyFromSeed(seed)}},
		PollClient:         e.poller.Client(),
		CursorKeys:         []core.CursorKey{{ID: 1, Secret: []byte("0123456789abcdef0123456789abcdef")}},
		Now:                clock.Now,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st, b)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = svc
	e.http = svc.HTTPHandler()
	return e
}

// addTeam stores a team projection row the way the team_created event does.
func (e *pgEnv) addTeam(id int64, name string) {
	e.t.Helper()
	e.mu.Lock()
	e.pubSeq++
	seq := e.pubSeq
	e.mu.Unlock()
	ev := map[string]any{"publisher": "team-registry", "publisher_seq": 100 + seq, "kind": "team_created", "occurred_at": "2026-11-07T12:00:00.000Z",
		"team_id": id, "team_name": name, "default_image": "registry.example/agent@sha256:" + strings.Repeat("b", 64), "ingested_at": "2026-11-01T10:00:00.000Z", "archived_at": nil}
	raw, _ := json.Marshal(ev)
	d := e.svc.HandleEvent(context.Background(), broker.Delivery{Subject: "team-registry.event.team_created.sub",
		Header: broker.Header{"team": "admin", "Nats-Msg-Id": fmt.Sprintf("team-registry-%d", 100+seq)}, Data: raw})
	if d != broker.Ack {
		e.t.Fatalf("team_created not applied: %v", d)
	}
}

func (e *pgEnv) ui(op string, team int64, params map[string]any, body any) (int, map[string]any, broker.Header) {
	e.t.Helper()
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
	rep := e.svc.HandleRequest(context.Background(), broker.Request{Subject: "source-controller.ui." + op + ".sub",
		Header: broker.Header{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(rid, 10), "operator_id": "501", "team_name": "display"}, Data: raw})
	status, _ := strconv.Atoi(rep.Header["status"])
	var out map[string]any
	if len(rep.Data) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(rep.Data)))
		dec.UseNumber()
		_ = dec.Decode(&out)
	}
	return status, out, rep.Header
}

// createSource creates a Source and returns its id and key.
func (e *pgEnv) createSource(team int64, name, mode string) (int64, string, error) {
	status, body, _ := e.ui("sources-create", team, nil, map[string]any{"name": name, "mode": mode, "contract_version": "source.v1", "implementation_version": "1.0.0", "filter_summary": "open issues"})
	if status != 201 {
		return 0, "", fmt.Errorf("create %s: %d %v", name, status, body)
	}
	id, _ := body["source"].(map[string]any)["id"].(json.Number).Int64()
	return id, body["api_key"].(string), nil
}

func (e *pgEnv) httpDo(method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "https://flowai.example"+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.http.ServeHTTP(rec, req)
	return rec
}

// pollerSource creates a complete poller Source that is due now.
func (e *pgEnv) pollerSource(team int64, name string) int64 {
	e.t.Helper()
	id, key, err := e.createSource(team, name, "poller")
	if err != nil {
		e.t.Fatal(err)
	}
	if rec := e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key, `{"contract_version":"source.v1","implementation_version":"1.0.0","fields":[]}`); rec.Code != 200 {
		e.t.Fatalf("schema: %d %s", rec.Code, rec.Body.String())
	}
	_, _, h := e.ui("sources-settings-get", team, map[string]any{"id": id}, nil)
	raw, _ := json.Marshal(map[string]any{"params": map[string]any{"id": id}, "body": map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 10}})
	rep := e.svc.HandleRequest(context.Background(), broker.Request{Subject: "source-controller.ui.sources-settings-put.sub",
		Header: broker.Header{"team": strconv.FormatInt(team, 10), "request_id": "6999", "operator_id": "501", "If-Match": h["ETag"]}, Data: raw})
	if rep.Header["status"] != "200" {
		e.t.Fatalf("settings: %s %s", rep.Header["status"], rep.Data)
	}
	// Due on the next cycle.
	e.clock.Advance(11 * time.Minute)
	return id
}

func (e *pgEnv) setPoll(fn http.HandlerFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pollFn = fn
}

func (e *pgEnv) flush() {
	e.t.Helper()
	if _, err := e.svc.FlushOutbox(context.Background()); err != nil {
		e.t.Fatalf("flush: %v", err)
	}
}

// published returns acknowledged messages with prefix, schema-checked.
func (e *pgEnv) published(prefix string) []broker.Message {
	e.t.Helper()
	var out []broker.Message
	for _, m := range e.broker.Published() {
		name, err := schemas.ForSubject(m.Subject)
		if err != nil {
			e.t.Fatalf("unknown subject %s", m.Subject)
		}
		if err := schemas.Validate(name, m.Data); err != nil {
			e.t.Fatalf("%s violates %s: %v", m.Subject, name, err)
		}
		if strings.HasPrefix(m.Subject, prefix) {
			out = append(out, m)
		}
	}
	return out
}

func decodeMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		t.Fatalf("not JSON: %s", raw)
	}
	return out
}

func asInt(v any) int64 {
	if n, ok := v.(json.Number); ok {
		i, _ := n.Int64()
		return i
	}
	return -1
}

func count(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// request sends one ui or service request with extra headers.
func (e *pgEnv) request(kind, op string, team int64, params map[string]any, body any, extra broker.Header) (int, map[string]any, broker.Header) {
	e.t.Helper()
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
	h := broker.Header{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(rid, 10)}
	if kind == "ui" {
		h["operator_id"] = "501"
	}
	for k, v := range extra {
		h[k] = v
	}
	rep := e.svc.HandleRequest(context.Background(), broker.Request{Subject: "source-controller." + kind + "." + op + ".sub", Header: h, Data: raw})
	status, _ := strconv.Atoi(rep.Header["status"])
	var out map[string]any
	if len(rep.Data) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(rep.Data)))
		dec.UseNumber()
		_ = dec.Decode(&out)
	}
	return status, out, rep.Header
}

// etagOf returns the current settings ETag of a Source.
func (e *pgEnv) etagOf(team, id int64) string {
	e.t.Helper()
	_, _, h := e.ui("sources-settings-get", team, map[string]any{"id": id}, nil)
	return h["ETag"]
}

// registerSchema registers fields with a Source key.
func (e *pgEnv) registerSchema(key string, fields string) {
	e.t.Helper()
	if rec := e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key, `{"contract_version":"source.v1","implementation_version":"1.0.0","fields":`+fields+`}`); rec.Code != 200 {
		e.t.Fatalf("schema: %d %s", rec.Code, rec.Body.String())
	}
}

// webhookSource creates a webhook Source with complete settings.
func (e *pgEnv) webhookSource(team int64, name string) (int64, string) {
	e.t.Helper()
	id, key, err := e.createSource(team, name, "webhook")
	if err != nil {
		e.t.Fatal(err)
	}
	e.registerSchema(key, `[]`)
	return id, key
}

// webhook posts one webhook request.
func (e *pgEnv) webhook(teamName, sourceName, key, body string, batch bool) *httptest.ResponseRecorder {
	suffix := "/task"
	if batch {
		suffix = "/tasks"
	}
	return e.httpDo(http.MethodPost, "/webhook/"+teamName+"/"+sourceName+suffix, key, body)
}

// deliver applies one consumer message.
func (e *pgEnv) deliver(subject string, team int64, payload map[string]any) broker.Disposition {
	raw, _ := json.Marshal(payload)
	header := broker.Header{"team": strconv.FormatInt(team, 10), "Nats-Msg-Id": fmt.Sprint(payload["publisher"]) + "-" + fmt.Sprint(payload["publisher_seq"])}
	// team-registry events carry the administrator request's `team: admin`.
	if strings.HasPrefix(subject, "team-registry.event.") {
		header["team"] = "admin"
	}
	return e.svc.HandleEvent(context.Background(), broker.Delivery{Subject: subject, Header: header, Data: raw})
}

func intakeProcessed(seq, team, source int64, outcome string) map[string]any {
	return map[string]any{"publisher": "task-manager", "publisher_seq": seq, "team_id": team, "occurred_at": "2026-11-07T12:00:00.000Z",
		"kind": "intake_processed", "intake_publisher_seq": 9, "intake_id": 1, "source_id": source, "external_id": "ISSUE-1",
		"outcome": outcome, "reason": nil, "task_ids": []any{}, "deduplicated_flow_ids": []any{}}
}

func teamEvent(seq int64, kind string, team int64, name string, archived bool) map[string]any {
	var at any
	if archived {
		at = "2026-11-07T12:00:00.000Z"
	}
	return map[string]any{"publisher": "team-registry", "publisher_seq": seq, "kind": kind, "occurred_at": "2026-11-07T12:00:00.000Z",
		"team_id": team, "team_name": name, "default_image": "registry.example/agent@sha256:" + strings.Repeat("b", 64), "ingested_at": "2026-11-01T10:00:00.000Z", "archived_at": at}
}

// view reads one Source view as team.
func (e *pgEnv) view(team, id int64) map[string]any {
	e.t.Helper()
	status, body, _ := e.ui("sources-get", team, map[string]any{"id": id}, nil)
	if status != 200 {
		e.t.Fatalf("sources-get %d/%d: %d %v", team, id, status, body)
	}
	return body
}

// revisionOf reads a stored Source revision as the admin.
func revisionOf(t *testing.T, db *sql.DB, team, id int64) int64 {
	t.Helper()
	var rev int64
	if err := db.QueryRowContext(context.Background(), `SELECT (doc->>'Revision')::bigint FROM `+defaultSchema+`.sources WHERE team_id = $1 AND source_id = $2`, team, id).Scan(&rev); err != nil {
		t.Fatalf("revision of %d/%d: %v", team, id, err)
	}
	return rev
}

// freshEnv is one migrated database, its admin connection and a service.
func freshEnv(t *testing.T) (*sql.DB, pgRole, *pgEnv) {
	t.Helper()
	admin, dbName := freshDB(t)
	role := serviceRole(t, admin, dbName, defaultSchema, true, false)
	return admin, role, newPGEnv(t, openStore(t, role, defaultSchema), nil, nil)
}

func fmtInt(v int64) string { return strconv.FormatInt(v, 10) }
