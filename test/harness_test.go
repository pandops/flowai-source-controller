//go:build !integration

package test

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/internal/core"
	"github.com/pandops/flowai-source-controller/internal/mocks"
	"github.com/pandops/flowai-source-controller/schemas"
)

const (
	publicBase   = "https://flowai.example"
	teamRegistry = "team-registry.service.get.pub"
)

// serviceDir is the module root, found from this file's location.
func serviceDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}

type envOptions struct {
	callWindow  time.Duration
	startWindow time.Duration
	keys        int
	logs        io.Writer
}

type option func(*envOptions)

func withCallWindow(d time.Duration) option  { return func(o *envOptions) { o.callWindow = d } }
func withStartWindow(d time.Duration) option { return func(o *envOptions) { o.startWindow = d } }
func withSigningKeys(n int) option           { return func(o *envOptions) { o.keys = n } }
func withLogs(w io.Writer) option            { return func(o *envOptions) { o.logs = w } }

// env is one source-controller core over in-process fakes: an in-memory
// store, a fake broker, a fake clock and an HTTPS poller.
type env struct {
	t       *testing.T
	clock   *mocks.Clock
	store   *mocks.MemStore
	broker  *mocks.Broker
	svc     *core.Service
	http    http.Handler
	poller  *httptest.Server
	pubKeys map[string]ed25519.PublicKey

	mu         sync.Mutex
	teams      map[int64]map[string]any
	pollFn     func(w http.ResponseWriter, r *http.Request, call pollCall)
	pollCalls  []pollCall
	requestSeq int64
	pubSeq     int64
}

// pollCall is one received poll callback.
type pollCall struct {
	Body      map[string]any
	Raw       []byte
	Timestamp string
	Signature string
	URL       string
}

func newEnv(t *testing.T, opts ...option) *env {
	t.Helper()
	o := envOptions{keys: 1, logs: io.Discard}
	for _, opt := range opts {
		opt(&o)
	}
	e := &env{t: t, clock: &mocks.Clock{}, broker: mocks.NewBroker(), teams: map[int64]map[string]any{}, pubKeys: map[string]ed25519.PublicKey{}}
	e.store = mocks.NewMemStore(e.clock)
	e.poller = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		call := pollCall{Raw: raw, Timestamp: r.Header.Get("X-FlowAI-Poll-Timestamp"), Signature: r.Header.Get("X-FlowAI-Poll-Signature"), URL: "https://" + r.Host + r.URL.RequestURI()}
		_ = json.Unmarshal(raw, &call.Body)
		e.mu.Lock()
		e.pollCalls = append(e.pollCalls, call)
		fn := e.pollFn
		e.mu.Unlock()
		if fn == nil {
			writeJSON(w, 200, map[string]any{"tasks": []any{}, "next_cursor": nil})
			return
		}
		fn(w, r, call)
	}))
	t.Cleanup(e.poller.Close)
	var keys []core.SigningKey
	for i := 1; i <= o.keys; i++ {
		seed := make([]byte, ed25519.SeedSize)
		seed[0] = byte(i)
		private := ed25519.NewKeyFromSeed(seed)
		id := "k" + strconv.Itoa(i)
		keys = append(keys, core.SigningKey{ID: id, Private: private})
		e.pubKeys[id] = private.Public().(ed25519.PublicKey)
	}
	e.broker.Responders[teamRegistry] = e.teamRegistry
	svc, err := core.New(core.Config{
		PublicBaseURL:      publicBase,
		PollAllowedOrigins: []string{e.poller.URL},
		PollSigningKeys:    keys,
		PollClient:         e.poller.Client(),
		CursorKeys:         []core.CursorKey{{ID: 1, Secret: []byte("0123456789abcdef0123456789abcdef")}},
		PollCallWindow:     o.callWindow,
		PollStartWindow:    o.startWindow,
		Now:                e.clock.Now,
		Logger:             slog.New(slog.NewJSONHandler(o.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}, e.store, e.broker)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = svc
	e.http = svc.HTTPHandler()
	return e
}

func (e *env) teamRegistry(r broker.Request) (broker.Reply, error) {
	var req struct {
		Params struct {
			TeamID int64 `json:"team_id"`
		} `json:"params"`
	}
	_ = json.Unmarshal(r.Data, &req)
	e.mu.Lock()
	team, ok := e.teams[req.Params.TeamID]
	e.mu.Unlock()
	if !ok {
		return jsonReply(404, map[string]any{"code": "canonical_team_not_found", "message": "not found", "request_id": 1}), nil
	}
	return jsonReply(200, team), nil
}

func jsonReply(status int, v any) broker.Reply {
	raw, _ := json.Marshal(v)
	return broker.Reply{Header: broker.Header{"status": strconv.Itoa(status)}, Data: raw}
}

// addTeam registers a team in team-registry (fake) and in the projection.
func (e *env) addTeam(id int64, name string) {
	e.registryTeam(id, name)
	e.store.PutTeam(core.Team{TeamID: id, TeamName: name})
}

// registryTeam registers a team only in the fake team-registry.
func (e *env) registryTeam(id int64, name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.teams[id] = map[string]any{"team_id": id, "team_name": name, "default_image": "registry.example/agent@sha256:" + strings.Repeat("a", 64), "ingested_at": "2026-11-01T10:00:00.000Z", "archived_at": nil}
}

func (e *env) setPoll(fn func(w http.ResponseWriter, r *http.Request, call pollCall)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pollFn = fn
}

func (e *env) calls() []pollCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]pollCall(nil), e.pollCalls...)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// reply is a parsed request/reply answer.
type reply struct {
	Status int
	Header broker.Header
	Raw    []byte
	Body   map[string]any
}

func (r reply) code() string {
	c, _ := r.Body["code"].(string)
	return c
}

func parseReply(t *testing.T, rep broker.Reply) reply {
	t.Helper()
	status, err := strconv.Atoi(rep.Header["status"])
	if err != nil {
		t.Fatalf("reply without status header: %v", rep.Header)
	}
	out := reply{Status: status, Header: rep.Header, Raw: rep.Data}
	if len(rep.Data) > 0 {
		decoder := json.NewDecoder(strings.NewReader(string(rep.Data)))
		decoder.UseNumber()
		if err := decoder.Decode(&out.Body); err != nil {
			t.Fatalf("reply body is not a JSON object: %s", rep.Data)
		}
	}
	return out
}

func (e *env) nextRequestID() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.requestSeq++
	return 7000 + e.requestSeq
}

// uiRequest builds a ui request as API Gateway sends it.
func (e *env) uiRequest(op string, team int64, params map[string]any, body any) broker.Request {
	envelope := map[string]any{"params": params}
	if params == nil {
		envelope["params"] = map[string]any{}
	}
	if body != nil {
		envelope["body"] = body
	}
	raw, _ := json.Marshal(envelope)
	return broker.Request{
		Subject: "source-controller.ui." + op + ".sub",
		Header:  broker.Header{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(e.nextRequestID(), 10), "operator_id": "501", "team_name": "display"},
		Data:    raw,
	}
}

func (e *env) do(r broker.Request) reply {
	e.t.Helper()
	return parseReply(e.t, e.svc.HandleRequest(context.Background(), r))
}

// ui sends one ui operation for team as operator 501.
func (e *env) ui(op string, team int64, params map[string]any, body any) reply {
	e.t.Helper()
	return e.do(e.uiRequest(op, team, params, body))
}

// uiIfMatch sends one ui operation with an If-Match header.
func (e *env) uiIfMatch(op string, team int64, params map[string]any, body any, ifMatch string) reply {
	e.t.Helper()
	r := e.uiRequest(op, team, params, body)
	r.Header["If-Match"] = ifMatch
	return e.do(r)
}

// service sends one flow-registry service request.
func (e *env) service(op string, team int64, body any) reply {
	e.t.Helper()
	raw, _ := json.Marshal(map[string]any{"params": map[string]any{}, "body": body})
	return e.do(broker.Request{
		Subject: "source-controller.service." + op + ".sub",
		Header:  broker.Header{"team": strconv.FormatInt(team, 10), "request_id": strconv.FormatInt(e.nextRequestID(), 10)},
		Data:    raw,
	})
}

func createBody(name, mode string) map[string]any {
	return map[string]any{"name": name, "mode": mode, "contract_version": "source.v1", "implementation_version": "1.0.0", "filter_summary": "open issues"}
}

// createSource creates a Source and returns its id and one-time key.
func (e *env) createSource(team int64, name, mode string) (int64, string) {
	e.t.Helper()
	r := e.ui("sources-create", team, nil, createBody(name, mode))
	if r.Status != 201 {
		e.t.Fatalf("create %s: %d %s", name, r.Status, r.Raw)
	}
	source := r.Body["source"].(map[string]any)
	id, err := source["id"].(json.Number).Int64()
	if err != nil {
		e.t.Fatalf("source id is not an integer: %v", source["id"])
	}
	return id, r.Body["api_key"].(string)
}

// httpDo sends one request to the HTTP edge.
func (e *env) httpDo(method, path, key, contentType, body string, headers ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	req := httptest.NewRequest(method, "https://flowai.example"+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	e.http.ServeHTTP(rec, req)
	return rec
}

func field(name, typ string, required bool, def any) map[string]any {
	return map[string]any{"name": name, "label": strings.ToUpper(name[:1]) + name[1:], "type": typ, "required": required, "default": def}
}

// registerSchema registers a settings schema with a Source key.
func (e *env) registerSchema(key, version string, fields ...map[string]any) *httptest.ResponseRecorder {
	e.t.Helper()
	if fields == nil {
		fields = []map[string]any{}
	}
	raw, _ := json.Marshal(map[string]any{"contract_version": "source.v1", "implementation_version": version, "fields": fields})
	return e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key, "application/json", string(raw))
}

// settingsGet reads a Source's settings and returns the reply and revision.
func (e *env) settingsGet(team, id int64) (reply, string) {
	e.t.Helper()
	r := e.ui("sources-settings-get", team, map[string]any{"id": id}, nil)
	return r, r.Header["ETag"]
}

// saveSettings writes settings with the current ETag.
func (e *env) saveSettings(team, id int64, body map[string]any) reply {
	e.t.Helper()
	_, etag := e.settingsGet(team, id)
	return e.uiIfMatch("sources-settings-put", team, map[string]any{"id": id}, body, etag)
}

// webhookSource creates a webhook Source with complete settings.
func (e *env) webhookSource(team int64, name string) (int64, string) {
	e.t.Helper()
	id, key := e.createSource(team, name, "webhook")
	if rec := e.registerSchema(key, "1.0.0"); rec.Code != 200 {
		e.t.Fatalf("register schema: %d %s", rec.Code, rec.Body.String())
	}
	return id, key
}

// pollerSource creates a poller Source with complete settings pointing at
// the fake poller, due immediately.
func (e *env) pollerSource(team int64, name string) (int64, string) {
	e.t.Helper()
	id, key := e.createSource(team, name, "poller")
	if rec := e.registerSchema(key, "1.0.0", field("project", "string", true, "OPS")); rec.Code != 200 {
		e.t.Fatalf("register schema: %d %s", rec.Code, rec.Body.String())
	}
	r := e.saveSettings(team, id, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 10})
	if r.Status != 200 || r.Body["settings_status"] != "complete" {
		e.t.Fatalf("poller settings: %d %s", r.Status, r.Raw)
	}
	e.makeDue(team, id)
	return id, key
}

// makeDue moves a Source's next poll time into the past.
func (e *env) makeDue(team, id int64) {
	s := e.store.Source(team, id)
	past := e.clock.Now().Add(-time.Second)
	s.NextPollAt = &past
	e.store.PutSource(*s)
}

// webhook posts one webhook request.
func (e *env) webhook(teamName, sourceName, key, body string, batch bool, headers ...string) *httptest.ResponseRecorder {
	e.t.Helper()
	suffix := "/task"
	if batch {
		suffix = "/tasks"
	}
	return e.httpDo(http.MethodPost, "/webhook/"+teamName+"/"+sourceName+suffix, key, "application/json", body, headers...)
}

func task(externalID string) map[string]any {
	return map[string]any{"external_id": externalID, "payload": map[string]any{"title": "Fix " + externalID}}
}

func mustJSON(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		t.Fatalf("not a JSON object: %s", raw)
	}
	return out
}

// flush publishes the outbox through the fake broker.
func (e *env) flush() {
	e.t.Helper()
	if _, err := e.svc.FlushOutbox(context.Background()); err != nil {
		e.t.Fatalf("flush outbox: %v", err)
	}
}

// messages returns acknowledged messages whose subject starts with prefix,
// after validating every one against its schema.
func (e *env) messages(prefix string) []broker.Message {
	e.t.Helper()
	if e.broker.Connected() {
		e.flush()
	}
	var out []broker.Message
	for _, m := range e.broker.Published() {
		validateMessage(e.t, m)
		if strings.HasPrefix(m.Subject, prefix) {
			out = append(out, m)
		}
	}
	return out
}

// intake returns the published intake messages.
func (e *env) intake() []broker.Message { return e.messages("source-controller.task.") }

func validateMessage(t *testing.T, m broker.Message) {
	t.Helper()
	name, err := schemas.ForSubject(m.Subject)
	if err != nil {
		t.Fatalf("published on an unknown subject %s: %v", m.Subject, err)
	}
	if err := schemas.Validate(name, m.Data); err != nil {
		t.Fatalf("%s violates %s: %v\n%s", m.Subject, name, err, m.Data)
	}
	body := decode(t, m.Data)
	if m.Header["team"] != fmt.Sprint(body["team_id"]) {
		t.Fatalf("team header %q differs from team_id %v", m.Header["team"], body["team_id"])
	}
	if m.Header["Nats-Msg-Id"] != "source-controller-"+fmt.Sprint(body["publisher_seq"]) {
		t.Fatalf("Nats-Msg-Id %q does not match publisher_seq %v", m.Header["Nats-Msg-Id"], body["publisher_seq"])
	}
}

// event delivers one consumer message.
func (e *env) event(subject string, team int64, payload map[string]any) broker.Disposition {
	e.t.Helper()
	raw, _ := json.Marshal(payload)
	header := broker.Header{"team": strconv.FormatInt(team, 10)}
	// team-registry events carry the header of the administrator request
	// that caused them (team-registry spec).
	if strings.HasPrefix(subject, "team-registry.event.") {
		header["team"] = "admin"
	}
	if seq, ok := payload["publisher_seq"]; ok {
		header["Nats-Msg-Id"] = fmt.Sprint(payload["publisher"]) + "-" + fmt.Sprint(seq)
	}
	return e.svc.HandleEvent(context.Background(), broker.Delivery{Subject: subject, Header: header, Data: raw})
}

func (e *env) nextPubSeq() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pubSeq++
	return 100 + e.pubSeq
}

// teamEvent builds a team-registry team event.
func (e *env) teamEvent(kind string, team int64, name string, archived bool) map[string]any {
	var archivedAt any
	if archived {
		archivedAt = "2026-11-07T12:00:00.000Z"
	}
	return map[string]any{"publisher": "team-registry", "publisher_seq": e.nextPubSeq(), "kind": kind, "occurred_at": "2026-11-07T12:00:00.000Z",
		"team_id": team, "team_name": name, "default_image": "registry.example/agent@sha256:" + strings.Repeat("b", 64), "ingested_at": "2026-11-01T10:00:00.000Z", "archived_at": archivedAt}
}

func (e *env) applyTeamEvent(kind string, team int64, name string, archived bool) {
	e.t.Helper()
	if got := e.event("team-registry.event."+kind+".sub", team, e.teamEvent(kind, team, name, archived)); got != broker.Ack {
		e.t.Fatalf("%s not acknowledged: %v", kind, got)
	}
}

// intakeProcessed builds a task-manager intake_processed event.
func (e *env) intakeProcessed(team, source int64, outcome string) map[string]any {
	var reason any
	if outcome == "rejected" {
		reason = "invalid_source_task"
	}
	return map[string]any{"publisher": "task-manager", "publisher_seq": e.nextPubSeq(), "team_id": team, "occurred_at": "2026-11-07T12:00:00.000Z",
		"kind": "intake_processed", "intake_publisher_seq": 9, "intake_id": 1, "source_id": source, "external_id": "ISSUE-1",
		"outcome": outcome, "reason": reason, "task_ids": []any{}, "deduplicated_flow_ids": []any{}}
}

// pollOnce starts every due poll and executes it.
func (e *env) pollOnce() []core.Poll {
	e.t.Helper()
	polls, err := e.svc.StartDuePolls(context.Background())
	if err != nil {
		e.t.Fatalf("start polls: %v", err)
	}
	for _, p := range polls {
		if err := e.svc.ExecutePoll(context.Background(), p); err != nil {
			e.t.Fatalf("execute poll: %v", err)
		}
	}
	e.flush()
	return polls
}

// pages answers poll callbacks with a scripted page sequence.
func pages(t *testing.T, script ...func(page int) (int, any)) func(w http.ResponseWriter, r *http.Request, call pollCall) {
	var mu sync.Mutex
	n := 0
	return func(w http.ResponseWriter, r *http.Request, call pollCall) {
		mu.Lock()
		i := n
		n++
		mu.Unlock()
		if i >= len(script) {
			t.Errorf("unexpected poll call %d", i+1)
			writeJSON(w, 500, map[string]any{})
			return
		}
		status, body := script[i](i + 1)
		writeJSON(w, status, body)
	}
}

// page returns a scripted page of n tasks with ids prefix-1..n.
func page(prefix string, n int, cursor any) func(int) (int, any) {
	return func(int) (int, any) {
		tasks := make([]any, n)
		for i := range tasks {
			tasks[i] = task(fmt.Sprintf("%s-%d", prefix, i+1))
		}
		return 200, map[string]any{"tasks": tasks, "next_cursor": cursor}
	}
}

func num(v any) int64 {
	switch n := v.(type) {
	case json.Number:
		i, _ := n.Int64()
		return i
	case float64:
		return int64(n)
	}
	return -1
}

func sourceView(t *testing.T, e *env, team, id int64) map[string]any {
	t.Helper()
	r := e.ui("sources-get", team, map[string]any{"id": id}, nil)
	if r.Status != 200 {
		t.Fatalf("sources-get %d/%d: %d %s", team, id, r.Status, r.Raw)
	}
	return r.Body
}

func readFile(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(serviceDir(), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// verifySignature checks a poll callback signature with the served keys.
func (e *env) verifySignature(call pollCall) bool {
	parts := strings.Split(call.Signature, ",")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "keyid=") || !strings.HasPrefix(parts[1], "sig=") {
		return false
	}
	key, ok := e.pubKeys[strings.TrimPrefix(parts[0], "keyid=")]
	sig, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(parts[1], "sig="))
	if !ok || err != nil {
		return false
	}
	return ed25519.Verify(key, append([]byte(call.Timestamp+"."+call.URL+"."), call.Raw...), sig)
}

// privateKey returns the deterministic signing key number i of newEnv.
func privateKey(i int) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = byte(i)
	return ed25519.NewKeyFromSeed(seed)
}

func assertSchema(t *testing.T, name string, raw []byte) {
	t.Helper()
	if err := schemas.Validate(name, raw); err != nil {
		t.Fatalf("%s: %v\n%s", name, err, raw)
	}
}

func sourceViewRaw(t *testing.T, e *env, team, id int64) []byte {
	t.Helper()
	return e.ui("sources-get", team, map[string]any{"id": id}, nil).Raw
}

func statusBody(activity string) map[string]any {
	return map[string]any{"activity": activity, "observed_at": "2026-11-07T12:00:00Z", "error_class": nil, "counters": map[string]any{"seen": 3, "submitted": 2, "failed": 0}}
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
