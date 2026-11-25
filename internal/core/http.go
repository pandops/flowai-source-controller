package core

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pandops/flowai-source-controller/internal/broker"
)

const (
	maxWebhookBytes = 1 << 20
	maxSelfBytes    = 64 << 10
)

var legacyHeaders = []string{"X-FlowAI-Team-Id", "X-FlowAI-Source-System-Id", "X-FlowAI-Listener-Identity", "X-FlowAI-Plugin-Instance-Id"}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		_, _ = w.Write(mustJSON(body))
	}
}

func writeErr(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_, _ = w.Write(errorBody(e, randomID()))
}

// HTTPHandler serves the public edge (webhooks, Source self routes) and the
// probes.
func (s *Service) HTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/v1/livez" && r.Method == http.MethodGet:
			writeJSON(w, 200, map[string]any{"status": "ok"})
		case path == "/v1/readyz" && r.Method == http.MethodGet:
			if err := s.ready(r.Context()); err != nil {
				writeJSON(w, 503, map[string]any{"status": "unavailable"})
				return
			}
			writeJSON(w, 200, map[string]any{"status": "ok"})
		case strings.HasPrefix(path, "/webhook/"):
			parts := strings.Split(strings.TrimPrefix(path, "/webhook/"), "/")
			if len(parts) != 3 || r.Method != http.MethodPost || parts[0] == "" || parts[1] == "" || (parts[2] != "task" && parts[2] != "tasks") {
				writeErr(w, errNotFound)
				return
			}
			s.webhook(w, r, parts[0], parts[1], parts[2] == "tasks")
		case path == "/v1/sources/self" && r.Method == http.MethodGet:
			s.selfGet(w, r)
		case path == "/v1/sources/self/settings" && r.Method == http.MethodGet:
			s.selfSettings(w, r)
		case path == "/v1/sources/self/settings-schema" && r.Method == http.MethodPut:
			s.selfSchema(w, r)
		case path == "/v1/sources/self/status" && r.Method == http.MethodPut:
			s.selfStatus(w, r)
		case path == "/v1/sources/poll-signing-keys" && r.Method == http.MethodGet:
			s.signingKeys(w, r)
		default:
			writeErr(w, errNotFound)
		}
	})
}

func (s *Service) ready(ctx context.Context) error {
	if !s.broker.Connected() {
		return errBrokerDown
	}
	pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s.store.Ping(pctx); err != nil {
		return err
	}
	for _, check := range s.cfg.ReadyChecks {
		if err := check(pctx); err != nil {
			return err
		}
	}
	return nil
}

func isJSON(r *http.Request) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && mt == "application/json"
}

// readBody reads at most limit bytes; ok is false when the body is larger.
func readBody(r *http.Request, limit int64) ([]byte, bool, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, false, err
	}
	return raw, int64(len(raw)) <= limit, nil
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
}

// authenticate resolves a Source key to its enabled Source and team.
func (s *Service) authenticate(ctx context.Context, key string) (*Source, *Team, error) {
	digest := keyDigest(key)
	if digest == nil {
		return nil, nil, errInvalidKey
	}
	var src *Source
	var team *Team
	err := s.read(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.SourceByDigest(digest, false); err != nil || src == nil {
			return err
		}
		team, err = tx.Team(src.TeamID)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if src == nil || !src.Enabled || team == nil {
		return nil, nil, errInvalidKey
	}
	return src, team, nil
}

// self authenticates a Source self request; archived teams are refused.
func (s *Service) self(w http.ResponseWriter, r *http.Request) (*Source, *Team, bool) {
	src, team, err := s.authenticate(r.Context(), bearer(r))
	if err == nil && team.ArchivedAt != nil {
		err = errInvalidKey
	}
	if err != nil {
		writeErr(w, asAPI(err))
		return nil, nil, false
	}
	return src, team, true
}

func (s *Service) webhook(w http.ResponseWriter, r *http.Request, teamName, sourceName string, batch bool) {
	ctx := r.Context()
	if !isJSON(r) {
		writeErr(w, errf(415, "unsupported_media_type", "the body must be application/json"))
		return
	}
	for _, h := range legacyHeaders {
		if r.Header.Get(h) != "" {
			writeErr(w, errInvalid)
			return
		}
	}
	raw, ok, err := readBody(r, maxWebhookBytes)
	if err != nil {
		writeErr(w, errInvalid)
		return
	}
	if !ok {
		writeErr(w, errf(413, "payload_too_large", "the body is too large"))
		return
	}
	var tasks []sourceTask
	var bad []string
	if batch {
		var obj map[string]json.RawMessage
		if decodeStrict(raw, &obj) != nil || obj == nil {
			writeErr(w, invalidFields("/"))
			return
		}
		for k := range obj {
			if k != "tasks" {
				bad = append(bad, "/"+k)
			}
		}
		list, errs := parseTaskList(obj["tasks"], "/tasks", 1, 100)
		tasks, bad = list, append(bad, errs...)
	} else {
		t, errs := parseTask(raw, "")
		tasks, bad = []sourceTask{t}, errs
	}
	if len(bad) > 0 {
		writeErr(w, invalidFields(bad...))
		return
	}
	src, team, err := s.authenticate(ctx, bearer(r))
	if err == nil && (!strings.EqualFold(team.TeamName, teamName) || !strings.EqualFold(src.Name, sourceName) || src.Mode != "webhook") {
		err = errInvalidKey
	}
	if err != nil {
		writeErr(w, asAPI(err))
		return
	}
	n := len(tasks)
	reject := func(code string, e *apiError) {
		s.afterWebhook(ctx, src, func(tx Tx, cur *Source) error {
			if code == "intake_unavailable" {
				cur.Counts.Rejected += int64(n)
			}
			return s.event(tx, cur.TeamID, cur.ID, "webhook_rejected", map[string]any{"error_code": code, "task_count": n})
		})
		writeErr(w, e)
	}
	if team.ArchivedAt != nil {
		reject("team_archived", errf(409, "team_archived", "the team is archived"))
		return
	}
	if src.SettingsStatus != "complete" {
		reject("source_settings_incomplete", errf(409, "source_settings_incomplete", "the Source settings are incomplete"))
		return
	}
	seq, err := s.store.NextPublisherSeq(ctx)
	if err != nil {
		writeErr(w, errf(503, "intake_unavailable", "intake is unavailable"))
		return
	}
	ids, err := s.store.NextIntakeIDs(ctx, src.TeamID, n)
	if err != nil {
		writeErr(w, errf(503, "intake_unavailable", "intake is unavailable"))
		return
	}
	now := s.cfg.Now()
	entries := make([]any, n)
	receipts := make([]any, n)
	for i, t := range tasks {
		entries[i] = intakeEntry(t, ids[i])
		receipts[i] = map[string]any{"external_id": t.ExternalID, "intake_id": ids[i]}
	}
	msg := map[string]any{"publisher": Publisher, "publisher_seq": seq, "team_id": src.TeamID, "occurred_at": ts(now), "source_id": src.ID,
		"origin": "webhook", "implementation_version": src.ImplementationVersion, "accepted_at": ts(now), "tasks": entries}
	pctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
	err = s.broker.Publish(pctx, broker.Message{Subject: "source-controller.task." + itoa(src.TeamID) + ".pub",
		Header: broker.Header{"team": itoa(src.TeamID), "Nats-Msg-Id": Publisher + "-" + itoa(seq)}, Data: mustJSON(msg)})
	cancel()
	if err != nil {
		reject("intake_unavailable", errf(503, "intake_unavailable", "intake is unavailable"))
		return
	}
	s.afterWebhook(ctx, src, func(tx Tx, cur *Source) error {
		cur.Counts.Published += int64(n)
		return s.event(tx, cur.TeamID, cur.ID, "webhook_accepted", map[string]any{"task_count": n, "intake_ids": ids, "intake_publisher_seq": seq})
	})
	if batch {
		writeJSON(w, 202, map[string]any{"tasks": receipts})
		return
	}
	writeJSON(w, 202, receipts[0])
}

// afterWebhook records counts and the webhook event after the reply is
// decided; a failure keeps the reply and is logged.
func (s *Service) afterWebhook(ctx context.Context, src *Source, fn func(tx Tx, cur *Source) error) {
	err := s.store.Tx(context.WithoutCancel(ctx), func(tx Tx) error {
		cur, err := tx.Source(src.TeamID, src.ID, true)
		if err != nil || cur == nil {
			return err
		}
		if err := fn(tx, cur); err != nil {
			return err
		}
		return tx.UpdateSource(cur)
	})
	if err != nil {
		s.log.Error("webhook follow-up not stored", "team_id", src.TeamID, "source_id", src.ID)
	}
}

func (s *Service) selfGet(w http.ResponseWriter, r *http.Request) {
	src, _, ok := s.self(w, r)
	if !ok {
		return
	}
	writeJSON(w, 200, map[string]any{"source_id": src.ID, "name": src.Name, "mode": src.Mode, "contract_version": src.ContractVersion,
		"implementation_version": src.ImplementationVersion, "settings_status": src.SettingsStatus, "revision": src.Revision,
		"schema_revision": intOrNil(src.SchemaRevision)})
}

func (s *Service) selfSettings(w http.ResponseWriter, r *http.Request) {
	src, team, ok := s.self(w, r)
	if !ok {
		return
	}
	if src.Mode != "webhook" {
		writeErr(w, errf(403, "not_webhook_source", "only webhook Sources read their settings"))
		return
	}
	writeJSON(w, 200, map[string]any{"values": effectiveValues(src), "webhook_url": s.webhookURL(src, team.TeamName), "revision": src.Revision,
		"settings_status": src.SettingsStatus})
}

var (
	fieldNameRegexp = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	reservedFields  = map[string]bool{"flow_id": true, "flow_ids": true, "source_id": true, "source_ids": true, "team_id": true, "team_name": true,
		"poll_url": true, "poll_interval_minutes": true, "webhook_url": true, "api_key": true, "external_id": true, "project_id": true, "environment_id": true}
)

// parseFields validates a registered settings schema.
func parseFields(raw json.RawMessage) ([]Field, []string) {
	var items []map[string]json.RawMessage
	if err := decodeStrict(raw, &items); err != nil || items == nil || len(items) > 50 {
		return nil, []string{"/fields"}
	}
	var bad []string
	fields := make([]Field, 0, len(items))
	seen := map[string]bool{}
	for i, item := range items {
		p := "/fields/" + strconv.Itoa(i)
		var f Field
		for k := range item {
			switch k {
			case "name", "label", "description", "type", "required", "default":
			default:
				bad = append(bad, p+"/"+k)
			}
		}
		if json.Unmarshal(item["name"], &f.Name) != nil || !fieldNameRegexp.MatchString(f.Name) || reservedFields[f.Name] || seen[f.Name] {
			bad = append(bad, p+"/name")
		}
		seen[f.Name] = true
		if json.Unmarshal(item["label"], &f.Label) != nil || f.Label == "" || len([]rune(f.Label)) > 100 {
			bad = append(bad, p+"/label")
		}
		if d, ok := item["description"]; ok {
			var desc string
			if isNull(d) || json.Unmarshal(d, &desc) != nil || len([]rune(desc)) > 200 {
				bad = append(bad, p+"/description")
			} else {
				f.Description = &desc
			}
		}
		if json.Unmarshal(item["type"], &f.Type) != nil || (f.Type != "string" && f.Type != "integer" && f.Type != "number" && f.Type != "boolean") {
			bad = append(bad, p+"/type")
		}
		var req bool
		if json.Unmarshal(item["required"], &req) != nil {
			bad = append(bad, p+"/required")
		}
		f.Required = &req
		def, ok := item["default"]
		if !ok || !validValue(f.Type, def) {
			bad = append(bad, p+"/default")
		}
		if !isNull(def) {
			f.Default = def
		}
		fields = append(fields, f)
	}
	return fields, bad
}

func sameSchema(a, b []Field) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return string(mustJSON(a)) == string(mustJSON(b))
}

func (s *Service) selfSchema(w http.ResponseWriter, r *http.Request) {
	src, _, ok := s.self(w, r)
	if !ok {
		return
	}
	if !isJSON(r) {
		writeErr(w, errf(415, "unsupported_media_type", "the body must be application/json"))
		return
	}
	raw, ok, err := readBody(r, maxSelfBytes)
	if err != nil || !ok {
		writeErr(w, errTooLarge)
		return
	}
	var body map[string]json.RawMessage
	if decodeStrict(raw, &body) != nil || body == nil {
		writeErr(w, errInvalid)
		return
	}
	var bad []string
	for k := range body {
		if k != "contract_version" && k != "implementation_version" && k != "fields" {
			bad = append(bad, "/"+k)
		}
	}
	var contract, version string
	if json.Unmarshal(body["contract_version"], &contract) != nil || contract != "source.v1" {
		bad = append(bad, "/contract_version")
	}
	if json.Unmarshal(body["implementation_version"], &version) != nil || version != src.ImplementationVersion {
		bad = append(bad, "/implementation_version")
	}
	fields, errs := parseFields(body["fields"])
	if bad = append(bad, errs...); len(bad) > 0 {
		writeErr(w, invalidFields(bad...))
		return
	}
	var cur *Source
	err = s.store.Tx(r.Context(), func(tx Tx) error {
		var err error
		if cur, err = tx.Source(src.TeamID, src.ID, true); err != nil {
			return err
		}
		if cur == nil || !cur.Enabled {
			return errInvalidKey
		}
		if sameSchema(cur.Schema, fields) {
			return nil
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		types := map[string]string{}
		for _, f := range fields {
			types[f.Name] = f.Type
		}
		prior := cur.Schema
		if prior == nil {
			prior = cur.PreviousSchema
		}
		priorTypes := map[string]string{}
		for _, f := range prior {
			priorTypes[f.Name] = f.Type
		}
		kept := map[string]json.RawMessage{}
		for name, v := range cur.Values {
			t, ok := types[name]
			if was, known := priorTypes[name]; known && was != t {
				ok = false
			}
			if ok && validValue(t, v) {
				kept[name] = v
			}
		}
		before := effectiveValues(cur)
		rev := 1
		if cur.SchemaRevision != nil {
			rev = *cur.SchemaRevision + 1
		}
		cur.PreviousSchema, cur.Schema, cur.SchemaRevision, cur.Values = cur.Schema, fields, &rev, kept
		cur.Revision++
		if !sameValues(before, effectiveValues(cur)) {
			cur.Generation++
			cur.Cursor = nil
		}
		applyStatus(cur, now, false)
		if err := tx.UpdateSource(cur); err != nil {
			return err
		}
		if err := tx.AppendSchemaRevision(cur.TeamID, cur.ID, rev, version, fields); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(cur.TeamID, cur.ID, cur.Revision, snapshot(cur)); err != nil {
			return err
		}
		return s.event(tx, cur.TeamID, cur.ID, "schema_registered", map[string]any{"schema_revision": rev, "implementation_version": version,
			"settings_status": cur.SettingsStatus, "revision": cur.Revision, "field_count": len(fields)})
	})
	if err != nil {
		writeErr(w, asAPI(err))
		return
	}
	writeJSON(w, 200, map[string]any{"settings_status": cur.SettingsStatus, "schema_revision": intOrNil(cur.SchemaRevision), "revision": cur.Revision})
}

var (
	activities   = map[string]bool{"idle": true, "polling": true, "ingesting": true, "degraded": true, "error": true}
	errorClasses = map[string]bool{"upstream_unavailable": true, "downstream_unavailable": true, "rate_limited": true, "mapping_error": true,
		"invalid_configuration": true, "contract_error": true}
)

func (s *Service) selfStatus(w http.ResponseWriter, r *http.Request) {
	src, _, ok := s.self(w, r)
	if !ok {
		return
	}
	if !isJSON(r) {
		writeErr(w, errf(415, "unsupported_media_type", "the body must be application/json"))
		return
	}
	raw, ok, err := readBody(r, maxSelfBytes)
	if err != nil || !ok {
		writeErr(w, errTooLarge)
		return
	}
	var body map[string]json.RawMessage
	if decodeStrict(raw, &body) != nil || body == nil {
		writeErr(w, errInvalid)
		return
	}
	var bad []string
	for k := range body {
		if k != "activity" && k != "observed_at" && k != "error_class" && k != "counters" {
			bad = append(bad, "/"+k)
		}
	}
	var activity, observedAt string
	if json.Unmarshal(body["activity"], &activity) != nil || !activities[activity] {
		bad = append(bad, "/activity")
	}
	if json.Unmarshal(body["observed_at"], &observedAt) != nil {
		bad = append(bad, "/observed_at")
	} else if _, err := time.Parse(time.RFC3339Nano, observedAt); err != nil {
		bad = append(bad, "/observed_at")
	}
	var errorClass *string
	if rawClass, ok := body["error_class"]; ok && !isNull(rawClass) {
		var c string
		if json.Unmarshal(rawClass, &c) != nil || !errorClasses[c] {
			bad = append(bad, "/error_class")
		} else {
			errorClass = &c
		}
	}
	var counters json.RawMessage
	if rawCounters, ok := body["counters"]; ok && !isNull(rawCounters) {
		var m map[string]json.Number
		if decodeStrict(rawCounters, &m) != nil || len(m) > 20 {
			bad = append(bad, "/counters")
		} else {
			counters = rawCounters
		}
	}
	if len(bad) > 0 {
		writeErr(w, invalidFields(bad...))
		return
	}
	err = s.store.Tx(r.Context(), func(tx Tx) error {
		cur, err := tx.Source(src.TeamID, src.ID, true)
		if err != nil {
			return err
		}
		if cur == nil || !cur.Enabled {
			return errInvalidKey
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		cur.LastSeenAt, cur.Activity, cur.ErrorClass, cur.ObservedCounters = &now, &activity, errorClass, counters
		if err := tx.UpdateSource(cur); err != nil {
			return err
		}
		return s.event(tx, cur.TeamID, cur.ID, "source_status_reported", map[string]any{"activity": activity, "observed_at": observedAt,
			"error_class": strOrNil(errorClass)})
	})
	if err != nil {
		writeErr(w, asAPI(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) signingKeys(w http.ResponseWriter, r *http.Request) {
	src, _, ok := s.self(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit := 50
	if v := q.Get("limit"); v != "" || q.Has("limit") {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeErr(w, invalidFields("/limit"))
			return
		}
		limit = n
	}
	start := 0
	if q.Has("cursor") {
		c, ok := s.openCursor(q.Get("cursor"), "poll-signing-keys", src.TeamID)
		if !ok || c.ID > int64(len(s.cfg.PollSigningKeys)) {
			writeErr(w, invalidFields("/cursor"))
			return
		}
		start = int(c.ID)
	}
	keys := []any{}
	end := start + limit
	if end > len(s.cfg.PollSigningKeys) {
		end = len(s.cfg.PollSigningKeys)
	}
	for _, k := range s.cfg.PollSigningKeys[start:end] {
		keys = append(keys, map[string]any{"key_id": k.ID, "algorithm": "Ed25519",
			"public_key": base64.RawURLEncoding.EncodeToString(k.Private.Public().(ed25519.PublicKey)), "not_after": nil})
	}
	var next any
	if end < len(s.cfg.PollSigningKeys) {
		next = s.signCursor(cursorState{Op: "poll-signing-keys", Team: src.TeamID, ID: int64(end)})
	}
	writeJSON(w, 200, map[string]any{"keys": keys, "next_cursor": next})
}
