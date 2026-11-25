//go:build !integration

package test

// Survivors of State Registry, Webhook Receiver and Source Dispatcher cases
// that v0048 deleted (decision ledger docs/testing/v0048-test-decisions.csv).
// Each test names the ledger rows it re-homes.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

func expect(t *testing.T, label string, r reply, status int, code string) {
	t.Helper()
	if r.Status != status || (code != "" && r.code() != code) {
		t.Fatalf("%s: %d %s, want %d %s", label, r.Status, r.Raw, status, code)
	}
}

func expectHTTP(t *testing.T, label string, rec interface {
	Result() *http.Response
}, status int, code string,
) map[string]any {
	t.Helper()
	res := rec.Result()
	raw := new(bytes.Buffer)
	_, _ = raw.ReadFrom(res.Body)
	if res.StatusCode != status {
		t.Fatalf("%s: %d %s, want %d %s", label, res.StatusCode, raw, status, code)
	}
	if raw.Len() == 0 {
		return nil
	}
	body := decode(t, raw.Bytes())
	if code != "" && body["code"] != code {
		t.Fatalf("%s: code %v, want %s (%s)", label, body["code"], code, raw)
	}
	return body
}

func sourceCount(t *testing.T, e *env, team int64) int {
	t.Helper()
	r := e.ui("sources-list", team, nil, nil)
	expect(t, "sources-list", r, 200, "")
	return len(r.Body["items"].([]any))
}

func fieldValue(t *testing.T, settings reply, name string) (any, bool) {
	t.Helper()
	for _, f := range settings.Body["fields"].([]any) {
		m := f.(map[string]any)
		if m["name"] == name {
			v, ok := m["value"]
			return v, ok
		}
	}
	return nil, false
}

// M135: a case-variant field key, a type outside the closed enum and a null
// field description are each 400 before the schema is stored.
func TestV0048SchemaFieldSpellingAndTypesAreClosed(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "closed-schema", "webhook")
	base := map[string]any{"name": "project", "label": "Project", "type": "string", "required": true, "default": nil}
	with := func(extra map[string]any) map[string]any {
		f := map[string]any{}
		for k, v := range base {
			f[k] = v
		}
		for k, v := range extra {
			f[k] = v
		}
		return f
	}
	for label, f := range map[string]map[string]any{
		"upper-case key":   with(map[string]any{"NAME": "overridden"}),
		"credential type":  with(map[string]any{"type": "credential"}),
		"null description": with(map[string]any{"description": nil}),
	} {
		expectHTTP(t, label, e.registerSchema(key, "1.0.0", f), 400, "invalid_request")
	}
	if n := len(e.store.SchemaRevisions(3, id)); n != 0 {
		t.Fatalf("rejected schemas stored %d revisions", n)
	}
	if s := e.store.Source(3, id); s.Schema != nil || s.SettingsStatus != "awaiting_schema" || s.Revision != 1 {
		t.Fatalf("rejected schemas changed the Source: schema %v status %s revision %d", s.Schema, s.SettingsStatus, s.Revision)
	}
	expectHTTP(t, "canonical schema", e.registerSchema(key, "1.0.0", base), 200, "")
}

// M136: an integer default 1.0000000000000001 (a float decoder rounds it to
// 1) and a create body with an invalid UTF-8 byte are each 400 before
// anything is stored.
func TestV0048IntegerDefaultsStayExactAndBodiesMustBeUTF8(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "exact-default", "webhook")
	inexact := `{"contract_version":"source.v1","implementation_version":"1.0.0","fields":[{"name":"count","label":"Count","type":"integer","required":true,"default":1.0000000000000001}]}`
	expectHTTP(t, "inexact integer default", e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key, "application/json", inexact), 400, "invalid_request")
	if n := len(e.store.SchemaRevisions(3, id)); n != 0 {
		t.Fatalf("inexact default stored %d schema revisions", n)
	}
	r := e.uiRequest("sources-create", 3, nil, nil)
	r.Data = append(append([]byte(`{"params":{},"body":{"name":"utf8-source","mode":"webhook","contract_version":"source.v1","implementation_version":"1","filter_summary":"","description":"`), 0xff), []byte(`"}}`)...)
	expect(t, "invalid UTF-8 create", e.do(r), 400, "invalid_request")
	if n := sourceCount(t, e, 3); n != 1 {
		t.Fatalf("invalid UTF-8 create stored a Source: %d Sources", n)
	}
	// A schema body with an invalid UTF-8 byte is refused the same way.
	invalid := append(append([]byte(`{"contract_version":"source.v1","implementation_version":"1.0.0","fields":[{"name":"count","label":"C`), 0xff), []byte(`","type":"integer","required":true,"default":1}]}`)...)
	expectHTTP(t, "invalid UTF-8 schema", e.httpDo(http.MethodPut, "/v1/sources/self/settings-schema", key, "application/json", string(invalid)), 400, "invalid_request")
	if n := len(e.store.SchemaRevisions(3, id)); n != 0 {
		t.Fatalf("invalid UTF-8 schema stored %d schema revisions", n)
	}
}

// M139: implementation_version is bounded by 64 Unicode characters, not
// bytes: 64 two-byte runes are accepted and returned, 65 are rejected before
// the store.
func TestV0048ImplementationVersionAllows64UnicodeCharacters(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.webhookSource(3, "unicode-version")
	version := strings.Repeat("я", 64)
	_, etag := e.settingsGet(3, id)
	changed := e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": version}, etag)
	expect(t, "64 runes", changed, 200, "")
	if got := changed.Body["source"].(map[string]any)["implementation_version"]; got != version {
		t.Fatalf("implementation_version %v", got)
	}
	_, etag = e.settingsGet(3, id)
	expect(t, "65 runes", e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": version + "я"}, etag), 400, "invalid_request")
	if s := e.store.Source(3, id); s.ImplementationVersion != version {
		t.Fatalf("stored version %q, want the 64-rune version", s.ImplementationVersion)
	}
}

// M237: the superseded default_image Source input is refused with 400 and
// creates nothing, while the same body without it is created.
func TestV0048SourceCreateRejectsDefaultImage(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	body := createBody("image-source", "webhook")
	body["default_image"] = map[string]any{"repository": "registry.invalid/removed", "digest": "sha256:" + strings.Repeat("c", 64)}
	expect(t, "default_image", e.ui("sources-create", 3, nil, body), 400, "invalid_request")
	if n := sourceCount(t, e, 3); n != 0 {
		t.Fatalf("default_image create stored %d Sources", n)
	}
	expect(t, "same body without default_image", e.ui("sources-create", 3, nil, createBody("image-source", "webhook")), 201, "")
}

// M134: Source create rejects a null description, a case-variant key, a
// caller team_id and duplicate JSON keys with 400 before creation.
func TestV0048SourceCreateRejectsNoncanonicalBodies(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	with := func(name string, extra map[string]any) map[string]any {
		b := createBody(name, "webhook")
		for k, v := range extra {
			b[k] = v
		}
		return b
	}
	for label, body := range map[string]map[string]any{
		"null description":  with("closed-one", map[string]any{"description": nil}),
		"case variant key":  with("closed-two", map[string]any{"NAME": "override"}),
		"caller team_id":    with("closed-three", map[string]any{"team_id": 3}),
		"null name":         with("closed-four", map[string]any{"name": nil}),
		"null filter field": with("closed-five", map[string]any{"filter_summary": nil}),
	} {
		expect(t, label, e.ui("sources-create", 3, nil, body), 400, "invalid_request")
	}
	dup := e.uiRequest("sources-create", 3, nil, nil)
	dup.Data = []byte(`{"params":{},"body":{"name":"dup-one","name":"dup-two","mode":"webhook","contract_version":"source.v1","implementation_version":"1","filter_summary":""}}`)
	expect(t, "duplicate key", e.do(dup), 400, "invalid_request")
	if n := sourceCount(t, e, 3); n != 0 {
		t.Fatalf("rejected bodies created %d Sources", n)
	}
	expect(t, "canonical", e.ui("sources-create", 3, nil, createBody("closed-ok", "webhook")), 201, "")
}

// M245: an unconfigured poller saved with its poll URL and the five-minute
// form default stores interval 5 and schedules the first poll one interval
// after the save.
func TestV0048UnconfiguredPollerFiveMinuteSaveSchedulesOneInterval(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "five-minutes", "poller")
	expectHTTP(t, "schema", e.registerSchema(key, "1.0.0"), 200, "")
	saved := e.clock.Now()
	r := e.saveSettings(3, id, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 5})
	expect(t, "save", r, 200, "")
	settings, _ := e.settingsGet(3, id)
	if got := num(settings.Body["platform"].(map[string]any)["poll_interval_minutes"]); got != 5 {
		t.Fatalf("platform.poll_interval_minutes %d, want 5", got)
	}
	next, err := time.Parse(time.RFC3339Nano, settings.Body["polling"].(map[string]any)["next_poll_at"].(string))
	if err != nil {
		t.Fatalf("next_poll_at: %v", err)
	}
	// The fake clock runs in real time, so the save commits a moment after
	// saved was read.
	if d := next.Sub(saved.Truncate(time.Millisecond)); d < 5*time.Minute || d > 5*time.Minute+time.Second {
		t.Fatalf("next_poll_at is %s after the save, want one five-minute interval", d)
	}
	if settings.Body["settings_status"] != "complete" {
		t.Fatalf("settings_status %v", settings.Body["settings_status"])
	}
}

// M268 and the baseline requirement "Operators update a source
// implementation without replacing its identity": a version change rotates
// the key, returns the Source to awaiting_schema with retained values, takes
// a schema only for the new version and is 412 on a stale revision; the same
// version is 400 without rotation and an unfinished poll is 409.
func TestV0048ImplementationUpdateRotatesKeyAndAwaitsTheNewSchema(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, oldKey := e.createSource(3, "event-source", "webhook")
	fields := func(label, countType string) []map[string]any {
		return []map[string]any{
			{"name": "project", "label": label, "type": "string", "required": false, "default": nil},
			{"name": "count", "label": "Count", "type": countType, "required": false, "default": nil},
		}
	}
	expectHTTP(t, "old schema", e.registerSchema(oldKey, "1.0.0", fields("Project", "integer")...), 200, "")
	expect(t, "configure", e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "retained-value", "count": 5}}), 200, "")
	expectHTTP(t, "accepted before the update", e.webhook("acme", "event-source", oldKey, mustJSON(task("before-version")), false), 202, "")
	_, beforeTag := e.settingsGet(3, id)
	generation := e.store.Source(3, id).Generation

	updated := e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0"}, beforeTag)
	expect(t, "version", updated, 200, "")
	view := updated.Body["source"].(map[string]any)
	if num(view["id"]) != id || view["implementation_version"] != "2.0.0" || view["settings_status"] != "awaiting_schema" || view["name"] != "event-source" || view["mode"] != "webhook" {
		t.Fatalf("updated source %v", view)
	}
	if updated.Header["Cache-Control"] != "no-store" || updated.Header["ETag"] == beforeTag {
		t.Fatalf("update headers %v", updated.Header)
	}
	newKey, _ := updated.Body["api_key"].(string)
	if newKey == "" || newKey == oldKey {
		t.Fatalf("version change key %q, want a new key", newKey)
	}
	if s := e.store.Source(3, id); s.Generation == generation || s.Cursor != nil {
		t.Fatalf("generation %d -> %d cursor %v", generation, s.Generation, s.Cursor)
	}
	expectHTTP(t, "old key", e.httpDo(http.MethodGet, "/v1/sources/self", oldKey, "", ""), 401, "invalid_source_key")
	expectHTTP(t, "intake waits for the new schema", e.webhook("acme", "event-source", newKey, mustJSON(task("during-update")), false), 409, "source_settings_incomplete")
	settings, _ := e.settingsGet(3, id)
	if fs := settings.Body["fields"].([]any); len(fs) != 0 || settings.Body["settings_status"] != "awaiting_schema" {
		t.Fatalf("settings while awaiting the new schema: %s", settings.Raw)
	}

	expectHTTP(t, "schema for the old version", e.registerSchema(newKey, "1.0.0", fields("New project label", "number")...), 400, "invalid_request")
	expectHTTP(t, "schema for the new version", e.registerSchema(newKey, "2.0.0", fields("New project label", "number")...), 200, "")
	settings, _ = e.settingsGet(3, id)
	if v, _ := fieldValue(t, settings, "project"); v != "retained-value" {
		t.Fatalf("same-name same-type value %v, want retained-value", v)
	}
	if v, _ := fieldValue(t, settings, "count"); v != nil {
		t.Fatalf("retyped field kept its old value %v", v)
	}
	expectHTTP(t, "accepted after the new schema", e.webhook("acme", "event-source", newKey, mustJSON(task("before-version")), false), 202, "")

	expect(t, "stale version", e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "3.0.0"}, beforeTag), 412, "stale_revision")
	_, etag := e.settingsGet(3, id)
	expect(t, "same version", e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0"}, etag), 400, "invalid_request")
	if s := e.store.Source(3, id); s.ImplementationVersion != "2.0.0" || s.SettingsStatus != "complete" {
		t.Fatalf("rejected updates changed the Source: %s %s", s.ImplementationVersion, s.SettingsStatus)
	}
	expectHTTP(t, "same-version update kept the key", e.httpDo(http.MethodGet, "/v1/sources/self", newKey, "", ""), 200, "")

	t.Run("unfinished poll", func(t *testing.T) {
		pid, pkey := e.pollerSource(3, "poller-busy")
		polls, err := e.svc.StartDuePolls(t.Context())
		if err != nil || len(polls) != 1 {
			t.Fatalf("start poll: %v %v", polls, err)
		}
		_, etag := e.settingsGet(3, pid)
		expect(t, "update during a poll", e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": pid}, map[string]any{"implementation_version": "2.0.0"}, etag), 409, "poll_in_progress")
		s := e.store.Source(3, pid)
		if s.ImplementationVersion != "1.0.0" || s.SettingsStatus != "complete" || s.Schema == nil {
			t.Fatalf("poll_in_progress changed the Source: %s %s", s.ImplementationVersion, s.SettingsStatus)
		}
		expectHTTP(t, "key kept", e.httpDo(http.MethodGet, "/v1/sources/self", pkey, "", ""), 200, "")
	})
}

// M278: while a replacement implementation has not registered its schema,
// an empty save without stored overrides is a no-op with the same ETag, an
// unknown value is 400, a stale ETag is 412, an empty save of a Source with
// a retained override clears it (new ETag, idempotent repeat), a control
// Source keeps its override, and the next registration yields the schema
// default for the cleared Source and the override for the control.
func TestV0048SchemaAbsentSavesDistinguishNoOpFromOverrideClearing(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	schema := func(key, version string) {
		t.Helper()
		expectHTTP(t, "schema "+version, e.registerSchema(key, version, field("project", "string", true, "schema-default")), 200, "")
	}
	put := func(id int64, values map[string]any, ifMatch string) reply {
		return e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": values}, ifMatch)
	}
	awaiting := func(label string, r reply) {
		t.Helper()
		if fs := r.Body["fields"].([]any); len(fs) != 0 || r.Body["settings_status"] != "awaiting_schema" {
			t.Fatalf("%s: %s, want no fields and awaiting_schema", label, r.Raw)
		}
	}
	first := func(r reply) any {
		fs := r.Body["fields"].([]any)
		if len(fs) == 0 {
			return "<no field>"
		}
		return fs[0].(map[string]any)["value"]
	}
	type src struct {
		id        int64
		key, etag string
	}
	var sources []src
	for _, name := range []string{"clear-target", "retain-control"} {
		id, key := e.createSource(3, name, "webhook")
		empty, emptyTag := e.settingsGet(3, id)
		awaiting(name+" initial", empty)
		noop := put(id, map[string]any{}, emptyTag)
		expect(t, name+" empty no-op", noop, 200, "")
		if noop.Header["ETag"] != emptyTag || string(noop.Raw) != string(empty.Raw) {
			t.Fatalf("%s: no-op changed settings: %s %s -> %s %s", name, emptyTag, empty.Raw, noop.Header["ETag"], noop.Raw)
		}
		expect(t, name+" unknown value", put(id, map[string]any{"unknown": "not registered"}, emptyTag), 400, "invalid_request")
		if after, _ := e.settingsGet(3, id); string(after.Raw) != string(empty.Raw) {
			t.Fatalf("%s: rejected save changed settings: %s", name, after.Raw)
		}
		schema(key, "1.0.0")
		_, registeredTag := e.settingsGet(3, id)
		saved := put(id, map[string]any{"project": "retained-override"}, registeredTag)
		expect(t, name+" override", saved, 200, "")
		if first(saved) != "retained-override" {
			t.Fatalf("%s: saved %s", name, saved.Raw)
		}
		version := e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "2.0.0"}, saved.Header["ETag"])
		expect(t, name+" version", version, 200, "")
		after, afterTag := e.settingsGet(3, id)
		awaiting(name+" after version change", after)
		if afterTag == saved.Header["ETag"] {
			t.Fatalf("%s: version change kept ETag %s", name, afterTag)
		}
		expect(t, name+" stale save", put(id, map[string]any{}, saved.Header["ETag"]), 412, "stale_revision")
		if _, tag := e.settingsGet(3, id); tag != afterTag {
			t.Fatalf("%s: stale save changed ETag %s -> %s", name, afterTag, tag)
		}
		sources = append(sources, src{id: id, key: version.Body["api_key"].(string), etag: afterTag})
	}
	target, control := sources[0], sources[1]
	clear := put(target.id, map[string]any{}, target.etag)
	expect(t, "clear retained override", clear, 200, "")
	if clear.Header["ETag"] == target.etag {
		t.Fatalf("clearing save kept ETag %s", target.etag)
	}
	awaiting("cleared", clear)
	repeated := put(target.id, map[string]any{}, clear.Header["ETag"])
	expect(t, "repeat clear", repeated, 200, "")
	if repeated.Header["ETag"] != clear.Header["ETag"] {
		t.Fatalf("repeated empty save changed ETag %s -> %s", clear.Header["ETag"], repeated.Header["ETag"])
	}
	if _, tag := e.settingsGet(3, control.id); tag != control.etag {
		t.Fatalf("control ETag changed %s -> %s", control.etag, tag)
	}
	for _, s := range sources {
		schema(s.key, "2.0.0")
	}
	if r, _ := e.settingsGet(3, target.id); first(r) != "schema-default" {
		t.Fatalf("cleared target value %v, want schema-default", first(r))
	}
	if r, _ := e.settingsGet(3, control.id); first(r) != "retained-override" {
		t.Fatalf("control value %v, want retained-override", first(r))
	}
}

// M288: a forbidden ownership field is named by its path and its value is
// never echoed.
func TestV0048WebhookNamesForbiddenFieldPathsWithoutValues(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "event-source")
	rec := e.webhook("acme", "event-source", key, `{"external_id":"forbidden","payload":{},"flow_id":"forbidden-value-canary"}`, false)
	body := expectHTTP(t, "forbidden field", rec, 400, "invalid_request")
	if got := body["fields"]; !reflect.DeepEqual(got, []any{"/flow_id"}) {
		t.Fatalf("fields %v, want [/flow_id]", got)
	}
	if strings.Contains(rec.Body.String(), "forbidden-value-canary") {
		t.Fatal("rejection echoed the forbidden value")
	}
	if n := len(e.intake()); n != 0 {
		t.Fatalf("forbidden field published %d intake messages", n)
	}
}

// M431 (rest): a duplicate nested JSON key and a missing Authorization
// header are refused and publish nothing.
func TestV0048WebhookRefusesDuplicateKeysAndMissingKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "event-source")
	for label, body := range map[string]string{
		"duplicate nested key":   `{"external_id":"a","payload":{"x":1,"x":2}}`,
		"duplicate top-level":    `{"external_id":"a","external_id":"b","payload":{}}`,
		"duplicate in batch":     `{"tasks":[{"external_id":"a","payload":{"y":{"z":1,"z":1}}}]}`,
		"duplicate tasks member": `{"tasks":[{"external_id":"a","payload":{}}],"tasks":[{"external_id":"b","payload":{}}]}`,
	} {
		expectHTTP(t, label, e.webhook("acme", "event-source", key, body, strings.HasPrefix(body, `{"tasks"`)), 400, "invalid_request")
	}
	expectHTTP(t, "missing key", e.webhook("acme", "event-source", "", `{"external_id":"a","payload":{}}`, false), 401, "invalid_source_key")
	if n := len(e.intake()); n != 0 {
		t.Fatalf("refused requests published %d intake messages", n)
	}
	expectHTTP(t, "control", e.webhook("acme", "event-source", key, `{"external_id":"a","payload":{"x":1}}`, false), 202, "")
}

// M289, M270 and M31 (response and log sweeps): neither the Source key, its
// digest, a setting value nor a rejected value appears in webhook responses,
// operator replies after creation, poll callbacks or logs, and the poll
// signing seed appears nowhere.
func TestV0048ResponsesCallbacksAndLogsExcludeSecretCanaries(t *testing.T) {
	var logs bytes.Buffer
	e := newEnv(t, withLogs(&logs))
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "canary-webhook", "webhook")
	expectHTTP(t, "schema", e.registerSchema(key, "1.0.0", field("project", "string", false, nil)), 200, "")
	const settingCanary = "setting-canary-3c1e9d"
	expect(t, "settings", e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": settingCanary}}), 200, "")
	const rejectedCanary = "provider-canary-8d2e4a17"
	const payloadSentinel = "opaque-positive-sentinel-55aa"
	responses := map[string]*bytes.Buffer{}
	record := func(label string, status int, body []byte, header http.Header) {
		buf := bytes.NewBuffer(append([]byte(nil), body...))
		for name, values := range header {
			buf.WriteString(name + ":" + strings.Join(values, ",") + "\n")
		}
		responses[label] = buf
		_ = status
	}
	rejected := e.webhook("acme", "canary-webhook", key, `{"external_id":"forbidden-secret","payload":{},"flow_id":"`+rejectedCanary+`"}`, false)
	accepted := e.webhook("acme", "canary-webhook", key, `{"external_id":"legitimate","payload":{"ordinary":"`+payloadSentinel+`"}}`, false)
	batch := e.webhook("acme", "canary-webhook", key, `{"tasks":[{"external_id":"b1","payload":{"ordinary":"`+payloadSentinel+`"}}]}`, true)
	wrongKey := e.webhook("acme", "canary-webhook", "tsk_v1_"+strings.Repeat("B", 43), `{"external_id":"x","payload":{"ordinary":"`+payloadSentinel+`"}}`, false)
	if rejected.Code != 400 || accepted.Code != 202 || batch.Code != 202 || wrongKey.Code != 401 {
		t.Fatalf("statuses %d %d %d %d", rejected.Code, accepted.Code, batch.Code, wrongKey.Code)
	}
	for label, rec := range map[string]*http.Response{"rejected": rejected.Result(), "accepted": accepted.Result(), "batch": batch.Result(), "wrong key": wrongKey.Result()} {
		raw := new(bytes.Buffer)
		_, _ = raw.ReadFrom(rec.Body)
		record("webhook "+label, rec.StatusCode, raw.Bytes(), rec.Header)
	}
	for _, op := range []string{"sources-get", "sources-settings-get"} {
		r := e.ui(op, 3, map[string]any{"id": id}, nil)
		h := http.Header{}
		for k, v := range r.Header {
			h.Set(k, v)
		}
		record(op, r.Status, r.Raw, h)
	}
	list := e.ui("sources-list", 3, nil, nil)
	record("sources-list", list.Status, list.Raw, nil)
	for _, path := range []string{"/v1/sources/self", "/v1/sources/self/settings"} {
		rec := e.httpDo(http.MethodGet, path, key, "", "")
		record(path, rec.Code, rec.Body.Bytes(), rec.Result().Header)
	}

	pid, pkey := e.pollerSource(3, "canary-poller")
	expect(t, "poller value", e.saveSettings(3, pid, map[string]any{"values": map[string]any{"project": settingCanary}}), 200, "")
	e.makeDue(3, pid)
	e.setPoll(pages(t, func(int) (int, any) {
		return 200, map[string]any{"tasks": []any{map[string]any{"external_id": "p1", "payload": map[string]any{"ordinary": payloadSentinel}}}, "next_cursor": nil}
	}))
	e.pollOnce()
	if n := len(e.intake()); n != 3 {
		t.Fatalf("intake messages %d, want 3", n)
	}
	calls := e.calls()
	if len(calls) != 1 {
		t.Fatalf("poll calls %d", len(calls))
	}
	digest := keyDigestOf(key)
	seed := privateKey(1).Seed()
	secrets := map[string]string{
		"webhook key": key, "poller key": pkey, "key digest hex": hex.EncodeToString(digest), "key digest base64": base64.StdEncoding.EncodeToString(digest),
		"rejected value": rejectedCanary, "seed raw url": base64.RawURLEncoding.EncodeToString(seed), "seed std": base64.StdEncoding.EncodeToString(seed),
		"seed hex": hex.EncodeToString(seed), "private key": base64.StdEncoding.EncodeToString(privateKey(1)),
	}
	for label, surface := range responses {
		for name, secret := range secrets {
			if strings.Contains(surface.String(), secret) {
				t.Fatalf("%s contains the %s", label, name)
			}
		}
	}
	callback := string(calls[0].Raw) + calls[0].Timestamp + calls[0].Signature
	for name, secret := range secrets {
		if strings.Contains(callback, secret) {
			t.Fatalf("poll callback contains the %s", name)
		}
	}
	if !strings.Contains(string(calls[0].Raw), settingCanary) {
		t.Fatal("control: the poll callback carries no effective setting value")
	}
	logSecrets := map[string]string{"setting value": settingCanary, "payload": payloadSentinel, "signature": strings.TrimPrefix(calls[0].Signature, "keyid=k1,sig=")}
	for name, secret := range secrets {
		logSecrets[name] = secret
	}
	for name, secret := range logSecrets {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("logs contain the %s", name)
		}
	}
	// The verifying public key is served; the private seed is not.
	if pub := privateKey(1).Public().(ed25519.PublicKey); !e.verifySignature(calls[0]) || len(pub) == 0 {
		t.Fatal("control: the callback signature does not verify")
	}
}

func keyDigestOf(key string) []byte {
	d := sha256.Sum256([]byte(key))
	return d[:]
}

// M26 and M29: a poll page with an unknown member, null tasks, a duplicate
// key, a duplicate nested payload key, a cursor over 1024 characters or a
// body over 1 MiB is invalid_response and publishes no task of that page.
func TestV0048PollPageRejectsAmbiguousOrUnboundedOutput(t *testing.T) {
	big := `{"tasks":[{"external_id":"a","payload":{"blob":"` + strings.Repeat("x", 1<<20) + `"}}],"next_cursor":null}`
	for label, body := range map[string]string{
		"unknown member":       `{"tasks":[],"next_cursor":null,"unknown":1}`,
		"null tasks":           `{"tasks":null,"next_cursor":null}`,
		"duplicate key":        `{"tasks":[],"next_cursor":null,"next_cursor":"hidden"}`,
		"duplicate payload":    `{"tasks":[{"external_id":"a","payload":{"x":1,"x":2}}],"next_cursor":null}`,
		"flow_id in a task":    `{"tasks":[{"external_id":"a","payload":{},"flow_id":"forbidden"}],"next_cursor":null}`,
		"cursor over 1024":     `{"tasks":[{"external_id":"a","payload":{}}],"next_cursor":"` + strings.Repeat("x", 1025) + `"}`,
		"body over 1 MiB":      big,
		"invalid UTF-8 member": "{\"tasks\":[{\"external_id\":\"a\xff\",\"payload\":{}}],\"next_cursor\":null}",
	} {
		t.Run(label, func(t *testing.T) {
			e := newEnv(t)
			e.addTeam(3, "acme")
			id, _ := e.pollerSource(3, "page-poller")
			e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			})
			e.pollOnce()
			last := sourceView(t, e, 3, id)["last_poll"].(map[string]any)
			if last["status"] != "failed" || last["error_code"] != "invalid_response" || num(last["published_tasks"]) != 0 {
				t.Fatalf("last_poll %v, want failed invalid_response with no task", last)
			}
			if n := len(e.intake()); n != 0 {
				t.Fatalf("invalid page published %d intake messages", n)
			}
			if len(e.calls()) != 1 {
				t.Fatalf("calls %d, want 1 (no retry)", len(e.calls()))
			}
		})
	}
	t.Run("cursor of exactly 1024 is accepted", func(t *testing.T) {
		e := newEnv(t)
		e.addTeam(3, "acme")
		id, _ := e.pollerSource(3, "page-poller")
		cursor := strings.Repeat("y", 1024)
		e.setPoll(pages(t, page("ok", 1, cursor), page("ok2", 0, nil)))
		e.pollOnce()
		if last := sourceView(t, e, 3, id)["last_poll"].(map[string]any); last["status"] != "succeeded" || num(last["pages_read"]) != 2 {
			t.Fatalf("last_poll %v", last)
		}
		if got := e.calls()[1].Body["cursor"]; got != cursor {
			t.Fatal("page 2 did not carry the 1024-character cursor")
		}
	})
}

// M23 (owner decision on D27, 2026-11-09): page 1 publishes its task and
// returns a cursor, page 2 fails with 503; the poll fails http_error after
// exactly two calls, the page-1 task stays published, the failure counts
// toward the five-failure block, and the stored cursor is the resume cursor
// of the last good page, which the next poll sends.
func TestV0048FailedLaterPageStoresTheLastGoodPageCursor(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "cursor-poller")
	s := e.store.Source(3, id)
	s.Cursor = ptrTo("stored-before")
	e.store.PutSource(*s)
	e.setPoll(pages(t, page("replayable", 1, "second"), func(int) (int, any) { return 503, map[string]any{} }))
	e.pollOnce()
	calls := e.calls()
	if len(calls) != 2 || calls[0].Body["cursor"] != "stored-before" || calls[1].Body["cursor"] != "second" {
		t.Fatalf("calls %d cursors %v", len(calls), cursorsOf(calls))
	}
	if n := len(e.intake()); n != 1 {
		t.Fatalf("intake messages %d, want the page-1 task", n)
	}
	last := sourceView(t, e, 3, id)["last_poll"].(map[string]any)
	if last["status"] != "failed" || last["error_code"] != "http_error" || num(last["pages_read"]) != 1 || num(last["published_tasks"]) != 1 {
		t.Fatalf("last_poll %v", last)
	}
	stored := e.store.Source(3, id)
	if got := stored.Cursor; got == nil || *got != "second" {
		t.Fatalf("stored cursor %v, want the last good page's cursor", got)
	}
	if stored.ConsecutiveFailures != 1 {
		t.Fatalf("consecutive failures %d, want the failed poll counted", stored.ConsecutiveFailures)
	}
	e.setPoll(nil)
	e.makeDue(3, id)
	e.pollOnce()
	if calls := e.calls(); len(calls) != 3 || calls[2].Body["cursor"] != "second" {
		t.Fatalf("next poll cursor %v", cursorsOf(calls))
	}
}

func cursorsOf(calls []pollCall) []any {
	out := []any{}
	for _, c := range calls {
		out = append(out, c.Body["cursor"])
	}
	return out
}

func ptrTo[T any](v T) *T { return &v }

// M244: no-op saves and interval edits keep the polling-configuration
// generation, a schema label rename keeps the generation and the stored
// cursor, and an effective default change invalidates both.
func TestV0048ConfigurationFencesKeepCursorUntilEffectiveValuesChange(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "fenced", "poller")
	schema := func(label string, def bool) {
		t.Helper()
		expectHTTP(t, "schema "+label, e.registerSchema(key, "1.0.0", map[string]any{"name": "enabled", "label": label, "type": "boolean", "required": false, "default": def}), 200, "")
	}
	settings := map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 1}
	schema("Original label", true)
	expect(t, "configure", e.saveSettings(3, id, settings), 200, "")
	s := e.store.Source(3, id)
	s.Cursor = ptrTo("preserved-cursor")
	e.store.PutSource(*s)
	initial := s.Generation

	before := e.store.Source(3, id).Revision
	expect(t, "no-op save", e.saveSettings(3, id, settings), 200, "")
	if s := e.store.Source(3, id); s.Revision != before || s.Generation != initial {
		t.Fatalf("no-op save revision %d generation %d", s.Revision, s.Generation)
	}
	for _, interval := range []int{2, 1} {
		current := e.store.Source(3, id).Revision
		edit := map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": interval}
		expect(t, "interval", e.saveSettings(3, id, edit), 200, "")
		if s := e.store.Source(3, id); s.Revision != current+1 || s.Generation != initial || s.Cursor == nil {
			t.Fatalf("interval %d: revision %d generation %d cursor %v", interval, s.Revision, s.Generation, s.Cursor)
		}
	}
	schema("Renamed display label", true)
	if s := e.store.Source(3, id); s.Generation != initial || s.Cursor == nil || *s.Cursor != "preserved-cursor" {
		t.Fatalf("label rename: generation %d -> %d cursor %v", initial, s.Generation, s.Cursor)
	}
	e.makeDue(3, id)
	e.pollOnce()
	if calls := e.calls(); len(calls) != 1 || calls[0].Body["cursor"] != "preserved-cursor" {
		t.Fatalf("poll after the label rename sent cursor %v", cursorsOf(calls))
	}
	s = e.store.Source(3, id)
	s.Cursor = ptrTo("schema-cursor")
	e.store.PutSource(*s)
	generation := s.Generation
	schema("Renamed display label", false)
	if s := e.store.Source(3, id); s.Generation == generation || s.Cursor != nil {
		t.Fatalf("default change: generation %d -> %d cursor %v", generation, s.Generation, s.Cursor)
	}
}

// M277: an omitted poll_url or poll_interval_minutes keeps the stored
// value, an explicit null clears it and a value replaces it.
func TestV0048ExplicitNullClearsPollFields(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "replaced")
	url := e.poller.URL + "/poll"
	platform := func() (any, any, any, any) {
		r, _ := e.settingsGet(3, id)
		p := r.Body["platform"].(map[string]any)
		return p["poll_url"], p["poll_interval_minutes"], r.Body["settings_status"], r.Body["polling"].(map[string]any)["next_poll_at"]
	}
	check := func(label string, body map[string]any, wantURL any, wantInterval int64, wantStatus string) {
		t.Helper()
		expect(t, label, e.saveSettings(3, id, body), 200, "")
		u, i, st, next := platform()
		if u != wantURL || (wantInterval == 0 && i != nil) || (wantInterval != 0 && num(i) != wantInterval) || st != wantStatus {
			t.Fatalf("%s: poll_url %v interval %v status %v", label, u, i, st)
		}
		if (st == "complete") != (next != nil) {
			t.Fatalf("%s: status %v next_poll_at %v", label, st, next)
		}
	}
	rev := e.store.Source(3, id).Revision
	check("omitted fields", map[string]any{"values": map[string]any{}}, url, 10, "complete")
	if got := e.store.Source(3, id).Revision; got != rev {
		t.Fatalf("omission changed the revision %d -> %d", rev, got)
	}
	check("null poll_url", map[string]any{"values": map[string]any{}, "poll_url": nil}, nil, 10, "incomplete")
	check("omitted after clear", map[string]any{"values": map[string]any{}}, nil, 10, "incomplete")
	check("replace poll_url", map[string]any{"values": map[string]any{}, "poll_url": url}, url, 10, "complete")
	check("null interval", map[string]any{"values": map[string]any{}, "poll_interval_minutes": nil}, url, 0, "incomplete")
	check("replace interval", map[string]any{"values": map[string]any{}, "poll_interval_minutes": 15}, url, 15, "complete")
	expect(t, "webhook null poll field", func() reply {
		wid, _ := e.webhookSource(3, "hook")
		return e.saveSettings(3, wid, map[string]any{"values": map[string]any{}, "poll_url": nil})
	}(), 400, "invalid_request")
}

var revTag = regexp.MustCompile(`^"rev-[1-9][0-9]*"$`)

// M359: unblock without a well-formed current If-Match, or with a body, is
// 400 and changes nothing; invalid states map to 409 polling_not_blocked,
// not_poller_source, source_unavailable and poll_in_progress. Settings and
// implementation writes refuse a malformed If-Match the same way.
func TestV0048UnblockRejectsMalformedPreconditionsAndInvalidStates(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "blocked")
	failPolls(e, id, 5)
	blocked := e.store.Source(3, id)
	if blocked.PollingBlockedAt == nil {
		t.Fatal("setup: poller is not blocked")
	}
	current := etagOf(blocked.Revision)
	if !revTag.MatchString(current) {
		t.Fatalf("setup: ETag %s", current)
	}
	unblock := func(id int64, ifMatch string) reply {
		return e.uiIfMatch("sources-unblock-polling", 3, map[string]any{"id": id}, nil, ifMatch)
	}
	for label, ifMatch := range map[string]string{
		"plain revision": `"` + itoa(int64(blocked.Revision)) + `"`, "zero revision": `"rev-0"`, "non-canonical revision": `"rev-0` + itoa(int64(blocked.Revision)) + `"`,
		"unquoted": "rev-" + itoa(int64(blocked.Revision)), "weak": `W/` + current, "list": current + `, "rev-1"`,
	} {
		expect(t, label, unblock(id, ifMatch), 400, "invalid_request")
	}
	withBody := e.uiRequest("sources-unblock-polling", 3, map[string]any{"id": id}, map[string]any{})
	withBody.Header["If-Match"] = current
	expect(t, "request body", e.do(withBody), 400, "invalid_request")
	if s := e.store.Source(3, id); s.PollingBlockedAt == nil || s.Revision != blocked.Revision || s.ConsecutiveFailures != 5 {
		t.Fatalf("rejected unblocks changed the Source: blocked %v revision %d failures %d", s.PollingBlockedAt, s.Revision, s.ConsecutiveFailures)
	}
	for label, ifMatch := range map[string]string{"settings zero revision": `"rev-0"`, "settings plain revision": `"1"`} {
		expect(t, label, e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{}}, ifMatch), 400, "invalid_request")
	}
	expect(t, "implementation plain revision", e.uiIfMatch("sources-update-implementation", 3, map[string]any{"id": id}, map[string]any{"implementation_version": "9.9.9"}, `"`+itoa(int64(blocked.Revision))+`"`), 400, "invalid_request")

	t.Run("poll in progress", func(t *testing.T) {
		s := e.store.Source(3, id)
		s.UnfinishedPollID = ptrTo(int64(999))
		e.store.PutSource(*s)
		expect(t, "unblock", unblock(id, current), 409, "poll_in_progress")
		s.UnfinishedPollID = nil
		e.store.PutSource(*s)
		if got := e.store.Source(3, id); got.PollingBlockedAt == nil || got.Revision != blocked.Revision {
			t.Fatal("poll_in_progress changed the Source")
		}
	})
	t.Run("webhook Source", func(t *testing.T) {
		wid, _ := e.webhookSource(3, "hook")
		_, tag := e.settingsGet(3, wid)
		expect(t, "unblock", unblock(wid, tag), 409, "not_poller_source")
	})
	t.Run("disabled blocked poller", func(t *testing.T) {
		did, _ := e.pollerSource(3, "disabled-blocked")
		failPolls(e, did, 5)
		expect(t, "disable", e.ui("sources-disable", 3, map[string]any{"id": did}, nil), 200, "")
		s := e.store.Source(3, did)
		expect(t, "unblock", unblock(did, etagOf(s.Revision)), 409, "source_unavailable")
		if got := e.store.Source(3, did); got.PollingBlockedAt == nil {
			t.Fatal("a disabled poller was unblocked")
		}
	})
	t.Run("not blocked and current unblock", func(t *testing.T) {
		ok := unblock(id, current)
		expect(t, "current unblock", ok, 200, "")
		expect(t, "not blocked", unblock(id, ok.Header["ETag"]), 409, "polling_not_blocked")
	})
}

func etagOf(rev int) string { return `"rev-` + itoa(int64(rev)) + `"` }
