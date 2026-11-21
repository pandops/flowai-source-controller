package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/pandops/flowai-source-controller/internal/broker"
	"github.com/pandops/flowai-source-controller/schemas"
)

// reqCtx is one decoded request (rule 9).
type reqCtx struct {
	kind      string
	op        string
	team      int64
	requestID int64
	operator  int64
	ifMatch   string
	params    map[string]json.RawMessage
	body      json.RawMessage
}

func (rc *reqCtx) id() int64 {
	var id int64
	_ = json.Unmarshal(rc.params["id"], &id)
	return id
}

type handler func(s *Service, ctx context.Context, rc *reqCtx) broker.Reply

var uiOps = map[string]handler{
	"sources-create":                (*Service).uiCreate,
	"sources-list":                  (*Service).uiList,
	"sources-get":                   (*Service).uiGet,
	"sources-settings-get":          (*Service).uiSettingsGet,
	"sources-settings-put":          (*Service).uiSettingsPut,
	"sources-rotate-key":            (*Service).uiRotate,
	"sources-disable":               (*Service).uiDisable,
	"sources-unblock-polling":       (*Service).uiUnblock,
	"sources-update-implementation": (*Service).uiUpdateImplementation,
}

var serviceOps = map[string]handler{
	"sources-lookup": (*Service).serviceLookup,
	"binding-guard":  (*Service).serviceGuard,
}

const maxRequestBytes = 128 << 10

// HandleRequest answers one core NATS request on a source-controller ui or
// service subject.
func (s *Service) HandleRequest(ctx context.Context, r broker.Request) broker.Reply {
	requestID, ok := parseID(r.Header["request_id"])
	parts := strings.Split(r.Subject, ".")
	if len(parts) != 4 || parts[0] != Publisher || parts[3] != "sub" {
		return replyErr(errNotFound, requestID)
	}
	var h handler
	switch parts[1] {
	case "ui":
		h = uiOps[parts[2]]
	case "service":
		h = serviceOps[parts[2]]
	}
	if h == nil {
		return replyErr(errNotFound, requestID)
	}
	if !ok {
		return replyErr(errInvalid, 0)
	}
	rc := &reqCtx{kind: parts[1], op: parts[2], requestID: requestID, ifMatch: r.Header["If-Match"]}
	if rc.team, ok = parseID(r.Header["team"]); !ok {
		return replyErr(errInvalid, requestID)
	}
	if rc.kind == "ui" {
		if rc.operator, ok = parseID(r.Header["operator_id"]); !ok {
			return replyErr(errInvalid, requestID)
		}
	}
	if len(r.Data) > maxRequestBytes {
		return replyErr(errTooLarge, requestID)
	}
	if err := schemas.Validate(rc.kind+"."+rc.op+".request.schema.json", r.Data); err != nil {
		return replyErr(errInvalid, requestID)
	}
	var env struct {
		Params map[string]json.RawMessage `json:"params"`
		Body   json.RawMessage            `json:"body"`
	}
	if err := decodeStrict(r.Data, &env); err != nil {
		return replyErr(errInvalid, requestID)
	}
	rc.params, rc.body = env.Params, env.Body
	return h(s, ctx, rc)
}

// sourceWithTeam loads a Source of rc.team with the team projection row.
func (s *Service) loadSource(ctx context.Context, rc *reqCtx) (*Source, *Team, error) {
	var src *Source
	var team *Team
	err := s.read(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.Source(rc.team, rc.id(), false); err != nil || src == nil {
			return err
		}
		team, err = tx.Team(rc.team)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	if src == nil {
		return nil, nil, errNotFound
	}
	if team == nil {
		if team, err = s.team(ctx, rc.team, rc.requestID); err != nil {
			return nil, nil, err
		}
	}
	return src, team, nil
}

func (s *Service) uiCreate(ctx context.Context, rc *reqCtx) broker.Reply {
	var body struct {
		Name                  string  `json:"name"`
		Mode                  string  `json:"mode"`
		ContractVersion       string  `json:"contract_version"`
		ImplementationVersion string  `json:"implementation_version"`
		Description           *string `json:"description"`
		FilterSummary         string  `json:"filter_summary"`
	}
	if err := decodeStrict(rc.body, &body); err != nil || !sourceNameRegexp.MatchString(body.Name) || body.Name == "." || body.Name == ".." {
		return replyErr(errInvalid, rc.requestID)
	}
	team, err := s.team(ctx, rc.team, rc.requestID)
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	if team.ArchivedAt != nil {
		return replyErr(errNotFound, rc.requestID)
	}
	key, digest := newKey()
	var src *Source
	err = s.store.Tx(ctx, func(tx Tx) error {
		id, err := tx.NextCounter(rc.team, "source")
		if err != nil {
			return err
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		src = &Source{TeamID: rc.team, ID: id, Name: body.Name, Mode: body.Mode, ContractVersion: body.ContractVersion,
			ImplementationVersion: body.ImplementationVersion, Description: body.Description, FilterSummary: body.FilterSummary,
			Enabled: true, KeyDigest: digest, Revision: 1, SettingsStatus: "awaiting_schema", CreatedAt: now, Generation: 1,
			Values: map[string]json.RawMessage{}}
		if err := tx.InsertSource(src); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(rc.team, id, 1, snapshot(src)); err != nil {
			return err
		}
		return s.audit(tx, rc, "source_created", &id, "succeeded", "", map[string]any{"name": body.Name, "mode": body.Mode})
	})
	if errors.Is(err, ErrNameConflict) {
		s.auditRejected(ctx, rc, "source_created", nil, "source_name_conflict")
		return replyErr(errNameConflict, rc.requestID)
	}
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(201, map[string]any{"source": s.view(src, team.TeamName), "api_key": key}, broker.Header{"Cache-Control": "no-store"})
}

func (s *Service) uiList(ctx context.Context, rc *reqCtx) broker.Reply {
	limit := 50
	if raw, ok := rc.params["limit"]; ok {
		_ = json.Unmarshal(raw, &limit)
	}
	var after *Position
	if raw, ok := rc.params["cursor"]; ok {
		var c string
		_ = json.Unmarshal(raw, &c)
		state, ok := s.openCursor(c, "sources-list", rc.team)
		if !ok {
			return replyErr(errInvalid, rc.requestID)
		}
		after = &Position{CreatedAt: unixMicro(state.At), ID: state.ID}
	}
	var items []Source
	var team *Team
	err := s.read(ctx, func(tx Tx) error {
		var err error
		if items, err = tx.ListSources(rc.team, after, limit+1); err != nil {
			return err
		}
		team, err = tx.Team(rc.team)
		return err
	})
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	teamName := ""
	if team != nil {
		teamName = team.TeamName
	}
	var next any
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		next = s.signCursor(cursorState{Op: "sources-list", Team: rc.team, At: last.CreatedAt.UnixMicro(), ID: last.ID})
	}
	out := make([]any, 0, len(items))
	for i := range items {
		out = append(out, s.view(&items[i], teamName))
	}
	return replyOK(200, map[string]any{"items": out, "next_cursor": next}, nil)
}

func (s *Service) uiGet(ctx context.Context, rc *reqCtx) broker.Reply {
	src, team, err := s.loadSource(ctx, rc)
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, s.view(src, team.TeamName), nil)
}

func (s *Service) uiSettingsGet(ctx context.Context, rc *reqCtx) broker.Reply {
	src, team, err := s.loadSource(ctx, rc)
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, s.settingsView(src, team.TeamName), broker.Header{"ETag": etag(src.Revision)})
}

// validValue reports whether raw is a JSON value of the field type.
func validValue(typ string, raw json.RawMessage) bool {
	if isNull(raw) {
		return true
	}
	var v any
	if err := decodeStrict(raw, &v); err != nil {
		return false
	}
	switch typ {
	case "string":
		_, ok := v.(string)
		return ok
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "number":
		_, ok := v.(json.Number)
		return ok
	case "integer":
		n, ok := v.(json.Number)
		if !ok {
			return false
		}
		_, err := n.Int64()
		return err == nil && !strings.ContainsAny(n.String(), ".eE")
	}
	return false
}

// allowedPollURL reports whether raw is an https URL with an allowed origin.
func (s *Service) allowedPollURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || len(raw) > 2048 {
		return false
	}
	origin := u.Scheme + "://" + u.Host
	for _, o := range s.cfg.PollAllowedOrigins {
		if strings.EqualFold(strings.TrimRight(o, "/"), origin) {
			return true
		}
	}
	return false
}

func sameValues(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || string(v) != string(w) {
			return false
		}
	}
	return true
}

func (s *Service) uiSettingsPut(ctx context.Context, rc *reqCtx) broker.Reply {
	if !validIfMatch(rc.ifMatch) {
		return replyErr(errInvalid, rc.requestID)
	}
	var body map[string]json.RawMessage
	if err := decodeStrict(rc.body, &body); err != nil {
		return replyErr(errInvalid, rc.requestID)
	}
	var values map[string]json.RawMessage
	if err := decodeStrict(body["values"], &values); err != nil {
		return replyErr(errInvalid, rc.requestID)
	}
	id := rc.id()
	var src *Source
	var team *Team
	err := s.store.Tx(ctx, func(tx Tx) error {
		var err error
		if team, err = tx.Team(rc.team); err != nil {
			return err
		}
		if src, err = tx.Source(rc.team, id, true); err != nil {
			return err
		}
		if src == nil || team == nil || team.ArchivedAt != nil {
			return errNotFound
		}
		if rc.ifMatch != etag(src.Revision) {
			return errStale
		}
		overrides := map[string]json.RawMessage{}
		for name, v := range values {
			var field *Field
			for i := range src.Schema {
				if src.Schema[i].Name == name {
					field = &src.Schema[i]
				}
			}
			if field == nil || !validValue(field.Type, v) {
				return invalidFields("/values/" + name)
			}
			if !isNull(v) {
				overrides[name] = v
			}
		}
		pollURL, urlSet := body["poll_url"]
		interval, intervalSet := body["poll_interval_minutes"]
		if src.Mode != "poller" && (urlSet || intervalSet) {
			return invalidFields("/poll_url")
		}
		newURL, newInterval := src.PollURL, src.PollInterval
		if urlSet {
			newURL = nil
			if !isNull(pollURL) {
				var u string
				_ = json.Unmarshal(pollURL, &u)
				if !s.allowedPollURL(u) {
					return invalidFields("/poll_url")
				}
				newURL = &u
			}
		}
		if intervalSet {
			newInterval = nil
			if !isNull(interval) {
				var n int
				_ = json.Unmarshal(interval, &n)
				newInterval = &n
			}
		}
		valuesChanged := !sameValues(overrides, src.Values)
		urlChanged := strOrNil(newURL) != strOrNil(src.PollURL)
		intervalChanged := intOrNil(newInterval) != intOrNil(src.PollInterval)
		if !valuesChanged && !urlChanged && !intervalChanged {
			return nil
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		src.Values, src.PollURL, src.PollInterval = overrides, newURL, newInterval
		src.Revision++
		if valuesChanged || urlChanged {
			src.Generation++
			src.Cursor = nil
		}
		applyStatus(src, now, intervalChanged)
		if err := tx.UpdateSource(src); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(rc.team, id, src.Revision, snapshot(src)); err != nil {
			return err
		}
		changes := map[string]any{"settings_status": src.SettingsStatus, "revision": src.Revision}
		if intervalChanged {
			changes["poll_interval_minutes"] = intOrNil(src.PollInterval)
		}
		return s.audit(tx, rc, "source_settings_updated", &id, "succeeded", "", changes)
	})
	if errors.Is(err, errStale) {
		s.auditRejected(ctx, rc, "source_settings_updated", &id, "stale_revision")
	}
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, s.settingsView(src, team.TeamName), broker.Header{"ETag": etag(src.Revision)})
}

func (s *Service) uiRotate(ctx context.Context, rc *reqCtx) broker.Reply {
	id := rc.id()
	key, digest := newKey()
	var src *Source
	var team *Team
	err := s.store.Tx(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.Source(rc.team, id, true); err != nil {
			return err
		}
		if src == nil {
			return errNotFound
		}
		if team, err = tx.Team(rc.team); err != nil {
			return err
		}
		if !src.Enabled || team == nil || team.ArchivedAt != nil {
			return errUnavailable
		}
		src.KeyDigest = digest
		if err := tx.UpdateSource(src); err != nil {
			return err
		}
		return s.audit(tx, rc, "source_key_rotated", &id, "succeeded", "", nil)
	})
	if errors.Is(err, errUnavailable) {
		s.auditRejected(ctx, rc, "source_key_rotated", &id, "source_unavailable")
	}
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, map[string]any{"source": s.view(src, team.TeamName), "api_key": key}, broker.Header{"Cache-Control": "no-store"})
}

func (s *Service) uiDisable(ctx context.Context, rc *reqCtx) broker.Reply {
	id := rc.id()
	var src *Source
	var team *Team
	err := s.store.Tx(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.Source(rc.team, id, true); err != nil {
			return err
		}
		if src == nil {
			return errNotFound
		}
		if team, err = tx.Team(rc.team); err != nil {
			return err
		}
		if team == nil {
			return errNotFound
		}
		if !src.Enabled {
			return nil
		}
		src.Enabled, src.KeyDigest = false, nil
		src.Revision++
		src.Generation++
		src.Cursor = nil
		if err := tx.UpdateSource(src); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(rc.team, id, src.Revision, snapshot(src)); err != nil {
			return err
		}
		return s.audit(tx, rc, "source_disabled", &id, "succeeded", "", map[string]any{"enabled": false})
	})
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, s.view(src, team.TeamName), nil)
}

func (s *Service) uiUnblock(ctx context.Context, rc *reqCtx) broker.Reply {
	if !validIfMatch(rc.ifMatch) {
		return replyErr(errInvalid, rc.requestID)
	}
	id := rc.id()
	var src *Source
	var team *Team
	err := s.store.Tx(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.Source(rc.team, id, true); err != nil {
			return err
		}
		if src == nil {
			return errNotFound
		}
		if team, err = tx.Team(rc.team); err != nil {
			return err
		}
		if team == nil || team.ArchivedAt != nil || !src.Enabled {
			return errUnavailable
		}
		if src.Mode != "poller" {
			return errNotPoller
		}
		if rc.ifMatch != etag(src.Revision) {
			return errStale
		}
		if src.PollingBlockedAt == nil {
			return errNotBlocked
		}
		if src.UnfinishedPollID != nil {
			return errPollBusy
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		src.PollingBlockedAt, src.ConsecutiveFailures = nil, 0
		src.Revision++
		src.NextPollAt = nil
		if src.SettingsStatus == "complete" && src.PollInterval != nil {
			src.NextPollAt = ptr(now.Add(minutes(*src.PollInterval)))
		}
		if err := tx.UpdateSource(src); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(rc.team, id, src.Revision, snapshot(src)); err != nil {
			return err
		}
		return s.audit(tx, rc, "source_polling_unblocked", &id, "succeeded", "", nil)
	})
	if errors.Is(err, errStale) || errors.Is(err, errNotBlocked) || errors.Is(err, errUnavailable) || errors.Is(err, errNotPoller) || errors.Is(err, errPollBusy) {
		s.auditRejected(ctx, rc, "source_polling_unblocked", &id, asAPI(err).code)
	}
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, s.view(src, team.TeamName), broker.Header{"ETag": etag(src.Revision)})
}

func (s *Service) uiUpdateImplementation(ctx context.Context, rc *reqCtx) broker.Reply {
	if !validIfMatch(rc.ifMatch) {
		return replyErr(errInvalid, rc.requestID)
	}
	var body struct {
		ImplementationVersion string `json:"implementation_version"`
	}
	if err := decodeStrict(rc.body, &body); err != nil {
		return replyErr(errInvalid, rc.requestID)
	}
	id := rc.id()
	key, digest := newKey()
	var src *Source
	var team *Team
	err := s.store.Tx(ctx, func(tx Tx) error {
		var err error
		if src, err = tx.Source(rc.team, id, true); err != nil {
			return err
		}
		if src == nil {
			return errNotFound
		}
		if team, err = tx.Team(rc.team); err != nil {
			return err
		}
		if team == nil || team.ArchivedAt != nil || !src.Enabled {
			return errUnavailable
		}
		if rc.ifMatch != etag(src.Revision) {
			return errStale
		}
		if src.UnfinishedPollID != nil {
			return errPollBusy
		}
		if body.ImplementationVersion == src.ImplementationVersion {
			return errInvalid
		}
		now, err := tx.Now()
		if err != nil {
			return err
		}
		src.ImplementationVersion = body.ImplementationVersion
		src.KeyDigest = digest
		src.Revision++
		src.Generation++
		src.Cursor = nil
		// Values stay stored until the new version registers its schema,
		// which keeps those with the same name and type.
		if src.Schema != nil {
			src.PreviousSchema = src.Schema
		}
		src.Schema = nil
		applyStatus(src, now, false)
		if err := tx.UpdateSource(src); err != nil {
			return err
		}
		if err := tx.AppendSourceRevision(rc.team, id, src.Revision, snapshot(src)); err != nil {
			return err
		}
		return s.audit(tx, rc, "source_implementation_updated", &id, "succeeded", "", map[string]any{"implementation_version": body.ImplementationVersion})
	})
	if errors.Is(err, errStale) || errors.Is(err, errUnavailable) || errors.Is(err, errPollBusy) || errors.Is(err, errInvalid) {
		s.auditRejected(ctx, rc, "source_implementation_updated", &id, asAPI(err).code)
	}
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, map[string]any{"source": s.view(src, team.TeamName), "api_key": key}, broker.Header{"ETag": etag(src.Revision), "Cache-Control": "no-store"})
}

func (s *Service) serviceLookup(ctx context.Context, rc *reqCtx) broker.Reply {
	var body struct {
		TeamID    int64   `json:"team_id"`
		SourceIDs []int64 `json:"source_ids"`
	}
	if err := decodeStrict(rc.body, &body); err != nil || body.TeamID != rc.team {
		return replyErr(errInvalid, rc.requestID)
	}
	out := []any{}
	err := s.read(ctx, func(tx Tx) error {
		for _, id := range body.SourceIDs {
			src, err := tx.Source(rc.team, id, false)
			if err != nil {
				return err
			}
			if src != nil {
				out = append(out, map[string]any{"source_id": src.ID, "name": src.Name, "mode": src.Mode, "settings_status": src.SettingsStatus,
					"enabled": src.Enabled, "polling_blocked": src.PollingBlockedAt != nil, "revision": src.Revision})
			}
		}
		return nil
	})
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, map[string]any{"sources": out}, nil)
}

func (s *Service) serviceGuard(ctx context.Context, rc *reqCtx) broker.Reply {
	var body struct {
		TeamID  int64 `json:"team_id"`
		Sources []struct {
			SourceID         int64 `json:"source_id"`
			ExpectedRevision int   `json:"expected_revision"`
		} `json:"sources"`
	}
	if err := decodeStrict(rc.body, &body); err != nil || body.TeamID != rc.team {
		return replyErr(errInvalid, rc.requestID)
	}
	out := []any{}
	err := s.store.Tx(ctx, func(tx Tx) error {
		for _, g := range body.Sources {
			src, err := tx.Source(rc.team, g.SourceID, true)
			if err != nil {
				return err
			}
			if src == nil || src.Revision != g.ExpectedRevision {
				return errRevConflict
			}
			src.Revision++
			if err := tx.UpdateSource(src); err != nil {
				return err
			}
			if err := tx.AppendSourceRevision(rc.team, src.ID, src.Revision, snapshot(src)); err != nil {
				return err
			}
			out = append(out, map[string]any{"source_id": src.ID, "revision": src.Revision})
		}
		return nil
	})
	if err != nil {
		return replyErr(asAPI(err), rc.requestID)
	}
	return replyOK(200, map[string]any{"sources": out}, nil)
}
