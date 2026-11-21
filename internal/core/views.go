package core

import (
	"encoding/json"
	"time"
)

var nullJSON = json.RawMessage("null")

func isNull(raw json.RawMessage) bool { return len(raw) == 0 || string(raw) == "null" }

// effective returns a field's effective value: the override or the default.
func effective(src *Source, f Field) json.RawMessage {
	if v, ok := src.Values[f.Name]; ok && !isNull(v) {
		return v
	}
	if isNull(f.Default) {
		return nullJSON
	}
	return f.Default
}

// effectiveValues returns every schema field's effective value.
func effectiveValues(src *Source) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, f := range src.Schema {
		out[f.Name] = effective(src, f)
	}
	return out
}

// settingsStatus derives awaiting_schema, incomplete or complete.
func settingsStatus(src *Source) string {
	if src.Schema == nil {
		return "awaiting_schema"
	}
	if src.Mode == "poller" && (src.PollURL == nil || src.PollInterval == nil) {
		return "incomplete"
	}
	for _, f := range src.Schema {
		if f.Required != nil && *f.Required && isNull(effective(src, f)) {
			return "incomplete"
		}
	}
	return "complete"
}

// applyStatus recomputes the settings status and the next poll time after a
// settings or schema change. intervalOnly keeps the poll timeline.
func applyStatus(src *Source, now time.Time, intervalChanged bool) {
	old := src.SettingsStatus
	src.SettingsStatus = settingsStatus(src)
	if src.Mode != "poller" {
		return
	}
	if src.SettingsStatus != "complete" {
		src.NextPollAt = nil
		return
	}
	interval := time.Duration(*src.PollInterval) * time.Minute
	switch {
	case old != "complete":
		src.NextPollAt = ptr(now.Add(interval))
	case intervalChanged:
		base := now
		if src.LastLeasedAt != nil {
			base = *src.LastLeasedAt
		}
		src.NextPollAt = ptr(base.Add(interval))
	}
}

func (s *Service) webhookURL(src *Source, teamName string) any {
	if src.Mode != "webhook" {
		return nil
	}
	return s.cfg.PublicBaseURL + "/webhook/" + teamName + "/" + src.Name + "/task"
}

func (s *Service) health(src *Source) string {
	switch {
	case src.LastSeenAt == nil:
		return "never_seen"
	case s.cfg.Now().Sub(*src.LastSeenAt) > 90*time.Second:
		return "stale"
	case src.ErrorClass != nil || src.Activity != nil && (*src.Activity == "error" || *src.Activity == "degraded"):
		return "error"
	}
	return "healthy"
}

func pollingView(src *Source) any {
	if src.Mode != "poller" {
		return nil
	}
	return map[string]any{"consecutive_failures": src.ConsecutiveFailures, "polling_blocked_at": tsOrNil(src.PollingBlockedAt), "next_poll_at": tsOrNil(src.NextPollAt)}
}

func lastPollView(src *Source) any {
	lp := src.LastPoll
	if lp == nil {
		return nil
	}
	return map[string]any{"poll_id": lp.PollID, "started_at": ts(lp.StartedAt), "outcome": strOrNil(lp.Outcome), "status": lp.Status,
		"pages_read": lp.PagesRead, "returned_tasks": lp.ReturnedTasks, "published_tasks": lp.PublishedTasks, "rejected_tasks": lp.RejectedTasks,
		"error_code": strOrNil(lp.ErrorCode), "finished_at": ts(lp.FinishedAt)}
}

// view is the operator Source view; it never carries a key or digest.
func (s *Service) view(src *Source, teamName string) map[string]any {
	var observed any
	if !isNull(src.ObservedCounters) {
		observed = src.ObservedCounters
	}
	return map[string]any{
		"id": src.ID, "source_id": src.ID, "team_id": src.TeamID, "name": src.Name, "mode": src.Mode,
		"contract_version": src.ContractVersion, "implementation_version": src.ImplementationVersion,
		"description": strOrNil(src.Description), "filter_summary": src.FilterSummary, "enabled": src.Enabled,
		"revision": src.Revision, "settings_status": src.SettingsStatus, "health": s.health(src), "activity": strOrNil(src.Activity),
		"intake_counts": src.Counts, "created_at": ts(src.CreatedAt), "polling": pollingView(src), "last_poll": lastPollView(src),
		"webhook_url": s.webhookURL(src, teamName), "last_seen_at": tsOrNil(src.LastSeenAt), "observed_counters": observed,
	}
}

// settingsView is the safe settings representation of one Source.
func (s *Service) settingsView(src *Source, teamName string) map[string]any {
	fields := make([]any, 0, len(src.Schema))
	for _, f := range src.Schema {
		def := json.RawMessage(nullJSON)
		if !isNull(f.Default) {
			def = f.Default
		}
		_, overridden := src.Values[f.Name]
		m := map[string]any{"name": f.Name, "label": f.Label, "type": f.Type, "required": f.Required != nil && *f.Required,
			"default": def, "value": effective(src, f), "overridden": overridden}
		if f.Description != nil {
			m["description"] = *f.Description
		}
		fields = append(fields, m)
	}
	platform := map[string]any{}
	if src.Mode == "webhook" {
		platform["webhook_url"] = s.webhookURL(src, teamName)
	} else {
		platform["poll_url"] = strOrNil(src.PollURL)
		platform["poll_interval_minutes"] = intOrNil(src.PollInterval)
	}
	return map[string]any{"source_id": src.ID, "mode": src.Mode, "settings_status": src.SettingsStatus, "schema_revision": intOrNil(src.SchemaRevision),
		"revision": src.Revision, "platform": platform, "fields": fields, "polling": pollingView(src), "last_poll": lastPollView(src)}
}

// snapshot is the stored settings revision of a Source (no key material).
func snapshot(src *Source) json.RawMessage {
	return mustJSON(map[string]any{"revision": src.Revision, "values": src.Values, "poll_url": strOrNil(src.PollURL),
		"poll_interval_minutes": intOrNil(src.PollInterval), "implementation_version": src.ImplementationVersion, "enabled": src.Enabled,
		"settings_status": src.SettingsStatus})
}
