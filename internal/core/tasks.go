package core

import (
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// sourceTask is one validated source task entry.
type sourceTask struct {
	ExternalID    string
	Payload       json.RawMessage
	ProjectID     json.RawMessage
	EnvironmentID json.RawMessage
}

const maxExternalID = 256

// parseTask validates one task object; prefix is its JSON pointer.
func parseTask(raw json.RawMessage, prefix string) (sourceTask, []string) {
	var t sourceTask
	var obj map[string]json.RawMessage
	if err := decodeStrict(raw, &obj); err != nil || obj == nil {
		return t, []string{prefixOr(prefix)}
	}
	var bad []string
	for k := range obj {
		switch k {
		case "external_id", "payload", "project_id", "environment_id":
		default:
			bad = append(bad, prefix+"/"+k)
		}
	}
	var id any
	if raw, ok := obj["external_id"]; !ok || decodeStrict(raw, &id) != nil {
		bad = append(bad, prefix+"/external_id")
	} else if s, ok := id.(string); !ok || s == "" || len(s) > maxExternalID || !utf8.ValidString(s) {
		bad = append(bad, prefix+"/external_id")
	} else {
		t.ExternalID = s
	}
	var payload map[string]json.RawMessage
	if raw, ok := obj["payload"]; !ok || decodeStrict(raw, &payload) != nil || payload == nil {
		bad = append(bad, prefix+"/payload")
	} else {
		t.Payload = raw
	}
	if raw, ok := obj["project_id"]; ok {
		var v any
		if decodeStrict(raw, &v) != nil {
			bad = append(bad, prefix+"/project_id")
		} else if s, isStr := v.(string); v != nil && (!isStr || utf8.RuneCountInString(s) > 128) {
			bad = append(bad, prefix+"/project_id")
		} else {
			t.ProjectID = raw
		}
	}
	if raw, ok := obj["environment_id"]; ok {
		if !isNull(raw) {
			if _, ok := parseID(string(raw)); !ok {
				bad = append(bad, prefix+"/environment_id")
			}
		}
		t.EnvironmentID = raw
	}
	return t, bad
}

func prefixOr(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// parseTaskList validates a task array under prefix (for example "/tasks")
// with 1..max entries and unique external IDs.
func parseTaskList(raw json.RawMessage, prefix string, min, max int) ([]sourceTask, []string) {
	var items []json.RawMessage
	if err := decodeStrict(raw, &items); err != nil || items == nil || len(items) < min || len(items) > max {
		return nil, []string{prefix}
	}
	var bad []string
	tasks := make([]sourceTask, 0, len(items))
	seen := map[string]bool{}
	for i, item := range items {
		p := prefix + "/" + strconv.Itoa(i)
		t, errs := parseTask(item, p)
		bad = append(bad, errs...)
		if t.ExternalID != "" {
			if seen[t.ExternalID] {
				bad = append(bad, p+"/external_id")
			}
			seen[t.ExternalID] = true
		}
		tasks = append(tasks, t)
	}
	return tasks, bad
}

// intakeEntry is the intake message form of a task.
func intakeEntry(t sourceTask, intakeID int64) map[string]any {
	e := map[string]any{"intake_id": intakeID, "external_id": t.ExternalID, "payload": t.Payload}
	if t.ProjectID != nil {
		e["project_id"] = t.ProjectID
	}
	if t.EnvironmentID != nil {
		e["environment_id"] = t.EnvironmentID
	}
	return e
}
