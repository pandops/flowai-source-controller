//go:build !integration

package test

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// Coverage: source-controller publishes non-human events and human audit.

func TestV0048PollFinalizationIsJournaled(t *testing.T) {
	e := newEnv(t, withCallWindow(500*time.Millisecond))
	e.addTeam(3, "acme")
	e.pollerSource(3, "poller-main")
	n := 0
	e.setPoll(func(w http.ResponseWriter, r *http.Request, call pollCall) {
		n++
		if n <= 2 {
			writeJSON(w, 200, map[string]any{"tasks": []any{task("P" + itoa(int64(n)) + "-a"), task("P" + itoa(int64(n)) + "-b")}, "next_cursor": "next"})
			return
		}
		<-r.Context().Done()
	})
	e.pollOnce()
	events := e.messages("source-controller.event.poll_finalized.pub")
	if len(events) != 1 {
		t.Fatalf("poll_finalized %d", len(events))
	}
	ev := decode(t, events[0].Data)
	if ev["outcome"] != "timeout" || num(ev["pages_read"]) != 2 || num(ev["published_count"]) != 4 || ev["status"] != "failed" {
		t.Fatalf("event %v", ev)
	}
	if strings.HasPrefix(events[0].Subject, "source-controller.audit.") {
		t.Fatal("a non-human action went to audit")
	}
}

func TestV0048KeyRotationIsAuditedWithoutTheKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, old := e.createSource(3, "jira-main", "webhook")
	r := e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	fresh := r.Body["api_key"].(string)
	audits := e.messages("source-controller.audit.source_key_rotated.pub")
	if len(audits) != 1 {
		t.Fatalf("audits %d", len(audits))
	}
	a := decode(t, audits[0].Data)
	if a["outcome"] != "succeeded" || num(a["actor_id"]) != 501 || num(a["resource_id"]) != id || audits[0].Header["request_id"] == "" {
		t.Fatalf("audit %v %v", a, audits[0].Header)
	}
	for _, secret := range []string{old, fresh, old[7:], fresh[7:]} {
		if strings.Contains(string(audits[0].Data), secret) {
			t.Fatal("audit contains a key")
		}
	}
}

func TestV0048RejectedOperatorActionIsAudited(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", false, nil))
	before := *e.store.Source(3, id)
	r := e.uiIfMatch("sources-settings-put", 3, map[string]any{"id": id}, map[string]any{"values": map[string]any{"project": "X"}}, `"rev-1"`)
	if r.Status != 412 {
		t.Fatalf("%d", r.Status)
	}
	audits := e.messages("source-controller.audit.source_settings_updated.pub")
	if len(audits) != 1 {
		t.Fatalf("audits %d", len(audits))
	}
	a := decode(t, audits[0].Data)
	if a["outcome"] != "rejected" || a["reason"] != "stale_revision" {
		t.Fatalf("audit %v", a)
	}
	if after := e.store.Source(3, id); after.Revision != before.Revision || after.Values["project"] != nil {
		t.Fatal("a rejected write changed settings")
	}
}
