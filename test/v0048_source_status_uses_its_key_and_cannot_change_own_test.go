//go:build !integration

package test

import (
	"net/http"
	"testing"
	"time"
)

// Coverage: Source status uses its key and cannot change ownership.

func TestV0048SourceReportsStatusWithItsKey(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "hook")
	if v := sourceView(t, e, 3, id); v["health"] != "never_seen" {
		t.Fatalf("health %v", v["health"])
	}
	rec := e.httpDo(http.MethodPut, "/v1/sources/self/status", key, "application/json", mustJSON(statusBody("ingesting")))
	if rec.Code != 204 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	v := sourceView(t, e, 3, id)
	if v["activity"] != "ingesting" || v["health"] != "healthy" || v["last_seen_at"] == nil || v["observed_counters"] == nil {
		t.Fatalf("view %v", v)
	}
	if len(e.messages("source-controller.event.source_status_reported.pub")) != 1 {
		t.Fatal("no source_status_reported event")
	}
	body := statusBody("idle")
	body["team_id"] = 5
	if rec := e.httpDo(http.MethodPut, "/v1/sources/self/status", key, "application/json", mustJSON(body)); rec.Code != 400 {
		t.Fatalf("ownership field accepted: %d", rec.Code)
	}
}

func TestV0048StatusKeyIsMissingOrStale(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.webhookSource(3, "hook")
	if rec := e.httpDo(http.MethodPut, "/v1/sources/self/status", "", "application/json", mustJSON(statusBody("idle"))); rec.Code != 401 {
		t.Fatalf("missing key %d", rec.Code)
	}
	e.httpDo(http.MethodPut, "/v1/sources/self/status", key, "application/json", mustJSON(statusBody("idle")))
	e.clock.Advance(91 * time.Second)
	if v := sourceView(t, e, 3, id); v["health"] != "stale" {
		t.Fatalf("health %v", v["health"])
	}
}
