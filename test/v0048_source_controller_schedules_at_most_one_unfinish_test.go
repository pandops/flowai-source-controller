//go:build !integration

package test

import (
	"context"
	"testing"
	"time"
)

// Coverage: source-controller schedules at most one unfinished poll per Source.

func TestV0048ReplicaStopsMidPoll(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.pollerSource(3, "poller-main")
	// Replica A starts the poll and stops before calling or finalizing it.
	polls, err := e.svc.StartDuePolls(context.Background())
	if err != nil || len(polls) != 1 {
		t.Fatalf("start: %v %d", err, len(polls))
	}
	p := polls[0]
	if !p.StartBy.Equal(p.LeasedAt.Add(5*time.Second)) || !p.CallDeadline.Equal(p.LeasedAt.Add(35*time.Second)) || !p.ExpiresAt.Equal(p.CallDeadline.Add(60*time.Second)) {
		t.Fatalf("windows %v %v %v %v", p.LeasedAt, p.StartBy, p.CallDeadline, p.ExpiresAt)
	}
	// Replica B: still unfinished, so no second poll starts even when due.
	due := *e.store.Source(3, id).NextPollAt
	if !due.Equal(p.LeasedAt.Add(10 * time.Minute)) {
		t.Fatalf("next due %v, want leased_at + interval", due)
	}
	e.makeDue(3, id)
	if again, _ := e.svc.StartDuePolls(context.Background()); len(again) != 0 {
		t.Fatal("a second poll started while one is unfinished")
	}
	// Restore the due time the poll start set (leased_at + interval).
	s := e.store.Source(3, id)
	s.NextPollAt = &due
	e.store.PutSource(*s)
	if n, _ := e.svc.FinalizeExpiredPolls(context.Background()); n != 0 {
		t.Fatal("finalized before expires_at")
	}
	e.clock.Advance(96 * time.Second)
	if n, err := e.svc.FinalizeExpiredPolls(context.Background()); n != 1 || err != nil {
		t.Fatalf("finalize expired: %d %v", n, err)
	}
	s = e.store.Source(3, id)
	if s.UnfinishedPollID != nil || s.LastPoll == nil || s.LastPoll.Status != "failed" || s.LastPoll.ErrorCode == nil || *s.LastPoll.ErrorCode != "lease_expired" || s.LastPoll.Outcome != nil || s.ConsecutiveFailures != 1 {
		t.Fatalf("after expiry %+v %+v", s, s.LastPoll)
	}
	// The next poll starts only at its due time.
	if again, _ := e.svc.StartDuePolls(context.Background()); len(again) != 0 {
		t.Fatal("successor started before its due time")
	}
	e.makeDue(3, id)
	if again, _ := e.svc.StartDuePolls(context.Background()); len(again) != 1 {
		t.Fatal("successor did not start at its due time")
	}
}

func TestV0048PollerIsDisabledOrIncomplete(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	disabled, _ := e.pollerSource(3, "disabled")
	e.ui("sources-disable", 3, map[string]any{"id": disabled}, nil)
	incomplete, key := e.createSource(3, "incomplete", "poller")
	e.registerSchema(key, "1.0.0", field("project", "string", true, nil))
	e.saveSettings(3, incomplete, map[string]any{"values": map[string]any{}, "poll_url": e.poller.URL + "/poll", "poll_interval_minutes": 5})
	if s := e.store.Source(3, incomplete); s.SettingsStatus != "incomplete" {
		t.Fatalf("status %s", s.SettingsStatus)
	}
	e.makeDue(3, disabled)
	e.makeDue(3, incomplete)
	if polls := e.pollOnce(); len(polls) != 0 || len(e.calls()) != 0 {
		t.Fatalf("polls %d calls %d", len(polls), len(e.calls()))
	}
}
