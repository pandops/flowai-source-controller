//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Coverage: service-platform service stays independent from execution and
// identity (source-controller form).

func TestV0048ExecutorCapacityIsNotDecidedCentrally(t *testing.T) {
	// Not applicable to source-controller: it publishes intake regardless of
	// any Executor capacity and makes no Executor request.
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "hook")
	e.webhook("acme", "hook", key, mustJSON(task("A")), false)
	for _, r := range e.broker.Requests() {
		if strings.Contains(r.Subject, "executor") {
			t.Fatalf("request to %s", r.Subject)
		}
	}
}

func TestV0048ImageEqualityAcrossTeamsGrantsNoAuthority(t *testing.T) {
	// Not applicable to source-controller: it stores no image. Equal Source
	// names across teams grant nothing either.
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.addTeam(5, "globex")
	_, k3 := e.webhookSource(3, "same")
	e.webhookSource(5, "same")
	if rec := e.webhook("globex", "same", k3, mustJSON(task("A")), false); rec.Code != 401 {
		t.Fatalf("%d", rec.Code)
	}
}

func TestV0048FlowCannotBeCreatedFromASourcePath(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.webhookSource(3, "hook")
	body := task("A")
	body["flow_id"] = 1
	if rec := e.webhook("acme", "hook", key, mustJSON(body), false); rec.Code != 400 {
		t.Fatalf("%d", rec.Code)
	}
	for _, r := range e.broker.Requests() {
		if strings.HasPrefix(r.Subject, "flow-registry.") {
			t.Fatal("a Source path called flow-registry")
		}
	}
}
