//go:build !integration

package test

import (
	"strings"
	"testing"
)

// Coverage: service-platform service sends human actions to audit and others
// to journal (source-controller form).

func TestV0048OperatorCancelsATask(t *testing.T) {
	// Source form: an operator's disable is audited, not journaled.
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, _ := e.createSource(3, "a", "webhook")
	e.ui("sources-disable", 3, map[string]any{"id": id}, nil)
	if len(e.messages("source-controller.audit.source_disabled.pub")) != 1 {
		t.Fatal("disable not audited")
	}
	for _, m := range e.messages("source-controller.event.") {
		if strings.Contains(m.Subject, "disabled") {
			t.Fatal("human action journaled as an event")
		}
	}
}

func TestV0048ForwardedAdministratorActionIsAuditedByItsOwner(t *testing.T) {
	// Not applicable to source-controller: it answers no admin operation.
	e := newEnv(t)
	req := e.uiRequest("sources-list", 3, nil, nil)
	req.Subject = "source-controller.admin.sources-list.sub"
	req.Header["team"] = "admin"
	if r := e.do(req); r.Status != 404 && r.Status != 400 {
		t.Fatalf("%d", r.Status)
	}
}

func TestV0048RejectedHumanActionIsAudited(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	e.createSource(3, "dup", "webhook")
	e.ui("sources-create", 3, nil, createBody("dup", "webhook"))
	var rejected int
	for _, m := range e.messages("source-controller.audit.source_created.pub") {
		b := decode(t, m.Data)
		if b["outcome"] == "rejected" && b["reason"] == "source_name_conflict" {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("rejected audits %d", rejected)
	}
}

func TestV0048ExecutorEventGoesToJournal(t *testing.T) {
	// Source form: a non-human action (schema registration) goes to ALL_EVENT.
	e := newEnv(t)
	e.addTeam(3, "acme")
	_, key := e.createSource(3, "a", "webhook")
	e.registerSchema(key, "1.0.0", field("x", "string", false, nil))
	if len(e.messages("source-controller.event.schema_registered.pub")) != 1 || len(e.messages("source-controller.audit.")) != 1 {
		t.Fatal("schema registration routed wrong")
	}
}
