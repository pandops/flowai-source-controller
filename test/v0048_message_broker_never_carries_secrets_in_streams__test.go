//go:build !integration

package test

import (
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
)

// Coverage: message-broker never carries secrets in streams or logs — stream
// content inspection.

func TestV0048StreamContentInspection(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("token_label", "string", false, nil))
	e.saveSettings(3, id, map[string]any{"values": map[string]any{"token_label": "SETTING-SECRET-VALUE"}})
	r := e.ui("sources-rotate-key", 3, map[string]any{"id": id}, nil)
	fresh := r.Body["api_key"].(string)
	e.webhook("acme", "jira-main", fresh, mustJSON(task("A")), false)
	e.httpDo(http.MethodPut, "/v1/sources/self/status", fresh, "application/json", mustJSON(statusBody("idle")))
	pid, _ := e.pollerSource(3, "poller-main")
	e.setPoll(pages(t, func(int) (int, any) {
		return 200, map[string]any{"tasks": []any{map[string]any{"external_id": "P", "payload": map[string]any{"body": "PAYLOAD-SECRET"}}}, "next_cursor": nil}
	}))
	e.pollOnce()
	digest := e.store.Source(3, id).KeyDigest
	secrets := []string{key, fresh, key[7:], fresh[7:], "SETTING-SECRET-VALUE", hex.EncodeToString(digest), base64.StdEncoding.EncodeToString(digest), e.calls()[0].Signature}
	for _, m := range e.messages("") {
		for _, s := range secrets {
			if s != "" && strings.Contains(string(m.Data), s) {
				t.Fatalf("%s carries a secret", m.Subject)
			}
		}
		if !strings.HasPrefix(m.Subject, "source-controller.task.") && strings.Contains(string(m.Data), "PAYLOAD-SECRET") {
			t.Fatalf("%s carries a task payload", m.Subject)
		}
	}
	_ = pid
}
