//go:build !integration

package test

import (
	"net/http"
	"testing"
)

// Coverage: Webhook sources read their own settings.

func TestV0048WebhookSourceRefreshesSettings(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	id, key := e.createSource(3, "jira-main", "webhook")
	e.registerSchema(key, "1.0.0", field("project", "string", false, "OPS"))
	e.saveSettings(3, id, map[string]any{"values": map[string]any{"project": "NEW"}})
	rec := e.httpDo(http.MethodGet, "/v1/sources/self/settings", key, "", "")
	if rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	b := decode(t, rec.Body.Bytes())
	if b["values"].(map[string]any)["project"] != "NEW" || b["webhook_url"] != publicBase+"/webhook/acme/jira-main/task" ||
		num(b["revision"]) != int64(e.store.Source(3, id).Revision) || b["settings_status"] != "complete" || len(b) != 4 {
		t.Fatalf("%v", b)
	}
}
