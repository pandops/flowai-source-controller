//go:build !integration

package test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/pandops/flowai-source-controller/internal/core"
)

// Coverage: message-broker message payloads follow publisher-owned JSON
// Schemas.

func TestV0048PublisherMessageViolatesItsOwnSchema(t *testing.T) {
	e := newEnv(t)
	e.addTeam(3, "acme")
	// An outbox row whose payload violates its schema is never published.
	bad := core.OutboxMessage{Seq: 999, Subject: "source-controller.event.poll_finalized.pub", Header: map[string]string{"team": "3", "Nats-Msg-Id": "source-controller-999"}, Data: []byte(`{"publisher":"source-controller","publisher_seq":999,"team_id":3}`)}
	if err := e.store.Tx(context.Background(), func(tx core.Tx) error { return tx.Enqueue(bad) }); err != nil {
		t.Fatal(err)
	}
	_, _ = e.svc.FlushOutbox(context.Background())
	for _, m := range e.broker.Published() {
		if m.Subject == bad.Subject {
			t.Fatal("an invalid message was published")
		}
	}
}

func contractFixtures(t *testing.T) *os.Root {
	t.Helper()
	dir := os.Getenv("FLOWAI_CONTRACT_FIXTURE_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("FLOWAI_CONTRACT_FIXTURE_DIR must name an absolute external fixture directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

func compileForeign(t *testing.T, root *os.Root, dir, name string, refs ...string) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.UseLoader(jsonschema.SchemeURLLoader{})
	for _, file := range append([]string{name}, refs...) {
		raw, err := root.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		var meta struct {
			ID string `json:"$id"`
		}
		if err := json.Unmarshal(raw, &meta); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		if err := c.AddResource(file, doc); err != nil {
			t.Fatal(err)
		}
		if meta.ID != "" && meta.ID != file {
			if err := c.AddResource(meta.ID, doc); err != nil {
				t.Fatal(err)
			}
		}
	}
	s, err := c.Compile(name)
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return s
}

func TestV0048ConsumerContractTestReadsThePublisherSchema(t *testing.T) {
	e := newEnv(t)
	root := contractFixtures(t)
	team := compileForeign(t, root, "team-registry", "event.team_updated.schema.json")
	tm := compileForeign(t, root, "task-manager", "event.intake_processed.schema.json", "defs.schema.json")
	for schema, payload := range map[*jsonschema.Schema]map[string]any{team: e.teamEvent("team_updated", 3, "acme", false), tm: e.intakeProcessed(3, 1, "accepted")} {
		raw, _ := json.Marshal(payload)
		doc, _ := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
		if err := schema.Validate(doc); err != nil {
			t.Fatalf("consumer fixture violates the publisher schema: %v", err)
		}
	}
}

func TestV0048SchemaChangeSelectsItsConsumers(t *testing.T) {
	raw, err := contractFixtures(t).ReadFile("shared-test-inputs.json")
	if err != nil {
		t.Fatal(err)
	}
	var inputs struct {
		Schemas map[string][]string `json:"schemas"`
	}
	if err := json.Unmarshal(raw, &inputs); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(serviceDir(), "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		if !strings.HasSuffix(en.Name(), ".schema.json") {
			continue
		}
		owners := inputs.Schemas["svc/source-controller/schemas/"+en.Name()]
		if len(owners) < 2 || owners[0] != "source-controller" {
			t.Fatalf("%s is not declared with its consumers: %v", en.Name(), owners)
		}
	}
	for _, consumed := range []string{"svc/team-registry/schemas/event.team_updated.schema.json", "svc/task-manager/schemas/event.intake_processed.schema.json"} {
		found := false
		for _, s := range inputs.Schemas[consumed] {
			found = found || s == "source-controller"
		}
		if !found {
			t.Fatalf("%s does not select source-controller", consumed)
		}
	}
}
