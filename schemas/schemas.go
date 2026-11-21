// Package schemas embeds the JSON Schemas of every message source-controller
// publishes and every request/reply body it answers. The files are the
// contract consumers read; this package lets the service validate what it
// publishes before it publishes it.
package schemas

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed *.json
var files embed.FS

// FS returns the embedded schema files.
func FS() embed.FS { return files }

var (
	once     sync.Once
	compiled map[string]*jsonschema.Schema
	loadErr  error
)

func load() {
	compiled = map[string]*jsonschema.Schema{}
	entries, err := files.ReadDir(".")
	if err != nil {
		loadErr = err
		return
	}
	compiler := jsonschema.NewCompiler()
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".schema.json") {
			continue
		}
		raw, err := files.ReadFile(name)
		if err != nil {
			loadErr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			loadErr = fmt.Errorf("%s: %w", name, err)
			return
		}
		if err := compiler.AddResource(name, doc); err != nil {
			loadErr = fmt.Errorf("%s: %w", name, err)
			return
		}
		names = append(names, name)
	}
	for _, name := range names {
		schema, err := compiler.Compile(name)
		if err != nil {
			loadErr = fmt.Errorf("%s: %w", name, err)
			return
		}
		compiled[name] = schema
	}
}

// Validate validates data against the schema file name (for example
// "event.poll_finalized.schema.json").
func Validate(name string, data []byte) error {
	once.Do(load)
	if loadErr != nil {
		return loadErr
	}
	schema, ok := compiled[name]
	if !ok {
		return fmt.Errorf("unknown schema %s", name)
	}
	if !json.Valid(data) {
		return fmt.Errorf("invalid JSON")
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return schema.Validate(value)
}

// ForSubject returns the schema file name of a subject source-controller
// publishes: source-controller.task.<team>.pub -> task.schema.json,
// source-controller.event.<kind>.pub -> event.<kind>.schema.json and
// source-controller.audit.<action>.pub -> audit.<action>.schema.json.
func ForSubject(subject string) (string, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != "source-controller" || parts[3] != "pub" {
		return "", fmt.Errorf("not a source-controller stream subject: %s", subject)
	}
	switch parts[1] {
	case "task":
		return "task.schema.json", nil
	case "event", "audit":
		return parts[1] + "." + parts[2] + ".schema.json", nil
	}
	return "", fmt.Errorf("not a source-controller stream subject: %s", subject)
}
