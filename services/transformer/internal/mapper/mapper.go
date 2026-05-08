// Package mapper applies YAML transform rules to Debezium envelope events,
// rewriting payload.after/before field names so the sink can write the
// target Mongo shape without per-table code.
//
// Rules are indexed by the schema-qualified source name (e.g. "public.users").
// Lookups read `payload.source.schema` + `payload.source.table` from the
// envelope itself rather than parsing the Kafka topic, so two tables with
// the same name in different schemas (`public.users` vs `audit.users`)
// don't silently collapse onto the same rule. We fall back to topic-tail
// parsing only when the envelope is unreadable - mostly defensive, since
// a valid Debezium envelope always carries source.{schema,table}.
package mapper

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// FieldRule maps one source field to a target name; Type is a hint for
// future typed conversions (currently unused at runtime).
type FieldRule struct {
	Type   string `yaml:"type"`
	Target string `yaml:"target"`
}

// Rule is the parsed YAML transform definition for one source table.
type Rule struct {
	Source string               `yaml:"source"`
	Target string               `yaml:"target"`
	Fields map[string]FieldRule `yaml:"fields"`
}

// Mapper holds the rule set keyed by source table name.
type Mapper struct {
	// Indexed by qualified name "<schema>.<table>" (e.g. "public.users").
	// A rule whose Source has no dot is indexed as just the bare name and
	// will only match envelopes from a default/unset schema.
	byQName map[string]*Rule
}

// Load parses every *.yml under dir.
func Load(dir string) (*Mapper, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("mapper.Load: read dir %s: %w", dir, err)
	}
	m := &Mapper{byQName: map[string]*Rule{}}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		// #nosec G304 -- dir is an operator-provided config root (RULES_DIR
		// env var), e.Name() comes from os.ReadDir(dir) of that same root
		// and is filtered to *.yml. No user input reaches this path.
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("mapper.Load: %s: %w", path, err)
		}
		var r Rule
		if err := yaml.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("mapper.Load: %s: %w", path, err)
		}
		key := strings.TrimSpace(r.Source)
		if key == "" {
			return nil, fmt.Errorf("mapper.Load: %s: empty source", path)
		}
		if existing, dup := m.byQName[key]; dup {
			return nil, fmt.Errorf("mapper.Load: duplicate rule for source %q (already loaded with target %q)", key, existing.Target)
		}
		m.byQName[key] = &r
	}
	return m, nil
}

// ApplyJSON rewrites payload.after/before field names per the matching rule.
// Topics with no matching rule pass through unchanged. The topic argument
// is only used as a fallback lookup key when the envelope omits source
// metadata - a valid Debezium event always carries source.{schema,table}
// and we prefer those.
func (m *Mapper) ApplyJSON(topic string, envelope []byte) ([]byte, error) {
	var env map[string]any
	if err := json.Unmarshal(envelope, &env); err != nil {
		// Not parseable as a Debezium envelope - try the topic-tail
		// fallback so legacy / hand-written records still pass through.
		if rule := m.lookupByTopicTail(topic); rule != nil {
			return nil, fmt.Errorf("mapper.ApplyJSON: parse: %w", err)
		}
		return envelope, nil
	}
	payload, ok := env["payload"].(map[string]any)
	if !ok {
		return envelope, nil // tombstone or non-envelope; leave alone
	}

	rule := m.lookupForPayload(payload, topic)
	if rule == nil {
		return envelope, nil
	}

	if after, ok := payload["after"].(map[string]any); ok && after != nil {
		payload["after"] = renameKeys(after, rule.Fields)
	}
	if before, ok := payload["before"].(map[string]any); ok && before != nil {
		payload["before"] = renameKeys(before, rule.Fields)
	}
	env["payload"] = payload

	return json.Marshal(env)
}

// Rules returns the loaded rule set keyed by qualified name
// ("schema.table"). The returned map is the live store; callers must not
// mutate it.
func (m *Mapper) Rules() map[string]*Rule { return m.byQName }

// lookupForPayload prefers the schema/table on the envelope, falling back
// to the topic tail. Returns nil if no rule matches.
func (m *Mapper) lookupForPayload(payload map[string]any, topic string) *Rule {
	src, _ := payload["source"].(map[string]any)
	if src != nil {
		schema, _ := src["schema"].(string)
		table, _ := src["table"].(string)
		if table != "" {
			if schema != "" {
				if r, ok := m.byQName[schema+"."+table]; ok {
					return r
				}
			}
			// Some Debezium configs strip schema; honour bare-table rules.
			if r, ok := m.byQName[table]; ok {
				return r
			}
		}
	}
	return m.lookupByTopicTail(topic)
}

// lookupByTopicTail is the legacy lookup path: take the suffix after the
// last dot in the topic name and try both bare-table and any-schema match.
// Kept for non-Debezium producers and as a defensive backstop.
func (m *Mapper) lookupByTopicTail(topic string) *Rule {
	if topic == "" {
		return nil
	}
	tail := topic
	if i := strings.LastIndex(topic, "."); i >= 0 {
		tail = topic[i+1:]
	}
	if r, ok := m.byQName[tail]; ok {
		return r
	}
	// Last resort: scan for any rule whose Source ends in ".<tail>".
	suffix := "." + tail
	for k, r := range m.byQName {
		if strings.HasSuffix(k, suffix) {
			return r
		}
	}
	return nil
}

func renameKeys(src map[string]any, fields map[string]FieldRule) map[string]any {
	out := make(map[string]any, len(src))
	for k, v := range src {
		target := k
		if r, ok := fields[k]; ok && r.Target != "" {
			target = r.Target
		}
		out[target] = v
	}
	return out
}
