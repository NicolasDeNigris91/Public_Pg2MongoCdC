package mapper_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"transformer/internal/mapper"
)

func writeRuleFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestApplyJSON_RenamesFieldsInAfterAndBefore(t *testing.T) {
	dir := t.TempDir()
	writeRuleFile(t, dir, "users.yml", `
source: public.users
target: users
fields:
  full_name: { type: string, target: fullName }
  created_at: { type: timestamptz, target: createdAt }
`)

	m, err := mapper.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	in := []byte(`{
		"payload": {
			"before": {"id": 1, "full_name": "Old"},
			"after":  {"id": 1, "full_name": "New", "email": "a@b.c", "created_at": "2026-01-01"},
			"source": {"lsn": 100, "table": "users"},
			"op": "u"
		}
	}`)

	out, err := m.ApplyJSON("cdc.users", in)
	if err != nil {
		t.Fatalf("ApplyJSON: %v", err)
	}

	var env map[string]any
	_ = json.Unmarshal(out, &env)
	payload := env["payload"].(map[string]any)

	after := payload["after"].(map[string]any)
	if _, bad := after["full_name"]; bad {
		t.Errorf("after still has full_name: %v", after)
	}
	if got := after["fullName"]; got != "New" {
		t.Errorf("after.fullName: want 'New', got %v", got)
	}
	if got := after["createdAt"]; got != "2026-01-01" {
		t.Errorf("after.createdAt: want date, got %v", got)
	}
	// Fields without a rule entry pass through unchanged.
	if got := after["email"]; got != "a@b.c" {
		t.Errorf("after.email passthrough: got %v", got)
	}

	before := payload["before"].(map[string]any)
	if _, bad := before["full_name"]; bad {
		t.Errorf("before still has full_name: %v", before)
	}
	if got := before["fullName"]; got != "Old" {
		t.Errorf("before.fullName: got %v", got)
	}
}

func TestApplyJSON_NoRuleForTopicPassesThrough(t *testing.T) {
	m, err := mapper.Load(t.TempDir()) // empty rules dir
	if err != nil {
		t.Fatal(err)
	}
	in := []byte(`{"payload":{"after":{"id":1,"full_name":"X"}}}`)
	out, _ := m.ApplyJSON("cdc.orders", in)
	// Bytes may differ (re-serialization is not attempted when no rule), but
	// semantic content must match.
	if string(out) != string(in) {
		t.Errorf("pass-through expected, got mutation:\n want %s\n got  %s", in, out)
	}
}

func TestApplyJSON_TombstoneOrNonEnvelopePassesThrough(t *testing.T) {
	dir := t.TempDir()
	writeRuleFile(t, dir, "users.yml", `
source: public.users
target: users
fields:
  full_name: { target: fullName }
`)
	m, _ := mapper.Load(dir)

	out, err := m.ApplyJSON("cdc.users", []byte(`null`))
	if err != nil {
		t.Fatalf("unexpected error for null payload: %v", err)
	}
	if string(out) != "null" {
		t.Errorf("want null passed through, got %s", out)
	}
}

// Two schemas with a same-named table must not collide on the same rule.
// Before the qname index this test was impossible to write because both
// "public.users" and "audit.users" resolved to the bare key "users".
func TestApplyJSON_MultiSchemaIsolation(t *testing.T) {
	dir := t.TempDir()
	writeRuleFile(t, dir, "public_users.yml", `
source: public.users
target: users
fields:
  full_name: { target: fullName }
`)
	writeRuleFile(t, dir, "audit_users.yml", `
source: audit.users
target: audit_users
fields:
  full_name: { target: fullNameAudited }
`)
	m, err := mapper.Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	publicEv := []byte(`{"payload":{
		"after":{"id":1,"full_name":"Alice"},
		"source":{"schema":"public","table":"users","lsn":1},"op":"c"
	}}`)
	auditEv := []byte(`{"payload":{
		"after":{"id":1,"full_name":"Alice"},
		"source":{"schema":"audit","table":"users","lsn":1},"op":"c"
	}}`)

	pubOut, err := m.ApplyJSON("cdc.public.users", publicEv)
	if err != nil {
		t.Fatalf("public ApplyJSON: %v", err)
	}
	audOut, err := m.ApplyJSON("cdc.audit.users", auditEv)
	if err != nil {
		t.Fatalf("audit ApplyJSON: %v", err)
	}

	var pub, aud map[string]any
	_ = json.Unmarshal(pubOut, &pub)
	_ = json.Unmarshal(audOut, &aud)
	pubAfter := pub["payload"].(map[string]any)["after"].(map[string]any)
	audAfter := aud["payload"].(map[string]any)["after"].(map[string]any)

	if _, ok := pubAfter["fullName"]; !ok {
		t.Errorf("public.users rule did not apply: %v", pubAfter)
	}
	if _, bad := pubAfter["fullNameAudited"]; bad {
		t.Errorf("public.users picked up audit.users rule: %v", pubAfter)
	}
	if _, ok := audAfter["fullNameAudited"]; !ok {
		t.Errorf("audit.users rule did not apply: %v", audAfter)
	}
	if _, bad := audAfter["fullName"]; bad {
		t.Errorf("audit.users picked up public.users rule: %v", audAfter)
	}
}

// When the envelope is missing source.{schema,table} (some snapshot reads
// or hand-published events), we still want the topic-tail fallback to
// resolve a single-schema rule rather than silently passing through.
func TestApplyJSON_TopicTailFallbackWhenSourceMissing(t *testing.T) {
	dir := t.TempDir()
	writeRuleFile(t, dir, "users.yml", `
source: public.users
target: users
fields:
  full_name: { target: fullName }
`)
	m, _ := mapper.Load(dir)

	// Envelope with NO source block.
	in := []byte(`{"payload":{"after":{"id":1,"full_name":"Alice"},"op":"c"}}`)

	out, err := m.ApplyJSON("cdc.public.users", in)
	if err != nil {
		t.Fatalf("ApplyJSON: %v", err)
	}
	var env map[string]any
	_ = json.Unmarshal(out, &env)
	after := env["payload"].(map[string]any)["after"].(map[string]any)
	if _, ok := after["fullName"]; !ok {
		t.Errorf("topic-tail fallback failed: %v", after)
	}
}

func TestLoad_DuplicateSourceIsError(t *testing.T) {
	dir := t.TempDir()
	writeRuleFile(t, dir, "a.yml", "source: public.users\ntarget: users\nfields: {}\n")
	writeRuleFile(t, dir, "b.yml", "source: public.users\ntarget: users_v2\nfields: {}\n")
	if _, err := mapper.Load(dir); err == nil {
		t.Fatal("want error on duplicate source, got nil")
	}
}
