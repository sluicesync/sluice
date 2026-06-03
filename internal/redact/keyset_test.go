// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

var sampleKeysetYAML = `
keyset:
  default: customer_pii
  keys:
    - name: customer_pii
      active: 3
      generations:
        - generation: 3
          created_at: 2026-05-15T00:00:00Z
          bytes: "` + b64("gen3-secret-material-aaaaaaaaaaaa") + `"
        - generation: 2
          created_at: 2026-03-01T00:00:00Z
          bytes: "` + b64("gen2-secret-material-bbbbbbbbbbbb") + `"
    - name: employee_pii
      active: 1
      generations:
        - generation: 1
          bytes: "` + b64("employee-secret-cccccccccccc") + `"
`

// TestKeyset_YAMLRoundTrip pins the YAML shape parses into the
// resolved keyset with the right generations + default.
func TestKeyset_YAMLRoundTrip(t *testing.T) {
	ks, err := LoadKeyset(context.Background(), "")
	if err != nil || ks != nil {
		t.Fatalf("empty source: want (nil,nil); got (%v,%v)", ks, err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "keyset.yaml")
	if err := os.WriteFile(path, []byte(sampleKeysetYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	ks, err = LoadKeyset(context.Background(), "file:"+path)
	if err != nil {
		t.Fatalf("LoadKeyset: %v", err)
	}
	if ks.Default != "customer_pii" {
		t.Errorf("Default = %q; want customer_pii", ks.Default)
	}
	if len(ks.Keys) != 2 {
		t.Fatalf("got %d keys; want 2", len(ks.Keys))
	}
	cp := ks.Keys["customer_pii"]
	if cp.Active != 3 || len(cp.Generations) != 2 {
		t.Errorf("customer_pii: active=%d gens=%d; want 3, 2", cp.Active, len(cp.Generations))
	}
	if !strings.HasPrefix(ks.Source, "file:") {
		t.Errorf("Source = %q; want file: prefix", ks.Source)
	}
}

// TestKeyset_ResolveKey covers the four name-resolution branches in
// ADR-0041 §"Strategy integration" + open-question #2.
func TestKeyset_ResolveKey(t *testing.T) {
	sole := &Keyset{Keys: map[string]KeysetKey{
		"only": {Name: "only", Active: 1, Generations: map[int]KeysetGeneration{1: {Generation: 1, Bytes: []byte("S")}}},
	}}
	multiDefault := &Keyset{Default: "d", Keys: map[string]KeysetKey{
		"d": {Name: "d", Active: 2, Generations: map[int]KeysetGeneration{2: {Generation: 2, Bytes: []byte("D")}}},
		"e": {Name: "e", Active: 1, Generations: map[int]KeysetGeneration{1: {Generation: 1, Bytes: []byte("E")}}},
	}}
	multiNoDefault := &Keyset{Keys: map[string]KeysetKey{
		"a": {Name: "a", Active: 1, Generations: map[int]KeysetGeneration{1: {Generation: 1, Bytes: []byte("A")}}},
		"b": {Name: "b", Active: 1, Generations: map[int]KeysetGeneration{1: {Generation: 1, Bytes: []byte("B")}}},
	}}

	t.Run("named", func(t *testing.T) {
		got, name, gen, err := multiDefault.ResolveKey("e")
		if err != nil || string(got) != "E" || name != "e" || gen != 1 {
			t.Errorf("named: got (%q,%q,%d,%v)", got, name, gen, err)
		}
	})
	t.Run("unnamed sole", func(t *testing.T) {
		got, _, _, err := sole.ResolveKey("")
		if err != nil || string(got) != "S" {
			t.Errorf("sole: got (%q,%v)", got, err)
		}
	})
	t.Run("unnamed default", func(t *testing.T) {
		got, name, _, err := multiDefault.ResolveKey("")
		if err != nil || string(got) != "D" || name != "d" {
			t.Errorf("default: got (%q,%q,%v)", got, name, err)
		}
	})
	t.Run("unnamed ambiguous refused", func(t *testing.T) {
		_, _, _, err := multiNoDefault.ResolveKey("")
		if err == nil || !strings.Contains(err.Error(), "no 'default'") {
			t.Errorf("ambiguous: want refusal mentioning default; got %v", err)
		}
	})
	t.Run("unknown name refused", func(t *testing.T) {
		_, _, _, err := sole.ResolveKey("nope")
		if err == nil || !strings.Contains(err.Error(), "not in the keyset") {
			t.Errorf("unknown: want refusal; got %v", err)
		}
	})
	t.Run("nil keyset refused", func(t *testing.T) {
		var k *Keyset
		_, _, _, err := k.ResolveKey("x")
		if err == nil || !strings.Contains(err.Error(), "no keyset is loaded") {
			t.Errorf("nil: want refusal; got %v", err)
		}
	})
}

// TestLoadKeyset_FileFailureModes covers the loud, actionable
// errors for the file: scheme.
func TestLoadKeyset_FileFailureModes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	if _, err := LoadKeyset(ctx, "bogus"); err == nil || !strings.Contains(err.Error(), "<scheme>:<value>") {
		t.Errorf("no-colon: want scheme refusal; got %v", err)
	}
	if _, err := LoadKeyset(ctx, "wat:x"); err == nil || !strings.Contains(err.Error(), "unknown scheme") {
		t.Errorf("unknown scheme: got %v", err)
	}
	if _, err := LoadKeyset(ctx, "file:/no/such/keyset.yaml"); err == nil {
		t.Errorf("missing file: want error")
	}
	empty := filepath.Join(dir, "empty.yaml")
	_ = os.WriteFile(empty, []byte("   \n"), 0o600)
	if _, err := LoadKeyset(ctx, "file:"+empty); err == nil || !strings.Contains(err.Error(), "file is empty") {
		t.Errorf("empty file: got %v", err)
	}
	nokeys := filepath.Join(dir, "nokeys.yaml")
	_ = os.WriteFile(nokeys, []byte("keyset:\n  keys: []\n"), 0o600)
	if _, err := LoadKeyset(ctx, "file:"+nokeys); err == nil || !strings.Contains(err.Error(), "no keys") {
		t.Errorf("no keys: got %v", err)
	}
	badb64 := filepath.Join(dir, "badb64.yaml")
	_ = os.WriteFile(badb64, []byte("keyset:\n  keys:\n    - name: k\n      active: 1\n      generations:\n        - generation: 1\n          bytes: \"!!!not-base64!!!\"\n"), 0o600)
	if _, err := LoadKeyset(ctx, "file:"+badb64); err == nil || !strings.Contains(err.Error(), "base64") {
		t.Errorf("bad base64: got %v", err)
	}
	badactive := filepath.Join(dir, "badactive.yaml")
	_ = os.WriteFile(badactive, []byte("keyset:\n  keys:\n    - name: k\n      active: 9\n      generations:\n        - generation: 1\n          bytes: \""+b64("xxxxxxxx")+"\"\n"), 0o600)
	if _, err := LoadKeyset(ctx, "file:"+badactive); err == nil || !strings.Contains(err.Error(), "active generation 9") {
		t.Errorf("bad active: got %v", err)
	}
}

// TestLoadKeyset_EnvFailureModes covers the env: scheme.
func TestLoadKeyset_EnvFailureModes(t *testing.T) {
	ctx := context.Background()
	if _, err := LoadKeyset(ctx, "env:"); err == nil || !strings.Contains(err.Error(), "variable name is empty") {
		t.Errorf("empty var name: got %v", err)
	}
	if _, err := LoadKeyset(ctx, "env:SLUICE_TEST_KEYSET_UNSET_XYZ"); err == nil || !strings.Contains(err.Error(), "empty or unset") {
		t.Errorf("unset var: got %v", err)
	}
	t.Setenv("SLUICE_TEST_KEYSET_YAML", sampleKeysetYAML)
	ks, err := LoadKeyset(ctx, "env:SLUICE_TEST_KEYSET_YAML")
	if err != nil {
		t.Fatalf("env happy: %v", err)
	}
	got, name, _, err := ks.ResolveKey("")
	if err != nil || name != "customer_pii" || len(got) == 0 {
		t.Errorf("env default resolve: (%q,%q,%v)", got, name, err)
	}
}

// TestLoadKeyset_DBUnregisteredOpener pins the loud error when no
// engine opener is registered (redact package alone, no engine
// blank-import in this test binary).
func TestLoadKeyset_DBUnregisteredOpener(t *testing.T) {
	_, err := LoadKeyset(context.Background(), "db:postgres://u@h:5432/db")
	if err == nil || !strings.Contains(err.Error(), "no keyset-store opener registered") {
		t.Errorf("db no-opener: want loud refusal; got %v", err)
	}
}

// TestKeysetFromRows covers the shared db: row-assembly helper.
func TestKeysetFromRows(t *testing.T) {
	rows := []KeysetRow{
		{Name: "k", Generation: 1, Bytes: []byte("g1"), Active: false},
		{Name: "k", Generation: 2, Bytes: []byte("g2"), Active: true},
	}
	ks, err := KeysetFromRows(rows, "db:test", "")
	if err != nil {
		t.Fatalf("KeysetFromRows: %v", err)
	}
	got, _, gen, err := ks.ResolveKey("k")
	if err != nil || string(got) != "g2" || gen != 2 {
		t.Errorf("resolve active: got (%q,%d,%v); want g2,2", got, gen, err)
	}

	if _, err := KeysetFromRows(nil, "db:test", ""); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty rows: want refusal; got %v", err)
	}
	noactive := []KeysetRow{{Name: "k", Generation: 1, Bytes: []byte("x"), Active: false}}
	if _, err := KeysetFromRows(noactive, "db:test", ""); err == nil || !strings.Contains(err.Error(), "no active=true") {
		t.Errorf("no active: want refusal; got %v", err)
	}
	twoactive := []KeysetRow{
		{Name: "k", Generation: 1, Bytes: []byte("a"), Active: true},
		{Name: "k", Generation: 2, Bytes: []byte("b"), Active: true},
	}
	if _, err := KeysetFromRows(twoactive, "db:test", ""); err == nil || !strings.Contains(err.Error(), "more than one active") {
		t.Errorf("two active: want refusal; got %v", err)
	}
}

// TestKeyset_AuditSummary pins the audit-line summary shape (no
// secret bytes; deterministic order).
func TestKeyset_AuditSummary(t *testing.T) {
	ks := &Keyset{Keys: map[string]KeysetKey{
		"z": {Name: "z", Active: 2, Generations: map[int]KeysetGeneration{1: {}, 2: {}}},
		"a": {Name: "a", Active: 1, Generations: map[int]KeysetGeneration{1: {}}},
	}}
	sum := ks.AuditSummary()
	if len(sum) != 2 || sum[0].Name != "a" || sum[1].Name != "z" {
		t.Fatalf("audit summary order wrong: %+v", sum)
	}
	if sum[1].Active != 2 || len(sum[1].Generations) != 2 {
		t.Errorf("z entry: %+v", sum[1])
	}
}

// TestKeyset_DeterminismContract pins ADR-0041 §"Determinism
// contract": a named key pins to its active generation regardless
// of rotation, and the same key bytes produce the same surrogate
// for hash + tokenize (cross-stream-stability primitive).
func TestKeyset_DeterminismContract(t *testing.T) {
	secret := []byte("shared-keyset-secret-material")

	// Same key → same hash surrogate, irrespective of strategy
	// instance (two "streams" sharing one keyset key).
	h1 := Hash{Algo: "hmac-sha256", Key: secret}
	h2 := Hash{Algo: "hmac-sha256", Key: secret}
	a, _ := h1.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
	b, _ := h2.Redact(&ir.Column{Name: "email"}, "alice@example.com", nil)
	if a != b {
		t.Errorf("hash: same key produced different surrogates: %v vs %v", a, b)
	}

	// Same key + same dict + same streamID → same tokenize entry.
	entries := []string{"P", "Q", "R", "S", "T", "U", "V", "W"}
	t1 := TokenizeDict{DictName: "d", Entries: entries, StreamID: "s", Key: secret}
	t2 := TokenizeDict{DictName: "d", Entries: entries, StreamID: "s", Key: secret}
	c, _ := t1.Redact(&ir.Column{Name: "n"}, "Alice", nil)
	d, _ := t2.Redact(&ir.Column{Name: "n"}, "Alice", nil)
	if c != d {
		t.Errorf("tokenize: same key produced different surrogates: %v vs %v", c, d)
	}

	// Named-key pinning: a keyset whose active generation differs
	// resolves the SAME bytes for a named reference (pinned to that
	// key's active gen) — rotation of a DIFFERENT key doesn't drift
	// it. Here ResolveKey("pinned") is stable across keyset
	// instances that share the pinned key's active generation bytes.
	ksA := &Keyset{Keys: map[string]KeysetKey{
		"pinned": {Name: "pinned", Active: 2, Generations: map[int]KeysetGeneration{
			1: {Generation: 1, Bytes: []byte("old")},
			2: {Generation: 2, Bytes: secret},
		}},
	}}
	got, _, gen, err := ksA.ResolveKey("pinned")
	if err != nil || !bytes.Equal(got, secret) || gen != 2 {
		t.Errorf("named-key pin: got (%q,%d,%v); want active gen 2 bytes", got, gen, err)
	}
}

// TestStrategyNeedsKeyButMissing covers the D2 defense-in-depth
// predicate the pipeline preflight uses.
func TestStrategyNeedsKeyButMissing(t *testing.T) {
	cases := []struct {
		name string
		s    Strategy
		want bool
	}{
		{"hmac no key", Hash{Algo: "hmac-sha256"}, true},
		{"hmac with key", Hash{Algo: "hmac-sha256", Key: []byte("k")}, false},
		{"sha256 no key", Hash{Algo: "sha256"}, false},
		{"tokenize no key", TokenizeDict{DictName: "d", Entries: []string{"x"}}, true},
		{"tokenize with key", TokenizeDict{DictName: "d", Entries: []string{"x"}, Key: []byte("k")}, false},
		{"null", Null{}, false},
	}
	for _, c := range cases {
		if got := StrategyNeedsKeyButMissing(c.s); got != c.want {
			t.Errorf("%s: got %v; want %v", c.name, got, c.want)
		}
	}
}
