// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestLoadYAML_RedactionOptionPresence pins the decode boundary the
// A0915-CFG-HIGH-1 fix depends on: for EVERY required numeric option
// of a `redactions:` entry (`length`, `m1`, `m2`, `min`, `max`), an
// omitted key must decode to nil and a present key — including the
// value 0, which is the whole point — must decode to a non-nil pointer
// holding that value. Config load is a codec (a YAML file round-trips
// through it), so this is the family matrix: each key × {omitted,
// present as 0, present as a non-zero value}.
//
// The `-1` cells exist because a decoder that clamped or dropped a
// negative would let the CLI layer's `< 0` refusals go dark.
func TestLoadYAML_RedactionOptionPresence(t *testing.T) {
	load := func(t *testing.T, body string) Redaction {
		t.Helper()
		path := filepath.Join(t.TempDir(), "sluice.yaml")
		yaml := "redactions:\n  - table: users.col\n    strategy: mask\n    form: inner\n" + body
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(path)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(c.Redactions) != 1 {
			t.Fatalf("got %d redactions; want 1", len(c.Redactions))
		}
		return c.Redactions[0]
	}

	t.Run("omitted keys are nil", func(t *testing.T) {
		r := load(t, "")
		if r.Length != nil || r.M1 != nil || r.M2 != nil || r.Min != nil || r.Max != nil {
			t.Fatalf("an omitted option decoded as present: %+v — the zero value would be indistinguishable from an operator's 0", r)
		}
	})

	intKeys := []struct {
		key string
		get func(Redaction) *int
	}{
		{"length", func(r Redaction) *int { return r.Length }},
		{"m1", func(r Redaction) *int { return r.M1 }},
		{"m2", func(r Redaction) *int { return r.M2 }},
	}
	for _, k := range intKeys {
		for _, v := range []int{0, 4, -1} {
			t.Run(k.key+"="+itoa(v), func(t *testing.T) {
				got := k.get(load(t, "    "+k.key+": "+itoa(v)+"\n"))
				if got == nil {
					t.Fatalf("`%s: %d` decoded as ABSENT (nil)", k.key, v)
				}
				if *got != v {
					t.Fatalf("`%s: %d` decoded as %d", k.key, v, *got)
				}
			})
		}
	}

	int64Keys := []struct {
		key string
		get func(Redaction) *int64
	}{
		{"min", func(r Redaction) *int64 { return r.Min }},
		{"max", func(r Redaction) *int64 { return r.Max }},
	}
	for _, k := range int64Keys {
		for _, v := range []int64{0, 9007199254740993, -1} { // 2^53+1: must not route through float64
			t.Run(k.key+"="+itoa64(v), func(t *testing.T) {
				got := k.get(load(t, "    "+k.key+": "+itoa64(v)+"\n"))
				if got == nil {
					t.Fatalf("`%s: %d` decoded as ABSENT (nil)", k.key, v)
				}
				if *got != v {
					t.Fatalf("`%s: %d` decoded as %d", k.key, v, *got)
				}
			})
		}
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
