// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	tryLoad := func(t *testing.T, body string) (Redaction, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "sluice.yaml")
		yaml := "redactions:\n  - table: users.col\n    strategy: mask\n    form: inner\n" + body
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			t.Fatal(err)
		}
		c, err := Load(path)
		if err != nil {
			return Redaction{}, err
		}
		if len(c.Redactions) != 1 {
			t.Fatalf("got %d redactions; want 1", len(c.Redactions))
		}
		return c.Redactions[0], nil
	}
	load := func(t *testing.T, body string) Redaction {
		t.Helper()
		r, err := tryLoad(t, body)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		return r
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

	// The SPELLING matrix (pre-tag value-fidelity review of v0.153.2):
	// every option × every YAML spelling that is not an integer must
	// REFUSE at load, naming the key — WeaklyTypedInput would otherwise
	// manufacture a present integer (`""` → 0 is the zero-value
	// substitution this fix headlines, through a different door; 2^63
	// wraps to -2^63; 5.7 truncates; true becomes 1) where the CLI's
	// strconv refuses the same text. And every spelling YAML resolves
	// to a real integer must keep decoding exactly as it did.
	allKeys := []struct {
		key string
		get func(Redaction) (present bool, value int64)
	}{
		{"length", func(r Redaction) (bool, int64) { return r.Length != nil, int64(deref(r.Length)) }},
		{"m1", func(r Redaction) (bool, int64) { return r.M1 != nil, int64(deref(r.M1)) }},
		{"m2", func(r Redaction) (bool, int64) { return r.M2 != nil, int64(deref(r.M2)) }},
		{"min", func(r Redaction) (bool, int64) { return r.Min != nil, deref(r.Min) }},
		{"max", func(r Redaction) (bool, int64) { return r.Max != nil, deref(r.Max) }},
	}
	refused := []struct{ name, spelling string }{
		{"empty string", `""`},
		{"bare colon (empty scalar, quoted form)", `''`},
		{"non-integer float", `5.7`},
		{"boolean", `true`},
		{"2^63 (would wrap to -2^63)", `9223372036854775808`},
		{"-2^63-1", `-9223372036854775809`},
		{"non-integer float, exponent form", `1.5e0`},
		{"quoted non-integer", `"5.7"`},
		{"quoted word", `"five"`},
	}
	kept := []struct {
		name, spelling string
		want           int64
	}{
		{"quoted integer", `"5"`, 5},
		{"negative zero", `-0`, 0},
		{"integral exponent float", `1e3`, 1000},
		{"hex", `0x10`, 16},
	}
	for _, k := range allKeys {
		for _, c := range refused {
			t.Run(k.key+" refuses "+c.name, func(t *testing.T) {
				_, err := tryLoad(t, "    "+k.key+": "+c.spelling+"\n")
				if err == nil {
					t.Fatalf("`%s: %s` LOADED — a non-integer spelling decoded as a present integer instead of refusing (WeaklyTypedInput coercion)", k.key, c.spelling)
				}
				if !strings.Contains(err.Error(), k.key) {
					t.Fatalf("`%s: %s` refused without naming the key: %v", k.key, c.spelling, err)
				}
			})
		}
		for _, c := range kept {
			t.Run(k.key+" keeps "+c.name, func(t *testing.T) {
				present, got := k.get(load(t, "    "+k.key+": "+c.spelling+"\n"))
				if !present || got != c.want {
					t.Fatalf("`%s: %s` decoded as present=%v value=%d; want present %d (unchanged from before the spelling rule)", k.key, c.spelling, present, got, c.want)
				}
			})
		}
		for _, null := range []string{"null", "~"} {
			t.Run(k.key+" "+null+" is omitted", func(t *testing.T) {
				if present, _ := k.get(load(t, "    "+k.key+": "+null+"\n")); present {
					t.Fatalf("`%s: %s` decoded as PRESENT; want nil (refused downstream as omitted)", k.key, null)
				}
			})
		}
	}
}

func deref[T int | int64](p *T) T {
	if p == nil {
		return 0
	}
	return *p
}

func itoa(n int) string { return strconv.Itoa(n) }

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
