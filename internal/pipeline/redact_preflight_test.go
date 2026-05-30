// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
)

// TestPreflightRedactTypes covers Bug 60's preflight refusal +
// every short-circuit path.
func TestPreflightRedactTypes(t *testing.T) {
	uuidCol := &ir.Column{Name: "id", Type: ir.UUID{}}
	textCol := &ir.Column{Name: "id", Type: ir.Text{}}
	emailCol := &ir.Column{Name: "email", Type: ir.Text{}}

	schemaWith := func(table string, cols ...*ir.Column) *ir.Schema {
		return &ir.Schema{
			Tables: []*ir.Table{{Name: table, Columns: cols}},
		}
	}

	t.Run("nil registry is no-op", func(t *testing.T) {
		if err := preflightRedactTypes(nil, schemaWith("users", uuidCol)); err != nil {
			t.Errorf("nil registry: got %v; want nil", err)
		}
	})
	t.Run("empty registry is no-op", func(t *testing.T) {
		if err := preflightRedactTypes(redact.New(), schemaWith("users", uuidCol)); err != nil {
			t.Errorf("empty registry: got %v; want nil", err)
		}
	})
	t.Run("nil schema is no-op", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		if err := preflightRedactTypes(r, nil); err != nil {
			t.Errorf("nil schema: got %v; want nil", err)
		}
	})

	t.Run("mask:uuid on UUID column refuses", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		err := preflightRedactTypes(r, schemaWith("users", uuidCol))
		if err == nil {
			t.Fatal("expected refusal; got nil")
		}
		if !errors.Is(err, errRedactTypeMismatch) {
			t.Errorf("error should wrap errRedactTypeMismatch; got %v", err)
		}
		msg := err.Error()
		for _, want := range []string{"users.id", "mask:uuid", "--type-override=users.id=text"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q should contain %q", msg, want)
			}
		}
	})

	t.Run("mask:uuid on text column passes (--type-override applied)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		if err := preflightRedactTypes(r, schemaWith("users", textCol)); err != nil {
			t.Errorf("got %v; want nil (type override should let mask:uuid pass)", err)
		}
	})

	t.Run("selector-unresolved table refuses loudly (Bug 99 / v0.91.1)", func(t *testing.T) {
		// Bug 99 (v0.91.1): a rule whose Table.Column doesn't resolve
		// to any column in the source schema is a typo-class silent
		// PII-leak hazard, NOT a "narrowed migration" no-op. The
		// pre-fix behaviour (silent skip on missing table) is the
		// exact failure mode the Bug-99 hotfix closes. Refuse loudly
		// at preflight so the operator's compliance posture is
		// visible at startup, not after PII has already moved.
		r := redact.New()
		r.Set("", "other_table", "id", redact.MaskUUID{})
		err := preflightRedactTypes(r, schemaWith("users", uuidCol))
		if err == nil {
			t.Fatal("got nil; want loud refusal — selector-unresolved silent skip = silent PII leak (Bug 99)")
		}
		if !errors.Is(err, errRedactSelectorUnresolved) {
			t.Errorf("want errRedactSelectorUnresolved sentinel; got: %v", err)
		}
		msg := err.Error()
		for _, want := range []string{"other_table.id", "typo"} {
			if !strings.Contains(msg, want) {
				t.Errorf("err should name the unresolved selector + hint typo class; got: %v", err)
				break
			}
		}
	})

	t.Run("typo'd column on hash:sha256 refuses loudly (Bug 99 / v0.91.1 canonical repro)", func(t *testing.T) {
		// Bug 99's CRITICAL silent-PII-loss repro: hash:sha256 has no
		// per-strategy preflight (no UUID check, no PK check, no
		// keyset check), so a typo'd column with this strategy hit
		// none of the existing guards — it silently passed preflight
		// and silently no-op'd at apply time. The selector-resolution
		// check is the load-bearing rejection that closes the leak
		// for strategies with no other type-level guard.
		r := redact.New()
		r.Set("", "users", "emial", redact.Hash{Algo: "sha256"}) // typo: "emial"
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{
					{Name: "id", Type: ir.UUID{}},
					{Name: "email", Type: ir.Text{}}, // the real column name
				}},
			},
		}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("got nil; want loud refusal — Bug 99: hash:sha256 typo silently no-ops → cleartext PII leak")
		}
		if !errors.Is(err, errRedactSelectorUnresolved) {
			t.Errorf("want errRedactSelectorUnresolved sentinel; got: %v", err)
		}
		if !strings.Contains(err.Error(), "users.emial") {
			t.Errorf("err should name the typo'd selector users.emial; got: %v", err)
		}
	})

	t.Run("non-mask:uuid strategy on UUID column passes", func(t *testing.T) {
		// hash:sha256 produces 64-char hex; lands as text in the operator's
		// target column choice. Out of scope for this preflight.
		r := redact.New()
		r.Set("", "users", "id", redact.Hash{Algo: "sha256"})
		if err := preflightRedactTypes(r, schemaWith("users", uuidCol)); err != nil {
			t.Errorf("got %v; want nil (hash:sha256 is not mask:uuid)", err)
		}
	})

	t.Run("mask:uuid on text + mask:email both pass", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		r.Set("", "users", "email", redact.MaskEmail{})
		schema := schemaWith("users", textCol, emailCol)
		if err := preflightRedactTypes(r, schema); err != nil {
			t.Errorf("got %v; want nil", err)
		}
	})

	t.Run("multiple offending rules are reported together", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})
		r.Set("", "orders", "uuid", redact.MaskUUID{})
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.UUID{}}}},
				{Name: "orders", Columns: []*ir.Column{{Name: "uuid", Type: ir.UUID{}}}},
			},
		}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("expected refusal; got nil")
		}
		msg := err.Error()
		for _, want := range []string{"users.id", "orders.uuid"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q should list %q (single-shot report)", msg, want)
			}
		}
	})

	// PII Phase 2.c (v0.59.0): randomize:* on a no-PK source table
	// refuses at startup. The strategy needs PK values to derive a
	// replay-stable seed; without a PK each row would draw an
	// unrelated random value on every run.
	t.Run("randomize:int on no-PK table refuses", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "age", redact.RandomizeInt{Min: 18, Max: 90})
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "age", Type: ir.Integer{Width: 32}}}, PrimaryKey: nil},
			},
		}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("expected refusal for randomize:int on no-PK table")
		}
		if !errors.Is(err, errRedactRandomizeNoPK) {
			t.Errorf("err should wrap errRedactRandomizeNoPK; got %v", err)
		}
		msg := err.Error()
		for _, want := range []string{"users.age", "randomize:int:18,90", "primary key"} {
			if !strings.Contains(msg, want) {
				t.Errorf("error %q should contain %q", msg, want)
			}
		}
	})

	t.Run("randomize:email on table WITH PK passes", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "email", redact.RandomizeEmail{})
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{
					Name: "users",
					Columns: []*ir.Column{
						{Name: "id", Type: ir.Integer{Width: 64}},
						{Name: "email", Type: ir.Text{}},
					},
					PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
				},
			},
		}
		if err := preflightRedactTypes(r, schema); err != nil {
			t.Errorf("got %v; want nil (table has PK)", err)
		}
	})

	t.Run("randomize:* on selector-unresolved table refuses loudly (Bug 99 / v0.91.1)", func(t *testing.T) {
		// Bug 99 (v0.91.1): same shape as the mask:uuid case above —
		// the selector-resolution check now fires before the
		// per-strategy randomize-no-PK check, so a randomize:* rule
		// against a typo'd table name refuses loudly with the
		// selector-unresolved sentinel (not the no-PK one).
		r := redact.New()
		r.Set("", "missing", "id", redact.RandomizeUUID{})
		schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}}}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("got nil; want loud refusal — selector-unresolved silent skip = silent PII leak (Bug 99)")
		}
		if !errors.Is(err, errRedactSelectorUnresolved) {
			t.Errorf("want errRedactSelectorUnresolved sentinel; got: %v", err)
		}
		if !strings.Contains(err.Error(), "missing.id") {
			t.Errorf("err should name the unresolved selector; got: %v", err)
		}
	})

	t.Run("mixed type-mismatch + randomize-no-PK refusals are reported together", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "id", redact.MaskUUID{})                      // type mismatch
		r.Set("", "events", "rng", redact.RandomizeInt{Min: 0, Max: 99}) // no PK
		schema := &ir.Schema{
			Tables: []*ir.Table{
				{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.UUID{}}}},
				{Name: "events", Columns: []*ir.Column{{Name: "rng", Type: ir.Integer{Width: 32}}}, PrimaryKey: nil},
			},
		}
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("expected refusal")
		}
		msg := err.Error()
		for _, want := range []string{"users.id", "events.rng"} {
			if !strings.Contains(msg, want) {
				t.Errorf("combined err %q should mention %q", msg, want)
			}
		}
	})

	t.Run("hash:hmac-sha256 with no keyset key refuses (D2)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "email", redact.Hash{Algo: "hmac-sha256"}) // no Key
		err := preflightRedactTypes(r, schemaWith("users", emailCol))
		if err == nil {
			t.Fatal("expected D2 keyset refusal; got nil")
		}
		if !errors.Is(err, errRedactKeysetMissing) {
			t.Errorf("error should wrap errRedactKeysetMissing; got %v", err)
		}
		for _, want := range []string{"users.email", "hash:hmac-sha256", "--keyset-source", "ADR-0041"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q should contain %q", err.Error(), want)
			}
		}
	})

	t.Run("tokenize:dict with no keyset key refuses (D2)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "email", redact.TokenizeDict{DictName: "names", Entries: []string{"x"}})
		err := preflightRedactTypes(r, schemaWith("users", emailCol))
		if err == nil || !errors.Is(err, errRedactKeysetMissing) {
			t.Fatalf("expected D2 keyset refusal wrapping errRedactKeysetMissing; got %v", err)
		}
	})

	t.Run("hash:hmac-sha256 with key passes", func(t *testing.T) {
		r := redact.New()
		r.Set("", "users", "email", redact.Hash{Algo: "hmac-sha256", Key: []byte("k")})
		if err := preflightRedactTypes(r, schemaWith("users", emailCol)); err != nil {
			t.Errorf("keyed hmac: got %v; want nil", err)
		}
	})

	// Bug 105 / v0.92.1 — randomize:int range-vs-column-width pins.
	// Pre-fix, an out-of-range Min/Max silently clamped to the column's
	// MAX at apply time (defeating randomization → PII compliance
	// failure). The new preflight surfaces it as a loud refusal.
	intCol := func(name string, width int8, unsigned bool) *ir.Column {
		return &ir.Column{Name: name, Type: ir.Integer{Width: width, Unsigned: unsigned}}
	}
	intSchemaWithPK := func(table string, pk string, cols ...*ir.Column) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{
			{Name: table, Columns: cols, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: pk}}}},
		}}
	}

	t.Run("randomize:int Min/Max fitting int32 column passes (Bug 105 control)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "events", "id", redact.RandomizeInt{Min: 0, Max: 999_999})
		schema := intSchemaWithPK("events", "id", intCol("id", 32, false))
		if err := preflightRedactTypes(r, schema); err != nil {
			t.Errorf("in-range randomize:int should pass; got: %v", err)
		}
	})

	t.Run("randomize:int Max overflows int32 column refuses (Bug 105 canonical repro)", func(t *testing.T) {
		r := redact.New()
		// Max=2_147_483_648 is INT32_MAX+1 — would silently clamp pre-fix.
		r.Set("", "events", "id", redact.RandomizeInt{Min: 0, Max: 2_147_483_648})
		schema := intSchemaWithPK("events", "id", intCol("id", 32, false))
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("got nil; want loud refusal — overflow would silently clamp to MAX (PII compliance failure)")
		}
		if !errors.Is(err, errRedactRandomizeRangeOverflow) {
			t.Errorf("want errRedactRandomizeRangeOverflow sentinel; got: %v", err)
		}
		for _, want := range []string{"events.id", "Max=2147483648", "[-2147483648,2147483647]", "PII compliance"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal should mention %q; got: %v", want, err)
				break
			}
		}
	})

	t.Run("randomize:int negative Min on unsigned column refuses (Bug 105 underflow shape)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "events", "id", redact.RandomizeInt{Min: -1, Max: 100})
		// UNSIGNED INT — negative Min is out-of-range.
		schema := intSchemaWithPK("events", "id", intCol("id", 32, true))
		err := preflightRedactTypes(r, schema)
		if err == nil {
			t.Fatal("got nil; want refusal on negative Min for UNSIGNED column")
		}
		if !errors.Is(err, errRedactRandomizeRangeOverflow) {
			t.Errorf("want errRedactRandomizeRangeOverflow sentinel; got: %v", err)
		}
	})

	t.Run("randomize:int on non-integer column passes (no false positives)", func(t *testing.T) {
		r := redact.New()
		r.Set("", "events", "tag", redact.RandomizeInt{Min: 0, Max: 999_999_999_999})
		// Column is TEXT — let the DB enforce type compatibility.
		schema := intSchemaWithPK("events", "tag", &ir.Column{Name: "tag", Type: ir.Text{Size: ir.TextLong}})
		if err := preflightRedactTypes(r, schema); err != nil {
			t.Errorf("non-integer column should pass without the range check; got: %v", err)
		}
	})

	t.Run("randomize:int int8 / int16 / int24 / int64 boundary pins", func(t *testing.T) {
		cases := []struct {
			name     string
			width    int8
			unsigned bool
			ok       []redact.RandomizeInt
			refused  []redact.RandomizeInt
		}{
			{
				name: "int8 signed", width: 8, unsigned: false,
				ok:      []redact.RandomizeInt{{Min: -128, Max: 127}, {Min: 0, Max: 99}},
				refused: []redact.RandomizeInt{{Min: -129, Max: 127}, {Min: 0, Max: 128}},
			},
			{
				name: "uint16", width: 16, unsigned: true,
				ok:      []redact.RandomizeInt{{Min: 0, Max: 65535}, {Min: 100, Max: 65535}},
				refused: []redact.RandomizeInt{{Min: -1, Max: 65535}, {Min: 0, Max: 65536}},
			},
			{
				name: "int24 signed (MySQL MEDIUMINT)", width: 24, unsigned: false,
				ok:      []redact.RandomizeInt{{Min: -8388608, Max: 8388607}},
				refused: []redact.RandomizeInt{{Min: 0, Max: 8388608}},
			},
			{
				name: "int64 signed", width: 64, unsigned: false,
				ok: []redact.RandomizeInt{{Min: -1 << 62, Max: 1 << 62}},
				// int64 range is [MinInt64, MaxInt64] — no Min/Max can
				// exceed without overflowing int64 itself, so the
				// "refused" list is empty for this width.
				refused: nil,
			},
		}
		for _, c := range cases {
			c := c
			t.Run(c.name, func(t *testing.T) {
				col := intCol("id", c.width, c.unsigned)
				schema := intSchemaWithPK("events", "id", col)
				for _, ok := range c.ok {
					r := redact.New()
					r.Set("", "events", "id", ok)
					if err := preflightRedactTypes(r, schema); err != nil {
						t.Errorf("%s in-range %v: got %v; want nil", c.name, ok, err)
					}
				}
				for _, ref := range c.refused {
					r := redact.New()
					r.Set("", "events", "id", ref)
					err := preflightRedactTypes(r, schema)
					if err == nil {
						t.Errorf("%s out-of-range %v: got nil; want refusal", c.name, ref)
						continue
					}
					if !errors.Is(err, errRedactRandomizeRangeOverflow) {
						t.Errorf("%s out-of-range %v: want errRedactRandomizeRangeOverflow; got: %v", c.name, ref, err)
					}
				}
			})
		}
	})
}
