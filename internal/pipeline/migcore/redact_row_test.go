// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pins for RedactRow's randomize:* PK seed lookup (audit 2026-08-26
// R-1). This is the bulk-copy/pipeline SIBLING of the CDC-side
// Registry.ApplyRow site — both must route PK extraction through
// redact.PKValuesFromRow, so a case-diverged PK spelling derives the
// same seed as the exact spelling and a genuinely missing PK key
// refuses loudly instead of silently seeding every row from nil. The
// full strategy × lookup-shape matrix lives with the redact package
// (TestRandomizeSeedLookup_FamilyMatrix); this file pins that THIS
// call site inherits it.

package migcore

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/redact"
)

func redactRowFixture() (*redact.Registry, []*ir.Column) {
	reg := redact.New()
	reg.Set("", "users", "payload", redact.RandomizeEmail{})
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "payload", Type: ir.Text{}},
	}
	return reg, cols
}

func TestRedactRow_PKSeedLookup(t *testing.T) {
	t.Run("case-diverged PK spelling derives the exact-spelling seed", func(t *testing.T) {
		reg, cols := redactRowFixture()
		exact := ir.Row{"id": int64(7), "payload": "secret"}
		diverged := ir.Row{"ID": int64(7), "payload": "secret"}
		if err := RedactRow(reg, "", "users", exact, cols, []string{"id"}, "s1"); err != nil {
			t.Fatalf("exact-spelling RedactRow: %v", err)
		}
		if err := RedactRow(reg, "", "users", diverged, cols, []string{"id"}, "s1"); err != nil {
			t.Fatalf("case-diverged RedactRow: %v", err)
		}
		if exact["payload"] == "secret" {
			t.Fatal("payload untouched; want redacted")
		}
		if exact["payload"] != diverged["payload"] {
			t.Fatalf("case-diverged row redacted to %v; want the exact-spelling value %v (same PK value must derive the same seed)", diverged["payload"], exact["payload"])
		}
	})

	t.Run("missing PK key refuses loudly", func(t *testing.T) {
		reg, cols := redactRowFixture()
		row := ir.Row{"payload": "secret"}
		err := RedactRow(reg, "", "users", row, cols, []string{"id"}, "s1")
		if err == nil {
			t.Fatal("RedactRow succeeded with the PK column absent from the row; want the R-1 loud refusal")
		}
		if !errors.Is(err, redact.ErrPKColumnMissing) {
			t.Fatalf("error is not redact.ErrPKColumnMissing: %v", err)
		}
		if !strings.Contains(err.Error(), `"id"`) {
			t.Fatalf("refusal must name the missing column; got: %v", err)
		}
		if row["payload"] != "secret" {
			t.Fatalf("payload was modified (%v) despite the refusal", row["payload"])
		}
	})
}
