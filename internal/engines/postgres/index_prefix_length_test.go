// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL index-prefix-length gate (audit 2026-08-01 S8).
//
// MySQL can index a leading substring of a column: `UNIQUE KEY (email(10))`.
// Postgres has no equivalent, and the emitter used to drop the prefix with a
// code comment reading "lossy if the source used it; documented" — documented
// in that comment and nowhere an operator would see.
//
// On a UNIQUE key that is not a cosmetic loss, it is a CONSTRAINT WEAKENING in
// the direction that always matters: MySQL forbids two rows whose first ten
// characters of email match, a Postgres UNIQUE over the whole column permits
// them, so the target silently accepts data the source rejects. Nothing failed
// and nothing warned.
//
// The split this pins: uniqueness-enforcing keys REFUSE, plain indexes warn
// (there the prefix is a size choice and every row legal on the source stays
// legal on the target).
//
// Covered at all FOUR PG key-emitting sites, because the same prefix arrives
// by four different routes and one of them does not share the others' code.
// emitAddUniqueConstraint renders its own column list, so a gate placed only
// in emitIndexColumnList would have left the deferred-constraint path — the
// one a normal migrate actually takes for constraint-backed unique keys —
// silently weakening. That is the recurring sibling shape, so the check lives
// in a shared helper both call.

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func prefixCol(name string, n int) ir.IndexColumn {
	return ir.IndexColumn{Column: name, Length: n}
}

// TestIndexPrefixLength_UniqueKeysRefuse_PlainIndexesDoNot is the class pin:
// every route by which a prefix-bearing key reaches the Postgres emitter, and
// the non-unique control that must still succeed.
func TestIndexPrefixLength_UniqueKeysRefuse_PlainIndexesDoNot(t *testing.T) {
	t.Run("primary key refuses", func(t *testing.T) {
		tbl := &ir.Table{
			Schema:  "public",
			Name:    "users",
			Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
			PrimaryKey: &ir.Index{
				Name:    "PRIMARY",
				Unique:  true,
				Columns: []ir.IndexColumn{prefixCol("email", 10)},
			},
		}
		_, err := emitTableDef("public", tbl, emitOpts{})
		requirePrefixRefusal(t, err, "email", 10)
	})

	t.Run("inline unique constraint refuses", func(t *testing.T) {
		tbl := &ir.Table{
			Schema:  "public",
			Name:    "users",
			Columns: []*ir.Column{{Name: "email", Type: ir.Varchar{Length: 255}}},
			Indexes: []*ir.Index{{
				Name:             "uq_email",
				Unique:           true,
				ConstraintBacked: true,
				Columns:          []ir.IndexColumn{prefixCol("email", 10)},
			}},
		}
		_, err := emitTableDef("public", tbl, emitOpts{})
		requirePrefixRefusal(t, err, "email", 10)
	})

	t.Run("unique index refuses", func(t *testing.T) {
		idx := &ir.Index{
			Name:    "uq_email",
			Unique:  true,
			Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}
		_, err := emitCreateIndex("public", "users", idx, emitOpts{})
		requirePrefixRefusal(t, err, "email", 10)
	})

	// The fourth site, and the one a real MySQL→PG migrate reaches for a
	// constraint-backed unique key: ALTER TABLE ... ADD CONSTRAINT ... UNIQUE,
	// emitted in the deferred constraints phase. It builds its column list
	// independently of emitIndexColumnList.
	t.Run("deferred ADD CONSTRAINT UNIQUE refuses", func(t *testing.T) {
		idx := &ir.Index{
			Name:             "uq_email",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{prefixCol("email", 10)},
		}
		_, err := emitAddUniqueConstraint("public", "users", idx)
		requirePrefixRefusal(t, err, "email", 10)
	})

	t.Run("deferred ADD CONSTRAINT UNIQUE without a prefix is untouched", func(t *testing.T) {
		idx := &ir.Index{
			Name:             "uq_email",
			Unique:           true,
			ConstraintBacked: true,
			Columns:          []ir.IndexColumn{{Column: "email"}},
		}
		if _, err := emitAddUniqueConstraint("public", "users", idx); err != nil {
			t.Fatalf("an ordinary unique constraint must emit unchanged: %v", err)
		}
	})

	// The control. A non-unique index constrains nothing, so dropping the
	// prefix cannot change which rows are legal — it must still emit. A
	// refusal here would be the over-fire that makes the gate unusable on
	// ordinary MySQL schemas, where prefix indexes on TEXT columns are
	// routine.
	t.Run("non-unique index still emits", func(t *testing.T) {
		idx := &ir.Index{
			Name:    "idx_email",
			Columns: []ir.IndexColumn{prefixCol("email", 10)},
		}
		stmt, err := emitCreateIndex("public", "users", idx, emitOpts{})
		if err != nil {
			t.Fatalf("a non-unique prefix index must still emit (the prefix is a size choice, "+
				"not a constraint): %v", err)
		}
		if !strings.Contains(stmt, "email") {
			t.Errorf("emitted statement does not name the column: %q", stmt)
		}
		// And it must not smuggle MySQL's prefix syntax into PG DDL.
		if strings.Contains(stmt, "(10)") {
			t.Errorf("emitted MySQL prefix syntax into Postgres DDL: %q", stmt)
		}
	})

	// A prefix of zero is the ordinary case — no prefix at all — and must be
	// untouched on a unique key. This is the anti-vacuity floor: without it a
	// gate that refused EVERY unique index would pass every case above.
	t.Run("unique index without a prefix is untouched", func(t *testing.T) {
		idx := &ir.Index{
			Name:    "uq_email",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "email"}},
		}
		if _, err := emitCreateIndex("public", "users", idx, emitOpts{}); err != nil {
			t.Fatalf("an ordinary unique index must emit unchanged: %v", err)
		}
	})
}

func requirePrefixRefusal(t *testing.T, err error, col string, n int) {
	t.Helper()
	if err == nil {
		t.Fatalf("a %d-character prefix on a uniqueness-enforcing key over %q was ACCEPTED. Postgres has no "+
			"prefix equivalent, so the emitted key covers the whole column and admits rows the source "+
			"forbids — silently, permanently, at exit 0 (audit 2026-08-01 S8)", n, col)
	}
	// The refusal has to be actionable, not merely present.
	for _, want := range []string{col, "prefix"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q; got: %v", want, err)
		}
	}
}
