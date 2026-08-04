// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SQLite has no prefix indexes, and dropping a prefix from a UNIQUE key
// changes which rows are legal (audit 2026-08-04).
//
// The first half of S8 added exactly this refusal on the Postgres side and
// enumerated four POSTGRES emit sites. It never asked whether the OTHER
// targets had the same gap. SQLite did — `ir.IndexColumn.Length` was read
// zero times in this package — so a MySQL source's `UNIQUE KEY u (email(20))`
// arrived as an unconditional `UNIQUE INDEX u ON t (email)` and the target
// silently accepted pairs the source forbids.
//
// That is the sibling sweep scoped one level too low: sites within an engine
// were enumerated, engines were not.

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func prefixIndex(name string, unique bool, length int) *ir.Index {
	return &ir.Index{
		Name:    name,
		Unique:  unique,
		Columns: []ir.IndexColumn{{Column: "email", Length: length}},
	}
}

// TestPrefixedUniqueIndexRefused is the silent-loss half.
func TestPrefixedUniqueIndexRefused(t *testing.T) {
	_, err := emitCreateIndex("users", prefixIndex("uq_email", true, 20))
	if err == nil {
		t.Fatal("a UNIQUE index carrying a 20-character prefix was emitted as a whole-column UNIQUE INDEX " +
			"with no complaint.\n\n" +
			"The source forbids two rows whose first 20 characters of email match and permits everything " +
			"else; the emitted index forbids only exact duplicates. The target therefore ACCEPTS rows the " +
			"source rejects — silently, permanently, at exit 0.")
	}
	for _, want := range []string{"uq_email", "email", "20", "substr("} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q — an operator needs the index, the column, the width and "+
				"a way forward.\ngot: %v", want, err)
		}
	}
}

// TestPrefixedNonUniqueIndexIsCarried is the must-not-refuse control. On a
// plain index the prefix is a size choice: the widened index covers a superset
// of the rows and every query still answers correctly. Over-refusal here would
// block migrations that are entirely safe.
func TestPrefixedNonUniqueIndexIsCarried(t *testing.T) {
	stmt, err := emitCreateIndex("users", prefixIndex("idx_email", false, 20))
	if err != nil {
		t.Fatalf("a NON-unique prefixed index was refused (%v); dropping its prefix changes cost, not "+
			"correctness, so it must be carried with a WARN", err)
	}
	if !strings.Contains(stmt, "idx_email") {
		t.Errorf("index not emitted: %q", stmt)
	}
	// SQLite has no syntax for a prefix, so it must not leak into the DDL.
	if strings.Contains(stmt, "(20)") {
		t.Errorf("emitted DDL carries a prefix length SQLite cannot parse: %q", stmt)
	}
}

// TestUnprefixedIndexUnaffected guards against the check firing on ordinary
// indexes — the overwhelmingly common case.
func TestUnprefixedIndexUnaffected(t *testing.T) {
	for _, unique := range []bool{true, false} {
		if _, err := emitCreateIndex("users", prefixIndex("i", unique, 0)); err != nil {
			t.Errorf("unique=%v: an index with no prefix length was refused: %v", unique, err)
		}
	}
}

// TestPartialIndexStillCarried pins the axis SQLite CAN express, so the new
// refusal is not read as covering partial indexes too. SQLite supports
// `CREATE INDEX … WHERE`, which is why the partial-predicate refusal belongs
// on MySQL and not here — the two halves of S8 land on different engines.
func TestPartialIndexStillCarried(t *testing.T) {
	idx := &ir.Index{
		Name:      "uq_live_email",
		Unique:    true,
		Columns:   []ir.IndexColumn{{Column: "email"}},
		Predicate: "deleted_at IS NULL",
	}
	stmt, err := emitCreateIndex("users", idx)
	if err != nil {
		t.Fatalf("a partial UNIQUE index was refused on SQLite, which supports partial indexes: %v", err)
	}
	if !strings.Contains(stmt, "WHERE deleted_at IS NULL") {
		t.Errorf("partial predicate dropped from the emitted index: %q", stmt)
	}
}
