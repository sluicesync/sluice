// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The NULL-vs-unchanged-TOAST distinction, pinned as ONE argument
// (audit 2026-08-01 S1).
//
// Both halves of this were already covered — TestDecodeTuple asserts that a
// 'u' column is omitted, TestDecodeTupleNullColumn asserts that an 'n' column
// decodes to nil. Two facts, each pinned, and nothing binding them. That is the
// shape CLAUDE.md calls out: two facts can each hold and still leave the
// ARGUMENT that rests on them unpinned, so a change that collapsed the two
// markers into one behaviour would keep both existing tests green.
//
// The argument, and who depends on it:
//
//	an absent column means UNCHANGED, and never means SET TO NULL
//
// pipeline/backup.mergeAfterImage is built on exactly that. When smart
// compaction collapses two CDC events it UNIONs their after-images, keeping a
// column the later image omits. If an omission could ever mean "set this to
// NULL", that union would resurrect a stale value over a deliberate SET NULL —
// silently, in a backup chain, with `backup verify` green over it because
// compaction re-stamps the hashes it then checks.
//
// So the two markers are asserted here TOGETHER, from one tuple, with the
// consequence named. The cross-package direction is deliberate: this package
// owns the wire contract, and the package that depends on it cannot pin it.

package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestNullAndUnchangedToastAreDistinguishable is the binding test.
func TestNullAndUnchangedToastAreDistinguishable(t *testing.T) {
	cols := []relationColumn{
		{Name: "id", OID: pgtype.Int8OID, Type: ir.Integer{Width: 64}},
		{Name: "set_to_null", OID: pgtype.TextOID, Type: ir.Text{Size: ir.TextLong}},
		{Name: "unchanged_toast", OID: pgtype.TextOID, Type: ir.Text{Size: ir.TextLong}},
	}
	row, err := decodeTuple(&pglogrepl.TupleData{
		ColumnNum: 3,
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Length: 1, Data: []byte("7")},
			{DataType: 'n'}, // the UPDATE set this to NULL
			{DataType: 'u'}, // the UPDATE did not touch this; it lives out of line
		},
	}, cols)
	if err != nil {
		t.Fatalf("decodeTuple: %v", err)
	}

	// A deliberate NULL is PRESENT with a nil value. Present-ness is the
	// signal; the value being nil is not what carries the meaning.
	v, present := row["set_to_null"]
	if !present {
		t.Errorf("a column the UPDATE set to NULL ('n') is ABSENT from the decoded row.\n\n" +
			"That collapses the two markers into one observable state, and pipeline/backup." +
			"mergeAfterImage then cannot tell a deliberate SET NULL from an untouched TOAST column. " +
			"It unions after-images when collapsing CDC events, so it would carry the PRE-update " +
			"value forward over the NULL — silent corruption in a backup chain. Either restore the " +
			"distinction here, or change mergeAfterImage and delete its premise note.")
	} else if v != nil {
		t.Errorf("set_to_null = %#v, want nil", v)
	}

	// An unchanged TOAST column is ABSENT — which downstream reads as
	// "preserve whatever is already there".
	if got, present := row["unchanged_toast"]; present {
		t.Errorf("a 'u' (unchanged TOAST) column is PRESENT with value %#v; it must be omitted so that "+
			"absent-means-unchanged holds", got)
	}

	if got := row["id"]; got != int64(7) {
		t.Errorf("id = %#v, want int64(7)", got)
	}
}

// TestUnknownTupleMarkerRefusesLoudly guards the other way the premise could
// fail: a NEW wire marker that decodeTuple silently skipped would join the
// omitted set without anyone deciding it should. pgoutput is a versioned
// protocol and pglogrepl tracks it, so this is a live possibility rather than a
// hypothetical.
//
// The refusal is what makes "only 'u' is omitted" a closed statement rather
// than a statement about the markers that happened to exist when it was
// written.
func TestUnknownTupleMarkerRefusesLoudly(t *testing.T) {
	cols := []relationColumn{{Name: "c", OID: pgtype.TextOID, Type: ir.Text{Size: ir.TextLong}}}
	_, err := decodeTuple(&pglogrepl.TupleData{
		ColumnNum: 1,
		Columns:   []*pglogrepl.TupleDataColumn{{DataType: 'z'}},
	}, cols)
	if err == nil {
		t.Fatal("an unrecognised tuple marker was accepted. Whatever it means, the row now silently omits " +
			"that column, and every consumer of absent-means-unchanged inherits the guess. Refuse instead.")
	}
}
