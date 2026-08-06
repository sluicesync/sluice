//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 141 — `bytea[]` reaching a Postgres target at all, and
// reaching it with its DIMENSIONS and its empty-vs-NULL distinction
// intact.
//
// Before the fix this table could not be migrated: `convertArray` had no
// arm for ir.Binary/ir.Varbinary/ir.Blob and the copy aborted with
// `postgres: array of element type ir.Blob not supported`. The read half
// had been pinned across three doors by item 135; nothing had ever asked
// whether the value could be WRITTEN.
//
// The matrix is the Bug-74 shape applied to one family, because that is
// what a new array arm owes: {1-D, 2-D, NULL-element, empty array, empty
// ELEMENT, whole-column NULL, 2-D mixing empty and NULL} — plus the two
// byte patterns that specifically stress bytea, a value containing 0x00
// and a value whose CONTENT spells the `\x`-hex prefix (item 135's own
// trap, from the other side of the wire).
//
// Ground truth is the real target, twice over: `::text` equality against
// the SOURCE per row (PG renders bytea elements as \x-hex, so this is a
// byte comparison), `array_dims` for the multi-dim rows (the Bug-74
// flatten oracle), and `octet_length`/`IS NULL` for the empty-vs-NULL
// distinction, which `::text` alone would show but which deserves its own
// unambiguous check since the writer's nil-slice wart is exactly there.

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const byteaArraySeedDDL = `
	CREATE TABLE byteaarr (
		id INT PRIMARY KEY,
		b  bytea[]
	);
	INSERT INTO byteaarr VALUES
		(1, ARRAY['\x00'::bytea, '\xff'::bytea, '\xdeadbeef'::bytea]),
		(2, ARRAY[ARRAY['\x01'::bytea, '\x02'::bytea], ARRAY['\x03'::bytea, '\x04'::bytea]]),
		(3, ARRAY['\xaa'::bytea, NULL, '\xbb'::bytea]),
		(4, ARRAY[]::bytea[]),
		(5, ARRAY[''::bytea, '\x41'::bytea]),
		(6, NULL),
		(7, ARRAY[decode('5c7834313432', 'hex')]),
		(8, ARRAY[ARRAY[''::bytea, NULL], ARRAY['\x00'::bytea, '\xffff'::bytea]]);
`

func TestMigrate_PGToPG_ByteaArrays(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, byteaArraySeedDDL)

	pg := pgEngineOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mig := &Migrator{Source: pg, Target: pg, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	if err := mig.Run(ctx); err != nil {
		// Pre-item-141: `postgres: array of element type ir.Blob not
		// supported` — the copy could not complete at all.
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Schema fidelity first: a target column that is not bytea[] would make
	// every value comparison below meaningless.
	const formatQ = `
		SELECT format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = 'byteaarr'::regclass AND a.attname = 'b'`
	if got := pgString(t, targetDSN, formatQ); got != "bytea[]" {
		t.Fatalf("target column type = %q; want bytea[]", got)
	}

	// Value fidelity, per row, src == dst. PG's bytea element rendering is
	// \x-hex, so ::text equality on the whole array is a byte comparison of
	// every element plus the array's own shape.
	for id := 1; id <= 8; id++ {
		q := fmt.Sprintf("SELECT COALESCE(b::text, 'null') FROM byteaarr WHERE id = %d", id)
		src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
		if src != dst {
			t.Errorf("bytea[] value mismatch row %d:\n src=%q\n dst=%q", id, src, dst)
		}
	}

	// The Bug-74 flatten oracle on BOTH multi-dim rows. A wrong element
	// leaf makes pgx fall through its wrap chain to a plain-slice encoder
	// that collapses {{a,b},{c,d}} to {a,b,c,d} with no error — the exact
	// silent-loss shape numeric[][] shipped with in v0.69.3.
	for _, id := range []int{2, 8} {
		q := fmt.Sprintf("SELECT COALESCE(array_dims(b)::text, 'null') FROM byteaarr WHERE id = %d", id)
		src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
		if src != dst {
			t.Errorf("array_dims mismatch row %d (2-D): src=%q dst=%q", id, src, dst)
		}
		if dst != "[1:2][1:2]" {
			t.Errorf("row %d landed with dims %q; want [1:2][1:2] — the array was FLATTENED", id, dst)
		}
	}

	// Empty element vs NULL element, unambiguously. The writer normalises a
	// nil Go slice to an empty one precisely because pgx encodes a nil
	// []byte as SQL NULL; without that, row 5's first element would arrive
	// as NULL and this check is what fails.
	const emptyQ = `
		SELECT (b[1] IS NULL)::text || '/' || COALESCE(octet_length(b[1])::text, 'null')
		FROM byteaarr WHERE id = 5`
	if got := pgString(t, targetDSN, emptyQ); got != "false/0" {
		t.Errorf("row 5 element 1 = %q; want %q (a PRESENT zero-length bytea, not NULL)", got, "false/0")
	}
	const nullQ = `SELECT (b[1][2] IS NULL)::text FROM byteaarr WHERE id = 8`
	if got := pgString(t, targetDSN, nullQ); got != "true" {
		t.Errorf("row 8 element [1][2] = %q; want true (a genuine NULL element)", got)
	}

	// And the item-135 trap from the write side: a bytea whose CONTENT is
	// the ASCII text `\x4142`. Its rendering is \x5c7834313432; a writer
	// that re-interpreted the rendering would land the two bytes 0x41 0x42.
	const trapQ = `SELECT encode(b[1], 'hex') FROM byteaarr WHERE id = 7`
	if got := pgString(t, targetDSN, trapQ); got != "5c7834313432" {
		t.Errorf("row 7 element = %q; want 5c7834313432 (the bytes that SPELL a hex rendering must survive "+
			"as bytes, not be re-read as one)", got)
	}
}
