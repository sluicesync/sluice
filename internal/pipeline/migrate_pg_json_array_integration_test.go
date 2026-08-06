//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 145 — `json[]` and `jsonb[]` reaching a Postgres target at all,
// and reaching it with their DIMENSIONS, their NULL-versus-JSON-null
// distinction, and their exact document bytes intact.
//
// Before the fix this table could not be migrated: `convertArray` had no
// ir.JSON arm and the copy aborted with `postgres: array of element type
// ir.JSON not supported`. The finding did not come from a field report — item
// 141's divergence-map gate surfaced it on its first run, and recorded it as an
// explicit exemption rather than a guess.
//
// # Why the matrix, and what it is watching for HERE
//
// The Bug-74 discipline says a new array-element arm owes {1-D, ≥2-D,
// NULL-element, empty array, whole-column NULL} on a real target, per family,
// with array_dims as the flatten oracle. That is run below for BOTH families,
// because "json and jsonb share one convertArray arm" is exactly the
// by-construction reasoning Bug 74 made untrustworthy — the two OIDs have
// DIFFERENT pgx codecs (JSONBCodec wraps a version byte around the binary form)
// even where sluice's code path is byte-identical.
//
// The hazard this family actually carries is not the usual one, and the pins
// differ accordingly. pgx's JSONCodec.PlanEncode is a TOTAL function — it falls
// through to a json.Marshal plan for any value it does not recognise — so
// ArrayCodec never declines for the _json/_jsonb element OIDs and the
// flattening wrap chain is never reached. That makes array_dims here a
// confirmation rather than the primary alarm; the primary alarm is the exact
// `::text` comparison, because a leaf pgx does not recognise is silently
// RE-ENCODED.
//
// That is a MEASUREMENT, not an argument — three mutations against this file on
// a real PG 16, each confirmed applied before its result was trusted:
//
//   - bypass buildPGArray and hand pgx the nested []any → BOTH families flatten
//     `[1:2][1:2]` → `[1:2]`, caught here. (The Bug-74 shape, still reachable
//     through the vehicle even though it is not reachable through the leaf.)
//   - keep pgtype.Array and swap the leaf to *pgtype.Text → the whole matrix
//     stays GREEN. Recorded as an EQUIVALENT MUTANT rather than a coverage gap:
//     it is the exact mutation that reproduced item 141's silent bytea flatten,
//     and on this family it does not diverge, because the JSON codec plans the
//     Text valuer just as happily. This is why the file says the flatten is
//     structurally absent here rather than merely untriggered.
//   - swap the leaf to one JSONCodec cannot plan (a bool) → dims stay correct
//     and every document silently becomes `true`, caught by the `::text` and
//     per-element comparisons and by the row-7 JSON-`null` oracle. That is the
//     hazard this family really has, and it is the one the value pins carry.
//
// Two rows carry traps of their own:
//
//   - the JSON document `null` sitting beside a SQL NULL element in the same
//     array. PG's array_out quotes an element whose text is literally "NULL"
//     (case-insensitively), which is the ONLY reason sluice's array-text parser
//     can tell the two apart — an unquoted bare NULL is its SQL-null marker.
//   - a document whose STRING content spells the array-literal metacharacters
//     (comma, braces, double quote, backslash), which is what a quoting bug in
//     either direction would mangle.
package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const jsonArraySeedDDL = `
	CREATE TABLE jsonarr (
		id INT PRIMARY KEY,
		j  json[],
		b  jsonb[]
	);
	INSERT INTO jsonarr VALUES
		(1, ARRAY[$${"a":1}$$::json, $$[1,2]$$::json, $$"str"$$::json, $$42$$::json],
		    ARRAY[$${"a":1}$$::jsonb, $$[1,2]$$::jsonb, $$"str"$$::jsonb, $$42$$::jsonb]),
		(2, ARRAY[ARRAY[$${"a":1}$$::json, $${"b":2}$$::json],
		          ARRAY[$${"c":3}$$::json, $${"d":4}$$::json]],
		    ARRAY[ARRAY[$${"a":1}$$::jsonb, $${"b":2}$$::jsonb],
		          ARRAY[$${"c":3}$$::jsonb, $${"d":4}$$::jsonb]]),
		(3, ARRAY[$${"a":1}$$::json, NULL, $${"b":2}$$::json],
		    ARRAY[$${"a":1}$$::jsonb, NULL, $${"b":2}$$::jsonb]),
		(4, ARRAY[]::json[], ARRAY[]::jsonb[]),
		(5, NULL, NULL),
		(6, ARRAY[ARRAY[$$1$$::json, NULL], ARRAY[NULL, $${"x":[1,2]}$$::json]],
		    ARRAY[ARRAY[$$1$$::jsonb, NULL], ARRAY[NULL, $${"x":[1,2]}$$::jsonb]]),
		(7, ARRAY[$$null$$::json, NULL, $$true$$::json],
		    ARRAY[$$null$$::jsonb, NULL, $$true$$::jsonb]),
		(8, ARRAY[$${"k":"a,b{c}\"d\" \\e"}$$::json],
		    ARRAY[$${"k":"a,b{c}\"d\" \\e"}$$::jsonb]);
`

func TestMigrate_PGToPG_JSONArrays(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, jsonArraySeedDDL)

	pg := pgEngineOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mig := &Migrator{Source: pg, Target: pg, SourceDSN: sourceDSN, TargetDSN: targetDSN}
	if err := mig.Run(ctx); err != nil {
		// Pre-item-145: `postgres: array of element type ir.JSON not
		// supported` — the copy could not complete at all.
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Schema fidelity first: a target column that is not json[]/jsonb[] would
	// make every value comparison below meaningless — and would in particular
	// change WHICH pgx element codec the encode planned against, which is the
	// variable this whole file exists to hold fixed.
	for col, want := range map[string]string{"j": "json[]", "b": "jsonb[]"} {
		q := fmt.Sprintf(`
			SELECT format_type(a.atttypid, a.atttypmod)
			FROM pg_attribute a
			WHERE a.attrelid = 'jsonarr'::regclass AND a.attname = '%s'`, col)
		if got := pgString(t, targetDSN, q); got != want {
			t.Fatalf("target column %q type = %q; want %q", col, got, want)
		}
	}

	// Value fidelity, per row per family, src == dst. `::text` on the whole
	// array is a byte comparison of every document plus the array's own shape
	// and quoting.
	for id := 1; id <= 8; id++ {
		for _, col := range []string{"j", "b"} {
			q := fmt.Sprintf("SELECT COALESCE(%s::text, 'null') FROM jsonarr WHERE id = %d", col, id)
			src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
			if src != dst {
				t.Errorf("%s value mismatch row %d:\n src=%q\n dst=%q", col, id, src, dst)
			}
		}
	}

	// The Bug-74 flatten oracle on BOTH multi-dim rows and BOTH families.
	// Structurally unreachable for this element family (see the file header) —
	// which is a claim, and this is the check that makes it one.
	for _, id := range []int{2, 6} {
		for _, col := range []string{"j", "b"} {
			q := fmt.Sprintf("SELECT COALESCE(array_dims(%s)::text, 'null') FROM jsonarr WHERE id = %d", col, id)
			src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
			if src != dst {
				t.Errorf("array_dims mismatch row %d col %s (2-D): src=%q dst=%q", id, col, src, dst)
			}
			if dst != "[1:2][1:2]" {
				t.Errorf("row %d col %s landed with dims %q; want [1:2][1:2] — the array was FLATTENED",
					id, col, dst)
			}
		}
	}

	// Per-ELEMENT ground truth on the multi-dim row: whole-array ::text equality
	// would also pass if BOTH sides were wrong in the same way, and it is the
	// element subscript that says the row-major order survived.
	for _, tc := range []struct{ expr, want string }{
		{"j[1][1]::text", `{"a":1}`},
		{"j[2][2]::text", `{"d":4}`},
		{"b[1][1]::text", `{"a": 1}`}, // jsonb re-renders with its own spacing
		{"b[2][2]::text", `{"d": 4}`},
	} {
		q := fmt.Sprintf("SELECT %s FROM jsonarr WHERE id = 2", tc.expr)
		if got := pgString(t, targetDSN, q); got != tc.want {
			t.Errorf("row 2 %s = %q; want %q", tc.expr, got, tc.want)
		}
	}

	// The JSON document `null` versus a SQL NULL element, unambiguously. Both
	// render as the four letters n-u-l-l once out of the array, so the
	// IS NULL predicate is the only honest oracle — and the distinction
	// survives ONLY because PG's array_out quotes the literal-NULL element.
	for _, col := range []string{"j", "b"} {
		q := fmt.Sprintf(
			`SELECT (%[1]s[1] IS NULL)::text || '/' || COALESCE(%[1]s[1]::text, '-')
			   || '|' || (%[1]s[2] IS NULL)::text
			   || '|' || (%[1]s[3] IS NULL)::text || '/' || COALESCE(%[1]s[3]::text, '-')
			 FROM jsonarr WHERE id = 7`, col,
		)
		const want = "false/null|true|false/true"
		src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
		if src != want {
			t.Fatalf("row 7 col %s SOURCE reads %q, not %q — the fixture no longer poses the question",
				col, src, want)
		}
		if dst != want {
			t.Errorf("row 7 col %s = %q; want %q (a PRESENT JSON `null` document must not collapse into "+
				"the SQL NULL element beside it)", col, dst, want)
		}
	}

	// And the metacharacter trap: a document whose STRING CONTENT spells the
	// array literal's own delimiters. A quoting bug on either side lands a
	// different document — or splits one element into several.
	for _, col := range []string{"j", "b"} {
		q := fmt.Sprintf(`SELECT array_length(%[1]s, 1)::text || '/' || (%[1]s[1] ->> 'k')
			FROM jsonarr WHERE id = 8`, col)
		const want = `1/a,b{c}"d" \e`
		src, dst := pgString(t, sourceDSN, q), pgString(t, targetDSN, q)
		if src != want {
			t.Fatalf("row 8 col %s SOURCE reads %q, not %q — the fixture no longer poses the question",
				col, src, want)
		}
		if dst != want {
			t.Errorf("row 8 col %s = %q; want %q", col, dst, want)
		}
	}
}
