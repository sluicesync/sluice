// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// A GENERATED column in the PRIMARY KEY destroys the row identity that
// [filterBeforeToPK] narrows to, and nothing noticed until the 2026-08-07
// invariant sweep built the PK-shape × row-image matrix
// (TestCDCReader_MinimalDeleteCarriesThePK).
//
// The mechanism is entirely sluice-side, which is why it survived every
// binlog_row_image pin: MySQL carries the generated column in the row image
// exactly as it carries any other PK column — the cell fails under FULL too.
// [decodeBinlogRow] then DROPS every generated column from the decoded row,
// deliberately and correctly for the INSERT/SET side (the target's own
// GENERATED clause recomputes the value; freezing the source's result would
// be wrong). But the DELETE and UPDATE arms narrow the before-image to the
// PK columns, so when the key IS the generated column the narrowing yields
// one entry with a nil value, and the apply goes one of two ways, neither
// acceptable:
//
//   - the target column is generated too (the faithful mirror every schema
//     writer emits): appliershared.NonGeneratedRowKeys strips it out of the
//     WHERE, the clause renders EMPTY, and `DELETE FROM t WHERE ` is a hard
//     SQL error — loud, but naming nothing an operator can act on;
//   - the target column is a plain materialised column (a type override, or
//     a target dialect that cannot express the expression): the WHERE
//     renders `key IS NULL`, matches zero rows, ADR-0010 absorbs the miss
//     for resume idempotency, and the position advances. The source's
//     DELETE never happens on the target and nothing says so — the Bug 88
//     silent-divergence class, arrived at from the other direction.
//
// So refuse, mid-stream, naming the table and the column. The refusal is
// deliberately NOT a preflight: the identity loss is per-table and the
// reader learns a table's shape lazily at its first TABLE_MAP, so a
// cold-start preflight would need its own information_schema sweep over the
// in-scope set. That is filed rather than built — see the scope note on
// TestCDCReader_MinimalDeleteCarriesThePK.
//
// Why refusal rather than carrying the value losslessly, since the binlog
// has it: carrying it needs three coordinated changes — the decoder must
// retain a generated PK in the BEFORE image only (never the after image,
// which becomes the SET clause), buildWhereClause must stop filtering
// generated columns out of a WHERE (they are legal there; the filter exists
// for INSERT/SET column lists), and the batched DELETE…IN path's key
// extraction has to agree. The Postgres sibling cannot be fixed that way at
// all: pgoutput does not transmit generated columns before PG 18, so the
// value is not on the wire to carry. A refusal is therefore the contract
// that can be uniform across engines. The lossless carriage is a follow-up,
// not a smaller version of this.

// refuseGeneratedPKIdentity returns a coded, stream-fatal error when tbl's
// PRIMARY KEY includes a generated column — i.e. when [filterBeforeToPK] is
// about to narrow a before-image down to a key the decoder has already
// dropped. op names the arm ("update" / "delete") for the message.
//
// nil for every table whose PK is entirely non-generated, which is all of
// them in practice: this costs a keyed table one slice walk per row event.
func refuseGeneratedPKIdentity(tbl *tableSchema, op string) error {
	if len(tbl.PrimaryKey) == 0 {
		// PK-less: there is no narrowing, so no identity to lose. The
		// partial-image belt already covers that arm.
		return nil
	}
	generated := generatedPKColumns(tbl)
	if len(generated) == 0 {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCGeneratedPrimaryKey,
		"Give the table a non-generated PRIMARY KEY (a surrogate key, or an existing NOT NULL UNIQUE column) "+
			"and demote the generated column to an ordinary indexed column; or copy this table with "+
			"`sluice migrate` instead, or drop it from the stream with `--exclude-table`.",
		fmt.Errorf(
			"mysql: cdc: %s on %s.%s cannot be applied: its PRIMARY KEY includes generated column(s) %s, and "+
				"sluice's row decoder drops generated columns (the target's GENERATED clause recomputes them). "+
				"That leaves the change with no usable row identity — the apply would render either an empty "+
				"WHERE or `%s IS NULL`, which matches no row and would silently leave the target row behind "+
				"while the stream position advanced. The stream stops here rather than diverging",
			op, tbl.Schema, tbl.Name, strings.Join(quoteEach(generated), ", "), generated[0],
		),
	)
}

// generatedPKColumns returns tbl's PRIMARY KEY columns that are generated,
// in key order. Empty for the overwhelmingly common all-plain key.
func generatedPKColumns(tbl *tableSchema) []string {
	var out []string
	for _, name := range tbl.PrimaryKey {
		for _, col := range tbl.Columns {
			if col.Name == name && col.IsGenerated() {
				out = append(out, name)
				break
			}
		}
	}
	return out
}

// quoteEach wraps each name in double quotes for the message.
func quoteEach(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = fmt.Sprintf("%q", n)
	}
	return out
}
