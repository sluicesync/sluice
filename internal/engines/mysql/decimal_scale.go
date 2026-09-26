// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// A DECIMAL value whose fractional digits exceed the target column's scale
// (GC-37 (c)).
//
// MySQL and MariaDB ROUND excess fractional digits into a DECIMAL column and
// report it as Note 1265 ("Data truncated") — a NOTE, not a warning, so even
// strict sql_mode accepts the write. MEASURED on MySQL 8.0.46 and MariaDB
// 11.4 through sluice (2026-09-25): a Postgres unconstrained `numeric`
// carried into its DECIMAL(65,30) mapping landed
//
//	0.1234567890123456789012345678901    →  0.123456789012345678901234567890
//	-0.99999999999999999999999999999999  →  -1.000000000000000000000000000000
//
// at exit 0 through the CDC applier (serial and concurrent lanes) — which
// also carries the added-column backfill, chain restore and `sync
// from-backup`. The bulk-copy writers were already loud: LOAD DATA and the
// batched INSERT path (PlanetScale's cold copy, the LOAD DATA fallback) both
// refuse a strict-mode write whose warning list is non-empty
// ([RowWriter.reportBulkWriteWarnings]), and the Note counts; the applier never reads
// the warning list in strict mode. A DEFAULT literal is rounded the same way at CREATE
// TABLE / ADD COLUMN, so every row inserted on the target without the column
// took the rounded value.
//
// Integer-part overflow is already loud (Error 1264), and so are NaN and
// ±Infinity (Error 1366), so only the fractional side needs a guard. Trailing
// zeros past the scale lose nothing (`1.5000…0` → `1.5`) and must NOT refuse.
//
// Every MySQL write lane funnels through [prepareValue], so the value guard
// sits there; the DEFAULT guard sits in [mysqlEmitter.emitColumnDef], the only
// caller of the DEFAULT emit.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// mysqlUnconstrainedDecimalPrecision / Scale are the DECIMAL an unconstrained
// source numeric lands as — MySQL's documented maxima (65, 30). MariaDB
// accepts a wider scale (38) but receives the same mapping.
const (
	mysqlUnconstrainedDecimalPrecision = 65
	mysqlUnconstrainedDecimalScale     = 30
)

// decimalScaleExceededMarker is the grep-stable marker of both refusals.
const decimalScaleExceededMarker = "DECIMAL-SCALE-EXCEEDED"

// decimalTargetScale is the scale a column of type t holds on a MySQL-family
// target, and false when t is not a DECIMAL (or carries a negative scale,
// which the emitter already refuses).
func decimalTargetScale(t ir.Type) (int, bool) {
	d, ok := ir.UnwrapDomain(t).(ir.Decimal)
	if !ok {
		return 0, false
	}
	if d.Unconstrained {
		return mysqlUnconstrainedDecimalScale, true
	}
	if d.Scale < 0 {
		return 0, false
	}
	return d.Scale, true
}

// decimalDigitsBeyondScale reports how many significant fractional digits of
// the decimal text s lie past scale — digits the target would round away.
// Trailing zeros are not significant. An exponent form (`1.5e-40`) is
// honoured. Text that is not a finite decimal (`NaN`, `Infinity`, garbage)
// reports 0: the server refuses those loudly on its own.
func decimalDigitsBeyondScale(s string, scale int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if s[0] == '-' || s[0] == '+' {
		s = s[1:]
	}
	var exp int64
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.ParseInt(s[i+1:], 10, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return 0 // not a number; the server refuses it on its own
		}
		// Clamp: any exponent this large is past every DECIMAL either way
		// (a negative one is all fraction, a positive one integer overflow
		// the server refuses), and clamping keeps the arithmetic below
		// from overflowing int64. ParseInt returns ±MaxInt64 on ErrRange.
		const expBound = int64(1) << 40
		exp = max(min(e, expBound), -expBound)
		s = s[:i]
	}
	intPart, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i+1:]
	}
	for _, part := range []string{intPart, frac} {
		for j := 0; j < len(part); j++ {
			if part[j] < '0' || part[j] > '9' {
				return 0
			}
		}
	}
	// Place the decimal point exp positions right of where it is written,
	// then count the significant (non-trailing-zero) digits after it —
	// arithmetically, never allocating in proportion to the exponent (an
	// NDJSON or mydumper number token can carry `1e-9223372036854775808`).
	sig := strings.TrimRight(intPart+frac, "0") // significant digit span, from the left
	if strings.Trim(sig, "0") == "" {
		return 0 // the value is zero
	}
	point := int64(len(intPart)) + exp
	fracDigits := int64(len(sig)) - point // digits of sig right of the point
	if fracDigits <= int64(scale) {
		return 0
	}
	return clampToInt(fracDigits - int64(scale))
}

// clampToInt bounds n into an int (a huge negative exponent yields a digit
// count past any real column's scale; its exact size does not matter).
func clampToInt(n int64) int {
	const maxInt = int64(^uint(0) >> 1)
	if n > maxInt {
		return int(maxInt)
	}
	return int(n)
}

// refuseUnrepresentableValue runs [prepareValue]'s value guards — the ones
// that refuse a value the target would otherwise corrupt or loop on: NaN /
// ±Inf floats ([refuseUnrepresentableFloat]) and a DECIMAL past the target
// scale ([refuseDecimalScaleLoss]).
func refuseUnrepresentableValue(v any, col *ir.Column) error {
	if err := refuseUnrepresentableFloat(v, col); err != nil {
		return err
	}
	return refuseDecimalScaleLoss(v, col)
}

// refuseDecimalScaleLoss is the value guard: a string decimal whose
// significant fractional digits exceed col's target scale would be rounded
// by the server at exit 0, so it is refused before the driver sees it. A nil
// col (the applier's cold-cache path) carries no type and is not checked.
func refuseDecimalScaleLoss(v any, col *ir.Column) error {
	if col == nil {
		return nil
	}
	s, ok := decimalValueText(v)
	if !ok {
		return nil
	}
	scale, ok := decimalTargetScale(col.Type)
	if !ok {
		return nil
	}
	lost := decimalDigitsBeyondScale(s, scale)
	if lost == 0 {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeValueUnrepresentable,
		decimalScaleRemedy,
		fmt.Errorf(
			"%s: column %q carries the decimal %s, which has %d significant fractional digit(s) beyond the target column's "+
				"scale of %d; MySQL and MariaDB would round it (Note 1265, accepted even under strict sql_mode) and land a "+
				"different value at exit 0, so sluice refuses it — %s",
			decimalScaleExceededMarker, columnNameForError(col), s, lost, scale, decimalScaleRemedy,
		),
	)
}

// decimalValueText renders the value a reader delivered for a DECIMAL
// column as decimal text, and false for a kind that cannot carry a
// fractional digit. Every Go type a reader can deliver for a decimal
// column, by reader (GC-37 (c) review, finding 1):
//
//   - string — postgres (pgoutput + copy), mysql (binlog + copy, which also
//     map []byte/int64/uint64 to string), VStream, sqlite/d1, mydumper,
//     flatfile, and every decimal leaf of the IR contract.
//   - json.Number — postgres-trigger for every NON-integer numeric (its
//     change payload is to_jsonb, decoded with UseNumber; integers become
//     int64). A named string type: the first cut's `v.(string)` missed it,
//     and the reviewer reproduced 31 fractional digits landing ROUNDED
//     through pgtrigger → MySQL at exit 0.
//   - []byte — a database/sql scan lane that did not normalise.
//   - float64 / float32 — a float source column retyped to DECIMAL with
//     --type-override (the CDC reader types values by the SOURCE schema);
//     the driver binds the double and MySQL rounds it with a Note. Rendered
//     with 'f', -1: the shortest text that round-trips the value the
//     driver would send.
//   - int64 / int / uint64 / … — no fractional digits; nothing to check.
func decimalValueText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return string(x), true
	case []byte:
		return string(x), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), true
	}
	return "", false
}

// decimalScaleRemedy is the shared remedy text.
const decimalScaleRemedy = "round the value on the source to the target scale if the digits are not needed, or map the " +
	"column to a text type with --type-override to carry every digit"

// refuseDecimalDefaultScaleLoss is the DEFAULT guard: a literal DEFAULT on a
// DECIMAL column with digits past the target scale would be stored rounded,
// and every row inserted on the target without the column would take the
// rounded value.
func refuseDecimalDefaultScaleLoss(tableName string, c *ir.Column) error {
	lit, ok := c.Default.(ir.DefaultLiteral)
	if !ok {
		return nil
	}
	scale, ok := decimalTargetScale(c.Type)
	if !ok {
		return nil
	}
	lost := decimalDigitsBeyondScale(lit.Value, scale)
	if lost == 0 {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeValueUnrepresentable,
		decimalScaleRemedy,
		fmt.Errorf(
			"%s: table %q column %q declares DEFAULT %s, which has %d significant fractional digit(s) beyond the target "+
				"column's scale of %d; the target would store the DEFAULT rounded (Note 1265) and every row inserted without "+
				"the column would take the rounded value — %s",
			decimalScaleExceededMarker, tableName, c.Name, lit.Value, lost, scale, decimalScaleRemedy,
		),
	)
}
