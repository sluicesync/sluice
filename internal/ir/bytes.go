// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

// Byte-size approximation for IR-canonical row values.
//
// The same helper drives both the progress ticker's MB/s metric (a
// human-eyeballs gauge introduced in v0.5.0 — see ADR-0019) and the
// memory-bounded streaming knob (`--max-buffer-bytes`, ADR-0028) that
// caps writer/applier batch accumulation by total byte size.
//
// The approximation is intentionally rough: fixed-width types use
// their natural byte width; strings and []byte use their length;
// time.Time uses a typical wire-format width (24 bytes covers
// TIMESTAMPTZ with sub-second precision and a timezone suffix); nil
// contributes nothing. Unknown types contribute zero rather than
// guessing — better to under-report than to bound a batch on a
// fabricated number.
//
// The helper lives in `internal/ir` (rather than the pipeline
// package, where it originated) so the engine packages can reuse it
// from their CDC apply and bulk-write paths without importing
// pipeline (which would be a layering inversion).

import "time"

// ApproximateRowBytes estimates the wire-size of a [Row] by walking
// its values once. See the file-level note for the policy.
func ApproximateRowBytes(row Row) int64 {
	if row == nil {
		return 0
	}
	var total int64
	for _, v := range row {
		total += ApproximateValueBytes(v)
	}
	return total
}

// ApproximateValueBytes returns the rough byte cost of a single
// IR-canonical value. See [ApproximateRowBytes] for the policy
// rationale.
func ApproximateValueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case bool:
		return 1
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, uint, int64, uint64, float64:
		return 8
	case time.Time:
		// 24 bytes covers TIMESTAMPTZ-with-tz at microsecond
		// precision in the canonical PG text form.
		return 24
	case []any:
		var n int64
		for _, e := range x {
			n += ApproximateValueBytes(e)
		}
		return n
	case []string:
		var n int64
		for _, s := range x {
			n += int64(len(s))
		}
		return n
	}
	// Approximation falls back to zero rather than guessing a value
	// that might be wildly wrong for engine-specific shapes (e.g.
	// pgtype.Numeric, geometry WKB). Callers that drive a flush
	// decision off the result get a lower bound — a batch that
	// would hit the cap on real wire bytes might over-shoot when
	// every value is an unknown type, which is acceptable for the
	// soft-target semantics documented in ADR-0028.
	return 0
}

// ApproximateChangeBytes returns the rough byte cost of a single
// [Change] event. Insert and Delete cost their row's value bytes;
// Update sums the Before and After images (both are buffered in the
// applier transaction's parameter slice during dispatch). Truncate
// and transaction-boundary events carry no row data and contribute
// zero.
//
// Used by [BatchedChangeApplier] implementations to bound the
// in-flight transaction's parameter buffer by accumulated value
// bytes (ADR-0028) in addition to the existing change-count cap.
func ApproximateChangeBytes(c Change) int64 {
	switch v := c.(type) {
	case Insert:
		return ApproximateRowBytes(v.Row)
	case Update:
		return ApproximateRowBytes(v.Before) + ApproximateRowBytes(v.After)
	case Delete:
		return ApproximateRowBytes(v.Before)
	}
	return 0
}
