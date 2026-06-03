// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"bytes"
	"cmp"
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
)

// # VStream COPY-phase dedup (GitHub issue #14)
//
// Vitess's VStream COPY mode is documented in the upstream RFC
// (vitessio/vitess#6277) as a copy + catchup loop:
//
//   "We copy a batch of rows until a particular PK using a
//   consistent snapshot. However once the copy is completed the
//   binlog position would have moved possibly containing updates to
//   the rows already transmitted. Hence we need to perform a
//   'catchup' where we play the events up to the current position.
//   We can only send updates to rows that we have already sent to
//   the stream."
//
// In other words, Vitess intentionally emits TWO kinds of ROW events
// on the same gRPC stream during COPY:
//
//  1. **Forward COPY emissions.** Rows ordered by PK ascending,
//     one batch at a time, from the consistent-snapshot scan.
//  2. **Catchup-phase replay.** Binlog events that modified rows
//     ALREADY past the COPY scan's lastpk. By the RFC's contract,
//     "we can only send updates to rows that we have already sent" —
//     so every catchup-phase emission has PK ≤ the highest forward
//     emission already seen.
//
// Pre-v0.43.0 sluice buffered both kinds as snapshot rows. The
// catchup emissions reached the bulk-copy writer as fresh INSERTs;
// the writer's destination already had the row from the forward
// emission and rejected the second INSERT with a duplicate-PK
// error. This is the GitHub issue #14 bug: the v0.42.0 retest
// reproduced it across all three target engines (vanilla MySQL,
// PS-Postgres, PS-MySQL) fed from a single PlanetScale-MySQL source,
// confirming the source-side common factor.
//
// Sluice's pre-fix VStream snapshot reader (cdc_vstream_snapshot.go)
// buffered every ROW event during COPY as a snapshot row, so the
// bulk-copy writer saw duplicate PKs and the second INSERT collided
// with the first (`Error 1062 (23000): Duplicate entry '<id>'` on
// MySQL targets; `SQLSTATE 23505` on PG targets).
//
// This dedup tracker drops behind-the-scan emissions. The invariant
// it relies on: Vitess's COPY scan emits ROW events in PK-ascending
// order within a single (keyspace, shard, table) scope. Any ROW
// event with PK ≤ the maximum PK already seen for that scope is a
// behind-the-scan emission and must be dropped from the snapshot
// rowBuffer.
//
// Dropped events are recovered on the post-COPY CDC phase: Vitess's
// streaming tail resumes from the COPY-phase terminal GTID and
// replays any changes that happened during the scan. The CDC
// applier's idempotent semantics (ADR-0010) absorb the partial-
// overlap.
//
// Tracking is per-(keyspace, shard, table) so multi-shard streams
// dedup each shard independently. Composite PKs compare
// lexicographically by column. Tables without a PK declared in
// FIELD events fall through unchanged — the tracker can't dedup
// what it can't identify, and PK-less tables are already best-effort
// on the apply path (see [ChangeApplier]'s package doc).

// copyDedupTracker is the per-stream dedup state. Owned by
// [vstreamSnapshotStream] and consulted on every COPY-phase ROW
// event. Stateless across stream-restarts: a fresh stream rebuilds
// its own state from FIELD events.
type copyDedupTracker struct {
	// pkColumnsByKey maps fieldCacheKey(shard, table) → ordered list
	// of PK column names. Derived at FIELD-event time from the
	// per-field MySqlFlag_PRI_KEY_FLAG bit. nil/empty for tables
	// that have no declared PK (or have not yet seen a FIELD event);
	// dedup is a no-op for such tables.
	pkColumnsByKey map[string][]string

	// maxPKSeenByKey maps the same scope key → the highest PK tuple
	// observed in any COPY-phase ROW event. Tuple is comparable
	// element-by-element via [comparePKTuple]. Absent key means no
	// rows seen yet — every incoming row is a forward emission by
	// definition.
	maxPKSeenByKey map[string][]any

	// dropCount counts dropped emissions per scope key. Drained into
	// the streamer's DEBUG-level summary log at COPY_COMPLETED.
	// Empty / zero for well-behaved streams.
	dropCount map[string]int
}

func newCopyDedupTracker() *copyDedupTracker {
	return &copyDedupTracker{
		pkColumnsByKey: make(map[string][]string),
		maxPKSeenByKey: make(map[string][]any),
		dropCount:      make(map[string]int),
	}
}

// recordFields parses a FIELD event's Fields slice and records the
// PK column names for the given scope. Called from the snapshot
// stream's FIELD-event dispatch. PK columns are identified via the
// MySqlFlag_PRI_KEY_FLAG bit; the order is the order in which they
// appear in the Fields slice, which matches the table's declared PK
// column ordering (Vitess preserves DDL order).
//
// Idempotent: a re-emitted FIELD event with the same columns
// overwrites the previous record cleanly. The maxPKSeenByKey for
// the scope is NOT reset — a FIELD re-emission during a stream
// shouldn't invalidate already-seen PK progress.
func (t *copyDedupTracker) recordFields(scopeKey string, fields []*query.Field) {
	if t == nil {
		return
	}
	var pkNames []string
	for _, f := range fields {
		if f == nil {
			continue
		}
		// query.MySqlFlag_PRI_KEY_FLAG = 2; field.Flags is uint32.
		// A field with the PRI_KEY_FLAG bit set is part of the
		// table's primary key. The order in the Fields slice is the
		// declared PK column order.
		if f.GetFlags()&uint32(query.MySqlFlag_PRI_KEY_FLAG) != 0 {
			pkNames = append(pkNames, f.GetName())
		}
	}
	if len(pkNames) == 0 {
		// Table has no PK or none of the fields carry the flag.
		// Defensive: clear any stale record so a later table with
		// the same scope key but no PK doesn't inherit stale state.
		delete(t.pkColumnsByKey, scopeKey)
		return
	}
	t.pkColumnsByKey[scopeKey] = pkNames
}

// shouldKeep reports whether a decoded row passes the dedup filter
// for the given scope. Returns true when:
//
//   - The tracker has no PK info for the scope (tables with no
//     declared PK; the FIELD event hasn't arrived yet; the table
//     isn't being tracked). Cannot dedup → keep the row.
//   - The row's PK tuple is strictly greater than the max seen
//     for the scope (forward COPY emission).
//
// Returns false when the row's PK tuple is ≤ the max seen — the
// row is a behind-the-scan emission that the post-COPY CDC phase
// will replay if needed. The caller drops the row from the
// snapshot rowBuffer.
//
// When returning true and the row's PK is strictly greater than
// the prior max, the tracker advances its max for the scope.
//
// Nil-safe: a nil tracker keeps every row (the dedup is opt-in via
// the snapshot stream's constructor).
func (t *copyDedupTracker) shouldKeep(scopeKey string, row ir.Row) bool {
	if t == nil {
		return true
	}
	pkNames, ok := t.pkColumnsByKey[scopeKey]
	if !ok || len(pkNames) == 0 {
		return true
	}
	pkTuple := extractPKTuple(row, pkNames)
	if pkTuple == nil {
		// Row is missing a PK column (shouldn't happen given the
		// FIELD event declared it, but defensive). Keep the row so
		// the operator sees the symptom rather than a silent drop.
		return true
	}
	prev, hasPrev := t.maxPKSeenByKey[scopeKey]
	if !hasPrev {
		t.maxPKSeenByKey[scopeKey] = pkTuple
		return true
	}
	cmpResult := comparePKTuple(pkTuple, prev)
	if cmpResult > 0 {
		t.maxPKSeenByKey[scopeKey] = pkTuple
		return true
	}
	// pkTuple <= prev → behind-the-scan, drop.
	t.dropCount[scopeKey]++
	return false
}

// summary returns a human-readable per-scope drop count, suitable
// for a single DEBUG-level log line at COPY_COMPLETED. Zero entries
// are omitted. Empty map → empty string ("no scopes dropped").
func (t *copyDedupTracker) summary() string {
	if t == nil || len(t.dropCount) == 0 {
		return ""
	}
	var b []byte
	first := true
	for scope, n := range t.dropCount {
		if n == 0 {
			continue
		}
		if !first {
			b = append(b, ", "...)
		}
		first = false
		b = fmt.Appendf(b, "%s=%d", scope, n)
	}
	return string(b)
}

// extractPKTuple pulls the PK column values out of a decoded row in
// the column order recorded by [recordFields]. Returns nil if any
// PK column is missing from the row.
func extractPKTuple(row ir.Row, pkNames []string) []any {
	out := make([]any, len(pkNames))
	for i, name := range pkNames {
		v, ok := row[name]
		if !ok {
			return nil
		}
		out[i] = v
	}
	return out
}

// comparePKTuple compares two PK tuples lexicographically. Returns
// -1, 0, or +1 in the conventional sense. Element types must match
// position-by-position; type mismatch returns 0 (treated as equal
// to avoid wrongful drop on schema drift).
//
// Supported element types match the IR-Row canonical Go types
// produced by [decodeVStreamCell]:
//   - int64 (signed integer family)
//   - uint64 (unsigned integer family)
//   - float64 (rarely used for PK; supported for completeness)
//   - string (VARCHAR/CHAR/TEXT/DECIMAL/TIME PKs)
//   - []byte (BINARY/VARBINARY/UUID-as-bytes PKs)
//   - time.Time (DATETIME/DATE PKs; uncommon)
//   - bool (TINYINT(1) PK; effectively unused but supported)
//
// Cross-signedness comparison (int64 vs uint64) uses int64
// conversion for positive uint64s; negatives stay int64. Operators
// using mixed signedness on PK columns within a single column
// position would be a schema bug we don't try to paper over.
func comparePKTuple(a, b []any) int {
	if len(a) != len(b) {
		// Shouldn't happen — same scope's PK column count is stable.
		// Compare by length to preserve a deterministic order.
		return cmp.Compare(len(a), len(b))
	}
	for i := range a {
		c := comparePKCell(a[i], b[i])
		if c != 0 {
			return c
		}
	}
	return 0
}

// comparePKCell compares two PK column values. NULL handling:
// nil < anything; nil == nil. NULL PKs are theoretical (MySQL
// disallows NULL in PK columns) but we handle them defensively.
func comparePKCell(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			return cmp.Compare(av, bv)
		}
		if bv, ok := b.(uint64); ok {
			// Compare via int64; if uint64 > MaxInt64, it's larger.
			if bv > 1<<63-1 {
				return -1
			}
			return cmp.Compare(av, int64(bv))
		}
	case uint64:
		if bv, ok := b.(uint64); ok {
			return cmp.Compare(av, bv)
		}
		if bv, ok := b.(int64); ok {
			if av > 1<<63-1 {
				return 1
			}
			return cmp.Compare(int64(av), bv)
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return cmp.Compare(av, bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return cmp.Compare(av, bv)
		}
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Compare(av, bv)
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Compare(bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			if av == bv {
				return 0
			}
			if !av && bv {
				return -1
			}
			return 1
		}
	}
	// Type mismatch — treat as equal to avoid wrongful drop. This
	// path is reachable only on schema drift or under unknown future
	// IR-Row value types; the dedup falls open rather than closed.
	return 0
}
