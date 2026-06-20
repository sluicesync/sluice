// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package laneapply

import (
	"fmt"
	"hash/fnv"
	"io"
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// Router maps each row-bearing change to one of `lanes` apply lanes by a
// stable hash of the change's (qualified table, ordered primary-key
// values). The mapping is deterministic and total: the same logical key
// always resolves to the same lane, which is the load-bearing
// same-key-closed property — all changes to one row are applied in source
// order on a single lane, so the dependent-row hazard (INSERT then
// DELETE/UPDATE of the same key racing on two transactions) cannot occur.
//
// The router is pure and immutable; it holds no state and is safe to call
// from the single routing goroutine. Keyless changes (no primary key) are
// NOT routed here — they take the barrier path (drain all lanes, apply
// single-row), so LaneFor is only ever called with a non-empty pkVals.
type Router struct {
	lanes int
}

// NewRouter returns a router over `lanes` lanes. lanes < 1 is clamped to 1
// (serial) so a misconfigured caller degrades to correct-but-serial rather
// than panicking on a modulo-by-zero.
func NewRouter(lanes int) *Router {
	if lanes < 1 {
		lanes = 1
	}
	return &Router{lanes: lanes}
}

// LaneFor returns the lane index in [0, lanes) for a change to `qualified`
// (schema.table) whose ordered primary-key column values are pkVals. The
// hash is FNV-1a over the qualified name and a canonical, type-tagged
// encoding of each key value, so two values that are equal-but-typed
// differently (int64(5) vs "5") never alias — a correctness requirement,
// not just balance: the SAME row must always hash identically, and the
// decode path guarantees a given column yields the same Go type across
// Insert/Update/Delete, so a per-value type tag keeps distinct keys
// distinct without depending on cross-type coincidences.
func (r *Router) LaneFor(qualified string, pkVals []any) int {
	if r.lanes <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(qualified))
	_, _ = h.Write([]byte{0}) // table/value domain separator
	for _, v := range pkVals {
		WriteCanonicalKeyValue(h, v)
		_, _ = h.Write([]byte{0}) // value separator (so ["a","b"] ≠ ["ab"])
	}
	return int(h.Sum64() % uint64(r.lanes))
}

// WriteCanonicalKeyValue writes a deterministic, type-tagged byte encoding
// of a single primary-key value to h. The tag prefix ensures values of
// different kinds never collide on identical byte content (int64(49) vs
// the string "1"). The set of kinds mirrors what the VStream/binlog decode
// path can place in a key column; an unrecognised kind falls back to the
// fmt-style %v rendering under a generic tag — deterministic for the
// scalar/byte-slice kinds that reach a primary key.
func WriteCanonicalKeyValue(h io.Writer, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = h.Write([]byte{'N'})
	case int64:
		_, _ = h.Write([]byte{'i'})
		_, _ = h.Write([]byte(strconv.FormatInt(t, 10)))
	case int:
		_, _ = h.Write([]byte{'i'})
		_, _ = h.Write([]byte(strconv.FormatInt(int64(t), 10)))
	case uint64:
		_, _ = h.Write([]byte{'u'})
		_, _ = h.Write([]byte(strconv.FormatUint(t, 10)))
	case string:
		_, _ = h.Write([]byte{'s'})
		_, _ = h.Write([]byte(t))
	case []byte:
		_, _ = h.Write([]byte{'b'})
		_, _ = h.Write(t)
	case bool:
		if t {
			_, _ = h.Write([]byte{'B', '1'})
		} else {
			_, _ = h.Write([]byte{'B', '0'})
		}
	default:
		// Float/decimal/temporal keys are rare but legal; render under a
		// generic tag. The encoding only needs determinism (same value →
		// same bytes), which the standard formatter provides for these.
		_, _ = h.Write([]byte{'?'})
		_, _ = fmt.Fprintf(h, "%v", t)
	}
}

// PKValuesFromRow extracts the ordered primary-key values from a change for
// routing, reading from the map appropriate to the change kind: Insert.Row,
// Update.After (the post-image — the row's current identity), Delete.Before.
// Returns ok=false when the change is not a routable row-change
// (TxBegin/TxCommit/Truncate/SchemaSnapshot — all barrier events) or when
// any key column is absent from the row (a malformed change that must take
// the safe barrier path rather than be silently mis-routed).
//
// pkCols is the table's ordered primary-key column list (from the engine's
// pk cache). An empty pkCols means a keyless table → ok=false → barrier
// path (the keyless guard applies single-row regardless).
//
// This is the PURE traversal half of the engine's PKValuesForRouting seam
// method: the engine loads pkCols (and decides PK-changing-update barrier
// detection) on its side, then calls this with the resolved columns.
//
// PK-changing UPDATEs (After's key differs from Before's) are a key
// migration, not a same-key op: routing on the After image keeps the new
// identity's lane consistent, but a concurrent op on the OLD key could be
// on a different lane. Such updates are rare and the engine treats a
// detected key change as a barrier so the old/new ordering is preserved;
// this helper reports the After-image key and leaves that detection to the
// engine.
func PKValuesFromRow(c ir.Change, pkCols []string) (vals []any, ok bool) {
	if len(pkCols) == 0 {
		return nil, false
	}
	var row ir.Row
	switch v := c.(type) {
	case ir.Insert:
		row = v.Row
	case ir.Update:
		row = v.After
	case ir.Delete:
		row = v.Before
	default:
		return nil, false
	}
	if row == nil {
		return nil, false
	}
	vals = make([]any, len(pkCols))
	for i, col := range pkCols {
		val, present := row[col]
		if !present {
			return nil, false
		}
		vals[i] = val
	}
	return vals, true
}
