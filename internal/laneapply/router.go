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
// stable hash of the change's [Route]. The mapping is deterministic and
// total: the same route always resolves to the same lane, which is the
// load-bearing same-route-closed property — all changes sharing a route are
// applied in source order on a single lane, so the dependent-row hazard
// (INSERT then DELETE/UPDATE racing on two transactions) cannot occur
// BETWEEN THEM.
//
// What "between them" covers is the [RouteScope]. Hashing the primary key
// closes the hazard for one row and ONLY for one row: two changes to
// DIFFERENT primary keys that collide on a table's SECONDARY UNIQUE index
// are dependent on each other and hash apart (roadmap item 131 / audit
// 2026-08-05 A-1). That is why the scope exists and why its zero value is
// the whole-table one.
//
// The router is pure and immutable; it holds no state and is safe to call
// from the single routing goroutine. Keyless changes (no primary key) are
// NOT routed here — they take the barrier path (drain all lanes, apply
// single-row).
type Router struct {
	lanes int
}

// RouteScope selects what a change's lane hash covers — i.e. which OTHER
// changes it is guaranteed to be ordered against.
//
// The zero value is [RouteScopeTable], the SAFE one, deliberately: a Route an
// engine leaves unset (a new implementor, a path that could not read the
// target's index metadata, a struct built by a test) gets whole-table
// ordering rather than the fast path. A fail-OPEN default would silently
// recreate item 131 for everything it missed, and a missed case is invisible
// — both lanes report success (the v0.99.51 zero-value discipline, applied to
// a correctness switch rather than a config bool).
type RouteScope uint8

const (
	// RouteScopeTable routes EVERY change on the table to ONE lane: the hash
	// covers the qualified table name alone. Required when the target table
	// can refuse two rows on something other than the primary key — a
	// secondary UNIQUE index, an exclusion constraint — because then changes
	// to different keys are dependent on each other and must not commit out
	// of source order. Cross-TABLE concurrency is preserved; concurrency
	// within the table is not.
	RouteScopeTable RouteScope = iota

	// RouteScopeKey is the fast path: the hash covers (table, primary key),
	// so distinct rows of the table spread across all lanes. An engine may
	// only choose it once it has PROVEN the target table carries no non-PK
	// uniqueness constraint. "Could not determine" is not a proof — it takes
	// [RouteScopeTable].
	RouteScopeKey
)

// Route is the lane-routing decision for one row change: which table it
// belongs to, which row within that table, and how much of that the lane hash
// must cover ([RouteScope]). It is produced by the engine
// ([LaneApplier.RouteForChange], which owns the target-metadata knowledge)
// and consumed by [Router.LaneForRoute], which is the single place the
// decision turns into a lane index.
type Route struct {
	// Qualified is the engine's qualified target table name (the form its
	// own caches key on). Never empty for a routable change.
	Qualified string

	// PKVals are the change's ordered primary-key values. Always populated
	// for a routable change — including under [RouteScopeTable], where the
	// hash ignores them — so a diagnostic or a future scope can read the row
	// identity without a second decode.
	PKVals []any

	// Scope selects what the hash covers. Zero value = [RouteScopeTable].
	Scope RouteScope
}

// LaneForRoute returns the lane index in [0, lanes) for rt, honouring its
// scope: [RouteScopeKey] hashes (table, primary key) so distinct rows spread;
// anything else — including the zero value — hashes the table alone so every
// change on it lands on one in-order lane. This is the ONLY place a Route
// becomes a lane, so the fail-closed default has exactly one enforcement
// point (pinned by TestLaneForRoute_ScopeDecidesTheHash).
func (r *Router) LaneForRoute(rt Route) int {
	if rt.Scope == RouteScopeKey {
		return r.LaneFor(rt.Qualified, rt.PKVals)
	}
	return r.LaneFor(rt.Qualified, nil)
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
// This is the PURE traversal half of the engine's RouteForChange seam
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
