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
// hash is FNV-1a over the qualified name and a canonical encoding of each
// key value ([WriteCanonicalKeyValue]).
//
// The two directions of a hash mistake are NOT symmetric, and the encoding
// is built around that. Two DIFFERENT keys that encode alike merely share a
// lane: their changes serialise where they could have run in parallel —
// balance, never correctness (nothing downstream treats a shared lane as
// a shared key; coalescing uses [valuesEqualForKey]). ONE row whose value
// encodes two ways lands on two lanes, and its changes commit in no
// defined order — silent loss. So the property that matters, and the one
// that can rot, is that the SAME row always hashes identically: every
// producer that can place a value in a given key column must produce the
// same canonical bytes, whatever Go kind it chose.
//
// ACROSS CHANGE KINDS that holds, and it is structural rather than lucky:
// every CDC reader in the tree decodes the before- and after-images with one
// function invoked BEFORE the Insert/Update/Delete switch, and both leaf
// decoders dispatch on the IR COLUMN TYPE, never on the change kind
// (mysql/postgres `value_decode.go`; pgtrigger and sqlite-trigger decode one
// image function per row; PG's key-only Before is copied out of After by
// `synthesizeKeyOnlyBefore`, values by reference). Ground-truthed against
// real servers by TestCDCDecodeTypeStableAcrossChangeKinds.
//
// ACROSS PROVENANCE it is a weaker claim than it reads, and the 2026-08-07
// invariant sweep is why this paragraph exists. The concurrent lanes are
// ALSO driven by chain-restore's incremental replay and the from-backup
// broker, whose changes come out of backup chunks rather than a live reader,
// and that round trip is lossy ON TYPE: `blobcodec.encodeValue` folds the
// whole signed-integer family onto one tag and the whole unsigned family
// onto another, and float32 onto float64. Those streams are 100%
// chunk-sourced today, so a run stays self-consistent — by stream
// composition, which nothing enforces. What IS enforced is that the fold is
// harmless: [WriteCanonicalKeyValue] encodes by VALUE rather than by Go
// kind, and TestCanonicalKeyValue_SurvivesTheBackupRoundTrip binds the two
// packages so a change to either side fails the build rather than
// splitting a row across two lanes.
//
// And a single stream CAN mix provenances now: a backup chain's ADD COLUMN
// fill (pipeline `incremental_add_column_fill.go`) is read with the COPY
// reader and replayed in the same chunk as the window's CDC changes. On
// MySQL that put two kinds in one unsigned key column — the text-protocol
// copy read hands back int64 for an INT UNSIGNED, the binlog decoder
// uint64 (the value contract, docs/value-types.md, permits both) — and the
// old kind-tagged encoding ('i' vs 'u') routed the fill's UPDATE and the
// window's INSERT of one row to different lanes, where the UPDATE could
// commit first and match nothing (the 2026-09-24 value-fidelity review,
// finding 2). The encoding is now value-canonical across those kinds.
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

// WriteCanonicalKeyValue writes a deterministic byte encoding of a single
// primary-key value to h, canonical by VALUE rather than by Go kind (see
// [Router.LaneFor] for why aliasing is safe and splitting is not). One tag,
// 's', covers every kind that is a sequence of bytes or renders to one
// exactly: an integer of ANY width and signedness as its decimal text, a
// string as its bytes, a []byte as its bytes. So each of the kinds a
// reader may legitimately produce for one key column encodes alike:
//
//   - int8/16/32/int/int64 and their unsigned twins — the backup
//     change-chunk round trip widens every sized integer to its 64-bit form
//     (`blobcodec.encodeValue`, the 2026-08-07 sweep), and the value
//     contract lets an unsigned column arrive as int64 OR uint64 depending
//     on the reader (text-protocol copy vs binlog; the 2026-09-24 sweep).
//   - an integer and its decimal text in bytes — the MySQL decoder keeps an
//     integer the driver returned as []byte as []byte (`decodeInteger`).
//   - string and []byte — a reader that hands raw bytes for a text key
//     where another hands a string.
//
// nil and bool keep their own tags; they cannot be the same row as a
// byte-string. An unrecognised kind falls back to the fmt-style %v
// rendering under a generic tag — deterministic for the scalar kinds that
// reach a primary key.
//
// Checked and NOT aliased, with the reason:
//
//   - Decimal: a string on every reader (docs/value-types.md), so it is
//     covered by the string arm; two readers rendering one value with
//     different scale ("1.5" vs "1.50") would still split. For MySQL the
//     copy read and the binlog agree, bound on a real server by
//     mysql's TestLaneKey_CopyReadAndBinlogRouteAlike, which reads every
//     key family through both readers (it also measured the unsigned
//     split this encoding closes: int64 from the copy read, uint64 from the
//     binlog, for TINYINT..INT UNSIGNED). The Postgres copy-read vs
//     pgoutput pair is NOT bound — Postgres has no unsigned integers, so
//     the integer arm cannot split there, but its numeric and temporal
//     renders are UNVERIFIED PREMISE across the two readers.
//   - float32 vs float64, and time.Time offset vs UTC: KNOWN RESIDUAL, the
//     backup fold normalises both and they land in the '?' fallback, where
//     %v is not width-neutral for an offset-bearing timestamp. Only a float
//     or temporal PRIMARY KEY reaches it; the round-trip test grades those
//     cells so the exposure is measured rather than assumed.
func WriteCanonicalKeyValue(h io.Writer, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = h.Write([]byte{'N'})
	case int64:
		writeByteString(h, strconv.FormatInt(t, 10))
	case int:
		writeByteString(h, strconv.FormatInt(int64(t), 10))
	case int32:
		writeByteString(h, strconv.FormatInt(int64(t), 10))
	case int16:
		writeByteString(h, strconv.FormatInt(int64(t), 10))
	case int8:
		writeByteString(h, strconv.FormatInt(int64(t), 10))
	case uint64:
		writeByteString(h, strconv.FormatUint(t, 10))
	case uint:
		writeByteString(h, strconv.FormatUint(uint64(t), 10))
	case uint32:
		writeByteString(h, strconv.FormatUint(uint64(t), 10))
	case uint16:
		writeByteString(h, strconv.FormatUint(uint64(t), 10))
	case uint8:
		writeByteString(h, strconv.FormatUint(uint64(t), 10))
	case string:
		writeByteString(h, t)
	case []byte:
		_, _ = h.Write([]byte{'s'})
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

// writeByteString writes the byte-string arm of [WriteCanonicalKeyValue].
func writeByteString(h io.Writer, s string) {
	_, _ = h.Write([]byte{'s'})
	_, _ = io.WriteString(h, s)
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
