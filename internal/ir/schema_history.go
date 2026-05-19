// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
)

// SchemaVersionKey derives the deterministic primary-key surrogate for
// a schema-history row from its natural identity tuple
// (stream_id, schema_name, table_name, anchor_position).
//
// anchor_position is an engine-opaque position token (ADR-0007) that is
// unbounded — GTID sets run to hundreds of bytes — so the natural tuple
// cannot be a SQL primary key directly: MySQL InnoDB caps an index key
// at 3072 bytes (four utf8mb4 VARCHAR(255)s alone are 4080), and a
// prefix index on the anchor would let two distinct long anchors that
// share a prefix COLLIDE in the PK and silently overwrite each other's
// schema version — a silent-loss class. The surrogate is a fixed-width
// SHA-256 over the full tuple: collision-free in practice, identical
// across engines, index-safe. Components are NUL-delimited so boundaries
// are unambiguous (a||b cannot alias a'||b').
func SchemaVersionKey(streamID, schemaName, tableName, anchorToken string) string {
	h := sha256.New()
	for _, part := range []string{streamID, schemaName, tableName, anchorToken} {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ADR-0049 CDC schema-history — engine-neutral resolve.
//
// The schema-history store persists, at every detected DDL boundary, a
// snapshot of the affected table's IR schema keyed by the source
// position the boundary was observed at (the "anchor"). On resume /
// replay the applier must decode each event against the schema version
// that was in effect AT THAT EVENT'S POSITION, not "schema now".
//
// Storage and the SQL that loads retained versions is engine-specific
// (it mirrors the sluice_cdc_state control-table layering exactly —
// per-engine DDL, placeholders, schema-qualification). What is
// engine-NEUTRAL is the resolve algorithm: given the retained versions
// for a key and an event position, pick the schema in effect there.
// That is this file. It depends only on [PositionOrderer] (the engine
// supplies the causal ordering; positions are engine-opaque per
// ADR-0007) and the [Table] codec, so it lives in the IR package the
// engines already depend on.
//
// Chunk-A scope fence: this is storage + serialization + resolve + the
// orderer impls only. No DDL-boundary detection, no hot-path cache, no
// applier wiring (ADR-0049 chunks B/C/D).

// SchemaSignature is the structural fingerprint a CDC reader compares
// across boundary events to decide whether a FIELD / Relation /
// post-DDL rebuild is a *true* schema delta (ADR-0049 DP-1 sign-off
// point ii) or a no-op re-emit. VStream re-emits FIELD on
// (re)start / per-table first-touch and PG re-sends Relation on
// reconnect *without* any DDL; snapshotting those would bloat the
// history with no-op versions and break DP-2's "retention ∝ DDL
// count" assumption. The signature is the (ordered column-name,
// ordered column-type) tuple — exactly the two axes the resolve path
// decodes against. It deliberately excludes nullability, defaults,
// comments, indexes, and constraints: those don't change how a ROW
// event's column layout is decoded, so a change confined to them is
// not a decode-affecting delta (a future chunk may widen this if a
// constraint-only delta ever needs a version; Chunk B scopes it to
// the decode contract).
type SchemaSignature struct {
	// names is the column list in declaration order.
	names []string
	// types is each column's IR type, parallel to names.
	types []Type
}

// SchemaSignatureOf derives the [SchemaSignature] of t. A nil table
// yields the zero signature, which differs from every non-empty
// table's signature (so the first boundary for a table is always a
// true delta).
func SchemaSignatureOf(t *Table) SchemaSignature {
	if t == nil {
		return SchemaSignature{}
	}
	sig := SchemaSignature{
		names: make([]string, len(t.Columns)),
		types: make([]Type, len(t.Columns)),
	}
	for i, c := range t.Columns {
		sig.names[i] = c.Name
		sig.types[i] = c.Type
	}
	return sig
}

// Equal reports whether two signatures describe the same decode
// contract: same column names in the same order with the same IR
// types. Type equality is structural ([reflect.DeepEqual]) because
// IR types are value structs (Array.Element is an interface,
// ExtensionType.Modifiers is a slice — both DeepEqual-correct) and a
// type-parameter change (VARCHAR(10)→VARCHAR(20), DECIMAL(10,2)→
// DECIMAL(12,4), ENUM value-set change) is a real decode-affecting
// delta that MUST snapshot a new version. Pinning to the IR type —
// not a representative scalar — is the Bug-74-class discipline:
// numeric/temporal/enum/blob parameter changes are silent-loss if
// treated as no-ops.
func (s SchemaSignature) Equal(other SchemaSignature) bool {
	if len(s.names) != len(other.names) {
		return false
	}
	for i := range s.names {
		if s.names[i] != other.names[i] {
			return false
		}
		if !reflect.DeepEqual(s.types[i], other.types[i]) {
			return false
		}
	}
	return true
}

// RetainedSchemaVersion is one persisted schema-history row as loaded
// from the engine's sluice_cdc_schema_history control table: the
// boundary's source position (engine-opaque token) and the IR table
// snapshot serialized via the [MarshalTable] codec. The engine-
// specific loader produces these; [ResolveSchemaVersion] consumes
// them.
type RetainedSchemaVersion struct {
	// Anchor is the source position the DDL boundary was observed at
	// (ADR-0049 HP-3: the boundary event's OWN position, captured at
	// detection — wired in a later chunk; Chunk A only stores/resolves).
	Anchor Position

	// TableJSON is the affected table's IR schema, [MarshalTable]-
	// encoded. Decoded lazily (only the selected version is
	// deserialized) via [UnmarshalTable].
	TableJSON []byte
}

// ResolveSchemaVersion selects, from versions, the schema in effect at
// event position p and returns its decoded [Table].
//
// Selection rule: among all retained anchors A with
// orderer.PositionAtOrAfter(p, A) == true (p is at or after the
// boundary A was snapshotted at), pick the GREATEST such A — the most
// recent boundary at or before p — and return its table. "Greatest" is
// defined by the same partial order: A1 is greater than A2 when
// PositionAtOrAfter(A1, A2) is true and the reverse is false. Because
// the order is PARTIAL (MySQL GTID sets — the Bug-74-class trap a
// total-order comparator would walk into), the satisfying set may have
// no unique greatest element; that ambiguity is itself a loud
// ErrPositionInvalid (a resolve that cannot pick a single in-effect
// schema must not guess).
//
// Loud floor (ADR-0049 DP-2):
//
//   - orderer == nil (engine does not implement [PositionOrderer]) →
//     a loud error, NOT a silent string-compare degrade. Ordering is
//     a correctness primitive; an engine without it cannot host the
//     schema-history feature, and pretending otherwise is exactly the
//     silent-mis-decode class this ADR exists to kill.
//   - No retained anchor satisfies p (p is older than the oldest
//     retained version — compacted past, or a replay before the first
//     boundary) → an error wrapping [ErrPositionInvalid], which the
//     pipeline's existing ADR-0022 path turns into a loud cold-start
//     re-snapshot. Never a silent mis-decode.
func ResolveSchemaVersion(orderer PositionOrderer, versions []RetainedSchemaVersion, p Position) (*Table, error) {
	if orderer == nil {
		return nil, errors.New("ir: schema-history resolve: engine does not implement PositionOrderer; " +
			"position ordering is required and there is no safe fallback (loud-failure tenet)")
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("ir: schema-history resolve: no retained schema version for position %+v "+
			"(below the retention floor / before the first boundary): %w", p, ErrPositionInvalid)
	}

	bestIdx := -1
	for i := range versions {
		atOrAfter, err := orderer.PositionAtOrAfter(p, versions[i].Anchor)
		if err != nil {
			return nil, fmt.Errorf("ir: schema-history resolve: ordering p vs anchor %+v: %w",
				versions[i].Anchor, err)
		}
		if !atOrAfter {
			continue
		}
		if bestIdx == -1 {
			bestIdx = i
			continue
		}
		// Keep whichever of (current best, candidate i) is the GREATER
		// anchor under the partial order. A candidate is strictly
		// greater when it is at-or-after the current best AND the
		// current best is not at-or-after it. If neither dominates the
		// other, the satisfying set has no unique most-recent version
		// — loud, never a guess.
		cAfterB, err := orderer.PositionAtOrAfter(versions[i].Anchor, versions[bestIdx].Anchor)
		if err != nil {
			return nil, fmt.Errorf("ir: schema-history resolve: ordering anchor %+v vs anchor %+v: %w",
				versions[i].Anchor, versions[bestIdx].Anchor, err)
		}
		bAfterC, err := orderer.PositionAtOrAfter(versions[bestIdx].Anchor, versions[i].Anchor)
		if err != nil {
			return nil, fmt.Errorf("ir: schema-history resolve: ordering anchor %+v vs anchor %+v: %w",
				versions[bestIdx].Anchor, versions[i].Anchor, err)
		}
		switch {
		case cAfterB && !bAfterC:
			// candidate strictly greater → it wins
			bestIdx = i
		case bAfterC && !cAfterB:
			// current best strictly greater → keep it
		case cAfterB && bAfterC:
			// mutually at-or-after = equivalent anchors (same boundary
			// re-observed); either is fine, keep the current best.
		default:
			// Neither dominates: two distinct boundaries both at-or-
			// before p but unordered relative to each other. No single
			// in-effect schema → loud.
			return nil, fmt.Errorf("ir: schema-history resolve: position %+v has two "+
				"incomparable candidate schema versions (anchors %+v and %+v); cannot pick a "+
				"single in-effect schema: %w", p, versions[bestIdx].Anchor, versions[i].Anchor,
				ErrPositionInvalid)
		}
	}

	if bestIdx == -1 {
		return nil, fmt.Errorf("ir: schema-history resolve: no retained schema version at or before "+
			"position %+v (below the retention floor / before the first boundary): %w", p, ErrPositionInvalid)
	}

	t, err := UnmarshalTable(versions[bestIdx].TableJSON)
	if err != nil {
		return nil, fmt.Errorf("ir: schema-history resolve: decode selected version (anchor %+v): %w",
			versions[bestIdx].Anchor, err)
	}
	if t == nil {
		return nil, fmt.Errorf("ir: schema-history resolve: selected version (anchor %+v) decoded to a "+
			"nil table (corrupt history row): %w", versions[bestIdx].Anchor, ErrPositionInvalid)
	}
	return t, nil
}
