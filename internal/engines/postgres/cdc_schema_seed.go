// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// The pgoutput lane's prior-shape seed (SLM-1c, audit 2026-09-01; the
// Postgres twin of the MySQL lanes' SLM-1/1b seed).
//
// # The gap
//
// [checkSchemaRace]'s prior is `relations[relationID]` — a cache that
// belongs to THIS process. The pgoutput protocol re-sends a
// RelationMessage on first touch after every (re)connect, and the
// classifier treats a RelationMessage with no cached prior as a cache
// PRIME, not a change. So a `timestamp`⇄`timestamptz` (or
// `time`⇄`timetz`) ALTER performed on the source while the stream was
// cleanly stopped arrives at the resumed process as the table's first
// RelationMessage, primes silently, and every post-swap row lands in the
// target's other-zone column at exit 0. Measured on postgres:16 (PG→PG
// and PG→MySQL, both directions): the target column type unchanged, the
// post-swap row applied, and the source's own pre-existing rows 9 h off
// the target's after an ALTER under `SET TIME ZONE 'Asia/Tokyo'`
// (pipeline.TestStreamer_PGSource_StoppedStreamZoneSwap records the
// numbers; the mechanism is stated in that file's comment).
//
// # The fix
//
// The streamer already knows the prior on both open paths and hands it
// over through [CDCReader.SetSchemaSeed] before [CDCReader.StreamChanges]
// — the raw source IR on a cold start, the TARGET's current column types
// (an independent witness) with the retained schema-history version as
// fallback on a warm resume (pipeline.loadWarmResumeSchemaSeed). The
// reader keys relations by OID and the seed is by (schema, name); the
// bridge is built exactly where it is needed — when a RelationMessage
// arrives for an OID the process has no prior for, its (Namespace,
// RelationName) looks the seed up, and a zone-sibling swap between the
// seeded column type and the projected wire type refuses BEFORE the
// entry is cached. That is the table's first boundary of this process,
// which is precisely the one the OID cache could never check.
//
// # Scope, stated so it cannot be read as broader
//
// The seed is consulted by the zone-sibling predicate ONLY, and only
// when the process-local prior is empty. It does not turn every other
// stopped-stream delta (ADD/DROP/RENAME COLUMN, a typmod change) into a
// resume-time classification — those keep priming exactly as before;
// the warm-resume source↔target shape reconciliation is the A2-1
// follow-up, not this. The seed is installed by the streamer only when a
// path re-applies deltas to the target (pipeline.schemaDeltaAppliesToTarget:
// `--schema-changes=forward`, the default, or Shape A); under
// `--schema-changes=refuse` no lane seeds, so a stopped-stream swap in
// that mode still primes here — stated, not hidden.
//
// Precision never enters the predicate (a `timestamp(3)` seed against a
// `timestamptz` wire type is a swap; `timestamp` against `timestamp(3)`
// is not), and a column absent from the seed — ADD COLUMN while stopped,
// or a witness that omitted it — is "no prior knowledge" and skipped.

// schemaSeedSetter mirrors the pipeline's optional seeding surface
// (pipeline.schemaSeedSetter) so this lane is pinned at compile time to
// implement it, exactly as the MySQL lanes pin theirs: the pipeline
// discovers the surface by type assertion, and a method renamed here
// would silently stop seeding this lane — back to the SLM-1c window with
// nothing failing. The cross-package pin is
// pipeline.TestEveryRuntimeDispatchedPipelineSurfaceIsPinnedOrFrozen's
// `_ schemaSeedSetter = (*postgres.CDCReader)(nil)`.
type schemaSeedSetter interface {
	SetSchemaSeed(tables []*ir.Table)
}

var _ schemaSeedSetter = (*CDCReader)(nil)

// SetSchemaSeed hands the reader the shape each in-scope table had when
// the stream it is about to serve was last known to be consistent — the
// cold-start source IR, or the target's zone witness / retained
// schema-history version on a warm resume — so the session-`TimeZone`
// cast refusal has a prior at the table's FIRST RelationMessage of this
// process (SLM-1c). Keyed by (schema, name) as pgoutput names the
// relation; a seed table with no Schema takes the reader's bound schema
// (the target witness carries bare names). Column names and IR types are
// all the check consults, so the seed's projection may come from the
// SchemaReader, from a persisted history row, or from the target's own
// catalog. Must be called before [StreamChanges]; replaces any earlier
// seed; read on the pump goroutine. Implements pipeline.schemaSeedSetter.
func (r *CDCReader) SetSchemaSeed(tables []*ir.Table) {
	r.schemaSeed = make(map[string]*ir.Table, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		schema := t.Schema
		if schema == "" {
			schema = r.schema
		}
		r.schemaSeed[seedKey(schema, t.Name)] = t
	}
}

// seedKey is the seed's (schema, name) key — the RelationMessage's own
// Namespace + RelationName, which is what the bridge to the OID cache
// resolves through.
func seedKey(schema, name string) string {
	return schema + "." + name
}

// checkSeededSchemaRace is the seeded half of the first-boundary check:
// for a RelationMessage whose OID this process has no prior for, the
// seeded shape stands in as prev for the zone-sibling predicate alone.
// A cached prior means [checkSchemaRace] owns the comparison and this
// returns nil; so does a relation the seed does not know.
func (r *CDCReader) checkSeededSchemaRace(relations map[uint32]*relationCacheEntry, relationID uint32, current *relationCacheEntry) error {
	if relations[relationID] != nil || current == nil {
		return nil
	}
	prior, ok := r.schemaSeed[seedKey(current.Schema, current.Name)]
	if !ok {
		return nil
	}
	col, pair, found := unforwardableSeededSessionTZColumn(prior, current)
	if !found {
		return nil
	}
	return fmt.Errorf("postgres: cdc: schema change on %s.%s (OID %d) while the stream was stopped is detected but cannot be forwarded: column %q changed between %s: the source's ALTER resolved every stored value against the SOURCE session's TimeZone, and a forwarded ALTER would re-cast the target's pre-existing rows against the TARGET session's TimeZone — when the two settings differ the casts silently disagree, so this swap refuses (the prior shape is the target's current column type, or the retained schema history where the target cannot witness the table). Apply the same ALTER on the target via the drained model, in a session whose TimeZone matches the source ALTER's. %s",
		current.Schema, current.Name, relationID, col, pair, schemaRaceRecoveryHint)
}

// unforwardableSeededSessionTZColumn scans a projected relation against
// the seeded prior for a column whose type moved across the zone-sibling
// pair, returning that column and the pair name. Name-keyed, like
// [unforwardableTypmodColumn]: the seed's column order is the source
// reader's or the target's, not the wire's.
func unforwardableSeededSessionTZColumn(prior *ir.Table, current *relationCacheEntry) (col, pair string, found bool) {
	if prior == nil {
		return "", "", false
	}
	priorTypes := make(map[string]ir.Type, len(prior.Columns))
	for _, c := range prior.Columns {
		if c != nil {
			priorTypes[c.Name] = c.Type
		}
	}
	for _, cc := range current.Columns {
		prev, ok := priorTypes[cc.Name]
		if !ok {
			continue
		}
		if p, swapped := seededSessionTZSwapPair(prev, cc.Type); swapped {
			return cc.Name, p, true
		}
	}
	return "", "", false
}

// seededSessionTZSwapPair is the IR-typed member of this lane's pair
// declaration: [ir.ZoneSiblingSwap] over the seeded prior and the
// projected wire type, naming the pair the way [sessionTZSwapPair] does
// for the refusal text. Its universe is BOUND to the wire declaration's
// by TestSeededSessionTZSwapPair_AgreesWithTheWireDeclaration — every
// OID pair the wire predicate refuses, this refuses once projected, and
// nothing else — so the two arms of one lane cannot drift apart.
func seededSessionTZSwapPair(prev, cur ir.Type) (string, bool) {
	if !ir.ZoneSiblingSwap(prev, cur) {
		return "", false
	}
	family, _, _ := ir.ZoneFamily(cur)
	suffix := strings.Repeat("[]", strings.Count(family, "array:"))
	switch strings.TrimPrefix(family, strings.Repeat("array:", strings.Count(family, "array:"))) {
	case "time":
		return "time" + suffix + " and timetz" + suffix, true
	default:
		return "timestamp" + suffix + " and timestamptz" + suffix, true
	}
}
