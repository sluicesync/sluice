// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0049 Chunk C — applier-side active-version cache + prime + lookup.
//
// The cache map itself lives on [ChangeApplier.activeSchema] (see the
// struct doc in change_applier.go for the concurrency contract and the
// cache-after-commit invariant). This file adds:
//
//   - [ChangeApplier.ActiveSchema]: O(1) lookup, exported so downstream
//     consumers (ADR-0049 Chunk D backup-envelope; future cross-engine
//     source-IR) can read the active schema without going through
//     storage. A miss returns (nil, false); the caller decides whether
//     the miss is loud.
//
//   - [ChangeApplier.PrimeSchemaHistoryCache]: one storage hit per
//     stream at startup. Loads every retained schema-history version
//     for the stream, groups by (schema, table), and seeds the cache
//     with the in-effect version at `currentPos`. Brand-new streams
//     (zero/initial position) skip the prime entirely — there is no
//     history yet, and the reader's first SchemaSnapshot will populate
//     the cache via the post-commit hook. Resume-from-non-zero primes;
//     a per-table loud floor (ir.ErrPositionInvalid) surfaces up to
//     the caller (the streamer; ADR-0022 cold-start path).
//
//   - [ChangeApplier.cacheActiveSchemaAfterCommit]: the post-commit
//     hook the apply paths (applyOne / applyOneBatch) call AFTER a
//     SchemaSnapshot's tx commits successfully. Cache writes are
//     applier-goroutine-owned; this helper just records the
//     same-goroutine mutation in one place so the apply paths stay
//     readable.
//
// Scope fence: this file does NOT touch the pkCache / colTypeCache
// (those are target-side caches for the writer path, orthogonal to the
// source-IR active-version concern); does NOT add a per-row resolve
// (storage hits are prime-time only — per-row is cache-only via
// ActiveSchema); does NOT wire backup-envelope (Chunk D); does NOT
// touch broker / filter / incremental paths (Chunk B's scope-fence
// handles SchemaSnapshot transit).

// ActiveSchema returns the IR schema in effect for (schema, table) at
// the most-recently durably-persisted ADR-0049 boundary, plus an `ok`
// flag. A miss (no boundary observed yet for this table on this
// stream, or the table is not in the stream's filter) returns
// (nil, false); the caller decides whether the miss is loud.
//
// O(1) map lookup, applier-goroutine-owned (no lock; see
// [ChangeApplier.activeSchema] doc). Returning (nil, false) for a
// miss matches the "consumer decides loudness" contract; in
// particular, ADR-0049 Chunk D will treat a miss-on-known-table as
// loud while a miss-on-untracked-table as a benign passthrough.
func (a *ChangeApplier) ActiveSchema(schema, table string) (*ir.Table, bool) {
	if a.activeSchema == nil {
		return nil, false
	}
	v, ok := a.activeSchema[qualifiedName(schema, table)]
	if !ok {
		return nil, false
	}
	return v.IR, true
}

// cacheActiveSchemaAfterCommit records the SchemaSnapshot's version
// into the active-version cache. Called by the apply paths
// (applyOne / applyOneBatch) AFTER the tx carrying the SchemaSnapshot
// dispatch has committed successfully — the cache-after-commit
// invariant (ADR-0049 Chunk C locked design point 2): a failed /
// rolled-back tx must NOT leave a cache entry that disagrees with
// persisted state.
//
// Lazily initialises the map for the benefit of unit tests that
// construct ChangeApplier struct-literal style (the engine's
// OpenChangeApplier initialises it; the struct-literal path in
// applier tests does not, and we shouldn't crash on a nil map).
func (a *ChangeApplier) cacheActiveSchemaAfterCommit(s ir.SchemaSnapshot) {
	if a.activeSchema == nil {
		a.activeSchema = make(map[string]activeSchemaVersion)
	}
	key := qualifiedName(s.Schema, s.Table)
	prior, hadPrior := a.activeSchema[key]
	a.activeSchema[key] = activeSchemaVersion{
		Anchor: s.Position,
		IR:     s.IR,
	}
	// Only invalidate on an ACTUAL schema change (prior version existed
	// AND its decode signature differs) — not on the first-touch baseline
	// or an identical re-send. Symmetric to the PG applier's GAP #3 fix;
	// keeps steady-state DML on the cached fast path.
	if hadPrior && prior.IR != nil && s.IR != nil &&
		!ir.SchemaSignatureOf(prior.IR).Equal(ir.SchemaSignatureOf(s.IR)) {
		a.invalidateTargetCachesForBoundary(s)
	}
}

// invalidateTargetCachesForBoundary drops the target-side per-table
// metadata caches (colTypeCache / pkCache) for the table named by the
// just-committed SchemaSnapshot, so the next DML on that table re-reads
// the live (post-DDL) catalog rather than binding against the stale
// pre-DDL view (ADR-0091 F7a — the symmetric fix to the PG applier's
// GAP #3). MySQL's text-protocol bind tolerates a stale numeric width
// for a widened column, so this is defense-in-depth on the MySQL side
// (a stale generated-column flag or a dropped column after a DDL
// boundary is the live hazard it forecloses); on PG the same gap is a
// hard encode failure.
//
// The batch path flushes a schema event as its own transaction, so this
// invalidation after the boundary commits is safe: everything before is
// durable, the schema event is its own tx, everything after is a fresh
// batch.
//
// The caches are keyed by the ROUTED (target) schema — the key the DML
// dispatch paths use via [ChangeApplier.routedSchema] — while the
// SchemaSnapshot carries the SOURCE schema, so route it before deleting.
func (a *ChangeApplier) invalidateTargetCachesForBoundary(s ir.SchemaSnapshot) {
	qn := qualifiedName(a.routedSchema(s.Schema), s.Table)
	a.invalidateMetadataCaches(qn)
}

// distinctSchemaTablesForStream returns the unique (schema, table)
// pairs present in the schema-history control table for streamID.
// One round-trip; used at prime time so the prime can call
// resolveSchemaVersion once per table-with-history rather than
// requiring the streamer to know the table list (warm resume skips
// the source-schema read).
//
// A stream with no retained versions returns an empty slice; the
// prime then has nothing to do (cold-start before any boundary, or
// the brand-new-stream sentinel filtered upstream).
func distinctSchemaTablesForStream(ctx context.Context, q schemaHistoryQueryer, streamID string) ([]struct{ Schema, Table string }, error) {
	const sel = "SELECT DISTINCT schema_name, table_name FROM `" + schemaHistoryTableName + "` " +
		"WHERE stream_id = ?"
	rows, err := q.QueryContext(ctx, sel, streamID)
	if err != nil {
		return nil, fmt.Errorf("mysql: list schema-history tables for stream: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []struct{ Schema, Table string }
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			return nil, fmt.Errorf("mysql: scan schema-history table row: %w", err)
		}
		out = append(out, struct{ Schema, Table string }{s, t})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list schema-history tables for stream: %w", err)
	}
	return out, nil
}

// PrimeSchemaHistoryCache seeds the applier's active-version cache
// from the schema-history control table, AS OF currentPos, for
// streamID. Called once at apply-loop entry on warm resume.
//
// Brand-new-stream discriminator: a zero/initial persisted position
// (empty Token) skips the prime entirely. The discriminator is the
// position token: a warm resume by definition carries a non-empty
// token (the streamer falls through to cold-start otherwise); a
// cold-start has nothing in the history yet (the first
// SchemaSnapshot of the stream will populate the cache via the
// post-commit hook). The streamer's [Streamer.runOnce] documents
// this contract at the call site.
//
// Per-table loud floor: when resolveSchemaVersion surfaces an error
// wrapping [ir.ErrPositionInvalid] (the table has retained versions
// but currentPos is below all of them — compacted past, or two
// incomparable anchors), the error propagates to the caller. The
// ADR-0022 pipeline path turns it into a cold-start re-snapshot for
// the affected stream. NEVER swallow the loud error: a silent miss
// makes every subsequent decode race the wrong schema, the exact
// silent-mis-decode class ADR-0049 exists to kill.
//
// A non-ErrPositionInvalid resolve error (e.g. engine doesn't
// implement PositionOrderer; a malformed anchor) also propagates
// loudly — those signal a real bug, not a recoverable resume event.
//
// Returns nil after a successful prime; the cache now holds at most
// one entry per table-with-history. Caches are reset before the
// load so a re-prime (e.g. after retry) produces a clean state.
func (a *ChangeApplier) PrimeSchemaHistoryCache(ctx context.Context, streamID string, currentPos ir.Position) error {
	if streamID == "" {
		return errors.New("mysql: applier: PrimeSchemaHistoryCache: streamID is empty")
	}
	// Brand-new-stream discriminator: skip the prime entirely. A
	// cold-start has no history yet; the first SchemaSnapshot on the
	// stream will populate the cache via the post-commit hook.
	if currentPos.Token == "" {
		slog.DebugContext(
			ctx, "mysql: applier: schema-history prime skipped (brand-new stream)",
			slog.String("stream_id", streamID),
		)
		return nil
	}
	if a.db == nil {
		return errors.New("mysql: applier: PrimeSchemaHistoryCache: db is nil")
	}

	tables, err := distinctSchemaTablesForStream(ctx, a.db, streamID)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		// Resume from a non-zero position but no history yet — the DDL
		// boundary detector has not snapshotted any version on this
		// stream. The cache stays empty; the reader's next
		// SchemaSnapshot will populate it.
		slog.DebugContext(
			ctx, "mysql: applier: schema-history prime found no retained versions",
			slog.String("stream_id", streamID),
			slog.String("position_token", currentPos.Token),
		)
		return nil
	}

	// Reset the cache so a re-prime (after a retry) doesn't leave
	// stale entries that disagree with the freshly-resolved versions.
	a.activeSchema = make(map[string]activeSchemaVersion, len(tables))

	// Bug 79 (v0.70.2): the orderer that compares persisted anchors
	// against currentPos must belong to the SOURCE engine, not the
	// applier's (target's) engine. v0.70.1's storage fix taught the
	// load path to preserve each row's source-engine identity on
	// Position.Engine; v0.70.0's streamer fix already taught
	// retagPositionForSource to tag currentPos with the source engine
	// name. Both sides are now source-tagged — but the orderer here
	// was still hardcoded to `Engine{}` (this package's MySQL
	// orderer), whose decodeBinlogPos strict-rejects any non-mysql /
	// non-planetscale tag. The loud `decode p: engine = "postgres";
	// want "mysql"…` crash class is Bug 79 mirrored.
	//
	// Hoisted ONCE per prime call (the source is per-stream, not
	// per-table); all retained anchors for a given stream share the
	// same source by construction. If a stale row in the table
	// disagrees with currentPos.Engine that's a data-inconsistency
	// edge — we use currentPos.Engine's orderer (the resume in-flight
	// IS what the prime is preparing for); a Position.Engine
	// disagreement between p and anchor surfaces loudly in the
	// orderer itself (the engine-strict decoders reject the mismatch).
	sourceEngineName := currentPos.Engine
	if sourceEngineName == "" {
		// Pre-Bug-78 row, a brand-new stream that didn't reach
		// retagPositionForSource, or a streamer path that omitted the
		// retag. Fall back to the applier's own engine name — the
		// pre-fix behaviour, which is correct for same-engine chains
		// (where target == source).
		sourceEngineName = engineNameMySQL
	}
	srcEng, ok := engines.Get(sourceEngineName)
	if !ok {
		return fmt.Errorf("mysql: applier: prime schema-history cache: source engine %q not registered",
			sourceEngineName)
	}
	orderer, ok := srcEng.(ir.PositionOrderer)
	if !ok {
		return fmt.Errorf("mysql: applier: prime schema-history cache: source engine %q does not implement ir.PositionOrderer",
			sourceEngineName)
	}

	for _, st := range tables {
		// Test-only counter: each iteration is one storage hit
		// (loadRetainedSchemaVersions) + one in-memory resolve.
		a.resolveCallsForTest.Add(1)
		t, err := resolveSchemaVersion(ctx, a.db, orderer, streamID, st.Schema, st.Table, currentPos)
		if err != nil {
			return fmt.Errorf("mysql: applier: prime schema-history cache for %s.%s: %w",
				st.Schema, st.Table, err)
		}
		if t == nil {
			// Defence-in-depth: ResolveSchemaVersion's contract is "never
			// (nil, nil)"; a nil table with a nil error would be a bug
			// in the resolve path. Surface loudly rather than silently
			// caching a nil entry that masquerades as a hit.
			return fmt.Errorf("mysql: applier: prime schema-history cache for %s.%s: "+
				"resolve returned nil table with nil error (resolve-path bug)",
				st.Schema, st.Table)
		}
		// We don't carry the resolved anchor through ResolveSchemaVersion's
		// return today (the resolver returns the table, not the anchor —
		// the anchor is internal to selection). For the cache's diagnostic
		// Anchor field we record currentPos as a "primed-at" marker; the
		// load-bearing field is IR. Chunk D may widen the resolver's
		// return to include the anchor if backup-envelope handling needs
		// it; until then, currentPos is the operationally-useful value.
		a.activeSchema[qualifiedName(st.Schema, st.Table)] = activeSchemaVersion{
			Anchor: currentPos,
			IR:     t,
		}
	}
	slog.DebugContext(
		ctx, "mysql: applier: schema-history cache primed",
		slog.String("stream_id", streamID),
		slog.String("position_token", currentPos.Token),
		slog.Int("tables", len(tables)),
	)
	return nil
}
