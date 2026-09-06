// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The multi-database / multi-schema fan-out's half of the CDC reader's
// prior-shape seed (SLM-1d, audit 2026-09-06 finding 1). schema_seed.go is
// the single-stream half; this file is the fan-out twin of both its arms.
//
// # The gap this closes
//
// The seed was wired at exactly the two SINGLE-stream reader-open sites
// ([Streamer.coldStartBeginCDC] and [Streamer.warmResume]). The two
// fan-out sites ([Streamer.coldStartMultiDatabase],
// [Streamer.warmResumeMultiDatabase]) wired the scope predicate and
// nothing else, and [Streamer.readerSchemaSeed] was never assigned on
// that path — so calling the wiring helper there would have been a no-op
// even if someone had added the call. A `--schemas` / `--databases`
// stream therefore ran with schemaSeed == nil on every open.
//
// OBSERVED on postgres:16 (2026-09-06), PG → PG with two selected
// schemas: cold start, one applied CDC transaction, a clean stop, then on
// the source under `SET TIME ZONE 'Asia/Tokyo'` an
// `ALTER TABLE sales.events ALTER COLUMN c TYPE timestamp` plus one
// INSERT, then a warm resume. No refusal, no WARN: the post-swap row was
// applied, the target column stayed `timestamp with time zone`, and the
// pre-existing rows read nine hours apart between source and target at
// exit 0. That is byte-for-byte the SLM-1c harm the single-stream fix
// measured, on the lane that was never wired.
//
// # What the fan-out needs that the single-stream path does not
//
// Namespaced KEYS. Both readers key their prior by (namespace, table) —
// the MySQL binlog reader through qualifiedName over the TABLE_MAP
// database, the pgoutput reader through the RelationMessage's Namespace —
// falling back to the reader's own bound namespace for a seed table with
// no Schema. A server-wide fan-out reader has no useful bound namespace,
// and two selected namespaces holding a same-named table are the ordinary
// case, so a bare-name seed would collide: one namespace would resume on
// the other's prior. Hence:
//
//   - Cold start accumulates the RAW per-namespace source IR the fan-out
//     already reads per database; those tables carry Table.Schema = their
//     source namespace, stamped by the scoped SchemaReader
//     ([ir.MultiDatabaseScoper]).
//   - Warm resume reads the target witness ONCE PER SELECTED NAMESPACE
//     through a per-namespace target DSN, and stamps each projected
//     witness table with its SOURCE namespace — never the target's, which
//     a `--map-database` run renames.
//
// # Reach, stated so it cannot be read as broader
//
// The seed is consulted by the Postgres lane's seeded door
// unconditionally, which is the lane the harm was measured on. The MySQL
// lanes gate their refusal on schemaDeltaAppliesToTarget, which is false
// in multi-database mode by construction ([singleStreamSchemaForwardActive]),
// so on a MySQL fan-out the seed is delivered and INERT today: it arms
// nothing, and it changes no behaviour. It is wired anyway rather than
// engine-gated, because the alternative is a reader-open site that
// deliberately does not seed — the exact state this whole surface exists
// to make impossible, and the state the parity gate now fails on.

import (
	"context"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// multiDatabaseWarmResumeSchemaSeedLoader is the fan-out twin of
// [Streamer.warmResumeSchemaSeedLoader]. It is installed inside
// [Streamer.warmResumeMultiDatabase] rather than in the dispatcher,
// because the selected namespace set is resolved from the live server
// there and the dispatcher does not have it.
// targetDeriver is resolved by the caller rather than asserted here on
// purpose: the assertion is the point at which a fan-out decides how the
// target namespaces, and TestMultiNamespaceFanOutRefusesAFlatTarget requires
// whoever makes it to have reached the flat-target refusal and the
// namespace-fold preflight first. The warm-resume open has; a seed loader
// running mid-resume has not and must not be the place that decides.
func (s *Streamer) multiDatabaseWarmResumeSchemaSeedLoader(
	applier ir.ChangeApplier,
	streamID string,
	persisted ir.Position,
	selected []string,
	targetDeriver ir.DatabaseDSNDeriver,
) schemaSeedLoader {
	return func(ctx context.Context) ([]*ir.Table, error) {
		return s.loadMultiDatabaseWarmResumeSchemaSeed(ctx, applier, streamID, persisted, selected, targetDeriver)
	}
}

// loadMultiDatabaseWarmResumeSchemaSeed builds the fan-out warm-resume
// prior: per selected SOURCE namespace, the target's zone witness for that
// namespace with the retained schema-history versions of that namespace as
// the per-table fallback, merged by the same
// [mergeWarmResumeSeed] the single-stream path uses and stamped with the
// source namespace.
//
// The history read is loud on error exactly as the single-stream loader's
// is; the per-namespace witness read DEGRADES with a WARN (see
// [Streamer.loadMultiDatabaseTargetZoneWitness]).
//
// A retained history row with no namespace recorded is dropped rather than
// attributed to a guess: a wrong-namespace prior is a phantom refusal on
// one table and a missing one on another, while dropping it leaves that
// table exactly where it is today (no prior), and the fan-out's own
// boundary emitter records the source database on every row it writes.
func (s *Streamer) loadMultiDatabaseWarmResumeSchemaSeed(
	ctx context.Context,
	applier ir.ChangeApplier,
	streamID string,
	persisted ir.Position,
	selected []string,
	targetDeriver ir.DatabaseDSNDeriver,
) ([]*ir.Table, error) {
	history, err := loadRetainedSchemaSeed(ctx, applier, s.Source, streamID, persisted)
	if err != nil {
		return nil, err
	}
	historyByNamespace := map[string][]*ir.Table{}
	unattributed := 0
	for _, t := range history {
		if t == nil {
			continue
		}
		if t.Schema == "" {
			unattributed++
			continue
		}
		historyByNamespace[t.Schema] = append(historyByNamespace[t.Schema], t)
	}
	if unattributed > 0 {
		slog.WarnContext(
			ctx, "multi-database warm resume: retained schema-history rows carry no source namespace and cannot be keyed for a server-wide reader; those tables resume without a history prior",
			slog.String("stream_id", streamID),
			slog.Int("rows", unattributed),
		)
	}

	var out []*ir.Table
	for _, namespace := range selected {
		witness, err := s.loadMultiDatabaseTargetZoneWitness(ctx, streamID, namespace, targetDeriver)
		if err != nil {
			return nil, err
		}
		merged, err := mergeWarmResumeSeed(ctx, streamID, namespace, witness, historyByNamespace[namespace], s.Mappings)
		if err != nil {
			return nil, err
		}
		out = append(out, merged...)
	}
	return out, nil
}

// loadMultiDatabaseTargetZoneWitness reads the target's zone witness for
// ONE source namespace, through a target DSN derived for that namespace's
// (possibly renamed, ADR-0142) target counterpart.
//
// Two arms degrade to an empty witness with a WARN rather than failing the
// resume, and the choice is deliberate in both:
//
//   - The caller resolved no [ir.DatabaseDSNDeriver] on the target. Every
//     shipping fan-out target implements one (MySQL and Postgres both), and
//     migcore.ValidateMultiNamespaceTarget refuses a flat target on both
//     legs — so this arm is a future-engine guard (a namespaced target that
//     routes by Table.Schema alone), not a live path.
//   - The per-namespace read failed. The commonest cause is a target
//     namespace that was never cold-started: warm resume re-resolves the
//     selected set from the LIVE server, so a source database created since
//     cold start is admitted by the scope predicate and has no target
//     counterpart yet. That configuration runs today (its first routed
//     change fails loudly, and until one arrives it is quiet), and turning
//     it into a refusal at every resume would be a loud failure on a
//     working configuration.
//
// The degrade is the same shape [mergeWarmResumeSeed] already applies one
// level down for "the target does not hold the table": WARN, fall back to
// whatever history exists, and say in the log that the affected tables'
// FIRST boundary is unchecked. What it is NOT is silent — an operator
// grepping the resume sees the namespace by name.
func (s *Streamer) loadMultiDatabaseTargetZoneWitness(
	ctx context.Context,
	streamID, sourceNamespace string,
	deriver ir.DatabaseDSNDeriver,
) (map[string]*ir.Table, error) {
	if s.Target == nil {
		return nil, fmt.Errorf("pipeline: load target zone witness for namespace %q: nil target engine", sourceNamespace)
	}
	targetNamespace := s.NamespaceMap.Apply(sourceNamespace)
	if deriver == nil {
		slog.WarnContext(
			ctx, "multi-database warm resume: the target engine cannot derive a per-namespace DSN, so no target zone witness can be read for this namespace; its tables resume on the retained schema history alone, and a table with no history row resumes with NO prior shape — its FIRST schema boundary is not checked for a session-zone cast",
			slog.String("stream_id", streamID),
			slog.String("namespace", sourceNamespace),
			slog.String("target_engine", s.Target.Name()),
		)
		return map[string]*ir.Table{}, nil
	}
	dsn, err := deriver.WithDatabase(s.TargetDSN, targetNamespace)
	if err != nil {
		return nil, fmt.Errorf("pipeline: derive target DSN for namespace %q: %w", targetNamespace, err)
	}
	witness, err := s.loadTargetZoneWitnessFromDSN(ctx, dsn)
	if err != nil {
		slog.WarnContext(
			ctx, "multi-database warm resume: the target namespace could not be read (it may not exist yet — the selected set is re-resolved from the live source on every resume), so no target zone witness is available for it; its tables resume on the retained schema history alone, and a table with no history row resumes with NO prior shape — its FIRST schema boundary is not checked for a session-zone cast",
			slog.String("stream_id", streamID),
			slog.String("namespace", sourceNamespace),
			slog.String("target_namespace", targetNamespace),
			slog.String("error", err.Error()),
		)
		return map[string]*ir.Table{}, nil
	}
	return witness, nil
}

// multiDatabaseColdStartSchemaSeed folds one selected namespace's RAW
// source tables into the fan-out cold-start seed. The tables are the
// scoped SchemaReader's own, captured before [translate.ApplyMappings]
// and the expression overrides rewrite types FOR THE TARGET — the same
// capture point, and the same rationale, as
// [Streamer.coldStartPrepareSchema]'s single-stream capture.
//
// Table.Schema is stamped by the scoped reader ([ir.MultiDatabaseScoper]);
// the defensive fill here covers an engine whose reader leaves it empty,
// because an unnamespaced table in a server-wide seed would resolve under
// the reader's bound namespace and speak for the wrong table.
func multiDatabaseColdStartSchemaSeed(seed []*ir.Table, namespace string, schema *ir.Schema) []*ir.Table {
	for _, t := range rawReaderSchemaSeed(schema) {
		if t.Schema == "" {
			clone := *t
			clone.Schema = namespace
			seed = append(seed, &clone)
			continue
		}
		seed = append(seed, t)
	}
	return seed
}
