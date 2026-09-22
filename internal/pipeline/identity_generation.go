// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// postCopyGates runs the two migrate-only checks that follow
// runBulkCopyPhases, in order:
//
//  1. Roadmap item 109: any FK the constraints phase added metadata-only
//     (the statement-wall recovery under foreign_key_checks=0) skipped
//     InnoDB's child-row validation. PROVE those child rows actually
//     satisfy the FK with a bounded chunked orphan scan — recovering the
//     loud-failure signal without re-incurring the wall. No-op unless a
//     metadata-only add happened (an armed VStream migrate whose FK
//     walled); a violation drops the FK and refuses
//     (SLUICE-E-FK-SOURCE-ORPHAN). The constraints phase re-runs
//     idempotently on --resume, so a refusal re-derives deterministically
//     rather than a resume silently completing with the FK absent.
//  2. GC-3: the GENERATED ALWAYS restore
//     ([Migrator.restoreIdentityGenerationPhase]), last so no row write
//     follows it.
//
// Bundled so the migrate ladder in runSingleDatabase stays one call
// per gate rather than growing a line per finding.
func (m *Migrator) postCopyGates(ctx context.Context, rc resumeContext, state ir.MigrationState, sw ir.SchemaWriter, schema, createSchema *ir.Schema) error {
	if err := m.verifyUnvalidatedForeignKeys(ctx, sw, schema); err != nil {
		return err
	}
	return m.restoreIdentityGenerationPhase(ctx, rc, state, sw, createSchema)
}

// restoreIdentityGenerationPhase is the migrate ladder's last phase
// (gap census 2026-09-22 S4): put GENERATED ALWAYS back on the identity
// columns the target writer relaxed to BY DEFAULT for the copy.
//
// MIGRATE ONLY, by construction — it is called after runBulkCopyPhases
// in [Migrator.runSingleDatabase], and the sync cold start
// (streamer_coldstart_parallel.go, coldStartRunCopy), `restore` and
// chain restore run their own ladders that never reach it. That is
// deliberate, not an omission: every one of those paths hands the target
// to a change applier that writes explicit ids (CDC rows, incremental
// links, a `sync` resumed from a restored position), which an ALWAYS
// column refuses with SQLSTATE 428C9; the writer's
// IDENTITY-ALWAYS-DOWNGRADED WARN is the operator's signal there. Last
// in the ladder so no row write follows it, and scoped to createSchema —
// the tables sluice itself created this run — so a table the ADR-0166
// pre-create gate skipped (the operator's own catalog) is never altered.
//
// Two migrate shapes are ALSO not the last explicit-id writer and keep
// BY DEFAULT: a shard-consolidation run (InjectShardColumn — the next
// shard's copy lands into these same tables and an ALWAYS column would
// refuse it) and --force-cold-start (the operator declared the target is
// being bulk-loaded into while populated).
func (m *Migrator) restoreIdentityGenerationPhase(ctx context.Context, rc resumeContext, state ir.MigrationState, sw ir.SchemaWriter, createSchema *ir.Schema) error {
	keepByDefault := m.InjectShardColumn.Engaged() || m.ForceColdStart
	if err := restoreIdentityGeneration(ctx, sw, createSchema, keepByDefault); err != nil {
		return migcore.WrapWithHint(migcore.PhaseSchemaApply, markFailed(ctx, rc, state, ir.MigrationPhaseConstraints, err))
	}
	return nil
}

// restoreIdentityGeneration runs the optional [ir.IdentityGenerationRestorer]
// phase (gap census 2026-09-22 S4): after every row-writing phase of a
// `migrate`, the target writer puts GENERATED ALWAYS back on the identity
// columns it created BY DEFAULT for the copy. Engines without the surface,
// and schemas with no ALWAYS column, are a no-op — so the phase is
// invisible to every existing phase-order pin and to every MySQL/SQLite
// target.
//
// keepByDefault is the caller's declaration that this target will take
// MORE explicit-id writes after this run (shard consolidation, a
// --force-cold-start load), in which case ALWAYS would refuse them with
// SQLSTATE 428C9; the phase then WARNs instead of restoring, so the
// operator sees the columns stay downgraded rather than finding out from
// the next shard's failure — or, worse, not finding out at all.
func restoreIdentityGeneration(ctx context.Context, sw ir.SchemaWriter, schema *ir.Schema, keepByDefault bool) error {
	restorer, ok := sw.(ir.IdentityGenerationRestorer)
	if !ok || schema == nil {
		return nil
	}
	cols := identityAlwaysColumns(schema)
	if len(cols) == 0 {
		return nil
	}
	if keepByDefault {
		slog.WarnContext(ctx, "pipeline: IDENTITY-ALWAYS-DOWNGRADED: GENERATED ALWAYS is NOT restored on this run because the target "+
			"will take further explicit-id bulk loads (shard consolidation or --force-cold-start); "+
			"re-apply `ALTER TABLE … ALTER COLUMN … SET GENERATED ALWAYS` after the last load",
			slog.Int("columns", len(cols)))
		return nil
	}
	return restorer.RestoreIdentityGeneration(ctx, schema)
}

// identityAlwaysColumns counts the columns whose source declared
// GENERATED ALWAYS AS IDENTITY — the pre-scan that keeps the phase a
// no-op (no dispatch, no log line) for the overwhelmingly common schema
// that has none.
func identityAlwaysColumns(schema *ir.Schema) []string {
	var out []string
	for _, t := range schema.Tables {
		if t == nil {
			continue
		}
		for _, c := range t.Columns {
			if c != nil && c.Identity != nil && c.Identity.Always {
				out = append(out, t.Name+"."+c.Name)
			}
		}
	}
	return out
}
