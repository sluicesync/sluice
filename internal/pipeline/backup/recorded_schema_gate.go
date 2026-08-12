// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The Bug 243 gate: a chain recorded by a pre-v0.120.0 MySQL-family
// reader can carry a MANGLED expression — a string literal that never
// closes — and emitting that recorded DDL failed `restore` mid-run with
// the target's raw parse error while `backup verify` passed the chain.
// The detector is [ir.TableExpressionLexProblems] (structural, never a
// repair); this file is its wiring into the three doors:
//
//   - `restore` (and therefore every chain segment's full, which runs
//     through the Restore path): a pre-DDL refusal, AFTER the table
//     filter — deliberately unlike the shape preflights, which run
//     pre-filter — because the remedy for a source that no longer
//     exists is `--exclude-table=<affected>` to salvage every other
//     table, and a filter-blind gate would make its own remedy
//     impossible.
//   - `chain restore`'s schema deltas: filter-aware BY CONSTRUCTION
//     since Bug 244 — the deltas are filtered before the door runs, so
//     `--exclude-table=<affected>` releases it exactly like the restore
//     door. (Pre-Bug-244 this read "unconditional because deltas apply
//     unfiltered"; the premise changed with the fix.)
//   - `backup verify`: chain-level, unfiltered — verify predicts the
//     UNFILTERED restore (the Bug 217/218 doctrine), so it refuses any
//     chain a plain `restore` would refuse.
//
// `backup incremental` WARNs instead (see incremental.go), and `backup
// compact` / `backup prune` WARN through
// [warnIfChainRecordedSchemaMalformed] below: their own work is valid,
// and refusing would stop an operation an operator may still want — but
// silently extending or maintaining an un-restorable chain is how
// Bug 243 presented, so the warning names the code.

// recordedSchemaMalformedHint is the operator remedy, shared by every
// door so one shape reports one (code, hint) pair.
const recordedSchemaMalformedHint = "take a fresh `backup full` of the live source with sluice v0.120.0 or newer " +
	"(the current reader records these expressions correctly); if the source is gone, " +
	"`restore --exclude-table=<the named table>` recovers every other table — the data chunks are intact"

// manifestRecordedSchemaProblems collects the lex problems of everything
// a manifest can ask a restore to emit: its full schema (when present)
// and every schema-delta table it adds or alters.
func manifestRecordedSchemaProblems(m *irbackup.Manifest) []string {
	if m == nil {
		return nil
	}
	problems := ir.SchemaExpressionLexProblems(m.Schema)
	for _, d := range m.SchemaDelta {
		problems = append(problems, ir.TableExpressionLexProblems(d.After)...)
	}
	return problems
}

// warnIfChainRecordedSchemaMalformed WARNs — once, on the first affected
// manifest — when the chain's restorable window carries a recorded
// schema that cannot be emitted as valid SQL (Bug 243's mangle class).
// The maintenance-door sibling of `backup incremental`'s
// warnIfParentChainUnrestorable (internal/pipeline/incremental.go):
// compact and prune rewrite/retire chain state at exit 0, and silently
// maintaining a chain whose restore is already broken is the exact shape
// Bug 243 presented. A WARN, deliberately not a refusal — the
// maintenance operation's own work is valid, and restore/verify hold the
// refusal.
//
// Coverage, stated per the gate rule: every RESTORABLE segment's full
// and incremental manifests (Schema + SchemaDelta.After via
// [manifestRecordedSchemaProblems]); retired segments sit outside the
// restore path and are unscanned. A manifest this scan cannot read is
// skipped without failing the maintenance op — the op reads the same
// files on its own behalf moments later and fails loudly there, so the
// skip cannot hide a read error.
func warnIfChainRecordedSchemaMalformed(ctx context.Context, op string, store irbackup.Store, cat *lineage.Catalog) {
	if cat == nil {
		return
	}
	for si := max(cat.RestorableFromSegment, 0); si < len(cat.Segments); si++ {
		seg := &cat.Segments[si]
		ss := seg.Store(store)
		paths := make([]string, 0, 1+len(seg.Incrementals))
		paths = append(paths, seg.FullManifestPath)
		paths = append(paths, seg.Incrementals...)
		for _, p := range paths {
			m, err := lineage.ReadManifestAt(ctx, ss, p)
			if err != nil {
				continue
			}
			problems := manifestRecordedSchemaProblems(m)
			if len(problems) == 0 {
				continue
			}
			slog.WarnContext(
				ctx,
				op+": this chain's recorded schema carries expressions that cannot be emitted as valid SQL — "+
					"the chain will NOT restore until a fresh `backup full` re-records the schema "+
					"(recorded by a sluice older than v0.120.0; restore/verify refuse it as SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED)",
				slog.String("manifest", p),
				slog.Int("segment", si),
				slog.Int("affected_expressions", len(problems)),
				slog.String("first", problems[0]),
			)
			return
		}
	}
}

// refuseMalformedRecordedSchema renders the coded refusal for a
// non-empty problem list. mode names the door ("restore", "chain
// restore", "verify") so the message reads in context.
func refuseMalformedRecordedSchema(mode string, problems []string) error {
	if len(problems) == 0 {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRecordedSchemaMalformed,
		recordedSchemaMalformedHint,
		fmt.Errorf("%s: the chain's recorded schema cannot be emitted as valid SQL "+
			"(recorded by a sluice older than v0.120.0, whose reader mangled apostrophe-carrying expressions):\n  %s",
			mode, strings.Join(problems, "\n  ")))
}

// filteredSchemaLexProblems is the restore-door variant: only tables the
// run will actually emit are checked, so `--exclude-table` on an
// affected table is a working remedy rather than a re-refusal.
func filteredSchemaLexProblems(s *ir.Schema, filter migcore.TableFilter) []string {
	if s == nil {
		return nil
	}
	var out []string
	for _, t := range s.Tables {
		if t == nil {
			continue
		}
		if !filter.IsEmpty() && !filter.Allows(t.Name) {
			continue
		}
		out = append(out, ir.TableExpressionLexProblems(t)...)
	}
	return out
}
