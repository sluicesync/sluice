// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
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
//   - `chain restore`'s schema deltas: delta tables are applied without
//     filtering, so their gate is unconditional.
//   - `backup verify`: chain-level, unfiltered — verify predicts the
//     UNFILTERED restore (the Bug 217/218 doctrine), so it refuses any
//     chain a plain `restore` would refuse.
//
// `backup incremental` WARNs instead (see incremental.go): the
// incremental's own data is valid, and refusing would stop a backup an
// operator may still want — but silently extending an un-restorable
// chain is how Bug 243 presented, so the warning names the code.

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
