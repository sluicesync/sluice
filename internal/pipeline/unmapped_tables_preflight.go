// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unmapped-table preflight (audit backlog C-11).
//
// A CDC stream can carry changes for a table that does not exist on the
// target. Before this preflight the two engines answered that differently and
// neither answer was good:
//
//   - Postgres logged a WARN and DROPPED the change — per row, for the life of
//     the stream, exiting 0. Every INSERT, UPDATE, DELETE and TRUNCATE for that
//     table vanished. Nothing counted the skips and nothing surfaced them, so
//     `sluice sync status` looked healthy while a table silently never synced.
//   - MySQL had no such skip: the write failed and the stream halted, mid-run,
//     naming one table.
//
// The scenario is ordinary rather than exotic. Postgres publications default to
// FOR ALL TABLES, so any source table an operator never migrated lands here.
//
// # What this does
//
// At stream start, enumerate the in-scope source tables, ask the TARGET which
// of them it cannot resolve, and refuse ONCE with the complete list and the
// exact command that fixes each one. Both engines, same answer, before any
// change is read.
//
// # Why it refuses rather than creating the table
//
// sluice can create it — `sluice schema add-table` does exactly that — but not
// implicitly from here, and the distinction is the whole point. That command
// drains the stream, creates the table, splices it into the publication, and
// BULK-COPIES the pre-existing rows from a consistent snapshot. A bare CREATE
// at this point would produce a table containing only the changes that happened
// after sluice noticed it, missing all prior history: a different silent-loss
// shape, arrived at while fixing this one. So the refusal names the command
// instead of guessing at the operator's intent.
//
// # Why the probe is an applier method rather than a schema diff here
//
// The apply path resolves a target table by looking the SOURCE's own
// schema-qualified name up in the target catalog — it performs no name mapping
// at all. A preflight that re-derived the mapping (namespace scope, case
// folding, engine conventions) could disagree with the runtime and refuse a
// stream that would have worked, which is the failure mode this exists to
// prevent. Asking the applier the same question the apply path asks keeps the
// preflight's verdict identical to the behaviour it predicts, by construction.

package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// phasePreflightUnmappedTables refuses a stream whose in-scope source tables
// include any the target cannot resolve, unless the operator has opted out with
// AllowUnmappedTables.
//
// It is a no-op — deliberately and quietly — when the target applier does not
// implement [ir.TargetTableProbe]. That is the same posture every other
// optional-surface phase takes, and it is stated here rather than implied: an
// engine without the probe keeps whatever behaviour it had, and adding an
// engine without one is a gap, not a pass.
func (s *Streamer) phasePreflightUnmappedTables(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	probe, ok := applier.(ir.TargetTableProbe)
	if !ok {
		return nil
	}

	read := s.inScopeSourceTables
	if s.testInScopeTables != nil {
		read = s.testInScopeTables
	}
	tables, err := read(ctx)
	if err != nil {
		// A source-schema read failure is NOT this preflight's business to
		// escalate: every path that genuinely needs the schema (cold start,
		// add-table) reads it again and reports its own error with better
		// context. Refusing here would convert an unrelated connectivity
		// problem into a confusing unmapped-table refusal.
		slog.DebugContext(ctx, "pipeline: unmapped-table preflight skipped; source schema unreadable",
			slog.String("stream_id", streamID), slog.String("err", err.Error()))
		return nil
	}

	var missing []string
	for _, t := range tables {
		exists, err := probe.TargetTableExists(ctx, t.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("pipeline: unmapped-table preflight: probe %s: %w", t.qualified(), err)
		}
		if !exists {
			missing = append(missing, t.qualified())
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)

	if s.AllowUnmappedTables {
		// The opt-out is loud on purpose. An operator who asked for this still
		// gets the list once, at INFO, so "which tables am I deliberately not
		// syncing?" has an answer that does not require reading WARN lines out
		// of a log tail.
		slog.InfoContext(
			ctx, "pipeline: replicating past tables that do not exist on the target (--allow-unmapped-tables)",
			slog.String("stream_id", streamID),
			slog.Int("unmapped_tables", len(missing)),
			slog.String("tables", strings.Join(missing, ", ")),
			slog.String("note", "every change for these tables is DROPPED; they will not appear on the target"),
		)
		return nil
	}

	return sluicecode.Wrap(
		sluicecode.CodeSyncUnmappedTables,
		"create the missing tables with `sluice schema add-table`, narrow the stream's table scope, or pass --allow-unmapped-tables to drop their changes deliberately",
		unmappedTablesRefusal(missing),
	)
}

// unmappedTablesRefusal builds the operator-facing refusal: what is missing,
// what happens if it is ignored, and the exact commands that fix it.
func unmappedTablesRefusal(missing []string) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "pipeline: %d replicated table(s) do not exist on the target, and every change for them would be DROPPED:\n", len(missing))
	for _, t := range missing {
		fmt.Fprintf(&sb, "  %s\n", t)
	}
	sb.WriteString("\nFix by one of:\n")
	sb.WriteString("  - create each on the target WITH its existing rows, one per table:\n")
	for _, t := range missing {
		fmt.Fprintf(&sb, "      sluice schema add-table %s\n", t)
	}
	sb.WriteString("    (that command copies the rows that already exist from a consistent snapshot; " +
		"creating the table by hand would leave it holding only changes made from now on)\n")
	sb.WriteString("  - narrow the stream so they are not replicated (--tables / --exclude-tables, or a " +
		"publication scoped to the tables the target actually has)\n")
	sb.WriteString("  - pass --allow-unmapped-tables to drop their changes deliberately\n")
	return fmt.Errorf("%s", sb.String())
}

// sourceTableRef is a schema-qualified source table the stream will replicate.
type sourceTableRef struct {
	Schema string
	Name   string
}

func (t sourceTableRef) qualified() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

// inScopeSourceTables reads the source schema and returns the tables this
// stream will actually replicate, after the stream's table filter.
func (s *Streamer) inScopeSourceTables(ctx context.Context) ([]sourceTableRef, error) {
	sr, err := s.Source.OpenSchemaReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, err
	}
	defer migcore.CloseIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]sourceTableRef, 0, len(schema.Tables))
	for _, tbl := range schema.Tables {
		if !s.Filter.Allows(tbl.Name) {
			continue
		}
		out = append(out, sourceTableRef{Schema: tbl.Schema, Name: tbl.Name})
	}
	return out, nil
}
