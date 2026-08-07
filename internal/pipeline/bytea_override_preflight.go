// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// The CDC half of item 135 / audit 2026-08-05 finding B-2, closed by
// REFUSAL rather than by disambiguation — because on this lane there is
// nothing left to disambiguate with.
//
// # The ambiguity
//
// `\xdead` is both PG's `bytea_output = hex` rendering of two bytes and
// six perfectly ordinary bytes. Item 135 removed the guesswork from the
// Postgres decoder by carrying PROVENANCE; the MySQL writer's remaining
// sniff was closed the same way, using [ir.Column.SourceColumnType] to
// tell a natively-binary column from one a `--type-override` MADE binary
// (see columnIsNativelyBinary in internal/engines/mysql/row_writer.go).
//
// # Why the CDC lane cannot use that answer
//
// A ChangeApplier resolves its column descriptors from the TARGET's
// information_schema, so `SourceColumnType` is nil for every column it
// ever sees — the override left no trace on the side the applier can
// read. And the CDC READER types its values from the SOURCE (pgoutput's
// Relation message; the pgtrigger reader's untyped JSON payload), so it
// is blind to the override in the other direction: a PG `text` column
// arrives as a Go string no matter what the override said.
//
// The result was a divergence WITHIN one run: cold-start decodes with the
// mapped type and stores the source's six bytes, then the first CDC
// update to the same cell stores two. Nothing errors, the row count
// matches, and both readings are individually plausible.
//
// # Why refusal is the right shape
//
// Closing it "properly" means carrying provenance on the VALUE across the
// engine-neutral IR Row, or teaching every ChangeApplier a source-side
// schema it does not have — a cross-cutting contract change, not a
// decoder change. The configuration that creates the ambiguity is narrow,
// explicit, and operator-chosen, so refusing it by name costs one flag
// combination and buys the elimination of a silent-corruption class.
// `migrate` is deliberately NOT refused: there is no CDC lane there, the
// reader decodes with the MAPPED type, and the value reaches the writer
// as `[]byte` — provenance-decided end to end.

// errBinaryOverrideOnCDC is the sentinel cause for the refusal. Wrapped
// with per-column detail naming each offending override.
var errBinaryOverrideOnCDC = errors.New(
	"pipeline: --type-override onto a binary target type is not supported on a continuous-sync run",
)

// preflightBinaryTypeOverrideOnCDC refuses a run whose `--type-override`
// / `mappings:` list lands any column on a BINARY target type AND whose
// schema is handed to a CDC lane.
//
// It runs on the mapping list alone — no schema, no connection — so it
// fires identically on cold-start AND on warm resume. That matters: warm
// resume never calls coldStartPrepareSchema, so a check hung off the
// mapped schema would have covered exactly the half of the runs that
// re-reads it and missed the half that does not.
//
// # Which entry points ask it, and why that list is a gate
//
// Two: `sync` ([Streamer.Run]) and `schema add-table` ([AddTable.Run]).
// Both carry their own `Mappings` and both hand the mapped table to a
// live CDC stream, so both create the configuration this refuses; the
// second was missed on the first cut (the flag is on the CLI, the mapped
// type creates the target column, and the stream then applies to it).
// `migrate`, `schema diff` and `schema preview` are exempt — the first
// has no CDC lane and the other two write nothing.
//
// That enumeration is not a promise: TestMappingsCDCLaneRoster derives
// the population from the AST — every struct in this package with a
// `Mappings []config.Mapping` field — and fails until each one either
// calls this preflight or is exempt with a reason.
//
// The known over-reach, stated rather than left implied: an override onto
// a binary type whose SOURCE column is ALREADY binary is a no-op and is
// refused too. Distinguishing it needs the source schema, which needs a
// connection, on a path that deliberately has none — and the remedy for
// that case is simply to drop the no-op override, which the message says.
func preflightBinaryTypeOverrideOnCDC(mappings []config.Mapping) error {
	var offenders []string
	for _, m := range mappings {
		ty, err := translate.ResolveTargetType(m)
		if err != nil {
			// A bad alias is ApplyMappings's error to report, with its
			// own message naming the recognised set. Skip it here rather
			// than shadowing that diagnostic with this one.
			continue
		}
		if !isBinaryFamily(ty) {
			continue
		}
		offenders = append(offenders, fmt.Sprintf("%s.%s=%s", m.Table, m.Column, m.TargetType))
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf(
		"%w: %s. A binary target column makes a source value ambiguous on the CDC lane: "+
			"the change reader types values from the SOURCE (so a text column still arrives as text) "+
			"and the applier reads column types from the TARGET (so it cannot see that an override "+
			"made the column binary). A value that spells `\\x` followed by an even run of hex digits "+
			"would then be read as PostgreSQL's bytea hex rendering and stored SHORTER than the source "+
			"holds — `\\xdead` as 2 bytes instead of 6, and a bare `\\x` as ZERO. Cold-start and CDC "+
			"would disagree about the same cell with no error on either side. Remedies: drop the "+
			"override and let the column carry its source type, or drop it as a no-op if the source "+
			"column is already binary. `sluice migrate` honours the override end to end (it has no CDC "+
			"lane), but ONLY as a one-shot: the binary target column it leaves behind carries the same "+
			"ambiguity, so do not later point a `sync` or `schema add-table` at that target — a source "+
			"value spelling `\\x`+hex would be stored short on every CDC apply",
		errBinaryOverrideOnCDC, strings.Join(offenders, ", "),
	)
}

// errBinaryTargetColumnOnCDC is the sentinel cause for the SCHEMA-keyed
// half of the same refusal. Wrapped with per-column detail.
var errBinaryTargetColumnOnCDC = errors.New(
	"pipeline: the target already carries a BINARY column for a source column that is not binary, " +
		"which a continuous-sync run cannot apply faithfully",
)

// preflightBinaryTargetColumnsOnCDC refuses a CDC-lane run whose TARGET
// already holds a binary column for a source column that is NOT binary.
//
// # Why the config-keyed refusal above is not enough
//
// [preflightBinaryTypeOverrideOnCDC] tests whether the flag is present in
// THIS invocation. The hazard is a property of the SCHEMA: the applier
// hex-decodes because the column it reads out of the target's
// information_schema is binary, and it neither knows nor cares which run
// made it binary. So `migrate --type-override t.c=binary_uuid` (honoured,
// the source's own bytes land) followed by a later `sync` with no
// override at all reproduces the refused configuration exactly, with
// `len(mappings) == 0` and nothing for a config check to see.
//
// This closes that by asking the two schemas instead. The source type is
// the column's pre-override type when an override fired and its declared
// type otherwise; the target type is what the target's own SchemaReader
// reports for the same column. Binary target + non-binary source is the
// precondition, whoever created it.
//
// # The independent expected value (the 2026-08-01 rule)
//
// The two sides come from different catalogs: `actual` is read from the
// TARGET server, `schema` from the SOURCE. Neither derives from the
// other, and neither derives from the value path this protects — which is
// the property that makes this a check rather than a restatement.
//
// # What it reaches, stated so the name cannot be read as broader
//
//   - `sync` single-database cold start, on EVERY branch of
//     coldStartGatePreflight. The two branches that CLEAR the target
//     first are covered by construction rather than by exemption: they
//     drop the in-scope tables before this runs, so the catalog read
//     finds nothing and the check is a no-op — and if a future branch
//     only truncates, the columns survive and it fires, which is also
//     right.
//   - `schema add-table`, against the target the live stream applies to.
//   - NOT the multi-database cold-start fan-out
//     (coldStartCopyOneDatabase): it reads no target catalog of its own
//     and the ADR-0166 shape gate is likewise absent there. That path is
//     covered only by the config-keyed refusal above. Filed rather than
//     half-built.
//   - NOT warm resume, which is the acknowledged blind half: it never
//     reads either schema, by design (the whole point is to resume
//     without a schema pass). A stream whose FIRST cold start ran before
//     this check existed, or whose target column was made binary while
//     the stream was stopped, resumes without either check firing unless
//     an override is still on the command line. Closing that needs a
//     schema read on the resume path; it is not free and it is not here.
func preflightBinaryTargetColumnsOnCDC(schema *ir.Schema, actual map[string]*ir.Table, mode string) error {
	if schema == nil || len(actual) == 0 {
		return nil
	}
	var offenders []string
	for _, t := range schema.Tables {
		act, exists := actual[t.Name]
		if !exists || act == nil {
			continue
		}
		actCols := targetColumnsByName(act)
		for _, c := range t.Columns {
			if c == nil {
				continue
			}
			src := c.Type
			if c.SourceColumnType != nil {
				src = c.SourceColumnType
			}
			if isBinaryFamily(src) {
				continue
			}
			actCol, ok := lookupTargetColumn(actCols, c.Name)
			if !ok || !isBinaryFamily(actCol.Type) {
				continue
			}
			offenders = append(offenders, fmt.Sprintf(
				"%s.%s (target %T, source %T)", t.Name, c.Name, actCol.Type, src,
			))
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	sort.Strings(offenders)
	return fmt.Errorf(
		"%w — refusing %s before any data moves: %s. The change applier reads column types from the "+
			"TARGET, so on a binary column it reads a source value that spells `\\x` followed by an even "+
			"run of hex digits as PostgreSQL's bytea rendering and stores it SHORTER than the source holds "+
			"(`\\xdead` as 2 bytes instead of 6, a bare `\\x` as ZERO), while the cold copy — which does "+
			"see the source type — stores the bytes verbatim. The same cell would disagree with itself "+
			"with no error on either side. Remedies: alter the target column to the source column's own "+
			"type (or drop it and let sluice create it), exclude the table with --exclude-table, or run "+
			"`sluice migrate` instead, which has no CDC lane",
		errBinaryTargetColumnOnCDC, mode, strings.Join(offenders, ", "),
	)
}

// targetColumnsByName indexes a catalog-read table's columns, mirroring
// the exact-name map [irdiff.TableColumnShape] compares with.
func targetColumnsByName(t *ir.Table) map[string]*ir.Column {
	out := make(map[string]*ir.Column, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil {
			out[c.Name] = c
		}
	}
	return out
}

// lookupTargetColumn resolves a source column name against the target's
// columns: exact first, then a UNIQUE case-insensitive match.
//
// The shape gate next door matches exact-only, and can afford to: a
// missed match there produces a LOUD "column absent" mismatch. Here a
// missed match is a silently skipped check — the failure direction that
// costs data — and MySQL folds column case while PG does not, so a
// PG `Payload` column read back from MySQL as `payload` must still be
// graded. The uniqueness requirement keeps the fallback from picking the
// wrong one of a PG pair that differs only by case.
func lookupTargetColumn(cols map[string]*ir.Column, name string) (*ir.Column, bool) {
	if c, ok := cols[name]; ok {
		return c, true
	}
	var (
		found *ir.Column
		n     int
	)
	for actName, c := range cols {
		if strings.EqualFold(actName, name) {
			found, n = c, n+1
		}
	}
	if n != 1 {
		return nil, false
	}
	return found, true
}

// isBinaryFamily reports whether ty is one of the IR types whose values
// are raw bytes — the family whose text rendering collides with its own
// value space.
//
// It delegates to [translate.IsBinaryFamily] so this refusal and the
// `schema preview` hint that suggests a binary override are decided by ONE
// predicate. They were two: preview kept recommending `binary_uuid` with no
// mention that a sync run refuses it, and nothing would have noticed if a
// second binary alias joined the hint registry.
func isBinaryFamily(ty ir.Type) bool {
	return translate.IsBinaryFamily(ty)
}
