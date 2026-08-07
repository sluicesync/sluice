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

// preflightBinaryTypeOverrideOnCDC refuses a sync run whose `--type-override`
// / `mappings:` list lands any column on a BINARY target type.
//
// It runs on `s.Mappings` alone — no schema, no connection — so it fires
// identically on cold-start AND on warm resume. That matters: warm resume
// never calls coldStartPrepareSchema, so a check hung off the mapped
// schema would have covered exactly the half of the runs that re-reads it
// and missed the half that does not.
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
			"would disagree about the same cell with no error on either side. Remedies: run `sluice "+
			"migrate` (no CDC lane — the override is honoured end to end there), drop the override and "+
			"let the column carry its source type, or drop it as a no-op if the source column is "+
			"already binary",
		errBinaryOverrideOnCDC, strings.Join(offenders, ", "),
	)
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
