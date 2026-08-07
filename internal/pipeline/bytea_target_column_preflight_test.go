// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// mappingsWithBinaryOverride is the smallest config that trips the
// config-keyed refusal, kept beside the prose pin that reads it.
func mappingsWithBinaryOverride() []config.Mapping {
	return []config.Mapping{{Table: "docs", Column: "body", TargetType: "bytea"}}
}

// The SCHEMA-keyed half of audit B-2's CDC refusal. The config-keyed
// sibling next door is graded by an ALIAS matrix because a flag value is
// what it reads; this one reads TYPES, so it is graded as a family
// matrix on BOTH sides — every binary target family against every
// non-binary source family — per the Bug 74 lesson. A representative
// pair would have proven nothing about the others: the predicate is a
// type switch, and a family missing from it is invisible until a real
// column lands on it.

// binaryTargetFamilies is every IR type whose values are raw bytes, plus
// the domain wrapper, which is the shape that reaches this check from a
// PG target whose column is a domain over bytea.
func binaryTargetFamilies() map[string]ir.Type {
	return map[string]ir.Type{
		"blob":      ir.Blob{Size: ir.BlobLong},
		"blob-tiny": ir.Blob{Size: ir.BlobTiny},
		"binary":    ir.Binary{Length: 16},
		"varbinary": ir.Varbinary{Length: 64},
		"domain-over-blob": ir.Domain{
			Name: "sig", BaseType: ir.Blob{Size: ir.BlobRegular},
		},
	}
}

// nonBinarySourceFamilies is the other axis: the source families that
// can deliver a Go string to a CDC applier, which is the value shape the
// hex reading destroys.
func nonBinarySourceFamilies() map[string]ir.Type {
	return map[string]ir.Type{
		"text":    ir.Text{Size: ir.TextLong},
		"varchar": ir.Varchar{Length: 255},
		"char":    ir.Char{Length: 36},
		"uuid":    ir.UUID{},
		"json":    ir.JSON{},
		"integer": ir.Integer{Width: 64},
		"decimal": ir.Decimal{Precision: 10, Scale: 2},
		"domain-over-text": ir.Domain{
			Name: "email", BaseType: ir.Text{Size: ir.TextLong},
		},
	}
}

func targetTable(name string, cols ...*ir.Column) *ir.Table {
	return &ir.Table{Name: name, Columns: cols}
}

func binCol(name string, ty ir.Type) *ir.Column {
	return &ir.Column{Name: name, Type: ty}
}

// TestPreflightBinaryTargetColumns_FamilyMatrix is the refusal half:
// every binary TARGET family × every non-binary SOURCE family must be
// refused, whether or not an override is what made the target binary.
func TestPreflightBinaryTargetColumns_FamilyMatrix(t *testing.T) {
	for tgtName, tgt := range binaryTargetFamilies() {
		for srcName, src := range nonBinarySourceFamilies() {
			tgtName, tgt, srcName, src := tgtName, tgt, srcName, src
			t.Run(tgtName+"/"+srcName, func(t *testing.T) {
				// (a) no override in this invocation at all — the cell the
				// config-keyed refusal cannot see, and the one a
				// `migrate --type-override` followed by a plain `sync`
				// produces.
				schema := &ir.Schema{Tables: []*ir.Table{targetTable("docs", binCol("body", src))}}
				actual := map[string]*ir.Table{"docs": targetTable("docs", binCol("body", tgt))}
				err := preflightBinaryTargetColumnsOnCDC(schema, actual, "sync cold-start")
				if err == nil {
					t.Fatalf("target %s fed by source %s was accepted: the applier reads the target's "+
						"type, so a `\\x`+hex value would be stored SHORT on every CDC apply", tgtName, srcName)
				}
				if !errors.Is(err, errBinaryTargetColumnOnCDC) {
					t.Errorf("err = %v; want the errBinaryTargetColumnOnCDC sentinel", err)
				}
				if !strings.Contains(err.Error(), "docs.body") {
					t.Errorf("err = %v; want it to name the offending column", err)
				}

				// (b) the same cell reached by an override in THIS run: the
				// mapped Type is already binary and the pre-override type is
				// the evidence. Both readings must refuse, or the check
				// would pass exactly when the operator asked for the hazard
				// out loud.
				overridden := &ir.Schema{Tables: []*ir.Table{targetTable(
					"docs",
					&ir.Column{Name: "body", Type: tgt, SourceColumnType: src},
				)}}
				if err := preflightBinaryTargetColumnsOnCDC(overridden, actual, "sync cold-start"); err == nil {
					t.Errorf("overridden %s→%s was accepted", srcName, tgtName)
				}
			})
		}
	}
}

// TestPreflightBinaryTargetColumns_AllowsMatchingProvenance is the
// anti-over-refusal half: a target binary column whose SOURCE column is
// also binary is the ordinary bytea→BLOB translation every PG→MySQL
// migration produces, and refusing it would refuse the common case.
func TestPreflightBinaryTargetColumns_AllowsMatchingProvenance(t *testing.T) {
	for tgtName, tgt := range binaryTargetFamilies() {
		for srcName, src := range binaryTargetFamilies() {
			tgtName, tgt, srcName, src := tgtName, tgt, srcName, src
			t.Run(tgtName+"/"+srcName, func(t *testing.T) {
				schema := &ir.Schema{Tables: []*ir.Table{targetTable("docs", binCol("body", src))}}
				actual := map[string]*ir.Table{"docs": targetTable("docs", binCol("body", tgt))}
				if err := preflightBinaryTargetColumnsOnCDC(schema, actual, "sync cold-start"); err != nil {
					t.Errorf("binary source %s onto binary target %s refused: %v", srcName, tgtName, err)
				}
			})
		}
	}
	// A non-binary target is never the hazard, whatever the source is.
	for srcName, src := range nonBinarySourceFamilies() {
		schema := &ir.Schema{Tables: []*ir.Table{targetTable("docs", binCol("body", src))}}
		actual := map[string]*ir.Table{"docs": targetTable("docs", binCol("body", ir.Text{Size: ir.TextLong}))}
		if err := preflightBinaryTargetColumnsOnCDC(schema, actual, "sync cold-start"); err != nil {
			t.Errorf("non-binary target fed by %s refused: %v", srcName, err)
		}
	}
	// An override that moves a BINARY source onto a non-binary target is
	// the reverse direction and carries no ambiguity.
	reverse := &ir.Schema{Tables: []*ir.Table{targetTable(
		"docs",
		&ir.Column{Name: "body", Type: ir.Text{Size: ir.TextLong}, SourceColumnType: ir.Blob{Size: ir.BlobLong}},
	)}}
	actual := map[string]*ir.Table{"docs": targetTable("docs", binCol("body", ir.Text{Size: ir.TextLong}))}
	if err := preflightBinaryTargetColumnsOnCDC(reverse, actual, "sync cold-start"); err != nil {
		t.Errorf("binary source onto a text target refused: %v", err)
	}
}

// TestPreflightBinaryTargetColumns_NoOpCases pins the shapes that must
// not refuse and must not panic: nothing read from the target (the
// branches that dropped the tables first, and the uncomputable-catalog
// fallback), a table the target does not have, a column the target does
// not have, and a nil schema.
func TestPreflightBinaryTargetColumns_NoOpCases(t *testing.T) {
	src := &ir.Schema{Tables: []*ir.Table{targetTable("docs", binCol("body", ir.Text{Size: ir.TextLong}))}}
	blob := targetTable("docs", binCol("body", ir.Blob{Size: ir.BlobLong}))

	if err := preflightBinaryTargetColumnsOnCDC(src, nil, "sync cold-start"); err != nil {
		t.Errorf("empty catalog refused: %v", err)
	}
	if err := preflightBinaryTargetColumnsOnCDC(src, map[string]*ir.Table{}, "sync cold-start"); err != nil {
		t.Errorf("zero-length catalog refused: %v", err)
	}
	if err := preflightBinaryTargetColumnsOnCDC(nil, map[string]*ir.Table{"docs": blob}, "sync cold-start"); err != nil {
		t.Errorf("nil schema refused: %v", err)
	}
	if err := preflightBinaryTargetColumnsOnCDC(src, map[string]*ir.Table{"other": blob}, "sync cold-start"); err != nil {
		t.Errorf("a table not in scope refused: %v", err)
	}
	if err := preflightBinaryTargetColumnsOnCDC(src,
		map[string]*ir.Table{"docs": targetTable("docs", binCol("other_col", ir.Blob{Size: ir.BlobLong}))},
		"sync cold-start"); err != nil {
		t.Errorf("a binary column the source does not have refused: %v", err)
	}
}

// TestPreflightBinaryTargetColumns_ColumnNameFolding pins the lookup
// fallback. MySQL folds column case and PG does not, so a source
// `Payload` read back from a MySQL target as `payload` must still be
// graded — a missed match here is a silently skipped check, which is the
// failure direction that costs data.
func TestPreflightBinaryTargetColumns_ColumnNameFolding(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{targetTable("docs", binCol("Payload", ir.Text{Size: ir.TextLong}))}}
	folded := map[string]*ir.Table{"docs": targetTable("docs", binCol("payload", ir.Blob{Size: ir.BlobLong}))}
	if err := preflightBinaryTargetColumnsOnCDC(schema, folded, "sync cold-start"); err == nil {
		t.Error("a case-folded target column was not graded; the check silently skipped it")
	}

	// The uniqueness requirement: a PG target legitimately carrying two
	// columns that differ only by case is ambiguous, and guessing is
	// worse than the check not firing. Stated here so the behaviour is a
	// decision rather than an accident.
	ambiguous := map[string]*ir.Table{"docs": targetTable(
		"docs",
		binCol("payload", ir.Blob{Size: ir.BlobLong}),
		binCol("PAYLOAD", ir.Blob{Size: ir.BlobLong}),
	)}
	if err := preflightBinaryTargetColumnsOnCDC(schema, ambiguous, "sync cold-start"); err != nil {
		t.Errorf("an ambiguous case-fold was graded anyway: %v", err)
	}
}

// TestPreflightBinaryTargetColumns_NamesEveryOffender: an operator who
// fixes one column and re-runs to find the next is paying a round-trip
// per column for an answer the check already had.
func TestPreflightBinaryTargetColumns_NamesEveryOffender(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		targetTable("a", binCol("x", ir.Text{Size: ir.TextLong}), binCol("ok", ir.Blob{Size: ir.BlobLong})),
		targetTable("b", binCol("y", ir.UUID{})),
	}}
	actual := map[string]*ir.Table{
		"a": targetTable("a", binCol("x", ir.Blob{Size: ir.BlobLong}), binCol("ok", ir.Blob{Size: ir.BlobLong})),
		"b": targetTable("b", binCol("y", ir.Binary{Length: 16})),
	}
	err := preflightBinaryTargetColumnsOnCDC(schema, actual, "sync cold-start")
	if err == nil {
		t.Fatal("no refusal for two offending columns")
	}
	for _, want := range []string{"a.x", "b.y"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v; want it to name %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "a.ok") {
		t.Errorf("err = %v; a.ok is binary on both sides and must not be named", err)
	}
	if !strings.Contains(err.Error(), "sync cold-start") {
		t.Errorf("err = %v; want the caller's mode named so the operator knows which phase refused", err)
	}
}

// TestBinaryOverrideRemedyDoesNotRecommendTheLossyPath pins the
// remediation text itself.
//
// The refusal used to open its remedy list with "run `sluice migrate`
// (no CDC lane — the override is honoured end to end there)", which is
// true of that one command and walks the operator straight into the
// sequence this file's other half exists to catch: migrate lands the
// override, the target column is now binary, and the next `sync` — with
// no flag left for the config check to see — hex-decodes every update.
// A remediation hint that recommends a silently-lossy path is the worst
// shape a refusal can have, so the qualifier is load-bearing prose and
// gets a test.
func TestBinaryOverrideRemedyDoesNotRecommendTheLossyPath(t *testing.T) {
	err := preflightBinaryTypeOverrideOnCDC(mappingsWithBinaryOverride())
	if err == nil {
		t.Fatal("no refusal")
	}
	msg := err.Error()
	if strings.Contains(msg, "Remedies: run `sluice migrate`") {
		t.Error("the remedy list still LEADS with `sluice migrate`, which leaves a binary target " +
			"column that the next sync silently hex-decodes")
	}
	if !strings.Contains(msg, "ONLY as a one-shot") {
		t.Error("the `sluice migrate` mention no longer carries its qualifier; an operator " +
			"following it lands on the target column this refusal exists to prevent")
	}
	if !strings.Contains(msg, "schema add-table") {
		t.Error("the qualifier names `sync` but not `schema add-table`, which is the other entry " +
			"point that hands the mapped column to a CDC lane")
	}
}
