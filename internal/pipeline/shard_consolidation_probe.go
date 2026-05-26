// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0054 Shape A Phase 2c — IR-delta classifier + per-shape probes.
//
// Per DP-E (added 2026-05-22): the lease-holder derives the structural
// change to apply from the IR delta between the pre-DDL and post-DDL
// SchemaSnapshots; the classifier maps each delta to a finite catalog
// of recognized shapes; the engine's ir.SchemaDeltaApplier applies the
// shape; the takeover-stream's probe uses the same classifier on the
// target schema to verify whether the prior holder's intended change
// landed.
//
// DP-B's "no allow-list, no parser" intent is preserved: the classifier
// compares two *ir.Table structs (sluice's own canonical schema
// representation, not SQL text), and the "shapes" are sluice's own
// categories of structural changes — not an operator-curated SQL
// allowlist. §4's probe catalog and the apply catalog are the SAME set
// by design.
//
// v1 catalog: ADD COLUMN, DROP COLUMN, CREATE INDEX, DROP INDEX,
// ALTER COLUMN type/nullability, RENAME COLUMN (single column, full
// attribute match — see ShapeKindRenameColumn). Unrecognized
// structural changes refuse loudly with operator-actionable
// drained-model recovery hint.

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/orware/sluice/internal/ir"
)

// ShapeKind classifies an IR-delta into one of the recognized shapes
// from ADR-0054 DP-E. Unrecognized deltas surface as
// ShapeKindUnrecognized so the caller refuses loudly.
type ShapeKind int

const (
	// ShapeKindNone — pre and post are structurally identical
	// (modulo cosmetic differences ADR-0049's SchemaSignature.Equal
	// already filters). The lease still records the boundary but
	// applies no DDL.
	ShapeKindNone ShapeKind = iota

	// ShapeKindAddColumn — exactly one or more columns appear in
	// post that are absent from pre; no other structural change.
	ShapeKindAddColumn

	// ShapeKindDropColumn — exactly one or more columns from pre
	// are absent in post; no other structural change.
	ShapeKindDropColumn

	// ShapeKindCreateIndex — exactly one or more named indexes
	// appear in post that are absent from pre; no other change.
	ShapeKindCreateIndex

	// ShapeKindDropIndex — exactly one or more named indexes from
	// pre are absent in post; no other change.
	ShapeKindDropIndex

	// ShapeKindAlterColumnType — exactly one column with the same
	// name appears in both pre and post, but its IR Type differs.
	ShapeKindAlterColumnType

	// ShapeKindAlterColumnNullability — same name in both, same
	// type, but Nullable differs.
	ShapeKindAlterColumnNullability

	// ShapeKindRenameColumn — exactly one column appears in post
	// that's absent from pre AND exactly one column appears in pre
	// that's absent from post AND the added column's IR Type +
	// Nullable + (full ir.Column equality minus Name) match the
	// dropped column's. The post-DDL IR comparison treats a same-
	// attribute drop+add as a rename per the v0.78.0 catalog
	// expansion (task #22): from a CDC-apply perspective preserving
	// the column's data under a new identifier is operationally
	// equivalent to a rename, and PG / MySQL `RENAME COLUMN`
	// preserves every column attribute except the name.
	//
	// Indistinguishable-from-drop-add-same-attributes edge: at the
	// IR level a literal `DROP COLUMN foo; ADD COLUMN bar <same-
	// attrs>` is byte-identical to `RENAME COLUMN foo TO bar`. The
	// classifier treats both as rename; the ADR-0054 v0.78.0
	// amendment documents this as intentional (operator intent
	// preserves the data under a new identifier either way).
	ShapeKindRenameColumn

	// ShapeKindAddCheck — one or more named CHECK constraints
	// appear in post that are absent from pre; no other structural
	// change. v1 catalog expansion per ADR-0064 (task #22 sub-task).
	ShapeKindAddCheck

	// ShapeKindDropCheck — one or more named CHECK constraints from
	// pre are absent in post; no other structural change. Per
	// ADR-0064.
	ShapeKindDropCheck

	// ShapeKindModifyCheck — exactly one same-named CHECK exists in
	// both pre and post but its Expr text differs. The engine applies
	// as DROP + ADD (neither PG nor MySQL supports in-place CHECK
	// expression rewrite). Per ADR-0064.
	ShapeKindModifyCheck

	// ShapeKindUnrecognized — the delta doesn't fit a single
	// recognized shape (e.g. multi-shape combo, multi-column
	// rename, generated-column expression change, FK change, ...).
	// Refuses loudly per the loud-failure tenet.
	ShapeKindUnrecognized
)

// String renders a ShapeKind for logs and refusal messages.
func (k ShapeKind) String() string {
	switch k {
	case ShapeKindNone:
		return "none"
	case ShapeKindAddColumn:
		return "add-column"
	case ShapeKindDropColumn:
		return "drop-column"
	case ShapeKindCreateIndex:
		return "create-index"
	case ShapeKindDropIndex:
		return "drop-index"
	case ShapeKindAlterColumnType:
		return "alter-column-type"
	case ShapeKindAlterColumnNullability:
		return "alter-column-nullability"
	case ShapeKindRenameColumn:
		return "rename-column"
	case ShapeKindAddCheck:
		return "add-check"
	case ShapeKindDropCheck:
		return "drop-check"
	case ShapeKindModifyCheck:
		return "modify-check"
	case ShapeKindUnrecognized:
		return "unrecognized"
	}
	return "unknown"
}

// Shape carries the classifier's verdict along with the
// shape-specific payload (which columns / indexes are affected). The
// applier consumes the payload to issue the right ALTER; the probe
// consumes it to query the target schema for the observable effect.
type Shape struct {
	Kind ShapeKind

	// AddedColumns is the slice of columns that exist in post but not
	// pre. Populated for ShapeKindAddColumn.
	AddedColumns []*ir.Column

	// DroppedColumns is the slice of columns that exist in pre but
	// not post. Populated for ShapeKindDropColumn.
	DroppedColumns []*ir.Column

	// CreatedIndexes is the slice of indexes that exist in post but
	// not pre. Populated for ShapeKindCreateIndex.
	CreatedIndexes []*ir.Index

	// DroppedIndexes is the slice of indexes that exist in pre but
	// not post. Populated for ShapeKindDropIndex.
	DroppedIndexes []*ir.Index

	// AlteredColumn is the column whose Type or Nullability
	// changed. Populated for ShapeKindAlterColumnType /
	// ShapeKindAlterColumnNullability. The pre-state is in
	// AlteredColumnBefore; the post-state is in AlteredColumn.
	AlteredColumn       *ir.Column
	AlteredColumnBefore *ir.Column

	// RenamedColumnBefore is the pre-DDL column whose Name appears
	// in pre but not post. Populated for ShapeKindRenameColumn.
	// RenamedColumnAfter is the post-DDL column whose Name appears
	// in post but not pre — same Type / Nullable / (other column
	// attributes) as Before, only Name differs. The pair is what
	// the engine's AlterRenameColumn needs to emit
	// `ALTER TABLE <t> RENAME COLUMN <Before.Name> TO <After.Name>`.
	RenamedColumnBefore *ir.Column
	RenamedColumnAfter  *ir.Column

	// AddedChecks is the slice of named CHECK constraints that
	// exist in post but not pre. Populated for ShapeKindAddCheck.
	AddedChecks []*ir.CheckConstraint

	// DroppedChecks is the slice of named CHECK constraints that
	// exist in pre but not post. Populated for ShapeKindDropCheck.
	DroppedChecks []*ir.CheckConstraint

	// ModifiedCheckBefore / ModifiedCheckAfter are the pre/post
	// constraints for the same name whose Expr differs. Populated
	// for ShapeKindModifyCheck. The engine applies as DROP + ADD —
	// AlterModifyCheck takes both so the recovery path on takeover
	// has the original-Expr to compare against when probing.
	ModifiedCheckBefore *ir.CheckConstraint
	ModifiedCheckAfter  *ir.CheckConstraint
}

// ClassifyShape derives the IR-delta shape from (pre, post). Returns
// ShapeKindNone when pre and post are structurally identical (per
// ADR-0049 SchemaSignature.Equal's contract — same column names and
// types in order); ShapeKindUnrecognized when the delta doesn't fit
// the v1 catalog (the caller refuses loudly with drained-model
// recovery hint).
//
// nil pre or post is treated as "absent": a fresh-table boundary (pre
// nil) is not a recognized DDL shape (the consolidated target's table
// was created at cold-start, not via DDL); the classifier returns
// ShapeKindUnrecognized + a clear error.
//
// The classifier is pure: it touches only the IR struct fields. No
// database queries, no engine-specific knowledge. The probes (below)
// handle the engine-specific schema lookups.
func ClassifyShape(pre, post *ir.Table) (Shape, error) {
	if pre == nil || post == nil {
		return Shape{Kind: ShapeKindUnrecognized}, errors.New("pipeline: classify shape: pre or post table is nil")
	}

	added, dropped := diffColumns(pre, post)
	createdIdx, droppedIdx := diffIndexes(pre, post)
	alteredCol, alteredBefore, alteredKind, hasAlter := diffAlteredColumn(pre, post)
	addedChecks, droppedChecks, modBefore, modAfter, hasModCheck := diffChecks(pre, post)

	// RENAME COLUMN detection (task #22): exactly one added + exactly
	// one dropped, with full ir.Column attribute equality minus Name.
	// Must fire BEFORE the multi-class combo refusal below — at the
	// add/dropped-column-count level it would otherwise look like a
	// combo of ShapeKindAddColumn + ShapeKindDropColumn. The rename
	// classification consumes BOTH the added and dropped slices so
	// the class-counter sees neither.
	//
	// No index / altered-column / CHECK overlap permitted — a rename
	// plus another delta class in the same boundary is still a combo
	// refusal (multi-shape combo deltas refuse loudly).
	if renamedBefore, renamedAfter, ok := diffRenameColumn(added, dropped); ok &&
		len(createdIdx) == 0 && len(droppedIdx) == 0 && !hasAlter &&
		len(addedChecks) == 0 && len(droppedChecks) == 0 && !hasModCheck {
		return Shape{
			Kind:                ShapeKindRenameColumn,
			RenamedColumnBefore: renamedBefore,
			RenamedColumnAfter:  renamedAfter,
		}, nil
	}

	// Count which delta classes fired. Exactly-one is recognized;
	// zero is None; more-than-one is Unrecognized (combo deltas
	// refuse loudly).
	classes := 0
	var kind ShapeKind
	if len(added) > 0 {
		classes++
		kind = ShapeKindAddColumn
	}
	if len(dropped) > 0 {
		classes++
		kind = ShapeKindDropColumn
	}
	if len(createdIdx) > 0 {
		classes++
		kind = ShapeKindCreateIndex
	}
	if len(droppedIdx) > 0 {
		classes++
		kind = ShapeKindDropIndex
	}
	if hasAlter {
		classes++
		kind = alteredKind
	}
	if len(addedChecks) > 0 {
		classes++
		kind = ShapeKindAddCheck
	}
	if len(droppedChecks) > 0 {
		classes++
		kind = ShapeKindDropCheck
	}
	if hasModCheck {
		classes++
		kind = ShapeKindModifyCheck
	}
	switch classes {
	case 0:
		return Shape{Kind: ShapeKindNone}, nil
	case 1:
		return Shape{
			Kind:                kind,
			AddedColumns:        added,
			DroppedColumns:      dropped,
			CreatedIndexes:      createdIdx,
			DroppedIndexes:      droppedIdx,
			AlteredColumn:       alteredCol,
			AlteredColumnBefore: alteredBefore,
			AddedChecks:         addedChecks,
			DroppedChecks:       droppedChecks,
			ModifiedCheckBefore: modBefore,
			ModifiedCheckAfter:  modAfter,
		}, nil
	default:
		// Multi-shape combo delta — refuse loudly per ADR-0054
		// DP-E. The caller surfaces the operator-actionable recovery
		// hint (drained model).
		return Shape{Kind: ShapeKindUnrecognized}, fmt.Errorf(
			"pipeline: classify shape: unrecognized multi-shape combo delta "+
				"(added=%d dropped=%d created-idx=%d dropped-idx=%d altered-col=%t "+
				"added-check=%d dropped-check=%d modified-check=%t)",
			len(added), len(dropped), len(createdIdx), len(droppedIdx), hasAlter,
			len(addedChecks), len(droppedChecks), hasModCheck,
		)
	}
}

// diffRenameColumn detects the RENAME COLUMN shape from the
// (added, dropped) result of diffColumns. The locked heuristic
// (task #22, ADR-0054 v0.78.0 amendment):
//
//   - len(added) == 1 AND len(dropped) == 1.
//   - The added column's Type, Nullable, and every other ir.Column
//     attribute (Default, AutoIncrement, Comment, GeneratedExpr,
//     GeneratedKind, Identity flags, etc.) match the dropped
//     column's — equality holds on the FULL ir.Column struct with
//     the Name field excluded.
//
// Why full-attribute-match is the right signal: both PG and MySQL
// `RENAME COLUMN` preserve every column attribute except the name.
// A drop+add of the same name with DIFFERENT type/nullable/attrs
// represents an operator reshaping the table (not a rename); the
// IR-level intent there is genuinely two separate columns, and the
// existing combo-Unrecognized refusal path is correct for that case.
//
// Indistinguishable-from-drop-add-same-attributes edge: a literal
// `DROP COLUMN foo; ADD COLUMN bar <same-attrs>` is byte-identical
// to `RENAME COLUMN foo TO bar` at the IR level. The classifier
// treats both as rename; from a CDC apply perspective the
// operator's intent — preserve column data under a new identifier
// — is preserved either way. The ADR-0054 v0.78.0 amendment
// documents this as intentional.
//
// Multi-column rename (len(added) == N + len(dropped) == N for N>1)
// is OUT of v1 scope: with the type-match heuristic the pair-up
// between old and new names is ambiguous (which dropped name maps
// to which added name?). The classifier returns ok=false in that
// case; the call site falls through to the combo-Unrecognized
// refusal — operators issuing multi-column ALTER ... RENAME use
// the drained model.
func diffRenameColumn(added, dropped []*ir.Column) (before, after *ir.Column, ok bool) {
	if len(added) != 1 || len(dropped) != 1 {
		return nil, nil, false
	}
	a, d := added[0], dropped[0]
	if a == nil || d == nil {
		return nil, nil, false
	}
	// Compare on a copy with Name zeroed — every other LOAD-BEARING
	// attribute on ir.Column must match. The load-bearing attributes
	// are exactly the ones PG and MySQL `RENAME COLUMN` preserve:
	// Type (via reflect.DeepEqual — mirrors diffAlteredColumn),
	// Nullable, Default, Comment, GeneratedExpr / GeneratedStored
	// (a generated column expression change is a separate shape),
	// and the source-tagging dialect.
	//
	// SourceColumnType + SluiceInjected are deliberately NOT compared:
	// they're translation-pass provenance fields the readers and CDC
	// projections populate asymmetrically (the cold-start seed
	// captures Schema-reader provenance; the CDC SchemaSnapshot
	// projects from the wire protocol and never sets them). Including
	// them in the equality lens would create false reshape verdicts on
	// every CDC-flowed boundary.
	aCopy, dCopy := *a, *d
	aCopy.Name = ""
	dCopy.Name = ""
	// Erase provenance-only fields per the comment above.
	aCopy.SourceColumnType = nil
	dCopy.SourceColumnType = nil
	aCopy.SluiceInjected = false
	dCopy.SluiceInjected = false
	// Normalize Default: the cold-start SchemaReader populates
	// ir.DefaultNone{} for columns with no DEFAULT; the CDC reader
	// leaves Default==nil for the same case (pgoutput's
	// RelationMessage doesn't carry attdefault). Both encode "no
	// default", so collapse to a single canonical form for equality.
	aCopy.Default = normalizeDefaultForRename(aCopy.Default)
	dCopy.Default = normalizeDefaultForRename(dCopy.Default)
	return d, a, reflect.DeepEqual(aCopy, dCopy)
}

// normalizeDefaultForRename folds the two encodings of "no default" —
// nil and ir.DefaultNone{} — into a single canonical nil. Other
// DefaultValue variants (DefaultLiteral, DefaultExpression) are
// returned unchanged so a genuine default-clause difference still
// reads as a reshape (drop+add).
func normalizeDefaultForRename(d ir.DefaultValue) ir.DefaultValue {
	if d == nil {
		return nil
	}
	if _, isNone := d.(ir.DefaultNone); isNone {
		return nil
	}
	return d
}

// diffColumns returns the columns present in post but not pre
// (added), and the columns present in pre but not post (dropped).
// Matching is by Name. The same-name-different-type case is handled
// by diffAlteredColumn, NOT here — added/dropped only counts names
// that appear on exactly one side.
func diffColumns(pre, post *ir.Table) (added, dropped []*ir.Column) {
	preNames := map[string]*ir.Column{}
	for _, c := range pre.Columns {
		preNames[c.Name] = c
	}
	postNames := map[string]*ir.Column{}
	for _, c := range post.Columns {
		postNames[c.Name] = c
	}
	for _, c := range post.Columns {
		if _, ok := preNames[c.Name]; !ok {
			added = append(added, c)
		}
	}
	for _, c := range pre.Columns {
		if _, ok := postNames[c.Name]; !ok {
			dropped = append(dropped, c)
		}
	}
	return added, dropped
}

// diffIndexes returns the named indexes present in post but not pre,
// and vice versa. Unnamed indexes are skipped (the catalog requires
// named indexes — engines synthesize names when the source omits one,
// so production data should not hit this case).
func diffIndexes(pre, post *ir.Table) (created, dropped []*ir.Index) {
	preNames := map[string]*ir.Index{}
	for _, idx := range pre.Indexes {
		if idx.Name == "" {
			continue
		}
		preNames[idx.Name] = idx
	}
	postNames := map[string]*ir.Index{}
	for _, idx := range post.Indexes {
		if idx.Name == "" {
			continue
		}
		postNames[idx.Name] = idx
	}
	for _, idx := range post.Indexes {
		if idx.Name == "" {
			continue
		}
		if _, ok := preNames[idx.Name]; !ok {
			created = append(created, idx)
		}
	}
	for _, idx := range pre.Indexes {
		if idx.Name == "" {
			continue
		}
		if _, ok := postNames[idx.Name]; !ok {
			dropped = append(dropped, idx)
		}
	}
	return created, dropped
}

// diffChecks returns the named CHECK constraints present in post but
// not pre (added), present in pre but not post (dropped), and at
// most one same-named constraint whose Expr differs between pre and
// post (modified — modBefore + modAfter populated, hasMod=true).
//
// Unnamed CHECKs are skipped (the classifier policy mirrors
// diffIndexes: the catalog requires named constraints; engines and
// SchemaReaders synthesize a name when the source omits one so
// production data should not hit this case).
//
// Multiple same-name modified constraints in a single boundary
// returns hasMod=false (modBefore/modAfter nil) and lets the
// class-counter fall through to the combo refusal. v1 catalog
// deliberately scopes to single-constraint-modified deltas —
// multi-modify is sufficiently uncommon that the drained model
// is the right recovery path (mirrors diffAlteredColumn's
// single-column policy).
func diffChecks(pre, post *ir.Table) (added, dropped []*ir.CheckConstraint, modBefore, modAfter *ir.CheckConstraint, hasMod bool) {
	preChecks := checksByName(pre)
	postChecks := checksByName(post)
	for name, postC := range postChecks {
		preC, ok := preChecks[name]
		if !ok {
			added = append(added, postC)
			continue
		}
		// Same-name match: compare Expr text. Expr is the
		// already-quote-normalized expression text the SchemaReader
		// captured; equality is byte-level so the classifier doesn't
		// have to interpret SQL.
		if preC.Expr != postC.Expr {
			if hasMod {
				// Second modify on the same boundary — combo refusal.
				return nil, nil, nil, nil, false
			}
			modBefore = preC
			modAfter = postC
			hasMod = true
		}
	}
	for name, preC := range preChecks {
		if _, ok := postChecks[name]; !ok {
			dropped = append(dropped, preC)
		}
	}
	return added, dropped, modBefore, modAfter, hasMod
}

// checksByName indexes a table's CheckConstraints by Name. Unnamed
// constraints are skipped per the classifier policy (see diffChecks).
// nil-table-safe — returns an empty map so callers can range cleanly.
func checksByName(t *ir.Table) map[string]*ir.CheckConstraint {
	if t == nil {
		return map[string]*ir.CheckConstraint{}
	}
	out := make(map[string]*ir.CheckConstraint, len(t.CheckConstraints))
	for _, c := range t.CheckConstraints {
		if c == nil || c.Name == "" {
			continue
		}
		out[c.Name] = c
	}
	return out
}

// diffAlteredColumn detects a same-name column whose Type or Nullable
// differs between pre and post. Returns the post-state column, the
// pre-state column, the shape (alter-type vs alter-nullability), and
// hasAlter=true when exactly one column qualifies.
//
// Multiple columns with type/nullability changes are NOT recognized
// (returns hasAlter=false; ClassifyShape's class-count will still
// fire on the column add/drop classes if any, or treat as combo).
// The v1 catalog deliberately scopes to single-column-altered deltas
// — operators issuing multi-column ALTERs use the drained model.
func diffAlteredColumn(pre, post *ir.Table) (alteredCol, alteredBefore *ir.Column, kind ShapeKind, hasAlter bool) {
	preNames := map[string]*ir.Column{}
	for _, c := range pre.Columns {
		preNames[c.Name] = c
	}
	var found *ir.Column
	var foundBefore *ir.Column
	var foundKind ShapeKind
	count := 0
	for _, postCol := range post.Columns {
		preCol, ok := preNames[postCol.Name]
		if !ok {
			continue
		}
		typeDiffers := !reflect.DeepEqual(preCol.Type, postCol.Type)
		nullDiffers := preCol.Nullable != postCol.Nullable
		switch {
		case typeDiffers:
			count++
			found = postCol
			foundBefore = preCol
			foundKind = ShapeKindAlterColumnType
		case nullDiffers:
			count++
			found = postCol
			foundBefore = preCol
			foundKind = ShapeKindAlterColumnNullability
		}
	}
	if count == 1 {
		return found, foundBefore, foundKind, true
	}
	return nil, nil, ShapeKindNone, false
}

// ProbeOutcome aliases [ir.ProbeOutcome] — defined in `ir` to avoid
// the engines→pipeline import cycle the integration-tagged tests
// expose.
type ProbeOutcome = ir.ProbeOutcome

// Re-exported ProbeOutcome constants for the pipeline-internal call
// sites (router, tests). The canonical values live in `ir`.
const (
	ProbeOutcomeApplied      = ir.ProbeOutcomeApplied
	ProbeOutcomeNotApplied   = ir.ProbeOutcomeNotApplied
	ProbeOutcomeInconsistent = ir.ProbeOutcomeInconsistent
)

// ShardConsolidationProber aliases [ir.ShardConsolidationProber].
type ShardConsolidationProber = ir.ShardConsolidationProber

// DispatchProbe runs the right probe for the given shape against the
// target's prober. Returns ProbeOutcomeInconsistent + an error when
// the shape is unrecognized (the lease takeover path then refuses
// loudly per ADR-0054 §4's "inconsistent" branch).
//
// table is the post-DDL IR table the holder recorded (the lease-row's
// view of what the schema should look like after the DDL lands).
func DispatchProbe(ctx context.Context, prober ShardConsolidationProber, table *ir.Table, shape Shape) (ProbeOutcome, error) {
	if prober == nil {
		return ProbeOutcomeInconsistent, errors.New("pipeline: dispatch probe: prober is nil")
	}
	if table == nil {
		return ProbeOutcomeInconsistent, errors.New("pipeline: dispatch probe: table is nil")
	}
	switch shape.Kind {
	case ShapeKindNone:
		// A no-op delta has no observable target-side effect; the
		// takeover-stream finalizes immediately. Treated as Applied
		// (idempotent record).
		return ProbeOutcomeApplied, nil
	case ShapeKindAddColumn:
		return prober.ProbeAddColumn(ctx, table, shape.AddedColumns)
	case ShapeKindDropColumn:
		return prober.ProbeDropColumn(ctx, table, shape.DroppedColumns)
	case ShapeKindCreateIndex:
		return prober.ProbeCreateIndex(ctx, table, shape.CreatedIndexes)
	case ShapeKindDropIndex:
		return prober.ProbeDropIndex(ctx, table, shape.DroppedIndexes)
	case ShapeKindAlterColumnType:
		return prober.ProbeAlterColumnType(ctx, table, shape.AlteredColumn)
	case ShapeKindAlterColumnNullability:
		return prober.ProbeAlterColumnNullability(ctx, table, shape.AlteredColumn)
	case ShapeKindRenameColumn:
		if shape.RenamedColumnBefore == nil || shape.RenamedColumnAfter == nil {
			return ProbeOutcomeInconsistent, errors.New(
				"pipeline: dispatch probe: rename-column shape missing before/after column",
			)
		}
		return prober.ProbeRenameColumn(
			ctx, table,
			shape.RenamedColumnBefore.Name, shape.RenamedColumnAfter.Name,
			shape.RenamedColumnAfter,
		)
	case ShapeKindAddCheck:
		return prober.ProbeAddCheck(ctx, table, shape.AddedChecks)
	case ShapeKindDropCheck:
		return prober.ProbeDropCheck(ctx, table, shape.DroppedChecks)
	case ShapeKindModifyCheck:
		if shape.ModifiedCheckBefore == nil || shape.ModifiedCheckAfter == nil {
			return ProbeOutcomeInconsistent, errors.New(
				"pipeline: dispatch probe: modify-check shape missing before/after constraint",
			)
		}
		return prober.ProbeModifyCheck(ctx, table, shape.ModifiedCheckBefore.Name, shape.ModifiedCheckAfter)
	case ShapeKindUnrecognized:
		return ProbeOutcomeInconsistent, errors.New(
			"pipeline: dispatch probe: unrecognized shape — refuse loudly per ADR-0054 DP-E",
		)
	}
	return ProbeOutcomeInconsistent, fmt.Errorf("pipeline: dispatch probe: unknown shape %v", shape.Kind)
}

// RecoveryHint formats a uniform recovery message for refusal paths:
// drained-model workflow + the relevant operator commands.
func RecoveryHint(tableName string) string {
	return fmt.Sprintf(
		"recovery: drained model — run 'sluice sync stop --wait' on every shard, "+
			"run one cross-shard schema migrate (manual or 'sluice schema migrate'), "+
			"then resume each shard via 'sluice sync start --resume'. "+
			"Per-table: %q. Pass --no-coordinate-live-ddl to keep the drained model "+
			"as the default for all shards.",
		tableName,
	)
}
