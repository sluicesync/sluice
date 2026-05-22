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
// ALTER COLUMN type/nullability. Unrecognized structural changes
// refuse loudly with operator-actionable drained-model recovery hint.

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

	// ShapeKindUnrecognized — the delta doesn't fit a single
	// recognized shape (e.g. multi-shape combo, RENAME, CHECK
	// constraint change, generated-column change, FK change, ...).
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
		}, nil
	default:
		// Multi-shape combo delta — refuse loudly per ADR-0054
		// DP-E. The caller surfaces the operator-actionable recovery
		// hint (drained model).
		return Shape{Kind: ShapeKindUnrecognized}, fmt.Errorf(
			"pipeline: classify shape: unrecognized multi-shape combo delta "+
				"(added=%d dropped=%d created-idx=%d dropped-idx=%d altered-col=%t)",
			len(added), len(dropped), len(createdIdx), len(droppedIdx), hasAlter,
		)
	}
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
