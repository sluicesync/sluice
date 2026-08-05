// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

// Classification of an incremental manifest's `alter_table` schema
// delta into the individual structural ASPECTS it carries, plus the
// per-aspect disposition the chain-replay side (chain restore + the
// broker) applies.
//
// Why this exists: [DiffSchemas] emits ONE alter_table entry per table
// whose before/after shapes differ for ANY reason — a retype, a
// nullability flip, an index change, a dropped column — but both replay
// sites used to look only for ADDED COLUMNS and `continue` when there
// were none. A pure retype (`ALTER TABLE orders ALTER COLUMN amount
// TYPE numeric(20,8)`) therefore reached a SILENT skip: restore
// recreated the column at its pre-window `numeric(10,2)`, replayed the
// window, and Postgres ROUNDED every replayed value on insert and
// returned success. Exit 0, silently rounded money.
//
// The fix is structural rather than a new `if`: every aspect the diff
// can see is named here, `tablesEqual` is IMPLEMENTED in terms of that
// same list (so the two cannot drift), and every named aspect must
// carry a disposition — apply it through the engine's delta surface,
// prove it needs no DDL, or refuse loudly. There is no fall-through.
// A new comparator added to the diff is a new aspect, and an aspect
// with no disposition fails [TestAlterAspectsAllClassified].

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
	"sluicesync.dev/sluice/internal/translate"
)

// AlterAspect names one structural difference the incremental schema
// diff can see between an alter_table delta's before- and after-shape.
// The set is exactly what [tablesEqual] compares — see
// [alterAspectComparators] and [alterColumnAspectComparators].
type AlterAspect string

const (
	// AlterAspectTableIdentity — the two shapes disagree on schema or
	// table name. Unreachable through [DiffSchemas] (it pairs tables BY
	// qualified name); kept because [tablesEqual] compares it.
	AlterAspectTableIdentity AlterAspect = "table-identity"

	// AlterAspectColumnAdded — a column name present in after and
	// absent from before.
	AlterAspectColumnAdded AlterAspect = "column-added"

	// AlterAspectColumnDropped — a column name present in before and
	// absent from after.
	AlterAspectColumnDropped AlterAspect = "column-dropped"

	// AlterAspectColumnOrder — same column-name SET, different
	// declaration order.
	AlterAspectColumnOrder AlterAspect = "column-order"

	// AlterAspectColumnType — a same-named column whose rendered IR
	// type differs. THE silent-rounding aspect.
	AlterAspectColumnType AlterAspect = "column-type"

	// AlterAspectColumnNullable — a same-named column whose Nullable
	// differs.
	AlterAspectColumnNullable AlterAspect = "column-nullable"

	// AlterAspectColumnGenerated — a same-named column that gained or
	// lost its GENERATED-ness.
	AlterAspectColumnGenerated AlterAspect = "column-generated"

	// AlterAspectColumnDefault — a same-named column whose DEFAULT
	// differs (canonicalized: nil and DefaultNone are one value).
	AlterAspectColumnDefault AlterAspect = "column-default"

	// AlterAspectPrimaryKey — the declared primary key differs.
	AlterAspectPrimaryKey AlterAspect = "primary-key"

	// AlterAspectIndexCreated — a named index present in after and
	// absent from before.
	AlterAspectIndexCreated AlterAspect = "index-created"

	// AlterAspectIndexDropped — a named index present in before and
	// absent from after.
	AlterAspectIndexDropped AlterAspect = "index-dropped"

	// AlterAspectIndexRedefined — a same-named index whose columns or
	// uniqueness differ.
	AlterAspectIndexRedefined AlterAspect = "index-redefined"

	// The CHECK and foreign-key aspects (roadmap item 113). Before these
	// existed the registry closed the DISPOSITION hole one level below a
	// COVERAGE hole: [tablesEqual] never compared CheckConstraints or
	// ForeignKeys at all, so a mid-window `ADD CHECK` produced no delta to
	// dispose OF. The item-60 integration ground truth printed it —
	// scenario="ADD CHECK CONSTRAINT only (no DML)" schemaDelta=0 — and a
	// restore of that chain landed a target missing the constraint, exit 0.

	// AlterAspectCheckAdded — a CHECK constraint present in after and
	// absent from before.
	AlterAspectCheckAdded AlterAspect = "check-added"

	// AlterAspectCheckDropped — a CHECK constraint present in before and
	// absent from after.
	AlterAspectCheckDropped AlterAspect = "check-dropped"

	// AlterAspectCheckRedefined — a same-named CHECK whose expression
	// differs.
	AlterAspectCheckRedefined AlterAspect = "check-redefined"

	// AlterAspectForeignKeyAdded — a foreign key present in after and
	// absent from before.
	AlterAspectForeignKeyAdded AlterAspect = "foreign-key-added"

	// AlterAspectForeignKeyDropped — a foreign key present in before and
	// absent from after.
	AlterAspectForeignKeyDropped AlterAspect = "foreign-key-dropped"

	// AlterAspectForeignKeyRedefined — a same-named foreign key whose
	// enforced shape (referenced table/columns, referential actions, match
	// rule, deferrability) differs.
	AlterAspectForeignKeyRedefined AlterAspect = "foreign-key-redefined"
)

// AlterColumnChange pairs a same-named column's before- and after-shape
// for the per-column aspects.
type AlterColumnChange struct {
	Aspect AlterAspect
	Before *ir.Column
	After  *ir.Column
}

// AlterResidual is the classified content of one alter_table delta:
// which aspects differ, plus the payload each disposition needs.
type AlterResidual struct {
	// Aspects lists every differing aspect in comparator-registry
	// order, deduplicated. Empty means the two shapes are equal — the
	// exact condition [tablesEqual] reports.
	Aspects []AlterAspect

	AddedColumns   []*ir.Column
	DroppedColumns []*ir.Column

	// ColumnChanges carries one entry per (same-named column × differing
	// per-column aspect).
	ColumnChanges []AlterColumnChange

	CreatedIndexes   []*ir.Index
	DroppedIndexes   []*ir.Index
	RedefinedIndexes []*ir.Index

	// DroppedChecks carries the payload for [AlterAspectCheckDropped], the
	// only one of the six constraint aspects with an AlterApply
	// disposition. The other five refuse, and a refusal names the table and
	// the aspect rather than the objects — carrying payload nobody reads
	// would be a claim that something acts on it.
	DroppedChecks []*ir.CheckConstraint
}

// Equal reports whether the two shapes were structurally identical.
func (r AlterResidual) Equal() bool { return len(r.Aspects) == 0 }

// Has reports whether the classified delta carries aspect a.
func (r AlterResidual) Has(a AlterAspect) bool {
	for _, got := range r.Aspects {
		if got == a {
			return true
		}
	}
	return false
}

// alterColumnAspectComparators is THE list of per-column properties the
// incremental diff compares on a pair of same-named columns. Adding a
// property to the diff means adding an entry here (which is what makes
// it visible to the disposition gate), not a new `if` in tablesEqual.
var alterColumnAspectComparators = []struct {
	Aspect AlterAspect
	Same   func(a, b *ir.Column) bool
}{
	{
		Aspect: AlterAspectColumnType,
		// Type.String() captures width / precision / element type on
		// every concrete IR type — the same property the cross-engine
		// diff relies on.
		Same: func(a, b *ir.Column) bool { return TypeString(a.Type) == TypeString(b.Type) },
	},
	{
		Aspect: AlterAspectColumnNullable,
		Same:   func(a, b *ir.Column) bool { return a.Nullable == b.Nullable },
	},
	{
		Aspect: AlterAspectColumnGenerated,
		Same:   func(a, b *ir.Column) bool { return a.IsGenerated() == b.IsGenerated() },
	},
	{
		Aspect: AlterAspectColumnDefault,
		// Canonicalized: nil and DefaultNone{} both mean "no default"
		// (the IR's UnmarshalJSON normalises absent defaults, so a
		// round-tripped manifest disagrees with an in-memory struct).
		Same: func(a, b *ir.Column) bool { return defaultsEqual(a.Default, b.Default) },
	},
}

// alterAspectComparators is THE list of whole-table properties the
// incremental diff compares. Same contract as the per-column list.
var alterAspectComparators = []struct {
	Aspect AlterAspect
	Same   func(a, b *ir.Table) bool
}{
	{
		Aspect: AlterAspectTableIdentity,
		Same:   func(a, b *ir.Table) bool { return a.Schema == b.Schema && a.Name == b.Name },
	},
	{
		Aspect: AlterAspectColumnAdded,
		Same:   func(a, b *ir.Table) bool { return len(AddedColumns(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectColumnDropped,
		Same:   func(a, b *ir.Table) bool { return len(AddedColumns(b, a)) == 0 },
	},
	{
		Aspect: AlterAspectColumnOrder,
		Same:   sameColumnOrder,
	},
	{
		Aspect: AlterAspectPrimaryKey,
		Same:   func(a, b *ir.Table) bool { return indicesEqual(a.PrimaryKey, b.PrimaryKey) },
	},
	{
		Aspect: AlterAspectIndexCreated,
		Same:   func(a, b *ir.Table) bool { return len(addedIndexes(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectIndexDropped,
		Same:   func(a, b *ir.Table) bool { return len(addedIndexes(b, a)) == 0 },
	},
	{
		Aspect: AlterAspectIndexRedefined,
		Same:   func(a, b *ir.Table) bool { return len(redefinedIndexes(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectCheckAdded,
		Same:   func(a, b *ir.Table) bool { return len(addedChecks(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectCheckDropped,
		Same:   func(a, b *ir.Table) bool { return len(addedChecks(b, a)) == 0 },
	},
	{
		Aspect: AlterAspectCheckRedefined,
		Same:   func(a, b *ir.Table) bool { return len(redefinedChecks(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectForeignKeyAdded,
		Same:   func(a, b *ir.Table) bool { return len(addedForeignKeys(a, b)) == 0 },
	},
	{
		Aspect: AlterAspectForeignKeyDropped,
		Same:   func(a, b *ir.Table) bool { return len(addedForeignKeys(b, a)) == 0 },
	},
	{
		Aspect: AlterAspectForeignKeyRedefined,
		Same:   func(a, b *ir.Table) bool { return len(redefinedForeignKeys(a, b)) == 0 },
	},
}

// AllAlterAspects returns every aspect the diff can report, DERIVED
// from the two comparator registries (never hand-listed) so a new
// comparator immediately shows up in the disposition gate.
func AllAlterAspects() []AlterAspect {
	out := make([]AlterAspect, 0, len(alterAspectComparators)+len(alterColumnAspectComparators))
	for _, c := range alterAspectComparators {
		out = append(out, c.Aspect)
	}
	for _, c := range alterColumnAspectComparators {
		out = append(out, c.Aspect)
	}
	return out
}

// ClassifyAlterDelta reports every structural aspect in which before
// and after differ, with the payload each aspect's disposition needs.
// A nil table on either side classifies as a table-identity difference
// (the caller refuses; [DiffSchemas] never emits a half-shaped alter).
func ClassifyAlterDelta(before, after *ir.Table) AlterResidual {
	var res AlterResidual
	if before == nil || after == nil {
		res.Aspects = append(res.Aspects, AlterAspectTableIdentity)
		return res
	}
	for _, c := range alterAspectComparators {
		if c.Same(before, after) {
			continue
		}
		res.Aspects = append(res.Aspects, c.Aspect)
	}
	res.AddedColumns = AddedColumns(before, after)
	res.DroppedColumns = AddedColumns(after, before)
	res.CreatedIndexes = addedIndexes(before, after)
	res.DroppedIndexes = addedIndexes(after, before)
	res.RedefinedIndexes = redefinedIndexes(before, after)
	res.DroppedChecks = addedChecks(after, before)

	// Per-column aspects, paired BY NAME (positional pairing would
	// report a reorder as a type change on every moved column).
	beforeByName := make(map[string]*ir.Column, len(before.Columns))
	for _, c := range before.Columns {
		if c != nil {
			beforeByName[c.Name] = c
		}
	}
	seen := make(map[AlterAspect]bool, len(alterColumnAspectComparators))
	for _, aCol := range after.Columns {
		if aCol == nil {
			continue
		}
		bCol, ok := beforeByName[aCol.Name]
		if !ok || bCol == nil {
			continue
		}
		for _, c := range alterColumnAspectComparators {
			if c.Same(bCol, aCol) {
				continue
			}
			res.ColumnChanges = append(res.ColumnChanges, AlterColumnChange{
				Aspect: c.Aspect, Before: bCol, After: aCol,
			})
			if !seen[c.Aspect] {
				seen[c.Aspect] = true
				res.Aspects = append(res.Aspects, c.Aspect)
			}
		}
	}
	return res
}

// sameColumnOrder reports whether the two shapes declare the same
// column names in the same order. Columns present on only one side are
// ignored — an add or a drop is its own aspect, and a table that gained
// a column has by definition not "reordered" the survivors.
func sameColumnOrder(a, b *ir.Table) bool {
	aNames := columnNameSequence(a, b)
	bNames := columnNameSequence(b, a)
	if len(aNames) != len(bNames) {
		return false
	}
	for i := range aNames {
		if aNames[i] != bNames[i] {
			return false
		}
	}
	return true
}

// columnNameSequence returns t's column names in declaration order,
// keeping only names that also appear on other.
func columnNameSequence(t, other *ir.Table) []string {
	keep := make(map[string]bool, len(other.Columns))
	for _, c := range other.Columns {
		if c != nil {
			keep[c.Name] = true
		}
	}
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c != nil && keep[c.Name] {
			out = append(out, c.Name)
		}
	}
	return out
}

// AddedColumns returns the columns in after but not in before,
// preserving after's declaration order. Lives here (rather than in the
// lineage package, its original home) because the alter-delta
// classifier owns the column-set diff both replay sites consume.
func AddedColumns(before, after *ir.Table) []*ir.Column {
	if after == nil {
		return nil
	}
	beforeNames := map[string]bool{}
	if before != nil {
		for _, c := range before.Columns {
			if c != nil {
				beforeNames[c.Name] = true
			}
		}
	}
	out := make([]*ir.Column, 0, len(after.Columns))
	for _, c := range after.Columns {
		if c != nil && !beforeNames[c.Name] {
			out = append(out, c)
		}
	}
	return out
}

// addedIndexes returns the indexes named in after but not before.
func addedIndexes(before, after *ir.Table) []*ir.Index {
	if after == nil {
		return nil
	}
	beforeNames := map[string]bool{}
	if before != nil {
		for _, idx := range before.Indexes {
			if idx != nil {
				beforeNames[idx.Name] = true
			}
		}
	}
	var out []*ir.Index
	for _, idx := range after.Indexes {
		if idx != nil && !beforeNames[idx.Name] {
			out = append(out, idx)
		}
	}
	return out
}

// redefinedIndexes returns after's indexes that share a name with a
// before index but differ in columns or uniqueness.
func redefinedIndexes(before, after *ir.Table) []*ir.Index {
	if before == nil || after == nil {
		return nil
	}
	byName := make(map[string]*ir.Index, len(before.Indexes))
	for _, idx := range before.Indexes {
		if idx != nil {
			byName[idx.Name] = idx
		}
	}
	var out []*ir.Index
	for _, idx := range after.Indexes {
		if idx == nil {
			continue
		}
		prev, ok := byName[idx.Name]
		if ok && !indicesEqual(prev, idx) {
			out = append(out, idx)
		}
	}
	return out
}

// constraintKey is the identity a CHECK or foreign key is matched on
// across the before/after shapes: its NAME when it has one, otherwise its
// full fingerprint.
//
// The fallback matters. Every other name-keyed comparison in sluice SKIPS
// unnamed constraints, which is right for `schema diff` (it reports the
// skip as a coverage figure the operator can act on) and wrong here:
// there is no operator in the loop on a chain replay, and two unnamed
// constraints would collide on the key "" and hide each other. Keying an
// unnamed constraint by its own fingerprint makes an edit to it surface
// as an add PLUS a drop rather than vanishing — noisier, and never
// silent.
func constraintKey(name, fingerprint string) string {
	if name != "" {
		return name
	}
	return "\x00fingerprint:" + fingerprint
}

// checkFingerprint renders a CHECK's enforced content: its expression.
// ExprDialect is deliberately excluded — a dialect retag with an
// unchanged expression is a manifest-provenance change, not a DDL the
// source ran.
func checkFingerprint(c *ir.CheckConstraint) string {
	return strings.TrimSpace(c.Expr)
}

// fkFingerprint renders a foreign key's enforced shape: everything that
// decides which rows are legal and what happens to them. Same attribute
// set the cross-engine `schema diff` comparison settled on.
func fkFingerprint(fk *ir.ForeignKey) string {
	return strings.Join([]string{
		strings.Join(fk.Columns, ","),
		fk.ReferencedSchema,
		fk.ReferencedTable,
		strings.Join(fk.ReferencedColumns, ","),
		fk.OnDelete.String(),
		fk.OnUpdate.String(),
		fk.Match.String(),
		strconv.FormatBool(fk.Deferrable),
		strconv.FormatBool(fk.InitiallyDeferred),
	}, "|")
}

// checksByKey indexes a table's CHECK constraints by [constraintKey].
func checksByKey(t *ir.Table) map[string]*ir.CheckConstraint {
	out := make(map[string]*ir.CheckConstraint, len(t.CheckConstraints))
	for _, c := range t.CheckConstraints {
		if c == nil {
			continue
		}
		out[constraintKey(c.Name, checkFingerprint(c))] = c
	}
	return out
}

// foreignKeysByKey indexes a table's foreign keys by [constraintKey].
func foreignKeysByKey(t *ir.Table) map[string]*ir.ForeignKey {
	out := make(map[string]*ir.ForeignKey, len(t.ForeignKeys))
	for _, fk := range t.ForeignKeys {
		if fk == nil {
			continue
		}
		out[constraintKey(fk.Name, fkFingerprint(fk))] = fk
	}
	return out
}

// addedChecks returns the CHECK constraints keyed in after but not
// before, preserving after's declaration order.
func addedChecks(before, after *ir.Table) []*ir.CheckConstraint {
	if after == nil {
		return nil
	}
	var beforeKeys map[string]*ir.CheckConstraint
	if before != nil {
		beforeKeys = checksByKey(before)
	}
	var out []*ir.CheckConstraint
	for _, c := range after.CheckConstraints {
		if c == nil {
			continue
		}
		if _, ok := beforeKeys[constraintKey(c.Name, checkFingerprint(c))]; !ok {
			out = append(out, c)
		}
	}
	return out
}

// redefinedChecks returns after's CHECKs that share a NAME with a before
// CHECK but carry a different expression. Unnamed constraints cannot be
// redefined by definition — their key IS their content, so a change makes
// them an add plus a drop.
func redefinedChecks(before, after *ir.Table) []*ir.CheckConstraint {
	if before == nil || after == nil {
		return nil
	}
	byName := make(map[string]*ir.CheckConstraint, len(before.CheckConstraints))
	for _, c := range before.CheckConstraints {
		if c != nil && c.Name != "" {
			byName[c.Name] = c
		}
	}
	var out []*ir.CheckConstraint
	for _, c := range after.CheckConstraints {
		if c == nil || c.Name == "" {
			continue
		}
		if prev, ok := byName[c.Name]; ok && checkFingerprint(prev) != checkFingerprint(c) {
			out = append(out, c)
		}
	}
	return out
}

// addedForeignKeys returns the foreign keys keyed in after but not
// before, preserving after's declaration order.
func addedForeignKeys(before, after *ir.Table) []*ir.ForeignKey {
	if after == nil {
		return nil
	}
	var beforeKeys map[string]*ir.ForeignKey
	if before != nil {
		beforeKeys = foreignKeysByKey(before)
	}
	var out []*ir.ForeignKey
	for _, fk := range after.ForeignKeys {
		if fk == nil {
			continue
		}
		if _, ok := beforeKeys[constraintKey(fk.Name, fkFingerprint(fk))]; !ok {
			out = append(out, fk)
		}
	}
	return out
}

// redefinedForeignKeys returns after's foreign keys that share a NAME
// with a before FK but enforce a different shape.
func redefinedForeignKeys(before, after *ir.Table) []*ir.ForeignKey {
	if before == nil || after == nil {
		return nil
	}
	byName := make(map[string]*ir.ForeignKey, len(before.ForeignKeys))
	for _, fk := range before.ForeignKeys {
		if fk != nil && fk.Name != "" {
			byName[fk.Name] = fk
		}
	}
	var out []*ir.ForeignKey
	for _, fk := range after.ForeignKeys {
		if fk == nil || fk.Name == "" {
			continue
		}
		if prev, ok := byName[fk.Name]; ok && fkFingerprint(prev) != fkFingerprint(fk) {
			out = append(out, fk)
		}
	}
	return out
}

// AlterDisposition is what the chain-replay side does with an aspect.
type AlterDisposition int

const (
	// AlterApply — emit the matching DDL through the engine's delta
	// surface. Refuses loudly when the target engine doesn't expose it.
	AlterApply AlterDisposition = iota

	// AlterNoDDL — the aspect needs no DDL, and the entry's Reason is
	// the PROOF (not a shrug). Logged, never silent.
	AlterNoDDL

	// AlterRefuse — no faithful replay exists; refuse loudly naming the
	// table, the aspect and the reason.
	AlterRefuse
)

// alterDisposition is one aspect's verdict plus the operator-facing
// reason it carries into the log line or the refusal.
type alterDisposition struct {
	Verdict AlterDisposition
	Reason  string
}

// alterDispositions classifies every aspect in [AllAlterAspects].
// TestAlterAspectsAllClassified fails if an aspect is missing, and
// TestAlterAspectMatrixIsAppliedOrRefused exercises one delta per
// aspect end-to-end so a new aspect cannot reach a silent skip.
//
// The rule is "apply where the engine has a delta surface, refuse where
// it does not" — with two deliberate departures, both written down
// here:
//
//   - column-dropped has a surface ([ir.ShapeDeltaApplier.AlterDropColumn])
//     and is still REFUSED, for the same reason chain restore does not
//     auto-DROP a dropped TABLE: the window's own change chunks may carry
//     pre-DDL events that still name the column, and dropping it before
//     the replay discards those values with no trace. Refusing is the
//     loud half of the same posture.
//
//   - check-dropped has a surface and APPLIES, while check-added and
//     check-redefined REFUSE despite [ir.ShapeDeltaApplier.AlterAddCheck]
//     existing. The asymmetry is about REPLAY ORDER, not about surfaces.
//     Dropping a constraint only ever widens what is legal, so applying
//     it before the window replays cannot reject anything. ADDING one
//     cannot be ordered faithfully at all: the source's own `ADD CHECK`
//     landed PARTWAY THROUGH the window and validated only the rows
//     present at that instant, so an earlier event in the same window can
//     legally carry a value the constraint forbids. Applying the CHECK
//     first makes the target reject that event; applying it after the
//     replay leaves the target disagreeing with the manifest for the rest
//     of the chain. Refusing names the table and sends the operator to a
//     fresh full — the same posture column-dropped takes.
//
//   - the three foreign-key aspects REFUSE for a plainer reason: the IR
//     exposes NO applier surface for foreign keys (there is no
//     AlterAddForeignKey / AlterDropForeignKey on any engine), so there
//     is nothing to apply through. foreign-key-dropped is the one that
//     WOULD be safely applicable on the check-dropped argument above if
//     such a surface existed; that is the follow-up, not a silent skip.
//
//   - column-default has NO surface and is still not refused: a DEFAULT
//     is only consulted for a column a statement omits, and row-based
//     CDC replay writes an explicit value for every column of every
//     event (that is what makes an INSERT's server-applied default
//     arrive as data at all). A stale target DEFAULT therefore cannot
//     change one replayed row; it can only affect writes made by
//     something other than sluice AFTER the restore, so it is a WARN
//     with the column named, not a chain-bricking refusal.
var alterDispositions = map[AlterAspect]alterDisposition{
	AlterAspectTableIdentity: {
		Verdict: AlterRefuse,
		Reason:  "the delta's before- and after-shapes name different tables (or one is missing) — the manifest entry is malformed; sluice will not guess which table it meant",
	},
	AlterAspectColumnAdded: {
		Verdict: AlterApply,
		Reason:  "ALTER TABLE ... ADD COLUMN via the engine's schema-delta surface",
	},
	AlterAspectColumnDropped: {
		Verdict: AlterRefuse,
		Reason:  "the window's change chunks can carry pre-DDL events that still name the dropped column; sluice does not auto-DROP on replay (same posture as a dropped table)",
	},
	AlterAspectColumnOrder: {
		Verdict: AlterNoDDL,
		Reason:  "column ORDER is not load-bearing: every reader, applier and shape classifier in sluice addresses columns by NAME (the ADR-0091 forwarding classifier calls a reorder ShapeKindNone for the same reason), and neither PG nor MySQL can reorder in place anyway",
	},
	AlterAspectColumnType: {
		Verdict: AlterApply,
		Reason:  "ALTER TABLE ... ALTER COLUMN TYPE via the engine's shape-delta surface",
	},
	AlterAspectColumnNullable: {
		Verdict: AlterApply,
		Reason:  "ALTER TABLE ... SET/DROP NOT NULL via the engine's shape-delta surface",
	},
	AlterAspectColumnGenerated: {
		Verdict: AlterRefuse,
		Reason:  "no engine surface converts a column to or from GENERATED, and the two shapes disagree about who computes the value — replaying into the wrong one is not recoverable after the fact",
	},
	AlterAspectColumnDefault: {
		Verdict: AlterNoDDL,
		Reason:  "a DEFAULT is consulted only for a column a statement omits, and CDC replay writes an explicit value for every column of every event, so the target's stale DEFAULT cannot alter a replayed row — re-apply it by hand if writes from outside sluice will follow the restore",
	},
	AlterAspectPrimaryKey: {
		Verdict: AlterRefuse,
		Reason:  "the applier keys every replayed UPDATE and DELETE on the primary key; a target PK that disagrees with the source's post-DDL PK mis-keys the replay silently",
	},
	AlterAspectIndexCreated: {
		Verdict: AlterApply,
		Reason:  "CREATE INDEX via the engine's shape-delta surface",
	},
	AlterAspectIndexDropped: {
		Verdict: AlterApply,
		Reason:  "DROP INDEX via the engine's shape-delta surface (an index carries no row data)",
	},
	AlterAspectIndexRedefined: {
		Verdict: AlterApply,
		Reason:  "DROP + CREATE INDEX via the engine's shape-delta surface",
	},
	AlterAspectCheckAdded: {
		Verdict: AlterRefuse,
		Reason:  "the source's own ADD CHECK landed partway through the window and validated only the rows present at that instant, so an earlier event in the same window can legally carry a value the constraint forbids — applying the CHECK before the replay rejects that event, and applying it after leaves the target disagreeing with the manifest",
	},
	AlterAspectCheckDropped: {
		Verdict: AlterApply,
		Reason:  "ALTER TABLE ... DROP CONSTRAINT via the engine's shape-delta surface; dropping a CHECK only widens what is legal, so removing it before the replay cannot reject an event the window carries",
	},
	AlterAspectCheckRedefined: {
		Verdict: AlterRefuse,
		Reason:  "a redefinition is a DROP followed by an ADD, and the ADD half carries the same mid-window validation hazard as check-added; the diff cannot tell whether the new expression is a widening or a tightening from its text",
	},
	AlterAspectForeignKeyAdded: {
		Verdict: AlterRefuse,
		Reason:  "the IR exposes no foreign-key applier surface on any engine, so there is no way to emit the constraint — and the mid-window ordering hazard that refuses check-added applies here too, since a replayed child row can precede its parent",
	},
	AlterAspectForeignKeyDropped: {
		Verdict: AlterRefuse,
		Reason:  "the IR exposes no foreign-key applier surface on any engine, so the target keeps enforcing a reference the source dropped and rejects replayed rows whose parent no longer exists; this is the one FK aspect that would be safely applicable if the surface existed",
	},
	AlterAspectForeignKeyRedefined: {
		Verdict: AlterRefuse,
		Reason:  "the IR exposes no foreign-key applier surface on any engine, so a re-pointed parent, a weakened referential action or a changed match rule cannot be reproduced on the target",
	},
}

// DispositionOf returns aspect's disposition and reason. The bool is
// false for an unclassified aspect — which the gate test makes
// impossible, and which [ApplyAlterDelta] treats as a refusal so a
// future gap fails CLOSED.
func DispositionOf(aspect AlterAspect) (AlterDisposition, string, bool) {
	d, ok := alterDispositions[aspect]
	return d.Verdict, d.Reason, ok
}

// AlterDeltaContext carries the caller's identity and engine pair into
// [ApplyAlterDelta]'s log lines and refusals.
type AlterDeltaContext struct {
	// SourceEngine / TargetEngine drive the after-shape retarget.
	SourceEngine string
	TargetEngine string

	// BackupID names the incremental in a refusal.
	BackupID string

	// Origin is the replay site: "chain restore" or "broker".
	Origin string

	// StreamID is the broker's stream (empty for chain restore).
	StreamID string
}

// logAttrs renders the context's identifying attributes for slog.
func (c AlterDeltaContext) logAttrs(table string) []any {
	attrs := []any{"table", table}
	if c.StreamID != "" {
		attrs = append(attrs, "stream_id", c.StreamID)
	}
	if c.BackupID != "" {
		attrs = append(attrs, "backup_id", c.BackupID)
	}
	return attrs
}

// ApplyAlterDelta applies ONE alter_table schema delta to the target,
// aspect by aspect. Shared by chain restore and the broker so the two
// replay sites cannot drift (they carried byte-identical add-columns-
// only loops, and therefore the identical silent-skip hole).
//
// Contract: every aspect [ClassifyAlterDelta] reports is applied,
// proven not to need DDL, or refused with
// [sluicecode.CodeBackupSchemaDeltaUnsupported]. There is no path that
// returns nil having ignored a difference.
func ApplyAlterDelta(ctx context.Context, sw ir.SchemaWriter, d *irbackup.SchemaDeltaEntry, ac AlterDeltaContext) error {
	if d == nil {
		return nil
	}
	res := ClassifyAlterDelta(d.Before, d.After)
	if res.Equal() {
		return nil
	}
	// Retarget the after-shape so cross-engine column types (UUID →
	// CHAR(36) etc.) are rewritten before any emit. Same call both
	// replay sites already made for the ADD COLUMN path.
	retargeted := d.After
	if d.After != nil {
		schema := translate.RetargetForEngine(
			&ir.Schema{Tables: []*ir.Table{d.After}},
			ac.SourceEngine, ac.TargetEngine,
		)
		if len(schema.Tables) > 0 {
			retargeted = schema.Tables[0]
		}
	}
	deltaApplier, _ := sw.(ir.SchemaDeltaApplier)
	shapeApplier, _ := sw.(ir.ShapeDeltaApplier)

	for _, aspect := range res.Aspects {
		verdict, reason, known := DispositionOf(aspect)
		if !known {
			// Fail closed: an aspect the registry doesn't classify is a
			// development gap, not an operator problem — but it must not
			// pass silently either.
			return alterDeltaRefusal(ac, d.Table, aspect,
				"this sluice build has no disposition for the aspect (development gap)")
		}
		switch verdict {
		case AlterRefuse:
			return alterDeltaRefusal(ac, d.Table, aspect, reason)
		case AlterNoDDL:
			slog.WarnContext(ctx, ac.Origin+": schema delta — no DDL emitted for "+string(aspect),
				append(ac.logAttrs(d.Table), "reason", reason)...)
		case AlterApply:
			if err := applyAlterAspect(ctx, aspect, res, retargeted, d, deltaApplier, shapeApplier, ac); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyAlterAspect emits the DDL for one AlterApply aspect.
func applyAlterAspect(
	ctx context.Context,
	aspect AlterAspect,
	res AlterResidual,
	retargeted *ir.Table,
	d *irbackup.SchemaDeltaEntry,
	deltaApplier ir.SchemaDeltaApplier,
	shapeApplier ir.ShapeDeltaApplier,
	ac AlterDeltaContext,
) error {
	switch aspect {
	case AlterAspectColumnAdded:
		if deltaApplier == nil {
			// Legacy fallback, deliberately kept: an engine with no
			// schema-delta surface can still survive an ADD COLUMN,
			// because the applier reconciles its INSERT column list
			// against the target catalog. Both shipping engines DO
			// implement the surface (capabilities_assert pins it), so
			// this is a test-double path in practice.
			slog.WarnContext(ctx, ac.Origin+": schema delta — added columns not applied; engine has no SchemaDeltaApplier; replay relies on the applier's column-list reconciliation. If inserts fail, force a fresh full + new chain.",
				append(ac.logAttrs(d.Table), "added_columns", len(res.AddedColumns))...)
			return nil
		}
		added := AddedColumns(d.Before, retargeted)
		if err := deltaApplier.AlterAddColumn(ctx, retargeted, added); err != nil {
			return fmt.Errorf("alter add column on %s: %w", d.Table, err)
		}
		slog.InfoContext(ctx, ac.Origin+": schema delta — applied ADD COLUMN",
			append(ac.logAttrs(d.Table), "added_columns", len(added))...)
	case AlterAspectColumnType, AlterAspectColumnNullable:
		if shapeApplier == nil {
			return alterDeltaRefusal(ac, d.Table, aspect,
				"the target engine's schema writer does not implement ir.ShapeDeltaApplier, so the change cannot be applied — and skipping it would leave the target's column shape disagreeing with the values the window replays")
		}
		for _, ch := range res.ColumnChanges {
			if ch.Aspect != aspect {
				continue
			}
			want := LookupColumn(retargeted, ch.After.Name)
			if want == nil {
				want = ch.After
			}
			var err error
			if aspect == AlterAspectColumnType {
				err = shapeApplier.AlterColumnType(ctx, retargeted, want)
			} else {
				err = shapeApplier.AlterColumnNullability(ctx, retargeted, want)
			}
			if err != nil {
				return fmt.Errorf("alter column %q on %s (%s): %w", ch.After.Name, d.Table, aspect, err)
			}
			slog.InfoContext(ctx, ac.Origin+": schema delta — applied "+string(aspect),
				append(ac.logAttrs(d.Table), "column", ch.After.Name)...)
		}
	case AlterAspectIndexCreated, AlterAspectIndexDropped, AlterAspectIndexRedefined:
		if shapeApplier == nil {
			return alterDeltaRefusal(ac, d.Table, aspect,
				"the target engine's schema writer does not implement ir.ShapeDeltaApplier, so the index change cannot be applied")
		}
		return applyAlterIndexAspect(ctx, aspect, res, retargeted, d, shapeApplier, ac)
	case AlterAspectCheckDropped:
		if shapeApplier == nil {
			return alterDeltaRefusal(ac, d.Table, aspect,
				"the target engine's schema writer does not implement ir.ShapeDeltaApplier, so the dropped CHECK cannot be removed — and leaving it makes the target reject rows the source now accepts")
		}
		if err := shapeApplier.AlterDropCheck(ctx, retargeted, res.DroppedChecks); err != nil {
			return fmt.Errorf("drop check on %s: %w", d.Table, err)
		}
		slog.InfoContext(ctx, ac.Origin+": schema delta — applied DROP CONSTRAINT (CHECK)",
			append(ac.logAttrs(d.Table), "dropped_checks", len(res.DroppedChecks))...)
	case AlterAspectTableIdentity, AlterAspectColumnDropped, AlterAspectColumnOrder,
		AlterAspectColumnGenerated, AlterAspectColumnDefault, AlterAspectPrimaryKey,
		AlterAspectCheckAdded, AlterAspectCheckRedefined,
		AlterAspectForeignKeyAdded, AlterAspectForeignKeyDropped, AlterAspectForeignKeyRedefined:
		// Not AlterApply aspects — unreachable; the caller dispatched on
		// the disposition. Refuse rather than fall through silently.
		return alterDeltaRefusal(ac, d.Table, aspect, "aspect is not an apply-disposition aspect")
	default:
		// A new AlterApply aspect with no arm above would otherwise fall
		// out of this switch and return nil having emitted nothing — the
		// exact silent skip this whole file exists to prevent. Fail closed.
		return alterDeltaRefusal(ac, d.Table, aspect,
			"this sluice build has an apply disposition for the aspect but no emit path for it (development gap)")
	}
	return nil
}

// applyAlterIndexAspect emits the index DDL for one index aspect. A
// redefinition is a DROP followed by a CREATE — neither engine can
// rewrite an index's column list in place.
func applyAlterIndexAspect(
	ctx context.Context,
	aspect AlterAspect,
	res AlterResidual,
	retargeted *ir.Table,
	d *irbackup.SchemaDeltaEntry,
	shapeApplier ir.ShapeDeltaApplier,
	ac AlterDeltaContext,
) error {
	switch aspect {
	case AlterAspectIndexCreated:
		if err := shapeApplier.CreateShapeIndex(ctx, retargeted, retargetedIndexes(retargeted, res.CreatedIndexes)); err != nil {
			return fmt.Errorf("create index on %s: %w", d.Table, err)
		}
	case AlterAspectIndexDropped:
		if err := shapeApplier.DropShapeIndex(ctx, retargeted, res.DroppedIndexes); err != nil {
			return fmt.Errorf("drop index on %s: %w", d.Table, err)
		}
	case AlterAspectIndexRedefined:
		if err := shapeApplier.DropShapeIndex(ctx, retargeted, res.RedefinedIndexes); err != nil {
			return fmt.Errorf("drop redefined index on %s: %w", d.Table, err)
		}
		if err := shapeApplier.CreateShapeIndex(ctx, retargeted, retargetedIndexes(retargeted, res.RedefinedIndexes)); err != nil {
			return fmt.Errorf("create redefined index on %s: %w", d.Table, err)
		}
	default:
		return alterDeltaRefusal(ac, d.Table, aspect, "not an index aspect")
	}
	slog.InfoContext(ctx, ac.Origin+": schema delta — applied "+string(aspect), ac.logAttrs(d.Table)...)
	return nil
}

// retargetedIndexes maps the classifier's index payload (taken from the
// SOURCE-shaped after-table) onto the retargeted table's own index
// values, so a cross-engine emit sees the target-shaped index. Falls
// back to the source-shaped index when the retarget dropped it.
func retargetedIndexes(retargeted *ir.Table, want []*ir.Index) []*ir.Index {
	if retargeted == nil {
		return want
	}
	byName := make(map[string]*ir.Index, len(retargeted.Indexes))
	for _, idx := range retargeted.Indexes {
		if idx != nil {
			byName[idx.Name] = idx
		}
	}
	out := make([]*ir.Index, 0, len(want))
	for _, idx := range want {
		if idx == nil {
			continue
		}
		if got, ok := byName[idx.Name]; ok {
			out = append(out, got)
			continue
		}
		out = append(out, idx)
	}
	return out
}

// alterDeltaRefusal renders the coded refusal for an aspect the replay
// side cannot apply faithfully.
func alterDeltaRefusal(ac AlterDeltaContext, table string, aspect AlterAspect, reason string) error {
	where := ac.Origin
	if where == "" {
		where = "chain replay"
	}
	backup := ac.BackupID
	if backup == "" {
		backup = "(unnamed incremental)"
	}
	return sluicecode.Wrap(
		sluicecode.CodeBackupSchemaDeltaUnsupported,
		"take a fresh full backup and start a new chain from it; the source DDL in this window has no faithful replay on the target",
		fmt.Errorf("%s: incremental %s carries an unsupportable %s schema delta on table %q: %s",
			where, backup, aspect, table, reason),
	)
}
