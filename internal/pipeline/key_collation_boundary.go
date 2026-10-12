// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The key-collation door (KEY-COLLATION-FIRST-BOUNDARY, option 2).
//
// # The gap
//
// The first-boundary witness compares a boundary with the target through
// [witnessCompareType], which erases a character column's character set and
// collation on both sides — rightly, for most columns: across engines a
// collation name is translation, not drift, and a change stream may not
// carry one at all. On a KEY column it is not storage-neutral. `_ci` →
// `_bin`, a PAD SPACE → NO PAD collation, or a character-set change in
// place changes which values are one key, and a target whose key still
// compares under the old collation upserts rows the source keeps apart
// into one. Measured on MySQL 8.0 and 8.4, FULL and MINIMAL alike, with
// the change met as the first boundary of a run (a warm resume, a live
// stream's first boundary after its cold start) or on any
// `--schema-changes=refuse` stream: the target kept `_ci` and the row
// written as `'A'` merged into `'a'` at exit 0.
//
// # The fix: judge the change against the source's own prior
//
// The source's own earlier shape of the table — the cold-start seed, or the
// stream's retained schema history at the boundary — carries each key
// column's collation as the source declared it. Where the boundary's
// collation differs from it the SOURCE changed the column, whatever the
// target holds; that is a change the stream must carry:
//
//   - forward mode: forwarded as an ALTER of the target column, exactly as
//     the CDC→CDC path forwards the same change seen live;
//   - refuse mode: refused (SCHEMA-CHANGE-REFUSED), naming the column and
//     both collations, before any row after the boundary is applied.
//
// With no prior the change cannot be told from a target that differs on
// purpose. Between two engines of the same name (no cross-flavor collation
// mapping sits between them) a key column whose collation differs from the
// target's then refuses rather than pass.
//
// # What it does not see, stated
//
//   - A source whose boundary carries no collation — VStream — gives this
//     door nothing to compare, so nothing here fires for it. pgoutput
//     carries none either, but the Postgres reader reads each column's
//     collation from the catalog at every Relation (cdc_relations.go), so a
//     Postgres boundary carries the collation in force when it was read,
//     not necessarily the one the change was made under. What the boundary
//     cannot see is the applier backstop's job (option 3,
//     [ir.ErrKeyCollationMerge], both appliers); a VStream source stays
//     uncovered by both (docs/production-readiness.md, F17).
//   - A non-key column is not judged: its collation decides no row identity.
//   - A collation from another dialect than the target's (MySQL → Postgres)
//     is not judged: the target's writer never carries a foreign collation,
//     so the target's key compares under its own default throughout.
//
// # The independent expected value
//
// The source's own prior shape (seed or history) — read by the schema
// reader at the cold start, or recorded at an earlier boundary — never the
// target and never the boundary itself. Keys are read from the TARGET's
// catalog: it is the target's key that merges.

import (
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// keyCollationMarker names the change in every refusal and log line.
const keyCollationMarker = "KEY-COLLATION-CHANGED"

// keyCollationChange is one key column whose character set or collation the
// boundary changed.
type keyCollationChange struct {
	column string
	// prior is the source's earlier declaration, "" when there is none (the
	// change was judged against the target alone — [keyCollationChange.unproven]).
	prior string
	now   string
	// target is the target column's declaration, and targetType its type.
	target     string
	targetType ir.Type
}

// unproven reports whether the change rests on the target alone.
func (c keyCollationChange) unproven() bool { return c.prior == "" }

// diff renders the change for a witness refusal or log line.
func (c keyCollationChange) diff() witnessColumnDiff {
	src := fmt.Sprintf("%s (no earlier shape of the source on record; %s)", c.now, keyCollationMarker)
	if c.prior != "" {
		src = fmt.Sprintf("%s (was %s on the source; %s)", c.now, c.prior, keyCollationMarker)
	}
	return witnessColumnDiff{column: c.column, source: src, target: c.target}
}

// renderCollation renders a (character set, collation) declaration.
func renderCollation(t ir.Type) string {
	cs, coll := translate.ColumnCollation(t)
	switch {
	case coll == "":
		return ""
	case cs == "":
		return "COLLATE " + coll
	}
	return "CHARACTER SET " + cs + " COLLATE " + coll
}

// sameDeclaredCollation reports whether two types declare the same character
// set and collation (names compared case-insensitively).
func sameDeclaredCollation(a, b ir.Type) bool {
	ac, al := translate.ColumnCollation(a)
	bc, bl := translate.ColumnCollation(b)
	return strings.EqualFold(ac, bc) && strings.EqualFold(al, bl)
}

// keyColumnNames is every column of t's primary key and unique indexes.
func keyColumnNames(t *ir.Table) map[string]bool {
	out := map[string]bool{}
	if t == nil {
		return out
	}
	add := func(idx *ir.Index) {
		if idx == nil {
			return
		}
		for _, c := range idx.Columns {
			if c.Column != "" {
				out[c.Column] = true
			}
		}
	}
	add(t.PrimaryKey)
	for _, idx := range t.Indexes {
		if idx != nil && idx.Unique {
			add(idx)
		}
	}
	return out
}

// targetCollationDialect is the collation dialect the target holds a
// character column in: the column's own where it names one, otherwise the
// target engine's ("postgres" for a Postgres target, whose default
// collation is unnamed; "" when it cannot be told).
func targetCollationDialect(col *ir.Column, targetEngine string) string {
	if col != nil {
		if cs, coll := translate.ColumnCollation(col.Type); coll != "" {
			return translate.CollationDialect(cs, coll)
		}
	}
	if targetEngine == "postgres" {
		return "postgres"
	}
	return ""
}

// judgeKeyCollation returns the key columns (on the target) whose character
// set or collation the boundary post changed, judged against the source's
// own prior shape where there is one, and against the target where there is
// none and source and target are the same engine (sameEngine).
func judgeKeyCollation(post, prior, target *ir.Table, sameEngine bool, targetEngine string) []keyCollationChange {
	if post == nil || target == nil {
		return nil
	}
	keys := keyColumnNames(target)
	targetCols := columnsByNameIR(target)
	var priorCols map[string]*ir.Column
	if prior != nil {
		priorCols = columnsByNameIR(prior)
	}
	var out []keyCollationChange
	for _, c := range post.Columns {
		if c == nil || !keys[c.Name] {
			continue
		}
		cs, coll := translate.ColumnCollation(c.Type)
		if coll == "" {
			continue // the change stream carries none: nothing to judge
		}
		tc := targetCols[c.Name]
		if tc == nil || targetCollationDialect(tc, targetEngine) != translate.CollationDialect(cs, coll) {
			continue // a foreign dialect: the target holds its own collation
		}
		if sameDeclaredCollation(tc.Type, c.Type) {
			continue // the target already holds it
		}
		change := keyCollationChange{column: c.Name, now: renderCollation(c.Type), target: renderCollation(tc.Type), targetType: tc.Type}
		if pc := priorCols[c.Name]; pc != nil && renderCollation(pc.Type) != "" {
			if sameDeclaredCollation(pc.Type, c.Type) {
				continue // the source did not change it
			}
			change.prior = renderCollation(pc.Type)
			out = append(out, change)
			continue
		}
		if sameEngine {
			out = append(out, change)
		}
	}
	return out
}

// withKeyCollation folds the key-collation changes into a forward-mode
// witness verdict v: one proven change on a table that otherwise matches is
// forwarded as an ALTER of the target column; any other combination — an
// unproven change, more than one, or a change beside any other difference —
// refuses, naming every column.
func withKeyCollation(v witnessVerdict, changes []keyCollationChange) witnessVerdict {
	if len(changes) == 0 {
		return v
	}
	if v.kind == witnessMatch && len(changes) == 1 && !changes[0].unproven() {
		return witnessVerdict{
			kind:       witnessForwardAlter,
			altered:    changes[0].column,
			targetType: changes[0].targetType,
			diffs:      []witnessColumnDiff{changes[0].diff()},
		}
	}
	out := v
	if out.kind == witnessMatch || out.kind == witnessUnwitnessed {
		out = witnessVerdict{}
	}
	out.kind = witnessRefuse
	for _, c := range changes {
		out.diffs = append(out.diffs, c.diff())
	}
	out.remedy = strings.TrimSpace(out.remedy + " " + keyCollationRemedy(changes))
	return out
}

// keyCollationRemedy is what a refused key-collation change tells the
// operator.
func keyCollationRemedy(changes []keyCollationChange) string {
	cols := make([]string, 0, len(changes))
	for _, c := range changes {
		cols = append(cols, fmt.Sprintf("%q to %s", c.column, c.now))
	}
	return "A key column's character set or collation decides which values are one key; a target key that still " +
		"compares under the old one merges rows the source keeps apart (" + keyCollationMarker + "). Change " +
		strings.Join(cols, ", ") + " on the target (the drained model) and restart, or re-copy the table " +
		"(`sluice sync start --reset-target-data --yes`)."
}
