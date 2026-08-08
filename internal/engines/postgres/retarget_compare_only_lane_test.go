// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 158 — the EVIDENCE for the mysql→postgres rule table
// being COMPARE-ONLY.
//
// [translate.RetargetForShapeCompare] rewrites a MySQL source's IR into
// the shapes a Postgres catalog reads back, so `sluice schema diff`
// stops reporting phantom type drift on a target `migrate` itself
// created. Item 153's mirror decision applies: the rewrite must NOT also
// run on [translate.RetargetForEngine], whose output goes to a
// SchemaWriter.
//
// The argument is a claim about THIS emitter's behaviour, so it is
// checked here rather than asserted in a comment. Read the two halves
// of each test together — the emit-lane pass must produce the thing,
// and the compare-lane pass must not; the second half is what makes the
// first load-bearing, and it is the assertion that fails if someone
// collapses the two entry points back into one.

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// setSourceSchema is the fixture both halves share: a MySQL-shaped table
// whose SET column carries a member list the Postgres emitter turns into
// a table-level membership CHECK.
func setSourceSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "prefs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32, AutoIncrement: true}},
			{Name: "flags", Type: ir.Set{Values: []string{"email", "sms"}}},
		},
	}}}
}

// TestEmitTableDef_CompareOnlyRewriteWouldDropTheSetCheck is the
// load-bearing half of the item-158 lane decision.
//
// MySQL SET has no Postgres equivalent; [emitColumnType] lands the
// column as TEXT[] and [emitTableDef] emits `CONSTRAINT
// "<table>_<column>_set" CHECK (<column> <@ ARRAY[…])` beside it — the
// only thing carrying the source's member list onto the target. That
// emission dispatches on `ir.Set` reaching it in `Column.Type`.
//
// The compare-lane rewrite replaces `ir.Set` with `Array<Text>`, which
// is exactly right for predicting a catalog read-back and exactly wrong
// ahead of a writer: `restore`, `chain restore`, the broker's
// schema-delta replay and schema-forward all emit from
// RetargetForEngine's output, and on the flattened shape the membership
// constraint vanishes with no error and no WARN. A silent constraint
// loss, strictly worse than the loud phantom-drift line item 158 fixes —
// the same trade item 153 refused for DOMAIN CHECKs one direction over.
func TestEmitTableDef_CompareOnlyRewriteWouldDropTheSetCheck(t *testing.T) {
	source := setSourceSchema()

	emit := func(t *testing.T, s *ir.Schema) string {
		t.Helper()
		stmt, err := emitTableDef("", s.Tables[0], emitOpts{})
		if err != nil {
			t.Fatalf("emitTableDef: %v", err)
		}
		return stmt
	}

	const wantCheck = `"prefs_flags_set"`

	keptDDL := emit(t, translate.RetargetForEngine(source, "mysql", "postgres"))
	if !strings.Contains(keptDDL, wantCheck) {
		t.Errorf("RetargetForEngine's output emitted NO %s membership CHECK — the emit lanes rely on ir.Set "+
			"surviving this pass, so a mysql→postgres rule that reaches RetargetForEngine has silently "+
			"dropped every SET column's value list on restore:\n%s", wantCheck, keptDDL)
	}

	flatDDL := emit(t, translate.RetargetForShapeCompare(source, "mysql", "postgres"))
	if strings.Contains(flatDDL, wantCheck) {
		t.Errorf("RetargetForShapeCompare's output emitted the %s membership CHECK; this test's whole premise "+
			"is that it CANNOT, which is why the compare-only table must stay off the emit lane. If the "+
			"emitter learned to read the SET members from SourceColumnType instead, that decision needs "+
			"re-taking rather than this assertion relaxing:\n%s", wantCheck, flatDDL)
	}
}

// TestRetargetForEngine_MySQLToPostgresIsStillIdentity is the wider,
// blunter statement of the same decision: the emit lane for this pair
// rewrites NOTHING. The SET CHECK above is the instance with a
// demonstrated cost; this is the class, so a future arm added to the
// wrong table fails here even if it has no CHECK to drop.
//
// The second family it names has a cost too, and it is latent rather
// than live: `ir.Integer.Unsigned` is what
// [translate.ScanUnsignedBigintNotices] dispatches on to warn about Bug
// 11's (2^63, 2^64) narrowing. `migrate` happens not to retarget today,
// so an emit-lane rewrite would not silence that notice YET — which is
// precisely the kind of accident worth failing a build over.
func TestRetargetForEngine_MySQLToPostgresIsStillIdentity(t *testing.T) {
	source := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "c_tinyint", Type: ir.Integer{Width: 8}},
			{Name: "c_bigint_u", Type: ir.Integer{Width: 64, Unsigned: true}},
			{Name: "c_text", Type: ir.Text{Size: ir.TextRegular}},
			{Name: "c_varbinary", Type: ir.Varbinary{Length: 64}},
			{Name: "c_blob", Type: ir.Blob{Size: ir.BlobTiny}},
			{Name: "c_set", Type: ir.Set{Values: []string{"a"}}},
		},
	}}}

	// Every column above is one the COMPARE lane rewrites, so if the emit
	// lane rewrote them too this loop would grade nothing meaningful —
	// assert that first.
	compared := translate.RetargetForShapeCompare(source, "mysql", "postgres")
	rewritten := 0
	for i, col := range compared.Tables[0].Columns {
		if col.Type.String() != source.Tables[0].Columns[i].Type.String() {
			rewritten++
		}
	}
	if rewritten != len(source.Tables[0].Columns) {
		t.Fatalf("the COMPARE lane rewrote %d of %d fixture columns; this gate's fixture no longer covers the "+
			"compare-only table and would pass against an emit-lane leak", rewritten, len(source.Tables[0].Columns))
	}

	emitted := translate.RetargetForEngine(source, "mysql", "postgres")
	for i, col := range emitted.Tables[0].Columns {
		want := source.Tables[0].Columns[i].Type.String()
		if got := col.Type.String(); got != want {
			t.Errorf("column %q: RetargetForEngine rewrote %s to %s. The mysql→postgres rules are COMPARE-ONLY "+
				"(translate.compareOnlyRuleFor states why); putting one on the emit lane silently changes what "+
				"restore, chain restore, the broker's schema-delta apply and schema-forward WRITE",
				col.Name, want, got)
		}
	}
}
