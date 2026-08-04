// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Smart compaction assumes an UPDATE's After is a COMPLETE row image
// (audit 2026-08-01 S1). Against a Postgres source it is not.
//
// The finding was filed from reading and marked UNVERIFIED end-to-end. This
// is the repro. It is a unit test rather than a live-PG one on purpose: the
// premise it depends on — that pgoutput omits an unchanged out-of-line TOAST
// column from the NEW tuple — is ALREADY ground-truthed against a real server
// by TestStreamer_PostgresToPostgres_TOASTedColumnPreservedOnSiblingUpdate
// (internal/pipeline/streamer_toast_unchanged_integration_test.go) and by the
// audit-2026-07-23 D0-1 work. What was never checked is the half downstream of
// it: what the compactor does when handed such an event. So this pins the
// consequence and cites the premise rather than re-proving it.
//
// The marker contract the merge rests on is pinned separately, in the package
// that owns it: postgres.TestNullAndUnchangedToastAreDistinguishable.
//
// # Why a partial After exists at all, and why it is CORRECT everywhere else
//
// postgres.decodeTuple omits a column pgoutput marks 'u' (unchanged TOAST).
// backfillUnchangedToast repairs it — but ONLY on the emitFullBefore path,
// i.e. FILTERED tables, where an absent predicate column would mis-classify a
// row-move. For an UNFILTERED table the omission is deliberate and documented:
// "absent key == preserve the target's existing value". That is exactly right
// for an UPDATE, whose SET list names only the columns present.
//
// It stops being right the moment something REINTERPRETS the event:
//
//   - INSERT+UPDATE collapses to an INSERT carrying `ev.After`. An INSERT has
//     no "preserve the existing value" semantics — there is no existing row.
//     The omitted column is simply not inserted, and the target lands on NULL
//     or the column default.
//   - UPDATE+UPDATE collapses to one UPDATE carrying the LAST After. A column
//     the FIRST update changed and the second left alone is present in After1
//     and absent from After2, so the merged event never carries the new value.
//     During replay the earlier UPDATE is gone, so "preserve the target's
//     existing value" preserves the value from BEFORE the first update.
//
// Both are silent: the row applies, the apply succeeds, and one column holds
// the wrong value. And `backup verify` is green over it, because compaction
// re-stamps the hashes it then verifies — the 2026-08-01 "name the independent
// expected value" shape, where the check and the thing checked share an
// artifact.
//
// The audit named the INSERT+UPDATE site. The UPDATE+UPDATE site is its
// sibling and is confirmed below too (the sibling-sweep step).
//
// # Why the existing guard does not catch it
//
// The compactor DOES refuse an incomplete payload — but only for PRIMARY KEY
// columns (pkValueKey). A TOASTed non-PK column is precisely the case that
// slips through, and TOASTed columns are almost never PK columns: a value
// reaches out-of-line storage by being large, and large columns are not what
// people key on. The guard's shape and the defect's shape are near-disjoint.

package backup

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// docsSchema is a table with a wide column of the kind PG pushes out of line.
func docsSchema() *ir.Schema {
	return &ir.Schema{
		Tables: []*ir.Table{
			{
				Schema: "public",
				Name:   "docs",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "title", Type: ir.Text{}},
					{Name: "body", Type: ir.Text{}}, // the TOASTable one
				},
				PrimaryKey: &ir.Index{
					Name:    "docs_pkey",
					Columns: []ir.IndexColumn{{Column: "id"}},
					Unique:  true,
				},
			},
		},
	}
}

// TestSmartCompaction_InsertPlusPartialUpdate_DropsTheUnchangedToastColumn is
// the audit's named site.
//
// The stream: INSERT the full row, then UPDATE only `title`. PG omits `body`
// from the after-image because it did not change and lives out of line.
func TestSmartCompaction_InsertPlusPartialUpdate_DropsTheUnchangedToastColumn(t *testing.T) {
	const bodyText = "the out-of-line value that must survive compaction"

	emitted, _ := runPolicy(t, docsSchema(), []ir.Change{
		ir.Insert{
			Position: pos(100), Schema: "public", Table: "docs",
			Row: ir.Row{"id": int64(1), "title": "draft", "body": bodyText},
		},
		// The partial after-image. `body` is ABSENT, not nil — that is the
		// distinction the whole finding turns on, and what decodeTuple
		// produces for a 'u' datum.
		ir.Update{
			Position: pos(110), Schema: "public", Table: "docs",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(1), "title": "final"},
		},
	})

	if len(emitted) != 1 {
		t.Fatalf("expected the pair to collapse to one event, got %d: %v", len(emitted), kindsOf(emitted))
	}
	ins, ok := emitted[0].(ir.Insert)
	if !ok {
		t.Fatalf("expected INSERT+UPDATE to collapse to an INSERT, got %T", emitted[0])
	}

	// The title must be the final value — that part of the policy works.
	if got := ins.Row["title"]; got != "final" {
		t.Errorf("title = %v, want \"final\"", got)
	}

	// The regression pin. The INSERT must carry the body the original INSERT
	// wrote, even though the UPDATE that replaced it never mentioned the
	// column: an INSERT has no "preserve the existing value" semantics, so a
	// missing column lands NULL or the column default on the target.
	got, present := ins.Row["body"]
	if !present {
		t.Fatalf("REGRESSION (audit S1): the collapsed INSERT carries no \"body\" column, so the value "+
			"survives the pre-compaction chain and not the compacted one.\n\nemitted row: %v\n\n"+
			"The INSERT arm of the merge must UNION the prior row with the UPDATE's after-image, not "+
			"replace it — see mergeAfterImage.", ins.Row)
	}
	if got != bodyText {
		t.Errorf("body = %#v, want %q — the merge overwrote a column the UPDATE never carried", got, bodyText)
	}
}

// TestSmartCompaction_UpdatePlusPartialUpdate_LosesTheFirstUpdatesValue is the
// SIBLING site the audit did not name. Same root cause, different merge arm.
//
// Here the first UPDATE is the one that CHANGES the wide column, so its
// after-image carries it; the second UPDATE touches only `title`, so PG marks
// `body` unchanged and omits it. The merge keeps the LAST after-image, and the
// new body value exists in neither the merged event nor the target.
func TestSmartCompaction_UpdatePlusPartialUpdate_LosesTheFirstUpdatesValue(t *testing.T) {
	const newBody = "the value the FIRST update wrote"

	emitted, _ := runPolicy(t, docsSchema(), []ir.Change{
		ir.Update{
			Position: pos(200), Schema: "public", Table: "docs",
			Before: ir.Row{"id": int64(2)},
			After:  ir.Row{"id": int64(2), "title": "t1", "body": newBody},
		},
		ir.Update{
			Position: pos(210), Schema: "public", Table: "docs",
			Before: ir.Row{"id": int64(2)},
			After:  ir.Row{"id": int64(2), "title": "t2"}, // body unchanged -> omitted
		},
	})

	if len(emitted) != 1 {
		t.Fatalf("expected the pair to collapse to one event, got %d: %v", len(emitted), kindsOf(emitted))
	}
	upd, ok := emitted[0].(ir.Update)
	if !ok {
		t.Fatalf("expected UPDATE+UPDATE to collapse to an UPDATE, got %T", emitted[0])
	}
	if got := upd.After["title"]; got != "t2" {
		t.Errorf("title = %v, want \"t2\"", got)
	}

	got, present := upd.After["body"]
	if !present {
		t.Fatalf("REGRESSION (audit S1, sibling arm): the collapsed UPDATE carries no \"body\" column, so "+
			"the value the FIRST update wrote is lost.\n\nemitted After: %v\n\n"+
			"Absent-means-preserve is sound for a single UPDATE because the target already holds the "+
			"value. It is NOT sound after a merge — the update that wrote the new value has been "+
			"collapsed away, so what the target preserves is the value from BEFORE it.", upd.After)
	}
	if got != newBody {
		t.Errorf("body = %#v, want %q", got, newBody)
	}
}

// TestMergeAfterImage_AbsentMeansUnchangedNotNull is the premise pin named in
// mergeAfterImage's doc, from this side of the boundary.
//
// The union is only sound because an ABSENT column and a column PRESENT WITH A
// NIL VALUE mean different things. If they were ever conflated, the merge would
// carry a stale value forward over a deliberate SET NULL. The wire half of this
// argument is pinned in the postgres package by
// TestNullAndUnchangedToastAreDistinguishable; this is the consuming half.
func TestMergeAfterImage_AbsentMeansUnchangedNotNull(t *testing.T) {
	earlier := ir.Row{"id": int64(1), "title": "t1", "body": "kept", "note": "old"}

	// `body` absent  -> unchanged, so the earlier value survives.
	// `note`  nil    -> deliberately NULLed, so nil must WIN over "old".
	later := ir.Row{"id": int64(1), "title": "t2", "note": nil}

	merged := mergeAfterImage(earlier, later)

	if got, present := merged["body"]; !present || got != "kept" {
		t.Errorf("body = %#v (present=%v), want \"kept\" — an ABSENT column means unchanged, so the "+
			"earlier value must survive the merge", got, present)
	}
	got, present := merged["note"]
	if !present {
		t.Errorf("note is absent from the merge; a column explicitly set to NULL must stay PRESENT, or " +
			"the applier will preserve the target's old value instead of nulling it")
	} else if got != nil {
		t.Errorf("note = %#v, want nil — a deliberate SET NULL was overwritten by the earlier value, "+
			"which is exactly the corruption the absent-vs-nil distinction prevents", got)
	}
	if merged["title"] != "t2" {
		t.Errorf("title = %#v, want \"t2\" — later wins for columns the later image carries", merged["title"])
	}

	// Neither input may be mutated: both are still referenced by events the
	// caller may yet emit.
	if _, mutated := earlier["note"]; mutated && earlier["note"] != "old" {
		t.Error("mergeAfterImage mutated its earlier input")
	}
	if _, leaked := later["body"]; leaked {
		t.Error("mergeAfterImage mutated its later input")
	}
}

// TestSmartCompaction_PKGuardDoesNotCoverNonPKOmissions shows WHY the existing
// completeness guard misses this: it refuses on a missing PRIMARY KEY column
// and says nothing about any other column. A TOASTed column is essentially
// never a PK column, so the guard and the defect barely overlap.
//
// This is not a defect in the guard — it is doing its own job. It is here so
// that "smart-compact refuses loudly on incomplete payloads" is not read as
// broader cover than it gives.
func TestSmartCompaction_PKGuardDoesNotCoverNonPKOmissions(t *testing.T) {
	c := newSmartCompactor(PKStrategyPK, docsSchema())

	// Missing PK column: refused, loudly.
	err := c.process(ir.Insert{
		Position: pos(300), Schema: "public", Table: "docs",
		Row: ir.Row{"title": "no id here"},
	})
	if err == nil {
		t.Fatal("a payload missing its PK column was accepted; the loud-refusal guard is not firing " +
			"and this test's premise about the guard is wrong")
	}
	if !strings.Contains(err.Error(), "id") {
		t.Errorf("refusal does not name the missing column: %v", err)
	}

	// Missing NON-PK column: accepted silently. Same compactor, same shape of
	// incompleteness, opposite outcome.
	if err := c.process(ir.Insert{
		Position: pos(310), Schema: "public", Table: "docs",
		Row: ir.Row{"id": int64(3), "title": "body omitted"},
	}); err != nil {
		t.Fatalf("a payload missing a non-PK column was refused (%v) — if the guard was widened, this "+
			"test is stale and the S1 tests above should be re-read", err)
	}
}
