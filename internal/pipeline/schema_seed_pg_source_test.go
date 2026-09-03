// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// TestWarmResumeSeed_PGSourceTargetMatrix is the SLM-1c family matrix
// for a POSTGRES source: {pg→pg, pg→mysql, pg→sqlite} × {witness,
// override, history-only, none}, with the expected prior derived from
// the merge rules and the engines' documented projections, not from
// the code.
//
// The premise it pins is the one SLM-1c's filing got wrong: a Postgres
// source's zone pair is NOT `ir.Timestamp{WithTimeZone:true}` ⇄
// `ir.Timestamp{WithTimeZone:false}`. Both the Postgres schema reader
// (types.go, "timestamp without time zone") and its CDC projection
// (cdc_relations.go, TimestampOID) project `timestamp` as
// [ir.DateTime] — the SAME naive member MySQL's DATETIME projects to —
// so [zoneFamilyMember] already admits every read-back a Postgres or
// MySQL target produces for a Postgres source's pair, with no
// source-aware membership needed. `ir.Timestamp{WithTimeZone:false}` is
// what a SQLite target reads BOTH members back as, which is exactly why
// it is excluded: a sync to SQLite (not a target today, pinned anyway)
// resumes on the history, or with no prior.
func TestWarmResumeSeed_PGSourceTargetMatrix(t *testing.T) {
	ctx := context.Background()
	// The Postgres source's own projection of the pair — what the
	// history row holds and what the reader's wire projection produces.
	srcZoned := ir.Timestamp{WithTimeZone: true, PrecisionUnspecified: true}
	srcNaive := ir.DateTime{PrecisionUnspecified: true}

	targets := []struct {
		name         string
		zoned, naive ir.Type
		// witnesses reports whether the target's read-back distinguishes
		// the pair — false for a target that collapses it.
		witnesses bool
	}{
		{"pg", ir.Timestamp{WithTimeZone: true, Precision: 6}, ir.DateTime{Precision: 6}, true},
		{"mysql", ir.Timestamp{WithTimeZone: true, Precision: 6}, ir.DateTime{Precision: 6}, true},
		{"sqlite", ir.Timestamp{PrecisionUnspecified: true}, ir.Timestamp{PrecisionUnspecified: true}, false},
	}
	priors := []struct {
		name string
		// history is the retained version's type for the column, nil for
		// a table with no history row.
		history ir.Type
		// override puts a --type-override on the column.
		override bool
	}{
		{"witness", nil, false},
		{"override", nil, true},
		{"history-only", srcNaive, false},
		{"none", nil, false},
	}
	for _, tgt := range targets {
		for _, p := range priors {
			for _, sourceZoned := range []bool{true, false} {
				name := tgt.name + "/" + p.name
				if sourceZoned {
					name += "/timestamptz"
				} else {
					name += "/timestamp"
				}
				t.Run(name, func(t *testing.T) {
					// The target holds the column in the family the
					// source last committed — except under an override,
					// where it holds the OPPOSITE family.
					targetType := tgt.naive
					if sourceZoned != p.override {
						targetType = tgt.zoned
					}
					witness := map[string]*ir.Table{}
					if p.name != "none" || tgt.witnesses {
						witness["events"] = &ir.Table{Schema: "public", Name: "events", Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
							{Name: "c", Type: targetType},
						}}
					}
					if p.name == "none" {
						// "none": the target does not hold the table and no
						// history exists — the honest no-prior cell.
						delete(witness, "events")
					}
					var history []*ir.Table
					if p.history != nil {
						var h ir.Type = srcNaive
						if sourceZoned {
							h = srcZoned
						}
						history = []*ir.Table{{Schema: "public", Name: "events", Columns: []*ir.Column{
							{Name: "id", Type: ir.Integer{Width: 64}},
							{Name: "c", Type: h},
						}}}
					}
					var mappings []config.Mapping
					if p.override {
						mappings = []config.Mapping{{Table: "events", Column: "c", TargetType: "timestamptz"}}
					}
					seed, err := mergeWarmResumeSeed(ctx, "s", witness, history, mappings)
					if err != nil {
						t.Fatal(err)
					}
					got := seedColumnType(seed, "events", "c")

					// Derive the expectation from the rules.
					var want ir.Type
					switch {
					case p.name == "none":
						want = nil
					case p.override:
						want = nil // history is nil in this cell: no prior
					case tgt.witnesses:
						want = targetType
					default: // a collapsing target: history or nothing
						want = p.history
						if want != nil && sourceZoned {
							want = srcZoned
						}
					}
					if want == nil && got != nil {
						t.Fatalf("prior for events.c = %v; want none", got)
					}
					if want != nil {
						if got == nil {
							t.Fatalf("no prior for events.c; want %v", want)
						}
						// The prior must be a zone-family member whose zone
						// flag equals the source's last-committed family —
						// that is what the reader's predicate consults.
						wf, wz, wok := ir.ZoneFamily(want)
						gf, gz, gok := ir.ZoneFamily(got)
						if !wok || !gok || wf != gf || wz != gz {
							t.Fatalf("prior for events.c = %v (family %q zoned=%v); want %v (family %q zoned=%v)", got, gf, gz, want, wf, wz)
						}
						if gz != sourceZoned {
							t.Fatalf("prior for events.c is zoned=%v; the source last committed zoned=%v — this prior would read the next RelationMessage as a phantom swap", gz, sourceZoned)
						}
					}
				})
			}
		}
	}
}
