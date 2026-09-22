// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-6 follow-up 6 (value-fidelity review): the anchored-resume drift
// guard compares a reader-fresh schema fingerprint to the one the
// interrupted attempt recorded, and GeneratedStored IS in that hash. So
// an interrupted backup attempt written by a pre-GC-6 binary against a
// PG 18 source with a VIRTUAL (or bare, = VIRTUAL on 18) generated column
// — which that binary recorded as STORED — resumed by this binary, which
// reads it as VIRTUAL, refuses "the source schema changed". The refusal
// is FALSE (nothing changed on the source) but LOUD, and its remedy
// (--force-overwrite → a fresh full) is the safe one; the decision is to
// keep it rather than exclude GeneratedStored from the hash, which would
// blind the guard to a real STORED⇄VIRTUAL retype on every cross-upgrade
// resume. Release-notes line: "resuming an interrupted backup of a PG 18
// source with VIRTUAL generated columns across the upgrade refuses with
// the schema-changed message; pass --force-overwrite once."
//
// The prior manifest below is HAND-WRITTEN in the pre-GC-6 wire shape,
// never regenerated from the new reader (the item-104 lesson: a fixture
// built from post-change values makes a compatibility gate
// self-referential).

package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// preGC6ManifestJSON is what a pre-GC-6 binary recorded for
// `CREATE TABLE gen (id int PRIMARY KEY, c int NOT NULL, v int GENERATED
// ALWAYS AS (c * 3) VIRTUAL)` on PG 18: the reader of that era set
// generated_stored to true unconditionally.
const preGC6ManifestJSON = `{
  "schema": {
    "Tables": [{
      "Name": "gen",
      "Columns": [
        {"name":"id","type":{"kind":"Integer","width":32},"default":{"kind":"None"}},
        {"name":"c","type":{"kind":"Integer","width":32},"default":{"kind":"None"}},
        {"name":"v","type":{"kind":"Integer","width":32},"nullable":true,"default":{"kind":"None"},
         "generated_expr":"(c * 3)","generated_stored":true,"generated_expr_dialect":"postgres"}
      ],
      "PrimaryKey":{"Name":"gen_pkey","Unique":true,"Columns":[{"Column":"id"}]}
    }]
  }
}`

func TestRefuseAnchoredResumeOnSchemaDrift_PreGC6VirtualColumnRefusesLoudlyWithRemedy(t *testing.T) {
	var prior irbackup.Manifest
	if err := json.Unmarshal([]byte(preGC6ManifestJSON), &prior); err != nil {
		t.Fatalf("decode hand-written prior manifest: %v", err)
	}
	if prior.Schema == nil || len(prior.Schema.Tables) != 1 || !prior.Schema.Tables[0].Columns[2].GeneratedStored {
		t.Fatalf("fixture did not decode as a STORED generated column: %+v", prior.Schema)
	}

	// What THIS binary reads from the unchanged PG 18 source: VIRTUAL.
	current := &ir.Schema{Tables: []*ir.Table{{
		Name: "gen",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}, Default: ir.DefaultNone{}},
			{Name: "c", Type: ir.Integer{Width: 32}, Default: ir.DefaultNone{}},
			{Name: "v", Type: ir.Integer{Width: 32}, Nullable: true, Default: ir.DefaultNone{}, GeneratedExpr: "(c * 3)", GeneratedStored: false, GeneratedExprDialect: "postgres"},
		},
		PrimaryKey: &ir.Index{Name: "gen_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	err := refuseAnchoredResumeOnSchemaDrift(current, prior.Schema)
	if err == nil {
		t.Fatal("resume across the GC-6 upgrade was accepted; the guard no longer sees GeneratedStored, which would also hide a real STORED⇄VIRTUAL retype")
	}
	if !strings.Contains(err.Error(), "--force-overwrite") || !strings.Contains(err.Error(), "schema changed") {
		t.Errorf("refusal must name the remedy and the reason; got: %v", err)
	}

	// Control: the same binary on both sides (VIRTUAL recorded, VIRTUAL
	// read) resumes — the refusal is about the recorded field, not about
	// VIRTUAL columns as such.
	same := *current
	if err := refuseAnchoredResumeOnSchemaDrift(current, &same); err != nil {
		t.Errorf("identical schema across attempts refused: %v", err)
	}
}
