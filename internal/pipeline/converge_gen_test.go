// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Container-free unit pins for the sync-convergence property core
// (converge_gen.go): model semantics, generated-op validity, the
// deterministic-regeneration property (same seed → byte-identical op
// script — the analogue of TestMigrate_FuzzRoundtrip_ReplayDumpedFixture),
// dialect rendering, and a generator-rot guard asserting every nasty
// interleaving the harness exists for actually gets generated.

package pipeline

import (
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// convTestCols is a fixed two-column shape for hand-built model and
// rendering pins.
var convTestCols = []convColumn{
	{name: "c00_text", fam: convFamText},
	{name: "c01_bool", fam: convFamBool},
}

func convV(payload string) convValue { return convValue{payload: payload} }

func convNull() convValue { return convValue{null: true} }

func TestConvergeGen_ModelSemantics(t *testing.T) {
	m := newConvModel()
	nCols := len(convTestCols)

	steps := []struct {
		name string
		op   convOp
		want []int64 // live PKs after the op
	}{
		{"insert 1", convOp{kind: convOpInsert, pk: 1, row: []convValue{convV("a"), convV("1")}}, []int64{1}},
		{"insert 2", convOp{kind: convOpInsert, pk: 2, row: []convValue{convNull(), convV("0")}}, []int64{1, 2}},
		{"update 1", convOp{kind: convOpUpdate, pk: 1, set: []int{0}, row: []convValue{convV("b")}}, []int64{1, 2}},
		{"pk-update 2->5", convOp{kind: convOpPKUpdate, pk: 2, newPK: 5}, []int64{1, 5}},
		{"delete 1", convOp{kind: convOpDelete, pk: 1}, []int64{5}},
		{"reuse pk 1", convOp{kind: convOpInsert, pk: 1, row: []convValue{convV("c"), convNull()}}, []int64{1, 5}},
		{"truncate", convOp{kind: convOpTruncate}, nil},
		{"insert after truncate", convOp{kind: convOpInsert, pk: 3, row: []convValue{convV("d"), convV("1")}}, []int64{3}},
	}
	for _, s := range steps {
		if err := m.apply(s.op, nCols); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		got := m.livePKs()
		if len(got) != len(s.want) {
			t.Fatalf("%s: live PKs = %v; want %v", s.name, got, s.want)
		}
		for i := range got {
			if got[i] != s.want[i] {
				t.Fatalf("%s: live PKs = %v; want %v", s.name, got, s.want)
			}
		}
	}

	// The truncate must really have cleared the earlier rows: the only
	// surviving payload is the post-truncate insert's.
	if got := m.rows[3][0].payload; got != "d" {
		t.Fatalf("post-truncate row payload = %q; want %q", got, "d")
	}
}

func TestConvergeGen_ModelRejectsInvalidOps(t *testing.T) {
	nCols := len(convTestCols)
	seed := func() *convModel {
		m := newConvModel()
		if err := m.apply(convOp{kind: convOpInsert, pk: 1, row: []convValue{convV("a"), convV("1")}}, nCols); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return m
	}

	cases := []struct {
		name string
		op   convOp
	}{
		{"duplicate insert", convOp{kind: convOpInsert, pk: 1, row: []convValue{convV("x"), convV("0")}}},
		{"insert wrong arity", convOp{kind: convOpInsert, pk: 2, row: []convValue{convV("x")}}},
		{"update of dead pk", convOp{kind: convOpUpdate, pk: 9, set: []int{0}, row: []convValue{convV("x")}}},
		{"update empty set", convOp{kind: convOpUpdate, pk: 1}},
		{"update set/row mismatch", convOp{kind: convOpUpdate, pk: 1, set: []int{0, 1}, row: []convValue{convV("x")}}},
		{"update column out of range", convOp{kind: convOpUpdate, pk: 1, set: []int{7}, row: []convValue{convV("x")}}},
		{"pk-update of dead pk", convOp{kind: convOpPKUpdate, pk: 9, newPK: 2}},
		{"pk-update onto live pk", convOp{kind: convOpPKUpdate, pk: 1, newPK: 1}},
		{"delete of dead pk", convOp{kind: convOpDelete, pk: 9}},
	}
	for _, c := range cases {
		if err := seed().apply(c.op, nCols); err == nil {
			t.Errorf("%s: apply succeeded; want loud refusal", c.name)
		}
	}
}

// TestConvergeGen_GeneratedOpsAreModelValid pins the generator's
// by-construction validity: every drawn case (any shrink of it goes
// through the same generator, so this covers shrunk cases too)
// replays cleanly through a fresh model, carries every column family,
// and respects the structural invariants the live harness relies on.
func TestConvergeGen_GeneratedOpsAreModelValid(t *testing.T) {
	for _, eng := range []engineKind{enginePG, engineMySQL} {
		gen := convCaseGen(eng, 8)
		t.Run(eng.String(), func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				c := gen.Draw(rt, "case")
				if _, err := c.finalModel(); err != nil {
					rt.Fatalf("generated case is model-invalid: %v", err)
				}
				if len(c.initial) == 0 {
					rt.Fatalf("no initial rows — the live harness can't observe bulk-copy completion")
				}
				seen := map[convFamily]bool{}
				for _, col := range c.cols {
					seen[col.fam] = true
				}
				for f := convFamily(0); f < convFamCount; f++ {
					if !seen[f] {
						rt.Fatalf("family %s missing from the table shape (pin the class)", f)
					}
				}
				for ti, tx := range c.txs {
					isTrunc := tx.pattern == convTxTruncate
					if isTrunc && len(tx.ops) != 1 {
						rt.Fatalf("tx %d: truncate tx has %d ops; want exactly 1", ti, len(tx.ops))
					}
					for _, op := range tx.ops {
						if !isTrunc && op.kind == convOpTruncate {
							rt.Fatalf("tx %d (%s): TRUNCATE inside a multi-op tx — dialect-divergent (MySQL implicit commit)", ti, tx.pattern)
						}
					}
				}
				switch c.applyBatch {
				case 0, 8, 64:
				default:
					rt.Fatalf("applyBatch = %d; want one of 0/8/64", c.applyBatch)
				}
			})
		})
	}
}

// TestConvergeGen_SameSeedRegeneratesByteIdenticalScript is the
// deterministic-regeneration pin: a failure's replay instructions are
// only honest if (seed, budget) regenerates the exact same op script.
func TestConvergeGen_SameSeedRegeneratesByteIdenticalScript(t *testing.T) {
	for _, eng := range []engineKind{enginePG, engineMySQL} {
		gen := convCaseGen(eng, 8)
		for seed := 1; seed <= 10; seed++ {
			a := gen.Example(seed)
			b := gen.Example(seed)
			sa, sb := a.renderScript("conv_pin"), b.renderScript("conv_pin")
			if sa != sb {
				t.Fatalf("non-deterministic generation [%s seed %d]:\n--- A ---\n%s\n--- B ---\n%s", eng, seed, sa, sb)
			}
		}
	}
}

// TestConvergeGen_NastyInterleavingsGenerated is the generator-rot
// guard: across a fixed sample of example cases, every interleaving
// class the harness exists for must actually occur. If a future edit
// to the weights or preconditions silently stops generating one of
// them, this trips. (Generation is dialect-independent — the engine
// only affects rendering — so one engine's sample suffices.)
func TestConvergeGen_NastyInterleavingsGenerated(t *testing.T) {
	gen := convCaseGen(enginePG, 8)
	patterns := map[convTxPattern]int{}
	pkUpdates, pkReuses, nulls := 0, 0, 0
	for seed := 0; seed < 200; seed++ {
		c := gen.Example(seed)
		deleted := map[int64]bool{}
		live := map[int64]bool{}
		for _, op := range c.initial {
			live[op.pk] = true
			for _, v := range op.row {
				if v.null {
					nulls++
				}
			}
		}
		for _, tx := range c.txs {
			patterns[tx.pattern]++
			for _, op := range tx.ops {
				switch op.kind {
				case convOpInsert:
					if deleted[op.pk] {
						pkReuses++
					}
					live[op.pk] = true
				case convOpPKUpdate:
					pkUpdates++
					deleted[op.pk] = true
					delete(live, op.pk)
					live[op.newPK] = true
				case convOpDelete:
					deleted[op.pk] = true
					delete(live, op.pk)
				case convOpTruncate:
					for pk := range live {
						deleted[pk] = true
						delete(live, pk)
					}
				}
			}
		}
	}
	for p := convTxPlain; p <= convTxTruncate; p++ {
		if patterns[p] == 0 {
			t.Errorf("tx pattern %s never generated across the sample", p)
		}
	}
	if pkUpdates == 0 {
		t.Error("no PK-changing UPDATE generated across the sample")
	}
	if pkReuses == 0 {
		t.Error("no PK reuse (insert of a previously-deleted PK) across the sample")
	}
	if nulls == 0 {
		t.Error("no NULL cell generated across the sample")
	}
}

func TestConvergeGen_StringLiteralEscaping(t *testing.T) {
	in := `it's c:\tmp`
	if got, want := quoteConvString(in, enginePG), `'it''s c:\tmp'`; got != want {
		t.Errorf("PG quote = %s; want %s", got, want)
	}
	if got, want := quoteConvString(in, engineMySQL), `'it''s c:\\tmp'`; got != want {
		t.Errorf("MySQL quote = %s; want %s", got, want)
	}
}

// TestConvergeGen_RenderScript_Dialects pins the rendered script for
// a hand-built case in both dialects: literal forms (TRUE vs 1),
// REPLICA IDENTITY FULL on PG, the InnoDB/utf8mb4 clause on MySQL,
// TRUNCATE standing outside BEGIN/COMMIT, and the stream-attach
// marker.
func TestConvergeGen_RenderScript_Dialects(t *testing.T) {
	base := convCase{
		cols: convTestCols,
		initial: []convOp{
			{kind: convOpInsert, pk: 1, row: []convValue{convV("it's"), convV("1")}},
		},
		txs: []convTx{
			{pattern: convTxPlain, ops: []convOp{
				{kind: convOpUpdate, pk: 1, set: []int{1}, row: []convValue{convV("0")}},
			}},
			{pattern: convTxTruncate, ops: []convOp{{kind: convOpTruncate}}},
			{pattern: convTxEmpty},
		},
	}

	pg := base
	pg.eng = enginePG
	wantPG := `DROP TABLE IF EXISTS conv_pin;
CREATE TABLE conv_pin (
  id BIGINT NOT NULL PRIMARY KEY,
  c00_text VARCHAR(255),
  c01_bool BOOLEAN
);
ALTER TABLE conv_pin REPLICA IDENTITY FULL;
INSERT INTO conv_pin (id, c00_text, c01_bool) VALUES (1, 'it''s', TRUE);
-- >>> sluice sync stream attaches here <<<
-- tx 0 (plain)
BEGIN;
UPDATE conv_pin SET c01_bool = FALSE WHERE id = 1;
COMMIT;
-- tx 1 (truncate)
TRUNCATE TABLE conv_pin;
-- tx 2 (empty)
BEGIN;
COMMIT;
`
	if got := pg.renderScript("conv_pin"); got != wantPG {
		t.Errorf("PG script:\n%s\nwant:\n%s", got, wantPG)
	}

	my := base
	my.eng = engineMySQL
	got := my.renderScript("conv_pin")
	for _, frag := range []string{
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		"c01_bool TINYINT(1)",
		"VALUES (1, 'it''s', 1);",
		"SET c01_bool = 0 WHERE id = 1;",
	} {
		if !strings.Contains(got, frag) {
			t.Errorf("MySQL script missing %q:\n%s", frag, got)
		}
	}
	if strings.Contains(got, "REPLICA IDENTITY") {
		t.Errorf("MySQL script must not carry REPLICA IDENTITY:\n%s", got)
	}
}
