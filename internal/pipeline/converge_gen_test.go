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

// convTestDirections is the full four-direction matrix the unit pins
// iterate (mirrors the live harness's four TestSyncConverges_*).
var convTestDirections = []convDirection{
	convDirPGToPG, convDirMySQLToMySQL, convDirPGToMySQL, convDirMySQLToPG,
}

// TestConvergeGen_GeneratedOpsAreModelValid pins the generator's
// by-construction validity, per direction: every drawn case (any
// shrink of it goes through the same generator, so this covers shrunk
// cases too) replays cleanly through a fresh model, carries every
// column family in the DIRECTION's family set (the full set
// same-engine; the cross-engine-safe subset cross-engine), carries NO
// family outside it, and respects the structural invariants the live
// harness relies on.
func TestConvergeGen_GeneratedOpsAreModelValid(t *testing.T) {
	for _, dir := range convTestDirections {
		gen := convCaseGen(dir, 8)
		want := map[convFamily]bool{}
		for _, f := range dir.families() {
			want[f] = true
		}
		t.Run(dir.String(), func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				c := gen.Draw(rt, "case")
				if c.dir != dir {
					rt.Fatalf("case direction = %s; want %s", c.dir, dir)
				}
				if _, err := c.finalModel(); err != nil {
					rt.Fatalf("generated case is model-invalid: %v", err)
				}
				if len(c.initial) == 0 {
					rt.Fatalf("no initial rows — the live harness can't observe bulk-copy completion")
				}
				seen := map[convFamily]bool{}
				for _, col := range c.cols {
					if !want[col.fam] {
						rt.Fatalf("family %s emitted for direction %s but is not in its safe set", col.fam, dir)
					}
					seen[col.fam] = true
				}
				for f := range want {
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

// TestConvergeGen_CrossEngineUsesSafeSubsetOnly pins design decision #1
// directly: a cross-engine direction's family set is exactly the safe
// subset (int/text/numeric/timestamp — bool EXCLUDED), while a
// same-engine direction carries every family including bool.
func TestConvergeGen_CrossEngineUsesSafeSubsetOnly(t *testing.T) {
	for _, dir := range convTestDirections {
		got := map[convFamily]bool{}
		for _, f := range dir.families() {
			got[f] = true
		}
		if dir.crossEngine() {
			if got[convFamBool] {
				t.Errorf("%s (cross-engine): bool must be excluded from the safe subset", dir)
			}
			for _, f := range []convFamily{convFamInt, convFamText, convFamNumeric, convFamTimestamp} {
				if !got[f] {
					t.Errorf("%s (cross-engine): safe family %s missing", dir, f)
				}
			}
			if len(got) != 4 {
				t.Errorf("%s (cross-engine): family set size = %d; want exactly 4 (the safe subset)", dir, len(got))
			}
		} else {
			for f := convFamily(0); f < convFamCount; f++ {
				if !got[f] {
					t.Errorf("%s (same-engine): family %s must be present (full set)", dir, f)
				}
			}
		}
	}
}

// TestConvergeGen_SameSeedRegeneratesByteIdenticalScript is the
// deterministic-regeneration pin, per direction: a failure's replay
// instructions are only honest if (direction, seed, budget)
// regenerates the exact same op script.
func TestConvergeGen_SameSeedRegeneratesByteIdenticalScript(t *testing.T) {
	for _, dir := range convTestDirections {
		gen := convCaseGen(dir, 8)
		for seed := 1; seed <= 10; seed++ {
			a := gen.Example(seed)
			b := gen.Example(seed)
			sa, sb := a.renderScript("conv_pin"), b.renderScript("conv_pin")
			if sa != sb {
				t.Fatalf("non-deterministic generation [%s seed %d]:\n--- A ---\n%s\n--- B ---\n%s", dir, seed, sa, sb)
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
	gen := convCaseGen(convDirPGToPG, 8)
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

// TestConvergeGen_CrossEngineCanonField pins design decision #2: the
// per-family normaliser folds the two engines' residual canonical-text
// differences (inside the safe subset) to a common form so a faithful
// cross-engine sync compares equal. The cases are the exact divergence
// shapes documented in convCanonField: numeric negative-zero (PG keeps
// the sign, MySQL drops it) and timestamp trailing-zero fraction (PG
// drops it in ::text, MySQL DATETIME(6) keeps six digits). int and
// text are pass-through (already byte-identical across engines).
func TestConvergeGen_CrossEngineCanonField(t *testing.T) {
	cases := []struct {
		name string
		fam  convFamily
		in   string
		want string
	}{
		{"int passthrough", convFamInt, "123", "123"},
		{"int negative", convFamInt, "-9223372036854775808", "-9223372036854775808"},
		{"text passthrough", convFamText, `a "b" \c`, `a "b" \c`},
		{"text empty", convFamText, "", ""},
		{"numeric plain", convFamNumeric, "12.3400", "12.3400"},
		{"numeric negative", convFamNumeric, "-12.3400", "-12.3400"},
		{"numeric negzero scaled (PG form)", convFamNumeric, "-0.0000", "0.0000"},
		{"numeric negzero int (PG form)", convFamNumeric, "-0", "0"},
		{"numeric positive zero", convFamNumeric, "0.0000", "0.0000"},
		// A negative value whose integer part is zero but fraction is not
		// must KEEP its sign.
		{"numeric small negative", convFamNumeric, "-0.0001", "-0.0001"},
		{"ts no-frac (PG form)", convFamTimestamp, "2020-01-02 03:04:05", "2020-01-02 03:04:05"},
		{"ts zero-frac (MySQL form)", convFamTimestamp, "2020-01-02 03:04:05.000000", "2020-01-02 03:04:05"},
		{"ts real frac kept", convFamTimestamp, "2020-01-02 03:04:05.000006", "2020-01-02 03:04:05.000006"},
		{"ts real frac trailing zeros kept", convFamTimestamp, "2020-01-02 03:04:05.100000", "2020-01-02 03:04:05.100000"},
	}
	for _, c := range cases {
		if got := convCanonField(c.fam, c.in); got != c.want {
			t.Errorf("%s: convCanonField(%s, %q) = %q; want %q", c.name, c.fam, c.in, got, c.want)
		}
	}

	// Bool is NOT in the cross-engine safe set, so convCanonField is
	// never called on it cross-engine; but if it were, it would be
	// pass-through (no normalisation) — pin that it does not silently
	// "fix" the true/false vs 1/0 divergence, which is exactly why bool
	// is excluded rather than canonicalised.
	if got := convCanonField(convFamBool, "true"); got != "true" {
		t.Errorf("convCanonField(bool, true) = %q; want pass-through %q", got, "true")
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
