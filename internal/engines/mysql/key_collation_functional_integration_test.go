//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-FUNCTIONAL-KEY-PART on the real server (the shared MySQL
// container; SLUICE_TEST_MYSQL_IMAGE=mysql:8.4 runs it on 8.4).
//
// The parser reads the server's normalized EXPRESSION spelling, and the
// guard renders the part itself, so both are pinned against the server
// rather than against the parser's own idea of the grammar: every form the
// grammar admits is created as a functional key, read back through
// uniqueKeyColumns, and the guard's rendering is compared with the server's
// own expression over real rows (the independent expected value: the
// server evaluating its stored expression). The copy writer then runs every
// shape a functional part can take — read through, opaque and collated
// (refuses), opaque and not a string (left out), and the hidden column
// unreadable (the vtgate fallback) — with the target read back by HEX(k).

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestUniqueKeyColumns_FunctionalPartsOnTheRealServer is the grammar matrix:
// each key's part is read through (with the collation its hidden column
// holds) or opaque, and a read-through part renders to the server's own
// value for every row.
func TestUniqueKeyColumns_FunctionalPartsOnTheRealServer(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "kcfn_grammar")
	defer cleanup()
	applyDDL(t, dsn, "CREATE TABLE kx (id INT PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, j INT, "+
		"UNIQUE KEY lo ((lower(k))), UNIQUE KEY up ((upper(k))), UNIQUE KEY lc ((lcase(k))), UNIQUE KEY tr ((trim(k))), "+
		"UNIQUE KEY lr ((ltrim(rtrim(k)))), UNIQUE KEY cs ((lower(cast(k as char charset utf8mb4)))), "+
		"UNIQUE KEY cn ((cast(k as char(10) charset utf8mb4))), UNIQUE KEY cl ((lower(k) collate utf8mb4_bin)), "+
		"UNIQUE KEY bc ((k collate utf8mb4_bin)), UNIQUE KEY l1 ((lower(cast(k as char charset latin1)))), "+
		"UNIQUE KEY bn ((cast(k as binary))), UNIQUE KEY mix (j, (upper(k))), "+
		"UNIQUE KEY md ((md5(k))), UNIQUE KEY nm ((j * 2)), UNIQUE KEY tb ((trim(both 'x' from k))), "+
		"UNIQUE KEY cc ((concat(k, j))), UNIQUE KEY lb ((concat(k, 'a`b'))), UNIQUE KEY mc ((lower(K))), UNIQUE KEY "+strings.Repeat("functional_key_", 4)+" ((upper(lower(k)))))")
	applyDDL(t, dsn, "INSERT INTO kx VALUES (1, ' Abc ', 1), (2, 'ÉtÉ', 2), (3, 'x', 3), (4, 'ß', 4)")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	keys, err := uniqueKeyColumns(ctx, db, FlavorVanilla, "", "kx")
	if err != nil {
		t.Fatal(err)
	}
	read := map[string]*functionalKeyPart{}
	for _, fns := range keys.uniqueFns {
		for _, f := range fns {
			if f != nil {
				read[f.index] = f
			}
		}
	}
	if len(keys.unresolved) != 0 {
		t.Fatalf("unresolved parts %+v", keys.unresolved)
	}
	long := strings.Repeat("functional_key_", 4)
	for idx, wantColl := range map[string]string{
		"lo": "utf8mb4_0900_ai_ci", "up": "utf8mb4_0900_ai_ci", "lc": "utf8mb4_0900_ai_ci", "tr": "utf8mb4_0900_ai_ci",
		"lr": "utf8mb4_0900_ai_ci", "cs": "utf8mb4_0900_ai_ci", "cn": "utf8mb4_0900_ai_ci", "cl": "utf8mb4_bin",
		"bc": "utf8mb4_bin", "l1": "latin1_swedish_ci", "bn": "", "mix": "utf8mb4_0900_ai_ci",
		"mc": "utf8mb4_0900_ai_ci", long: "utf8mb4_0900_ai_ci",
	} {
		f := read[idx]
		if f == nil || !f.readable {
			t.Errorf("key %s: not read through (%+v)", idx, f)
			continue
		}
		// mc spells the column K: resolved to the table's k (review F1).
		if len(f.columns) != 1 || f.columns[0] != "k" || !f.known || f.collation != wantColl {
			t.Errorf("key %s (%s): columns %q collation %q known %v; want k, %q, known", idx, f.expr, f.columns, f.collation, f.known, wantColl)
		}
		// The guard's rendering against the server's own expression, row by
		// row, byte for byte.
		var differ int
		q := "SELECT COUNT(*) FROM kx WHERE NOT (CAST(" + f.render("kx.`k`") + " AS BINARY) <=> CAST(" + f.expr + " AS BINARY))"
		if err := db.QueryRowContext(ctx, q).Scan(&differ); err != nil {
			t.Errorf("key %s: %s: %v", idx, q, err)
		} else if differ != 0 {
			t.Errorf("key %s: the rendering %s differs from the server's %s on %d row(s)", idx, f.render("k"), f.expr, differ)
		}
	}
	// The parts the guard cannot read: the columns each reads, resolved, and
	// whether its value is a collated string (it arms per row) or not (left
	// out).
	for idx, want := range map[string]struct {
		columns  string
		collated bool
	}{"md": {"k", true}, "nm": {"j", false}, "tb": {"k", true}, "cc": {"k|j", true}, "lb": {"k", true}} {
		f := read[idx]
		if f == nil || f.readable {
			t.Errorf("key %s: %+v; want an unreadable part", idx, f)
			continue
		}
		if strings.Join(f.columns, "|") != want.columns || !f.known || f.collated() != want.collated {
			t.Errorf("key %s (%s): columns %q known %v collated %v; want %q, known, %v", idx, f.expr, f.columns, f.known, f.collated(), want.columns, want.collated)
		}
	}

	// The Vitess flavors never read the hidden columns (vtgate does not parse
	// SHOW EXTENDED): every part is unknown, and so compares TRUE on the
	// target side — it never refuses the table.
	vk, err := uniqueKeyColumns(ctx, db, FlavorVitess, "", "kx")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range vk.functionalParts() {
		if f.known {
			t.Errorf("vitess flavor: key %s claims a known collation %q", f.index, f.collation)
		}
	}
	if len(vk.unresolved) != 0 {
		t.Errorf("vitess flavor: unresolved parts %+v", vk.unresolved)
	}
}

// TestRowWriterKeyCollationGuard_FunctionalKeys is the copy writer through
// every functional-part shape, from a MySQL `_bin` source and a Postgres
// default-collation source.
func TestRowWriterKeyCollationGuard_FunctionalKeys(t *testing.T) {
	n := 0
	cell := func(t *testing.T, ddl string) string {
		t.Helper()
		n++
		dsn, cleanup := newSharedDB(t, fmt.Sprintf("kcfn_%d", n))
		t.Cleanup(cleanup)
		applyDDL(t, dsn, ddl)
		return dsn
	}
	write := func(t *testing.T, eng Engine, dsn string, table *ir.Table, rows ...ir.Row) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rw, err := eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		ch := make(chan ir.Row, len(rows))
		for _, r := range rows {
			ch <- r
		}
		close(ch)
		return rw.(*RowWriter).WriteRowsIdempotent(ctx, table, ch)
	}
	cols := func(key ir.Type) []*ir.Column {
		return []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: key},
			{Name: "j", Type: ir.Integer{Width: 32}, Nullable: true},
		}
	}
	pk := &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
	const tgt = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL, j INT, "
	e, eAcute := ir.Row{"id": int64(1), "k": "e", "j": int64(1)}, ir.Row{"id": int64(2), "k": "é", "j": int64(2)}
	f := ir.Row{"id": int64(2), "k": "f", "j": int64(2)}

	for _, src := range []struct {
		name string
		key  ir.Type
	}{
		{"source _bin", kcgVarchar("utf8mb4_bin")},
		{"source postgres default", ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}},
	} {
		tbl := &ir.Table{Name: "kd", Columns: cols(src.key), PrimaryKey: pk}
		for _, flavor := range []struct {
			name string
			eng  Engine
		}{
			{"hidden column read", Engine{}},
			// The vtgate fallback: the part compares TRUE on the target side.
			{"hidden column unknown (vitess flavor)", Engine{Flavor: FlavorVitess}},
		} {
			t.Run(src.name+"/"+flavor.name, func(t *testing.T) {
				for _, shape := range []string{
					"UNIQUE KEY fk ((lower(cast(k as char charset utf8mb4))))",
					"UNIQUE KEY fk ((upper(k)), j)",
					"UNIQUE KEY fk ((trim(k)))",
				} {
					t.Run(shape+"/merge refuses", func(t *testing.T) {
						dsn := cell(t, tgt+shape+")")
						if err := write(t, flavor.eng, dsn, tbl, e); err != nil {
							t.Fatalf("seed: %v", err)
						}
						row := eAcute
						if strings.Contains(shape, ", j") {
							row = ir.Row{"id": int64(2), "k": "é", "j": int64(1)}
						}
						err := write(t, flavor.eng, dsn, tbl, row)
						if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), "KEY-COLLATION-MERGE") {
							t.Fatalf("err = %v; want KEY-COLLATION-MERGE (target %s)", err, kcgRows(t, dsn, "kd", "id"))
						}
						if got := kcgRows(t, dsn, "kd", "id"); got != "1:65" {
							t.Fatalf("after the refusal the target is %s; want 1:65", got)
						}
					})
					t.Run(shape+"/no merge lands and a re-sent row passes", func(t *testing.T) {
						dsn := cell(t, tgt+shape+")")
						for i := 0; i < 2; i++ { // the second pass is a resumed chunk re-sending both rows
							if err := write(t, flavor.eng, dsn, tbl, e, f); err != nil {
								t.Fatalf("pass %d: %v", i, err)
							}
						}
						if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:66" {
							t.Fatalf("target %s; want 1:65,2:66", got)
						}
					})
				}
				// Review F4: an unreadable part compares per row (TRUE on the
				// target, the columns' bytes on the source) and never refuses
				// the table; a non-string one is left out.
				t.Run("opaque collated part copies what merges nothing", func(t *testing.T) {
					dsn := cell(t, tgt+"UNIQUE KEY fk ((md5(k))))")
					for i := 0; i < 2; i++ {
						if err := write(t, flavor.eng, dsn, tbl, e, eAcute); err != nil {
							t.Fatalf("pass %d: %v", i, err)
						}
					}
					if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:C3A9" {
						t.Fatalf("target %s; want 1:65,2:C3A9", got)
					}
				})
				t.Run("opaque non-string part is left out", func(t *testing.T) {
					dsn := cell(t, tgt+"UNIQUE KEY fk ((j * 2)), UNIQUE KEY uk (k))")
					if err := write(t, flavor.eng, dsn, tbl, e, f); err != nil {
						t.Fatalf("a non-string functional part refused: %v", err)
					}
					if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:66" {
						t.Fatalf("target %s; want 1:65,2:66", got)
					}
				})
			})
		}
	}
}

func fmtErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestRowWriterKeyCollationGuard_FunctionalKeysReview is the independent
// review's cells (2026-10-15), each graded by the target's rows as HEX:
//
//   - F1: the key's expression spells the column in another case than the
//     column (the catalog keeps the DDL's spelling);
//   - F2: a column whose collation the source shares (unarmed) under a key
//     whose CAST compares it under another collation;
//   - F4: an opaque collated part (`md5(k)`) on an armed table must not
//     refuse a copy that merges nothing, a non-string part must not refuse
//     where its hidden column cannot be read, and an index name longer than
//     the 52 characters a hidden column's name keeps must still be guarded;
//   - F5: the remedy's collation must keep a trailing-space pair apart.
func TestRowWriterKeyCollationGuard_FunctionalKeysReview(t *testing.T) {
	n := 0
	cell := func(t *testing.T, ddl string) string {
		t.Helper()
		n++
		dsn, cleanup := newSharedDB(t, fmt.Sprintf("kcfr_%d", n))
		t.Cleanup(cleanup)
		applyDDL(t, dsn, ddl)
		return dsn
	}
	write := func(t *testing.T, eng Engine, dsn string, table *ir.Table, rows ...ir.Row) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rw, err := eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		ch := make(chan ir.Row, len(rows))
		for _, r := range rows {
			ch <- r
		}
		close(ch)
		return rw.(*RowWriter).WriteRowsIdempotent(ctx, table, ch)
	}
	table := func(key ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: key},
			{Name: "j", Type: ir.Integer{Width: 32}, Nullable: true},
		}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}}
	}
	pgDefault := table(ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic})
	srcBin := table(kcgVarchar("utf8mb4_bin"))
	row := func(id int64, k string) ir.Row { return ir.Row{"id": id, "k": k, "j": id} }
	wantMerge := func(t *testing.T, err error, dsn string) {
		t.Helper()
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(fmtErr(err), keyCollationMergeMarker) ||
			strings.Contains(fmtErr(err), keyCollationGuardUnsupportedMarker) {
			t.Fatalf("err = %v; want KEY-COLLATION-MERGE (target %s)", err, kcgRows(t, dsn, "kd", "id"))
		}
		if got := kcgRows(t, dsn, "kd", "id"); got != "1:65" {
			t.Fatalf("after the refusal the target is %s; want 1:65 (nothing merged)", got)
		}
	}
	const ci = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, j INT, "
	const bin = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, j INT, "

	for _, flavor := range []struct {
		name string
		eng  Engine
	}{{"hidden column read", Engine{}}, {"hidden column unknown (vitess flavor)", Engine{Flavor: FlavorVitess}}} {
		t.Run(flavor.name, func(t *testing.T) {
			t.Run("F1 mixed-case expression identifier", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY fk ((lower(cast(K as char charset utf8mb4)))))")
				if err := write(t, flavor.eng, dsn, pgDefault, row(1, "e")); err != nil {
					t.Fatal(err)
				}
				wantMerge(t, write(t, flavor.eng, dsn, pgDefault, row(2, "é")), dsn)
			})
			t.Run("F2 unarmed column under a CAST-collated key", func(t *testing.T) {
				dsn := cell(t, bin+"UNIQUE KEY fk ((lower(cast(k as char charset utf8mb4)))))")
				if err := write(t, flavor.eng, dsn, srcBin, row(1, "e")); err != nil {
					t.Fatal(err)
				}
				wantMerge(t, write(t, flavor.eng, dsn, srcBin, row(2, "é")), dsn)
			})
			t.Run("F4 md5 key on an armed table copies what merges nothing", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY fk ((md5(k))))")
				for i := 0; i < 2; i++ { // the second pass re-sends both rows
					if err := write(t, flavor.eng, dsn, pgDefault, row(1, "e"), row(2, "é")); err != nil {
						t.Fatalf("pass %d: %v", i, err)
					}
				}
				if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:C3A9" {
					t.Fatalf("target %s; want 1:65,2:C3A9", got)
				}
			})
			t.Run("F4 non-string opaque part copies", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY fk ((j * 2)), UNIQUE KEY uk (id, k))")
				if err := write(t, flavor.eng, dsn, pgDefault, row(1, "e"), row(2, "f")); err != nil {
					t.Fatal(err)
				}
				if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:66" {
					t.Fatalf("target %s; want 1:65,2:66", got)
				}
			})
			t.Run("F4 index name longer than a hidden column keeps", func(t *testing.T) {
				long := strings.Repeat("functional_key_", 4) // 60 characters
				dsn := cell(t, ci+"UNIQUE KEY "+long+" ((lower(k))))")
				if err := write(t, flavor.eng, dsn, pgDefault, row(1, "e")); err != nil {
					t.Fatal(err)
				}
				wantMerge(t, write(t, flavor.eng, dsn, pgDefault, row(2, "é")), dsn)
			})
			t.Run("F5 the remedy keeps a trailing-space pair apart", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY fk ((lower(cast(k as char charset utf8mb4)))))")
				if err := write(t, flavor.eng, dsn, pgDefault, row(1, "e")); err != nil {
					t.Fatal(err)
				}
				err := write(t, flavor.eng, dsn, pgDefault, row(2, "é"))
				wantMerge(t, err, dsn)
				// Apply the statement the refusal names, verbatim.
				msg := fmtErr(err)
				start := strings.Index(msg, "DROP INDEX `fk`, ADD UNIQUE KEY `fk` (")
				if start < 0 {
					t.Fatalf("the refusal names no statement to recreate the key: %s", msg)
				}
				end := strings.Index(msg[start:], ")`")
				if end < 0 {
					t.Fatalf("the refusal names no statement to recreate the key: %s", msg)
				}
				applyDDL(t, dsn, "ALTER TABLE kd "+msg[start:start+end+1])
				if err := write(t, flavor.eng, dsn, pgDefault, row(2, "é"), row(3, "e ")); err != nil {
					t.Fatalf("after the remedy: %v", err)
				}
				if got := kcgRows(t, dsn, "kd", "id"); got != "1:65,2:C3A9,3:6520" {
					t.Fatalf("after the remedy the target is %s; want 1:65,2:C3A9,3:6520", got)
				}
			})
		})
	}
}

// TestRowWriterKeyCollationGuard_FunctionalKeysReReview is the re-review's
// cells (2026-10-15, second pass), graded by the target's rows as HEX:
//
//   - a functional part over a GENERATED column, which no insert carries:
//     same-engine it must copy (it refused the table), cross-engine it must
//     still refuse a merge, through the generated column's base columns;
//   - MySQL → MySQL with the source's own key: a re-copy and a stale
//     respelled row must not refuse where the key is compared alike on both
//     sides (where the hidden column cannot be read the guard assumes it may
//     differ, and the respelling refuses: the stated cost);
//   - a merge through UNIQUE (j, (lower(k))) and UNIQUE ((j*2), (lower(k)));
//   - the long-name discriminator: a conflict through ANOTHER unique key
//     (uk on j) with the functional key's values apart must proceed when
//     the long name's hidden column is matched, and refuses when it is not
//     known — so dropping the prefix match turns the first red.
func TestRowWriterKeyCollationGuard_FunctionalKeysReReview(t *testing.T) {
	n := 0
	cell := func(t *testing.T, ddl string) string {
		t.Helper()
		n++
		dsn, cleanup := newSharedDB(t, fmt.Sprintf("kcfrr_%d", n))
		t.Cleanup(cleanup)
		applyDDL(t, dsn, ddl)
		return dsn
	}
	write := func(t *testing.T, eng Engine, dsn string, table *ir.Table, rows ...ir.Row) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		rw, err := eng.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer func() { _ = rw.(*RowWriter).Close() }()
		ch := make(chan ir.Row, len(rows))
		for _, r := range rows {
			ch <- r
		}
		close(ch)
		return rw.(*RowWriter).WriteRowsIdempotent(ctx, table, ch)
	}
	wantMerge := func(t *testing.T, err error, dsn, want string) {
		t.Helper()
		if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(fmtErr(err), keyCollationMergeMarker) ||
			strings.Contains(fmtErr(err), keyCollationGuardUnsupportedMarker) {
			t.Fatalf("err = %v; want KEY-COLLATION-MERGE (target %s)", err, kcgRows(t, dsn, "kd", "id"))
		}
		if got := kcgRows(t, dsn, "kd", "id"); got != want {
			t.Fatalf("after the refusal the target is %s; want %s (nothing merged)", got, want)
		}
	}
	wantRows := func(t *testing.T, err error, dsn, want string) {
		t.Helper()
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if got := kcgRows(t, dsn, "kd", "id"); got != want {
			t.Fatalf("target %s; want %s", got, want)
		}
	}
	pgVC := ir.Varchar{Length: 20, Determinism: ir.CollationDeterministic}
	myVC := kcgVarchar("utf8mb4_0900_ai_ci")
	pk := &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
	kj := func(key ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", PrimaryKey: pk, Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: key}, {Name: "j", Type: ir.Integer{Width: 32}, Nullable: true},
		}}
	}
	gen := func(key ir.Type) *ir.Table {
		return &ir.Table{Name: "kd", PrimaryKey: pk, Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "k", Type: key},
			{Name: "b", Type: key},
			{Name: "g", Type: key, GeneratedExpr: "concat(k, b)", GeneratedStored: true, Nullable: true},
		}}
	}
	row := func(id int64, k string, j int64) ir.Row { return ir.Row{"id": id, "k": k, "j": j} }
	grow := func(id int64, k, b string) ir.Row { return ir.Row{"id": id, "k": k, "b": b} }
	const ci = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, j INT, "
	const genDDL = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, " +
		"b VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, " +
		"g VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci AS (concat(k, b)) STORED, UNIQUE KEY fk ((lower(g))))"
	long := strings.Repeat("functional_key_", 4)

	for _, flavor := range []struct {
		name    string
		eng     Engine
		unknown bool
	}{{"hidden column read", Engine{}, false}, {"hidden column unknown (vitess flavor)", Engine{Flavor: FlavorVitess}, true}} {
		t.Run(flavor.name, func(t *testing.T) {
			t.Run("generated column, same engine, copies and re-copies", func(t *testing.T) {
				dsn := cell(t, genDDL)
				for i := 0; i < 2; i++ {
					wantRows(t, write(t, flavor.eng, dsn, gen(myVC), grow(1, "e", "x"), grow(2, "f", "x")), dsn, "1:65,2:66")
				}
			})
			t.Run("generated column, postgres source, merge refuses", func(t *testing.T) {
				dsn := cell(t, genDDL)
				wantRows(t, write(t, flavor.eng, dsn, gen(pgVC), grow(1, "e", "x")), dsn, "1:65")
				wantMerge(t, write(t, flavor.eng, dsn, gen(pgVC), grow(2, "é", "x")), dsn, "1:65")
				wantRows(t, write(t, flavor.eng, dsn, gen(pgVC), grow(3, "f", "x")), dsn, "1:65,3:66")
			})
			t.Run("mysql to mysql, the source's own key, re-copy and a stale respelling", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY fk ((lower(k))))")
				for i := 0; i < 2; i++ {
					wantRows(t, write(t, flavor.eng, dsn, kj(myVC), row(1, "a@x", 1), row(2, "b", 2)), dsn, "1:614078,2:62")
				}
				// The target holds row 3 'A@y'; the source has since moved it to
				// row 4 as 'a@y' (a delete the copy did not see): ODKU converges.
				wantRows(t, write(t, flavor.eng, dsn, kj(myVC), row(3, "A@y", 3)), dsn, "1:614078,2:62,3:414079")
				err := write(t, flavor.eng, dsn, kj(myVC), row(4, "a@y", 4))
				if flavor.unknown {
					// The stated cost: with the hidden column unread the guard
					// cannot tell the key is the source's, and refuses.
					wantMerge(t, err, dsn, "1:614078,2:62,3:414079")
					return
				}
				wantRows(t, err, dsn, "1:614078,2:62,3:614079")
			})
			for _, shape := range []string{"UNIQUE KEY fk (j, (lower(k)))", "UNIQUE KEY fk ((j * 2), (lower(k)))"} {
				t.Run(shape+" merge refuses", func(t *testing.T) {
					dsn := cell(t, ci+shape+")")
					wantRows(t, write(t, flavor.eng, dsn, kj(pgVC), row(1, "e", 5)), dsn, "1:65")
					wantMerge(t, write(t, flavor.eng, dsn, kj(pgVC), row(2, "é", 5)), dsn, "1:65")
					wantRows(t, write(t, flavor.eng, dsn, kj(pgVC), row(3, "é", 6)), dsn, "1:65,3:C3A9")
				})
			}
			t.Run("long key name, a conflict through another unique key", func(t *testing.T) {
				dsn := cell(t, ci+"UNIQUE KEY "+long+" ((lower(k))), UNIQUE KEY uk (j))")
				wantRows(t, write(t, flavor.eng, dsn, kj(pgVC), row(1, "e", 5)), dsn, "1:65")
				err := write(t, flavor.eng, dsn, kj(pgVC), row(2, "f", 5))
				if flavor.unknown {
					wantMerge(t, err, dsn, "1:65")
					return
				}
				// Matched: lower('e') and lower('f') differ, so the long key does
				// not match and the upsert proceeds through uk.
				wantRows(t, err, dsn, "1:66")
			})
		})
	}
}
