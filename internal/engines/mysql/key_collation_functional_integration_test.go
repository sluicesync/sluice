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
		"UNIQUE KEY md ((md5(k))), UNIQUE KEY nm ((j * 2)), UNIQUE KEY tb ((trim(both 'x' from k))))")
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
	opaque := map[string]opaqueKeyPart{}
	for _, o := range keys.opaque {
		opaque[o.index] = o
	}
	for idx, wantColl := range map[string]string{
		"lo": "utf8mb4_0900_ai_ci", "up": "utf8mb4_0900_ai_ci", "lc": "utf8mb4_0900_ai_ci", "tr": "utf8mb4_0900_ai_ci",
		"lr": "utf8mb4_0900_ai_ci", "cs": "utf8mb4_0900_ai_ci", "cn": "utf8mb4_0900_ai_ci", "cl": "utf8mb4_bin",
		"bc": "utf8mb4_bin", "l1": "latin1_swedish_ci", "bn": "", "mix": "utf8mb4_0900_ai_ci",
	} {
		f := read[idx]
		if f == nil {
			t.Errorf("key %s: not read through (opaque: %+v)", idx, opaque[idx])
			continue
		}
		if f.column != "k" || !f.known || f.collation != wantColl {
			t.Errorf("key %s (%s): column %q collation %q known %v; want k, %q, known", idx, f.expr, f.column, f.collation, f.known, wantColl)
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
	for idx, refuses := range map[string]bool{"md": true, "nm": false, "tb": true} {
		o, ok := opaque[idx]
		if !ok {
			t.Errorf("key %s: read through; want opaque", idx)
			continue
		}
		if got := !o.known || o.collation != ""; got != refuses {
			t.Errorf("key %s (%s): refuses %v (collation %q known %v); want %v", idx, o.expr, got, o.collation, o.known, refuses)
		}
	}
	if len(keys.refusesOpaque()) != 2 {
		t.Errorf("refusing opaque parts %+v; want md and tb", keys.refusesOpaque())
	}

	// The Vitess flavors never read the hidden columns (vtgate does not parse
	// SHOW EXTENDED): every part is unknown, so it refuses or compares TRUE.
	vk, err := uniqueKeyColumns(ctx, db, FlavorVitess, "", "kx")
	if err != nil {
		t.Fatal(err)
	}
	for _, fns := range vk.uniqueFns {
		for _, f := range fns {
			if f != nil && f.known {
				t.Errorf("vitess flavor: key %s claims a known collation %q", f.index, f.collation)
			}
		}
	}
	if len(vk.refusesOpaque()) != 3 {
		t.Errorf("vitess flavor: refusing opaque parts %+v; want all three", vk.refusesOpaque())
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
				t.Run("opaque collated part refuses before any row", func(t *testing.T) {
					dsn := cell(t, tgt+"UNIQUE KEY fk ((md5(k))))")
					err := write(t, flavor.eng, dsn, tbl, e, f)
					if !errors.Is(err, ir.ErrKeyCollationMerge) || !strings.Contains(err.Error(), keyCollationGuardUnsupportedMarker) ||
						!strings.Contains(err.Error(), "md5") {
						t.Fatalf("err = %v; want %s naming the key's expression", err, keyCollationGuardUnsupportedMarker)
					}
					if got := kcgRows(t, dsn, "kd", "id"); got != "" {
						t.Fatalf("the refusal wrote rows: %s", got)
					}
				})
				t.Run("opaque non-string part is left out", func(t *testing.T) {
					dsn := cell(t, tgt+"UNIQUE KEY fk ((j * 2)), UNIQUE KEY uk (k))")
					err := write(t, flavor.eng, dsn, tbl, e, f)
					if flavor.eng.Flavor == FlavorVitess {
						// Unknown: the part may be a string, so it refuses.
						if !strings.Contains(fmtErr(err), keyCollationGuardUnsupportedMarker) {
							t.Fatalf("err = %v; want %s", err, keyCollationGuardUnsupportedMarker)
						}
						return
					}
					if err != nil {
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
