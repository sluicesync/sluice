//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The sample-mode row hash, family by family, on real Postgres — the
// sibling of the MySQL matrix of the same name.
//
// VERIFY-SAMPLE-COLUMN-ORDER's fix rewrote the per-column rendering the
// hash reads ([pgSampleToken]: NULL as 'N', every value length-prefixed).
// Postgres renders every type through one ::text path, so there is no
// family branch in sluice's own code — but per the Bug 74 rule the
// rendering the server produces differs by type, so the pin is still
// every family × {scalar, 1-D array, multi-dim array, NULL element}: each
// column gets one row where ONLY that column is corrupted on the twin
// table, and every such row must hash differently, while a byte-identical
// twin hashes equal on every row.
//
// Independent expected value: the corruption is written by this file, per
// row, so which rows must differ is known before any hash is computed.

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

type pgSampleFamily struct {
	col, ddl, base, corrupt string
}

var pgSampleFamilies = []pgSampleFamily{
	{"i", "integer", "7", "8"},
	{"bi", "bigint", "9223372036854775807", "9223372036854775806"},
	{"num", "numeric(10,2)", "1.50", "1.51"},
	{"f4", "real", "1.5", "1.25"},
	{"f4p", "real", "1234567", "1234568"}, // the MySQL FLOAT precision class, as a guard

	{"f8", "double precision", "0.1", "0.2"},
	{"bo", "boolean", "true", "false"},
	{"ch", "char(5)", "'ab'", "'ac'"},
	{"vc", "varchar(20)", "'hello'", "'hellp'"},
	{"tx", "text", "'text'", "'texu'"},
	{"uu", "uuid", "'00000000-0000-0000-0000-000000000001'", "'00000000-0000-0000-0000-000000000002'"},
	{"ip", "inet", "'10.0.0.1'", "'10.0.0.2'"},
	{"cd", "cidr", "'10.0.0.0/8'", "'10.0.0.0/16'"},
	{"mac", "macaddr", "'08:00:2b:01:02:03'", "'08:00:2b:01:02:04'"},
	{"jb", "jsonb", `'{"a":1}'`, `'{"a":2}'`},
	{"js", "json", `'{"a":1}'`, `'{"a":3}'`},
	{"dt", "date", "'2024-01-02'", "'2024-01-03'"},
	{"ts", "timestamp(6)", "'2024-01-02 03:04:05.123456'", "'2024-01-02 03:04:05.123457'"},
	{"tz", "timestamptz", "'2024-01-02 03:04:05+00'", "'2024-01-02 03:04:05+01'"},
	{"tm", "time", "'12:34:56.5'", "'12:34:56.6'"},
	{"iv", "interval", "'1 day'", "'2 days'"},
	{"by", "bytea", `'\xff00'`, `'\xfe00'`},
	{"ai", "integer[]", "'{1,2}'", "'{2,1}'"},
	{"a2", "integer[][]", "'{{1,2},{3,4}}'", "'{1,2,3,4}'"},
	{"an", "text[]", "'{a,NULL}'", `'{a,""}'`},
	{"tsa", "timestamp[]", `'{"2024-01-02 03:04:05"}'`, `'{"2024-01-02 03:04:06"}'`},
}

const (
	pgRowNullVsEmpty = 101 // t1 NULL on the source, '' on the twin
	pgRowSeparator   = 102 // ('a|', 'b') on the source, ('a', '|b') on the twin
	// t1 is the four characters `\101` on the source and `A` on the twin.
	// The --strict-hash path used to reach sha256() through a `::bytea`
	// cast, which parses `\101` as an octal escape for `A`, so under the
	// old rendering the two hashed equal. Two things now stop that, and
	// this row alone cannot tell them apart: CONVERT_TO hashes the
	// characters as they are, and the token's length prefix (4 vs 1)
	// differs even after an escape-parse. Re-introducing the cast is
	// caught LOUDLY by this matrix regardless — the bytea family renders
	// `\xff00` mid-string, which a bytea literal parse refuses
	// ("invalid input syntax for type bytea"), failing the SHA-256 leg.
	pgRowBackslash = 103
)

func TestSampleRowHashes_FamilyMatrix(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "vsfam")
	defer cleanup()

	cols := []string{"id"}
	defs := []string{"id bigint PRIMARY KEY"}
	for _, f := range pgSampleFamilies {
		cols = append(cols, f.col)
		defs = append(defs, f.col+" "+f.ddl)
	}
	cols = append(cols, "t1", "t2")
	defs = append(defs, "t1 text", "t2 text")

	insert := func(table string, id int, override map[string]string) string {
		vals := []string{fmt.Sprint(id)}
		for _, f := range pgSampleFamilies {
			v := f.base
			if o, ok := override[f.col]; ok {
				v = o
			}
			vals = append(vals, v)
		}
		for _, c := range []string{"t1", "t2"} {
			v := "'" + c + "'"
			if o, ok := override[c]; ok {
				v = o
			}
			vals = append(vals, v)
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);\n", table, strings.Join(cols, ", "), strings.Join(vals, ", "))
	}
	var script strings.Builder
	for _, table := range []string{"vs_src", "vs_same", "vs_bad"} {
		fmt.Fprintf(&script, "CREATE TABLE %s (%s);\n", table, strings.Join(defs, ", "))
	}
	for i, f := range pgSampleFamilies {
		id := i + 1
		script.WriteString(insert("vs_src", id, nil))
		script.WriteString(insert("vs_same", id, nil))
		script.WriteString(insert("vs_bad", id, map[string]string{f.col: f.corrupt}))
	}
	src := map[int]map[string]string{
		pgRowNullVsEmpty: {"t1": "NULL"},
		pgRowSeparator:   {"t1": "'a|'", "t2": "'b'"},
		pgRowBackslash:   {"t1": `'\101'`},
	}
	bad := map[int]map[string]string{
		pgRowNullVsEmpty: {"t1": "''"},
		pgRowSeparator:   {"t1": "'a'", "t2": "'|b'"},
		pgRowBackslash:   {"t1": "'A'"},
	}
	for _, id := range []int{pgRowNullVsEmpty, pgRowSeparator, pgRowBackslash} {
		script.WriteString(insert("vs_src", id, src[id]))
		script.WriteString(insert("vs_same", id, src[id]))
		script.WriteString(insert("vs_bad", id, bad[id]))
	}
	applyPGSQL(t, dsn, script.String())

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rdr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tables := map[string]*ir.Table{}
	for _, tb := range schema.Tables {
		tables[tb.Name] = tb
	}
	plan := ir.SampleColumns{Key: []string{"id"}, Hashed: cols}
	sv := rdr.(ir.SampleVerifier)

	for _, algo := range []ir.HashAlgorithm{ir.HashMD5, ir.HashSHA256} {
		t.Run(algo.String(), func(t *testing.T) {
			hash := func(table string) map[string]string {
				rows, err := sv.SampleRowHashes(ctx, tables[table], plan, 1000, 42, algo)
				if err != nil {
					t.Fatalf("SampleRowHashes %s: %v", table, err)
				}
				out := make(map[string]string, len(rows))
				for _, r := range rows {
					out[r.PrimaryKey] = r.Hash
				}
				return out
			}
			srcH, sameH, badH := hash("vs_src"), hash("vs_same"), hash("vs_bad")
			want := len(pgSampleFamilies) + 3
			if len(srcH) != want || len(sameH) != want || len(badH) != want {
				t.Fatalf("sampled %d/%d/%d rows; want %d each", len(srcH), len(sameH), len(badH), want)
			}
			for pk, h := range srcH {
				if sameH[pk] != h {
					t.Errorf("row %s: byte-identical twin hashes differently — false mismatch", pk)
				}
			}
			label := func(pk string) string {
				switch pk {
				case fmt.Sprint(pgRowNullVsEmpty):
					return "NULL vs ''"
				case fmt.Sprint(pgRowSeparator):
					return "separator borrow"
				case fmt.Sprint(pgRowBackslash):
					return `\101 vs A`
				}
				var id int
				_, _ = fmt.Sscan(pk, &id)
				return pgSampleFamilies[id-1].ddl + " (" + pgSampleFamilies[id-1].col + ")"
			}
			for pk, h := range srcH {
				if badH[pk] == h {
					t.Errorf("row %s [%s]: corrupted value hashes EQUAL to the source — false clean", pk, label(pk))
				}
			}
		})
	}
}

// TestSampleRowHashes_PlanColumnNotOnTableRefused pins the loud half of
// the column contract: a plan naming a column the table lacks is an error,
// never a silently narrower hash.
func TestSampleRowHashes_PlanColumnNotOnTableRefused(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "vsplan")
	defer cleanup()
	applyPGSQL(t, dsn, "CREATE TABLE kd (id bigint PRIMARY KEY, b text); INSERT INTO kd VALUES (1, 'x');")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rdr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := rdr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	sv := rdr.(ir.SampleVerifier)
	_, err = sv.SampleRowHashes(ctx, schema.Tables[0], ir.SampleColumns{Key: []string{"id"}, Hashed: []string{"b", "c"}}, 10, 1, ir.HashMD5)
	if err == nil || !strings.Contains(err.Error(), `column "c" is not on the table`) {
		t.Fatalf("err = %v; want the column-not-on-table refusal", err)
	}
}
