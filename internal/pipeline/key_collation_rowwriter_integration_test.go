//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// KEY-COLLATION-ROWWRITER end to end: every orchestrator path that copies
// through the idempotent row writer, driven with the REAL source schema
// reader and row reader, so the pin also binds the premise the writer's
// guard rests on — that the source's own IR carries the key's collation
// (a MySQL `_bin` charset/collation, a Postgres collation name or the
// default's determinism stamp). The engine packages pin the writer itself
// over every key shape and both MySQL write cores
// (row_writer_key_collation_integration_test.go there).
//
// Pairs: Postgres → MySQL onto the target table sluice itself creates for a
// default-collation text key (`utf8mb4_0900_ai_ci`, measured on a fresh
// migrate 2026-10-12 — no hand-set collation needed to arm it), Postgres
// NONDETERMINISTIC ICU keys (level 2 and level 3) → that same MySQL target,
// MySQL `_bin` → MySQL `_ci`, and Postgres "C" → a nondeterministic ICU key.
//
// The independent expected value is the source's own rows: the target is
// read back as (key bytes, value) and every row on it must be one the
// source holds, byte for byte. A merge lands ('A', 2) — a key from one
// source row with another's value — or drops a row.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// krwPath is one orchestrator copy path that writes through the idempotent
// row writer.
type krwPath struct {
	name string
	copy func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error
}

func krwPaths() []krwPath {
	return []krwPath{
		{"add-table", func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
			return copyTableIdempotent(ctx, rr, rw, table, nil, "")
		}},
		{"vstream-copy (serial idempotent cold start)", func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
			_, err := copyTableColdStartIdempotent(ctx, rr, rw, table, nil, ShardColumnSpec{})
			return err
		}},
		{"vstream-copy fan-out", func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
			_, err := copyTableColdStartIdempotentMaybeParallel(ctx, rr, rw, table, nil, ShardColumnSpec{}, 2)
			return err
		}},
		{"resume (per-batch cursor)", func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
			state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{table.Name: {State: ir.TableProgressInProgress}}}
			var mu sync.Mutex
			return copyTableWithCursor(ctx, resumeContext{}, state, &mu, rw, rr, table, 1000, nil, ShardColumnSpec{})
		}},
		{"chunked resume (idempotent chunk)", func(ctx context.Context, rr ir.RowReader, rw ir.RowWriter, table *ir.Table) error {
			chunk := ir.TableChunkProgress{ChunkIndex: 0, State: ir.TableProgressInProgress}
			state := &ir.MigrationState{TableProgress: map[string]ir.TableProgress{
				table.Name: {State: ir.TableProgressInProgress, Chunks: []ir.TableChunkProgress{chunk}},
			}}
			var mu sync.Mutex
			return copyChunk(ctx, resumeContext{}, state, &mu, rr, rw, table, migcore.PrimaryKeyColumnNames(table),
				0, 1000, nil, ShardColumnSpec{}, true /*resuming*/, false, false, ir.RawCopyFormat(0))
		}},
	}
}

// krwPair is one source → target pairing with its tables already created.
type krwPair struct {
	name           string
	src, tgt       ir.Engine
	srcDSN, tgtDSN string
	tgtDriver      string
	hexKey         string // the target's expression for the key's bytes, hex
}

// TestKeyCollationRowWriter_CopyPaths: on every pair and every path, a
// source holding 'A' and 'a' refuses KEY-COLLATION-MERGE and leaves no
// merged row; a source with no such pair copies byte-exact.
func TestKeyCollationRowWriter_CopyPaths(t *testing.T) {
	mysqlSrc, mysqlTgt, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	pgSrc, pgTgt, pgCleanup := startPostgres(t)
	defer pgCleanup()
	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	const seedRows = "('A', 1), ('a', 2), ('b', 3)"
	const okRows = "('A', 1), ('b', 2), ('c', 3)"
	applyPGDDL(t, pgSrc, "CREATE TABLE kpm (k varchar(20) PRIMARY KEY, v int); INSERT INTO kpm VALUES "+seedRows+";"+
		"CREATE TABLE okpm (k varchar(20) PRIMARY KEY, v int); INSERT INTO okpm VALUES "+okRows+";"+
		`CREATE TABLE kpp (k text COLLATE "C" PRIMARY KEY, v int); INSERT INTO kpp VALUES `+seedRows+";"+
		`CREATE TABLE okpp (k text COLLATE "C" PRIMARY KEY, v int); INSERT INTO okpp VALUES `+okRows+";")
	applyMySQLDDL(t, mysqlSrc, "CREATE TABLE kmm (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, v INT); INSERT INTO kmm VALUES "+seedRows+";"+
		"CREATE TABLE okmm (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, v INT); INSERT INTO okmm VALUES "+okRows+";")
	// The target tables. kpm/okpm are exactly what a sluice migrate creates
	// on MySQL for a Postgres varchar key (the table's default collation).
	applyMySQLDDL(t, mysqlTgt, "CREATE TABLE kpm (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE okpm (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE kmm (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY, v INT);"+
		"CREATE TABLE okmm (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY, v INT);")
	applyPGDDL(t, pgTgt, "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);"+
		"CREATE TABLE kpp (k text COLLATE ci PRIMARY KEY, v int); CREATE TABLE okpp (k text COLLATE ci PRIMARY KEY, v int);")

	// Review 2026-10-12 finding 1: a NONDETERMINISTIC Postgres key. The MySQL
	// emitter cannot declare an ICU collation and creates the key under the
	// target default, `utf8mb4_0900_ai_ci`, which equates what the source
	// keeps apart: 'e'/'é' at level 2 (accent-sensitive, case-insensitive),
	// 'a'/'A' at level 3. Before the fix the guard left these keys unarmed and
	// the second source row merged into the first at exit 0. The copy arms
	// them in the primary key and in a UNIQUE key beside one.
	applyPGDDL(t, pgSrc, "CREATE COLLATION l2 (provider = icu, locale = 'und-u-ks-level2', deterministic = false);"+
		"CREATE COLLATION l3 (provider = icu, locale = 'und-u-ks-level3', deterministic = false);"+
		"CREATE TABLE kn2 (k varchar(20) COLLATE l2 PRIMARY KEY, v int); INSERT INTO kn2 VALUES ('e', 1), ('é', 2), ('b', 3);"+
		"CREATE TABLE okn2 (k varchar(20) COLLATE l2 PRIMARY KEY, v int); INSERT INTO okn2 VALUES ('e', 1), ('b', 2), ('c', 3);"+
		"CREATE TABLE kn3 (k varchar(20) COLLATE l3 PRIMARY KEY, v int); INSERT INTO kn3 VALUES "+seedRows+";"+
		"CREATE TABLE okn3 (k varchar(20) COLLATE l3 PRIMARY KEY, v int); INSERT INTO okn3 VALUES "+okRows+";"+
		"CREATE TABLE kn2u (id int PRIMARY KEY, k varchar(20) COLLATE l2 NOT NULL UNIQUE, v int); INSERT INTO kn2u VALUES (1, 'e', 1), (2, 'é', 2), (3, 'b', 3);"+
		"CREATE TABLE okn2u (id int PRIMARY KEY, k varchar(20) COLLATE l2 NOT NULL UNIQUE, v int); INSERT INTO okn2u VALUES (1, 'e', 1), (2, 'b', 2), (3, 'c', 3);")
	applyMySQLDDL(t, mysqlTgt, "CREATE TABLE kn2 (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE okn2 (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE kn3 (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE okn3 (k varchar(20) NOT NULL PRIMARY KEY, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE kn2u (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"+
		"CREATE TABLE okn2u (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, v int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")

	pairs := []struct {
		krwPair
		merge, ok string
	}{
		{krwPair{"postgres default → mysql (sluice-created target)", pgEng, mysqlEng, pgSrc, mysqlTgt, "mysql", "HEX(k)"}, "kpm", "okpm"},
		{krwPair{"postgres ICU level2 primary key → mysql 0900_ai_ci", pgEng, mysqlEng, pgSrc, mysqlTgt, "mysql", "HEX(k)"}, "kn2", "okn2"},
		{krwPair{"postgres ICU level3 primary key → mysql 0900_ai_ci", pgEng, mysqlEng, pgSrc, mysqlTgt, "mysql", "HEX(k)"}, "kn3", "okn3"},
		{krwPair{"postgres ICU level2 unique beside a primary key → mysql 0900_ai_ci", pgEng, mysqlEng, pgSrc, mysqlTgt, "mysql", "HEX(k)"}, "kn2u", "okn2u"},
		{krwPair{"mysql _bin → mysql _ci", mysqlEng, mysqlEng, mysqlSrc, mysqlTgt, "mysql", "HEX(k)"}, "kmm", "okmm"},
		{krwPair{"postgres C → postgres nondeterministic", pgEng, pgEng, pgSrc, pgTgt, "pgx", "upper(encode(convert_to(k, 'UTF8'), 'hex'))"}, "kpp", "okpp"},
	}
	for _, p := range pairs {
		for _, path := range krwPaths() {
			p, path := p, path
			t.Run(p.name+"/"+path.name, func(t *testing.T) {
				// Only the MySQL writer fans out; elsewhere the fan-out path
				// falls back to the serial copy, which is still a path.
				src := krwSourceRows(t, p.krwPair, p.merge)
				err := krwCopy(t, p.krwPair, p.merge, path)
				got := krwTargetRows(t, p.krwPair, p.merge)
				if !errors.Is(err, ir.ErrKeyCollationMerge) {
					t.Fatalf("err = %v; want %s (the target holds %v)", err, ir.ErrKeyCollationMerge, got)
				}
				for _, r := range got {
					if !src[r] {
						t.Fatalf("the target holds %s, which is no source row (source %v)", r, src)
					}
				}

				want := krwSourceRows(t, p.krwPair, p.ok)
				if err := krwCopy(t, p.krwPair, p.ok, path); err != nil {
					t.Fatalf("a copy with no merge refused: %v", err)
				}
				gotOK := krwTargetRows(t, p.krwPair, p.ok)
				if len(gotOK) != len(want) {
					t.Fatalf("target %v; want %v", gotOK, want)
				}
				for _, r := range gotOK {
					if !want[r] {
						t.Fatalf("target %v; want %v", gotOK, want)
					}
				}
			})
		}
	}
}

// krwCopy empties the target table and copies the source's into it through
// path.
func krwCopy(t *testing.T, p krwPair, table string, path krwPath) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	krwExec(t, p.tgtDriver, p.tgtDSN, "DELETE FROM "+table)
	sr, err := p.src.OpenSchemaReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := findTable(schema, table)
	if tbl == nil {
		t.Fatalf("source table %s missing", table)
	}
	rr, err := p.src.OpenRowReader(ctx, p.srcDSN)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer migcore.CloseIf(rr)
	rw, err := p.tgt.OpenRowWriter(ctx, p.tgtDSN)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer migcore.CloseIf(rw)
	if p.tgt.Name() == "postgres" {
		// One row per statement: Postgres refuses two rows of ONE
		// statement that conflict with each other (cardinality_violation,
		// loud without the guard), so the merge the guard exists for is a
		// row meeting one an earlier statement landed. The MySQL pairs keep
		// the multi-row statement — the other shape.
		rw.(ir.MaxBufferBytesSetter).SetMaxBufferBytes(1)
	}
	return path.copy(ctx, rr, rw, tbl)
}

// krwSourceRows is the source's rows as "<HEX(key)>:<v>".
func krwSourceRows(t *testing.T, p krwPair, table string) map[string]bool {
	t.Helper()
	driver, hexKey := "mysql", "HEX(k)"
	if p.src.Name() == "postgres" {
		driver, hexKey = "pgx", "upper(encode(convert_to(k, 'UTF8'), 'hex'))"
	}
	out := map[string]bool{}
	for _, r := range krwRows(t, driver, p.srcDSN, hexKey, table) {
		out[r] = true
	}
	return out
}

func krwTargetRows(t *testing.T, p krwPair, table string) []string {
	t.Helper()
	return krwRows(t, p.tgtDriver, p.tgtDSN, p.hexKey, table)
}

func krwRows(t *testing.T, driver, dsn, hexKey, table string) []string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(fmt.Sprintf("SELECT %s, v FROM %s", hexKey, table))
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var k string
		var v int
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s:%d", strings.ToUpper(k), v))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

func krwExec(t *testing.T, driver, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}
