//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end validation of the flat-file sources (ADR-0163) against REAL
// targets: the full `migrate` path (stage → schema → auto-infer promotions →
// bulk copy) from csv/tsv/ndjson fixtures into Postgres AND MySQL, with
// every landed cell ground-truthed through the target's own SQL rendering
// (::text / CAST), plus the verify count-depth leg. The quoting/escape/NULL
// pin matrix itself is unit-pinned (csv_test.go / ndjson_test.go); this
// suite pins that the SAME corpus survives the cross-engine product path —
// including the auto-engaged --infer-types promotions (timestamp/jsonb/
// uuid) and the kept-safe non-conforming column.
package flatfile

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"

	// Target engines self-register for the cross-engine legs.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	itMySQLImage  = "ghcr.io/sluicesync/sluice-mysql:8.0-prebaked"
	itPGImage     = "ghcr.io/sluicesync/sluice-postgres:16-prebaked"
	itBootTimeout = 4 * time.Minute
)

// fixtureCSV is the value corpus: every quoting/escape shape from the unit
// matrix PLUS the infer-types columns — created_at (ISO, promotes to naive
// timestamp), synced_at (the Postgres `COPY … CSV` timestamptz rendering:
// space separator + 2-digit UTC offset, with and without a fraction — the
// F2 flagship input, promotes to timestamptz), padded_at (zoned values with
// a trailing space / tab — the audit-HIGH-2 shape: a whitespace tail must
// not hide the zone from classification, which pre-fix resolved the column
// NAIVE and silently UTC-shifted every wall clock), meta_json (objects,
// promotes to jsonb), user_uuid (promotes to uuid), customer_id (the cus_*
// case: hinted but non-conforming — MUST stay text), big/dec (exact digit
// text in a plain text column).
const fixtureCSV = "id,name,created_at,synced_at,padded_at,meta_json,user_uuid,customer_id,big,dec\n" +
	"1,\"comma, \"\"quote\"\", and\nnewline\",2024-01-02 03:04:05,2026-07-15 08:09:10.123456+00,\"2026-07-15 08:09:10.123456+05:30 \",\"{\"\"a\"\":1}\",6dfa5e5a-2b64-4c5e-9f6a-0a2b3c4d5e6f,cus_abc123,9007199254740993,007.1500\n" +
	"2,héllo 🚀,\\N,2026-07-14 09:08:09+02,\"2026-07-14 09:08:09+02\t\",\\N,0e984725-c51c-4bf4-9960-e1c80e27aba0,cus_def456,18446744073709551615,-0.000\n"

func writeFixtureIT(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func csvEngineIT(t *testing.T) ir.Engine {
	t.Helper()
	base, ok := engines.Get("csv")
	if !ok {
		t.Fatal("csv engine not registered")
	}
	null := `\N`
	e, err := base.(Engine).WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, NullRepr: &null})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func runMigrateIT(t *testing.T, source, target ir.Engine, srcDSN, tgtDSN string) {
	t.Helper()
	mig := &pipeline.Migrator{
		Source:     source,
		Target:     target,
		SourceDSN:  srcDSN,
		TargetDSN:  tgtDSN,
		InferTypes: true, // what the CLI auto-engages for flat-file sources
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run(%s → %s): %v", source.Name(), target.Name(), err)
	}
}

func startPostgresIT(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), itBootTimeout)
	defer cancel()
	container, err := pgtc.Run(
		ctx,
		itPGImage,
		pgtc.WithDatabase("postgres"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		testcontainers.WithWaitStrategyAndDeadline(
			itBootTimeout,
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections"),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		t.Fatalf("postgres boot: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	})
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}
	return dsn
}

func startMySQLIT(t *testing.T) string {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), itBootTimeout)
	defer cancel()
	container, err := mysqltc.Run(
		ctx,
		itMySQLImage,
		mysqltc.WithDatabase("flat"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
		testcontainers.WithWaitStrategyAndDeadline(
			itBootTimeout,
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(itBootTimeout),
		),
	)
	if err != nil {
		t.Fatalf("mysql boot: %v", err)
	}
	t.Cleanup(func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	})
	dsn, err := container.ConnectionString(context.Background(), "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("mysql connection string: %v", err)
	}
	// The pre-baked image skips first-boot init, so MYSQL_DATABASE is inert —
	// create the target database explicitly.
	db, err := sql.Open("mysql", strings.Replace(dsn, "/flat?", "/mysql?", 1))
	if err != nil {
		t.Fatalf("open admin conn: %v", err)
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS flat"); err != nil {
		_ = db.Close()
		t.Fatalf("create flat: %v", err)
	}
	_ = db.Close()
	return dsn
}

// queryText scans a single nullable-text cell.
func queryText(t *testing.T, db *sql.DB, q string, args ...any) *string {
	t.Helper()
	var v *string
	if err := db.QueryRow(q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

func wantCell(t *testing.T, got *string, want string, label string) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s = NULL; want %q", label, want)
	}
	if *got != want {
		t.Errorf("%s = %q; want %q", label, *got, want)
	}
}

func TestIntegration_CSVToPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pgDSN := startPostgresIT(t)
	src := writeFixtureIT(t, "people.csv", fixtureCSV)
	source := csvEngineIT(t)
	pg, _ := engines.Get("postgres")

	runMigrateIT(t, source, pg, src, pgDSN)

	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Promotions: created_at → naive timestamp, meta_json → jsonb,
	// user_uuid → uuid; the non-conforming customer_id stays text, and the
	// un-hinted digit columns stay text (exact digits, no numeric coercion).
	typeOf := func(col string) string {
		v := queryText(t, db,
			`SELECT data_type FROM information_schema.columns WHERE table_name = 'people' AND column_name = $1`, col)
		if v == nil {
			t.Fatalf("column %q missing on target", col)
		}
		return *v
	}
	if got := typeOf("created_at"); got != "timestamp without time zone" {
		t.Errorf("created_at type = %q; want naive timestamp", got)
	}
	if got := typeOf("synced_at"); got != "timestamp with time zone" {
		t.Errorf("synced_at type = %q; want timestamptz (the PG-COPY zoned rendering must classify zoned)", got)
	}
	if got := typeOf("padded_at"); got != "timestamp with time zone" {
		t.Errorf("padded_at type = %q; want timestamptz (a whitespace tail must not hide the zone — audit HIGH-2)", got)
	}
	if got := typeOf("meta_json"); got != "jsonb" {
		t.Errorf("meta_json type = %q; want jsonb", got)
	}
	if got := typeOf("user_uuid"); got != "uuid" {
		t.Errorf("user_uuid type = %q; want uuid", got)
	}
	for _, col := range []string{"customer_id", "big", "dec", "name"} {
		if got := typeOf(col); got != "text" {
			t.Errorf("%s type = %q; want text", col, got)
		}
	}

	// Values, rendered by Postgres itself.
	wantCell(t, queryText(t, db, `SELECT name FROM people WHERE id = '1'`),
		"comma, \"quote\", and\nnewline", "row1 name")
	wantCell(t, queryText(t, db, `SELECT created_at::text FROM people WHERE id = '1'`),
		"2024-01-02 03:04:05", "row1 created_at")
	// Instant-exact through the F2 path: rendered in UTC regardless of the
	// server TimeZone setting.
	wantCell(t, queryText(t, db, `SELECT to_char(synced_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM people WHERE id = '1'`),
		"2026-07-15 08:09:10.123456", "row1 synced_at (PG-COPY +00 rendering)")
	wantCell(t, queryText(t, db, `SELECT to_char(synced_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM people WHERE id = '2'`),
		"2026-07-14 07:08:09.000000", "row2 synced_at (+02 offset applied, not stripped)")
	// The padded column: the stored instant must equal the source wall clock
	// at its offset — the pre-fix failure resolved this column NAIVE and stored
	// the offset-shifted clock (row1 would read 02:39:10 as a naive wall time).
	wantCell(t, queryText(t, db, `SELECT to_char(padded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM people WHERE id = '1'`),
		"2026-07-15 02:39:10.123456", "row1 padded_at (trailing space; +05:30 applied, instant exact)")
	wantCell(t, queryText(t, db, `SELECT to_char(padded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') FROM people WHERE id = '2'`),
		"2026-07-14 07:08:09.000000", "row2 padded_at (trailing tab; +02 applied)")
	wantCell(t, queryText(t, db, `SELECT meta_json::text FROM people WHERE id = '1'`),
		`{"a": 1}`, "row1 meta_json (jsonb-normalized)")
	wantCell(t, queryText(t, db, `SELECT user_uuid::text FROM people WHERE id = '1'`),
		"6dfa5e5a-2b64-4c5e-9f6a-0a2b3c4d5e6f", "row1 user_uuid")
	wantCell(t, queryText(t, db, `SELECT customer_id FROM people WHERE id = '1'`),
		"cus_abc123", "row1 customer_id")
	wantCell(t, queryText(t, db, `SELECT big FROM people WHERE id = '1'`),
		"9007199254740993", "row1 big (2^53+1 exact text)")
	wantCell(t, queryText(t, db, `SELECT dec FROM people WHERE id = '1'`),
		"007.1500", "row1 dec (leading/trailing zeros)")

	wantCell(t, queryText(t, db, `SELECT name FROM people WHERE id = '2'`), "héllo 🚀", "row2 name")
	if v := queryText(t, db, `SELECT created_at::text FROM people WHERE id = '2'`); v != nil {
		t.Errorf("row2 created_at = %q; want NULL (the \\N repr)", *v)
	}
	if v := queryText(t, db, `SELECT meta_json::text FROM people WHERE id = '2'`); v != nil {
		t.Errorf("row2 meta_json = %q; want NULL", *v)
	}
	wantCell(t, queryText(t, db, `SELECT big FROM people WHERE id = '2'`),
		"18446744073709551615", "row2 big (uint64 max exact text)")
	wantCell(t, queryText(t, db, `SELECT dec FROM people WHERE id = '2'`), "-0.000", "row2 dec")

	// verify --depth count over the same pair (the ir.Verifier surface).
	verifier := &pipeline.Verifier{
		Source:    source,
		Target:    pg,
		SourceDSN: src,
		TargetDSN: pgDSN,
		Depth:     pipeline.VerifyDepthCount,
		Out:       io.Discard,
	}
	res, err := verifier.Run(context.Background())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.HasMismatch() {
		t.Fatalf("verify found mismatches: %+v", res.Tables)
	}
}

func TestIntegration_TSVToPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pgDSN := startPostgresIT(t)
	// TAB-delimited with quoting: an embedded tab, a quoted empty, a NULL.
	content := "a\tb\tc\n" +
		"one\t\"tab\there\"\t\\N\n" +
		"two\t\"\"\tplain\n"
	src := writeFixtureIT(t, "cells.tsv", content)

	base, _ := engines.Get("tsv")
	null := `\N`
	source, err := base.(Engine).WithFlatFileOptions(Options{HeaderDeclared: true, Header: true, NullRepr: &null})
	if err != nil {
		t.Fatal(err)
	}
	pg, _ := engines.Get("postgres")
	runMigrateIT(t, source, pg, src, pgDSN)

	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	wantCell(t, queryText(t, db, `SELECT b FROM cells WHERE a = 'one'`), "tab\there", "row1 b (embedded tab)")
	if v := queryText(t, db, `SELECT c FROM cells WHERE a = 'one'`); v != nil {
		t.Errorf("row1 c = %q; want NULL", *v)
	}
	wantCell(t, queryText(t, db, `SELECT b FROM cells WHERE a = 'two'`), "", "row2 b (quoted empty)")
}

func TestIntegration_NDJSONToPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	pgDSN := startPostgresIT(t)
	content := `{"id":9007199254740993,"payload":{"k":[1,2]},"seen_at":"2024-06-07T08:09:10","flag":true,"note":null}` + "\n" +
		`{"id":123456789012345678901234567890,"payload":{"z":0},"seen_at":"2024-06-08 09:10:11","note":"n2"}` + "\n"
	src := writeFixtureIT(t, "events.ndjson", content)

	base, _ := engines.Get("ndjson")
	source, err := base.(Engine).WithFlatFileOptions(Options{})
	if err != nil {
		t.Fatal(err)
	}
	pg, _ := engines.Get("postgres")
	runMigrateIT(t, source, pg, src, pgDSN)

	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// payload (json hint, all objects) → jsonb; seen_at (temporal hint, all
	// ISO) → naive timestamp; id/flag/note stay text.
	wantCell(t, queryText(t, db,
		`SELECT data_type FROM information_schema.columns WHERE table_name = 'events' AND column_name = 'payload'`),
		"jsonb", "payload column type")
	wantCell(t, queryText(t, db,
		`SELECT data_type FROM information_schema.columns WHERE table_name = 'events' AND column_name = 'seen_at'`),
		"timestamp without time zone", "seen_at column type")

	wantCell(t, queryText(t, db, `SELECT id FROM events WHERE flag = 'true'`),
		"9007199254740993", "row1 id (2^53+1 raw text)")
	wantCell(t, queryText(t, db, `SELECT payload::text FROM events WHERE flag = 'true'`),
		`{"k": [1, 2]}`, "row1 payload (jsonb-normalized)")
	wantCell(t, queryText(t, db, `SELECT seen_at::text FROM events WHERE flag = 'true'`),
		"2024-06-07 08:09:10", "row1 seen_at")
	if v := queryText(t, db, `SELECT note FROM events WHERE flag = 'true'`); v != nil {
		t.Errorf("row1 note = %q; want NULL (explicit JSON null)", *v)
	}
	wantCell(t, queryText(t, db, `SELECT id FROM events WHERE note = 'n2'`),
		"123456789012345678901234567890", "row2 id (beyond-int64 raw text)")
	if v := queryText(t, db, `SELECT flag FROM events WHERE note = 'n2'`); v != nil {
		t.Errorf("row2 flag = %q; want NULL (absent key)", *v)
	}
}

func TestIntegration_CSVToMySQL(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	myDSN := startMySQLIT(t)
	src := writeFixtureIT(t, "people.csv", fixtureCSV)
	source := csvEngineIT(t)
	my, _ := engines.Get("mysql")

	runMigrateIT(t, source, my, src, myDSN)

	db, err := sql.Open("mysql", myDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	wantCell(t, queryText(t, db, "SELECT name FROM people WHERE id = '1'"),
		"comma, \"quote\", and\nnewline", "row1 name")
	wantCell(t, queryText(t, db, "SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') FROM people WHERE id = '1'"),
		"2024-01-02 03:04:05", "row1 created_at")
	// The promoted timestamptz column, instant-exact on the MySQL target too:
	// CONVERT_TZ to a fixed +00:00 makes the assertion session-tz-independent.
	wantCell(t, queryText(t, db,
		"SELECT DATE_FORMAT(CONVERT_TZ(synced_at, @@session.time_zone, '+00:00'), '%Y-%m-%d %H:%i:%s.%f') FROM people WHERE id = '1'"),
		"2026-07-15 08:09:10.123456", "row1 synced_at (PG-COPY +00 rendering)")
	wantCell(t, queryText(t, db,
		"SELECT DATE_FORMAT(CONVERT_TZ(synced_at, @@session.time_zone, '+00:00'), '%Y-%m-%d %H:%i:%s.%f') FROM people WHERE id = '2'"),
		"2026-07-14 07:08:09.000000", "row2 synced_at (+02 offset applied)")
	wantCell(t, queryText(t, db, "SELECT user_uuid FROM people WHERE id = '1'"),
		"6dfa5e5a-2b64-4c5e-9f6a-0a2b3c4d5e6f", "row1 user_uuid")
	wantCell(t, queryText(t, db, "SELECT big FROM people WHERE id = '1'"),
		"9007199254740993", "row1 big")
	// `dec` is a MySQL reserved word (DECIMAL synonym) — the writer quotes it
	// when creating the column; the assertion query must too.
	wantCell(t, queryText(t, db, "SELECT `dec` FROM people WHERE id = '2'"), "-0.000", "row2 dec")
	wantCell(t, queryText(t, db, "SELECT name FROM people WHERE id = '2'"), "héllo 🚀", "row2 name")
	if v := queryText(t, db, "SELECT created_at FROM people WHERE id = '2'"); v != nil {
		t.Errorf("row2 created_at = %q; want NULL", *v)
	}
}
