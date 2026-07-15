// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// getEngine looks a registered engine up (the sqlite engine registers via
// this package's own import of internal/engines/sqlite).
func getEngine(t *testing.T, name string) (ir.Engine, bool) {
	t.Helper()
	return engines.Get(name)
}

// TestMigrateCSVToSQLiteEndToEnd drives the FULL pipeline.Migrator from a
// csv source into a real sqlite target file — no containers needed — and
// ground-truths every cell in the landed database. This is the fast, untagged
// end-to-end net; the real Postgres/MySQL targets are pinned by the
// integration suite (flatfile_integration_test.go).
func TestMigrateCSVToSQLiteEndToEnd(t *testing.T) {
	e := csvEngine(t, Options{HeaderDeclared: true, Header: true, NullRepr: strp(`\N`)})
	srcPath := writeSource(t, "people.csv",
		"name,note,big,dec,nil\n"+
			`Ada,"comma, ""quote"", and`+"\nnewline\""+`,9007199254740993,007.50,\N`+"\n"+
			`Bo,"",18446744073709551615,-0.000,plain`+"\n")

	tgtPath := filepath.Join(t.TempDir(), "target.db")
	tgt, ok := getEngine(t, "sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	mig := &pipeline.Migrator{
		Source:    e,
		Target:    tgt,
		SourceDSN: srcPath,
		TargetDSN: tgtPath,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run(csv → sqlite): %v", err)
	}

	db, err := sql.Open("sqlite", tgtPath)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `SELECT name, note, big, dec, nil FROM "people" ORDER BY name`)
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	type rec struct {
		name, note, big, dec string
		nilv                 *string
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.name, &r.note, &r.big, &r.dec, &r.nilv); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("target holds %d rows; want 2", len(got))
	}
	ada, bo := got[0], got[1]
	if ada.note != "comma, \"quote\", and\nnewline" {
		t.Errorf("ada.note = %q", ada.note)
	}
	if ada.big != "9007199254740993" || ada.dec != "007.50" {
		t.Errorf("ada big/dec = %q/%q; want exact text", ada.big, ada.dec)
	}
	if ada.nilv != nil {
		t.Errorf("ada.nil = %v; want NULL", *ada.nilv)
	}
	if bo.note != "" || bo.big != "18446744073709551615" || bo.dec != "-0.000" {
		t.Errorf("bo = %+v; want quoted-empty note and exact number text", bo)
	}
	if bo.nilv == nil || *bo.nilv != "plain" {
		t.Errorf("bo.nil = %v; want \"plain\"", bo.nilv)
	}
}

// TestMigrateNDJSONToSQLiteEndToEnd drives the full pipeline from an ndjson
// source, pinning big-number raw text and absent-key NULLs through the
// whole migrate path.
func TestMigrateNDJSONToSQLiteEndToEnd(t *testing.T) {
	e := ndjsonEngine(t)
	srcPath := writeSource(t, "events.ndjson",
		`{"id":9007199254740993,"kind":"start","meta":{"a":[1,2]}}`+"\n"+
			`{"id":123456789012345678901234567890,"kind":null}`+"\n")

	tgtPath := filepath.Join(t.TempDir(), "target.db")
	tgt, ok := getEngine(t, "sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	mig := &pipeline.Migrator{Source: e, Target: tgt, SourceDSN: srcPath, TargetDSN: tgtPath}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run(ndjson → sqlite): %v", err)
	}

	db, err := sql.Open("sqlite", tgtPath)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var id, meta *string
	if err := db.QueryRowContext(ctx, `SELECT id, meta FROM "events" WHERE kind = 'start'`).Scan(&id, &meta); err != nil {
		t.Fatalf("query row 1: %v", err)
	}
	if id == nil || *id != "9007199254740993" {
		t.Errorf("id = %v; want the exact 2^53+1 text", id)
	}
	if meta == nil || *meta != `{"a":[1,2]}` {
		t.Errorf("meta = %v; want the raw JSON text", meta)
	}
	var id2, kind2 *string
	if err := db.QueryRowContext(ctx, `SELECT id, kind FROM "events" WHERE kind IS NULL`).Scan(&id2, &kind2); err != nil {
		t.Fatalf("query row 2: %v", err)
	}
	if id2 == nil || *id2 != "123456789012345678901234567890" {
		t.Errorf("id2 = %v; want the beyond-int64 raw text", id2)
	}
}
