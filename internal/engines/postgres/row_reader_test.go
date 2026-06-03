//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres RowReader. Exercises the full
// SchemaReader → RowReader path end-to-end against a real Postgres
// container.

package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowReader_TypeMatrix writes a row representative of the
// canonical IR-typed value contract, reads it back via the RowReader,
// and asserts every column matches expectation. The same load-bearing
// test as MySQL's RowWriter round-trip — failures here usually
// indicate a value-encoding bug somewhere in the data path.
func TestRowReader_TypeMatrix(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const ddl = `
		CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

		CREATE TABLE samples (
			id           BIGINT       PRIMARY KEY,
			active       BOOLEAN      NOT NULL,
			name         VARCHAR(64)  NOT NULL,
			score        NUMERIC(8,2) NOT NULL,
			role         user_role    NOT NULL,
			tags         INTEGER[]    NOT NULL,
			profile      JSONB        NULL,
			data         BYTEA        NULL,
			external_id  UUID         NULL,
			network      CIDR         NULL,
			mac          MACADDR      NULL,
			created_at   TIMESTAMPTZ  NOT NULL,
			birthday     DATE         NULL
		);

		INSERT INTO samples (
			id, active, name, score, role, tags, profile, data,
			external_id, network, mac, created_at, birthday
		) VALUES (
			1, TRUE, 'Alice', 19.95, 'admin', ARRAY[10, 20, 30],
			'{"plan":"free"}'::jsonb, '\xdeadbeef'::bytea,
			'00112233-4455-6677-8899-aabbccddeeff'::uuid,
			'192.168.1.0/24'::cidr, '08:00:2b:01:02:03'::macaddr,
			'2026-05-01 12:34:56+00'::timestamptz,
			'1990-01-15'::date
		);
	`
	applyDDL(t, dsn, ddl)

	// Read the schema so we have an IR Table to drive the reader.
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "samples")
	if table == nil {
		t.Fatalf("samples table not found; have %v", tableNames(schema))
	}

	// Read rows.
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)

	out, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got []ir.Row
	for row := range out {
		got = append(got, row)
	}
	if rrConcrete, ok := rr.(*RowReader); ok {
		if err := rrConcrete.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}

	if len(got) != 1 {
		t.Fatalf("got %d rows; want 1", len(got))
	}
	row := got[0]

	// ---- Strict equality on the values we can pin down ----
	mustEq(t, row, "id", int64(1))
	mustEq(t, row, "active", true)
	mustEq(t, row, "name", "Alice")
	mustEq(t, row, "score", "19.95")
	mustEq(t, row, "role", "admin")
	mustEqAny(t, row, "tags", []any{int64(10), int64(20), int64(30)})
	mustEq(t, row, "external_id", "00112233-4455-6677-8899-aabbccddeeff")
	mustEq(t, row, "network", "192.168.1.0/24")
	mustEq(t, row, "mac", "08:00:2b:01:02:03")

	// ---- Looser checks where exact value depends on format quirks ----

	// jsonb is normalised by Postgres on insert; the exact byte form
	// (with or without spaces) varies. Just confirm it's non-empty
	// bytes containing the key we inserted.
	if profile, ok := row["profile"].([]byte); !ok || len(profile) == 0 {
		t.Errorf("profile = %#v; want non-empty []byte", row["profile"])
	}

	// bytea: \xdeadbeef → []byte{0xde, 0xad, 0xbe, 0xef}
	if data, ok := row["data"].([]byte); !ok || !reflect.DeepEqual(data, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("data = %#v; want [0xde, 0xad, 0xbe, 0xef]", row["data"])
	}

	// timestamptz: instant equality (location may differ).
	wantCreated := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	if created, ok := row["created_at"].(time.Time); !ok || !created.Equal(wantCreated) {
		t.Errorf("created_at = %#v; want %v", row["created_at"], wantCreated)
	}

	// date: midnight UTC of the inserted day.
	wantBirthday := time.Date(1990, 1, 15, 0, 0, 0, 0, time.UTC)
	if bday, ok := row["birthday"].(time.Time); !ok || !bday.Equal(wantBirthday) {
		t.Errorf("birthday = %#v; want %v", row["birthday"], wantBirthday)
	}
}

// mustEq asserts row[col] equals want using reflect.DeepEqual, which
// handles int64, string, bool, etc. uniformly.
func mustEq(t *testing.T, row ir.Row, col string, want any) {
	t.Helper()
	if got := row[col]; !reflect.DeepEqual(got, want) {
		t.Errorf("row.%s = %#v (%T); want %#v (%T)", col, got, got, want, want)
	}
}

// mustEqAny is the same as mustEq but documents that the expected
// value is intentionally an []any (the canonical IR Row type for
// arrays).
func mustEqAny(t *testing.T, row ir.Row, col string, want []any) {
	t.Helper()
	got, ok := row[col].([]any)
	if !ok {
		t.Errorf("row.%s = %#v (%T); want []any", col, row[col], row[col])
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("row.%s = %#v; want %#v", col, got, want)
	}
}
