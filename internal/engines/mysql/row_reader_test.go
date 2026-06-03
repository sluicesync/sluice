//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the MySQL RowReader. Boots a real MySQL
// container, applies a fixture schema and data, and verifies that
// rows round-trip through the IR-typed value table.
//
// Skipped on hosts without a usable Docker provider.

package mysql

import (
	"context"
	"reflect"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowReader_TypeRoundTrip exercises a fixture covering the common
// types. Each row asserts the IR-typed value matches the fixture.
func TestRowReader_TypeRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE samples (
			id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
			active      TINYINT(1)      NOT NULL,
			name        VARCHAR(64)     NOT NULL,
			price       DECIMAL(10,2)   NOT NULL,
			ratio       DOUBLE          NOT NULL,
			role        ENUM('admin','user','guest') NOT NULL,
			tags        SET('go','sql','mysql','postgres') NOT NULL,
			payload     JSON            NULL,
			data        BLOB            NULL,
			created_at  TIMESTAMP(0)    NOT NULL,
			birthday    DATE            NULL,
			start_time  TIME(0)         NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

		INSERT INTO samples (active, name, price, ratio, role, tags,
		                     payload, data, created_at, birthday, start_time)
		VALUES
			(1, 'Alice', 19.95, 0.5,  'admin', 'go,sql',
			 '{"plan":"free"}', X'DEADBEEF',
			 '2026-05-01 12:34:56', '1990-01-15', '08:30:00'),
			(0, 'Bob',    0.00, 1.25, 'user',  '',
			 NULL,            NULL,
			 '2026-05-01 13:00:00', NULL,        NULL);
	`

	applyDDL(t, dsn, ddl)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Read the schema first so the RowReader gets the IR table shape.
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
		t.Fatalf("samples table not found in schema; have %v", tableNames(schema))
	}

	// Now the RowReader.
	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	ch, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}

	var rows []ir.Row
	for row := range ch {
		rows = append(rows, row)
	}
	if rrErr, ok := rr.(*RowReader); ok {
		if err := rrErr.Err(); err != nil {
			t.Fatalf("Err after streaming: %v", err)
		}
	}

	if len(rows) != 2 {
		t.Fatalf("got %d rows; want 2", len(rows))
	}

	// ----- Row 0: Alice -----
	alice := rows[0]
	expectEq(t, "id", alice["id"], uint64(1))
	expectEq(t, "active", alice["active"], true)
	expectEq(t, "name", alice["name"], "Alice")
	expectEq(t, "price", alice["price"], "19.95")
	expectEq(t, "ratio", alice["ratio"], 0.5)
	expectEq(t, "role", alice["role"], "admin")
	expectEq(t, "tags", alice["tags"], []string{"go", "sql"})
	expectEq(t, "payload", alice["payload"], []byte(`{"plan": "free"}`))
	expectEq(t, "data", alice["data"], []byte{0xde, 0xad, 0xbe, 0xef})
	if got, ok := alice["created_at"].(time.Time); !ok ||
		!got.Equal(time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)) {
		t.Errorf("created_at = %#v; want 2026-05-01T12:34:56Z", alice["created_at"])
	}
	if got, ok := alice["birthday"].(time.Time); !ok ||
		!got.Equal(time.Date(1990, 1, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("birthday = %#v; want 1990-01-15", alice["birthday"])
	}
	expectEq(t, "start_time", alice["start_time"], "08:30:00")

	// ----- Row 1: Bob -----
	bob := rows[1]
	expectEq(t, "id", bob["id"], uint64(2))
	expectEq(t, "active", bob["active"], false)
	expectEq(t, "name", bob["name"], "Bob")
	expectEq(t, "tags", bob["tags"], []string{})
	if bob["payload"] != nil {
		t.Errorf("payload = %#v; want nil", bob["payload"])
	}
	if bob["data"] != nil {
		t.Errorf("data = %#v; want nil", bob["data"])
	}
	if bob["birthday"] != nil {
		t.Errorf("birthday = %#v; want nil", bob["birthday"])
	}
	if bob["start_time"] != nil {
		t.Errorf("start_time = %#v; want nil", bob["start_time"])
	}
}

// expectEq compares using reflect.DeepEqual and reports the column
// name on mismatch.
func expectEq(t *testing.T, col string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %#v (%T); want %#v (%T)", col, got, got, want, want)
	}
}

// closeIf calls Close on v if it implements io.Closer-shaped interface.
func closeIf(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
