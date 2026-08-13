//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The behavioural half of the audit-2026-08-11 D-1 delegation pin: the
// pgtrigger package asserts `var _ ir.RowFilterSetter =
// (*postgres.RowReader)(nil)` so the docsync migrate-where engine list
// attributes the `--where` pushdown to `postgres-trigger` — and a
// compile-time pin on a DELEGATED type proves only that the type
// implements the interface, not that this engine's OpenRowReader
// actually hands that type back. This test is the binding: the real
// engine's reader, a real predicate, a real Postgres, filtered rows.
package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestPostgresTriggerRowReaderHonorsWhereFilters(t *testing.T) {
	pgSource, _, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, pgSource, `
		CREATE TABLE regions (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO regions VALUES (1, 'keep-a'), (2, 'keep-b'), (3, 'drop-c');
	`)

	eng, ok := engines.Get("postgres-trigger")
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	reader, err := eng.OpenRowReader(ctx, pgSource)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer migcore.CloseIf(reader)

	// The exact call migrate makes: a configured filter map against the
	// reader the engine handed back. A reader without the setter would
	// refuse here — which is what every non-pinned engine does, and what
	// this engine must NOT do.
	if err := migcore.ApplyRowFilters(reader, map[string]string{"regions": "id < 3"}, "postgres-trigger"); err != nil {
		t.Fatalf("ApplyRowFilters refused the delegated reader — the D-1 delegation pin's premise is gone: %v", err)
	}

	rows, err := reader.ReadRows(ctx, &ir.Table{
		Name: "regions",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Text{}},
		},
	})
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	got := 0
	for range rows {
		got++
	}
	if got != 2 {
		t.Fatalf("filtered read returned %d rows; want 2 — the predicate did not reach the delegated reader", got)
	}
}
