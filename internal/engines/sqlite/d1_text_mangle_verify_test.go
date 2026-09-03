//go:build d1verify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// LA-4 (audit 2026-09-01): D1 stores invalid-UTF-8 TEXT intact and replaces
// every invalid byte with U+FFFD in its /query JSON response — SERVER-SIDE.
// The mangled cell therefore arrives as valid UTF-8, which is why the
// decode path's own UTF-8 guard cannot fire for this vector and why the
// code comment there used to say there was no independent expected value.
// There is one, on the server: the summed byte length of its text-storage
// cells, read in the same round trip as the closing COUNT(*).
//
// Measured on live D1 (2026-09-03) before the fix, and the numbers this
// test rests on:
//
//	stored x'FFFE61'  typeof=text  length(CAST(c AS BLOB))=3  delivered 7 bytes
//	stored 'plain'    typeof=text  length(CAST(c AS BLOB))=5  delivered 5 bytes
//	stored x'00FF'    typeof=blob                             delivered a JSON ARRAY
//	stored NULL       typeof=null                             delivered null
//
// The blob row is why the two sums can be aligned at all: a blob never
// reaches the decoder's string branch, and typeof() keeps it out of the
// server's sum too.

package sqlite

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestD1Verify_MangledTextIsRefusedNotCopied(t *testing.T) {
	token := os.Getenv("CLOUDFLARE_API_TOKEN")
	account := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || account == "" {
		t.Skip("CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID not set; d1verify needs live credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	dbID := createThrowawayD1Database(ctx, t, account, token)
	client, err := openD1Client("d1://" + account + "/" + dbID)
	if err != nil {
		t.Fatalf("openD1Client: %v", err)
	}

	exec := func(sql string) {
		t.Helper()
		if _, err := client.queryRows(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// A table whose text column is clean except for one poisoned cell, plus
	// a blob and a NULL so the alignment is exercised rather than assumed.
	exec(`CREATE TABLE la4 (id INTEGER PRIMARY KEY, c TEXT, b BLOB)`)
	exec(`INSERT INTO la4 (id, c, b) VALUES (1, 'plain', NULL)`)
	exec(`INSERT INTO la4 (id, c, b) VALUES (2, 'caf' || char(233), x'00FF')`)
	exec(`INSERT INTO la4 (id, c, b) VALUES (3, CAST(x'FFFE61' AS TEXT), NULL)`)

	table := &ir.Table{
		Name: "la4",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: ir.Text{}},
			{Name: "b", Type: ir.Blob{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}

	reader := &D1RowReader{client: client}
	rows, err := reader.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var got int
	for range rows {
		got++
	}
	readErr := reader.Err()

	if readErr == nil {
		t.Fatalf("the read returned %d rows and NO error — the poisoned cell (stored 3 bytes, delivered 7 as U+FFFD) "+
			"was copied silently, which is LA-4", got)
	}
	var coded *sluicecode.CodedError
	if !errors.As(readErr, &coded) || coded.Code != sluicecode.CodeD1TextMangled {
		t.Fatalf("read failed with %v; want %s — an operator greps the code", readErr, sluicecode.CodeD1TextMangled)
	}
	// The message has to carry both numbers, or it cannot be acted on.
	if !strings.Contains(readErr.Error(), "hex(col)") {
		t.Errorf("the refusal does not tell the operator the source is intact and readable as hex(col): %v", readErr)
	}
	t.Logf("LA-4 refusal: %v", readErr)

	// The floor, and the half that keeps this from being a table that always
	// refuses: with the poison removed the same table reads clean.
	exec(`DELETE FROM la4 WHERE id = 3`)
	reader2 := &D1RowReader{client: client}
	rows2, err := reader2.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows (clean): %v", err)
	}
	clean := 0
	for range rows2 {
		clean++
	}
	if err := reader2.Err(); err != nil {
		t.Fatalf("the clean table refused: %v — the byte bracket is firing on legitimate data "+
			"(a blob and a NULL are in this table on purpose)", err)
	}
	if clean != 2 {
		t.Fatalf("clean read delivered %d rows; want 2", clean)
	}
}
