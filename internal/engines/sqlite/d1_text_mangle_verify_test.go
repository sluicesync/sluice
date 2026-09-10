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

// TestD1Verify_LengthPreservingRewriteIsRefused is the LIVE half of audit
// A0909-SLP-MEDIUM-2 — the case the byte sum above structurally cannot see.
//
// D1 substitutes one U+FFFD per MAXIMAL INVALID SUBPART, and a subpart is
// one, two or three bytes. At exactly three the substitution is byte-length
// PRESERVING: 'A' || x'F09F98' (a 4-byte emoji severed at a byte boundary,
// which is what fixed-width truncation produces) stores four bytes and is
// delivered as "A" + U+FFFD, also four bytes. The byte sums agree and the
// cell was still rewritten. The second number — how many U+FFFD the source
// ALREADY STORES — is the only thing that sees it.
//
// The canned-transport gate is TestD1RowReader_ReplacementCountBracket, whose
// SOURCE numbers come from a real SQLite executing the same expressions. This
// one grades the premise that gate cannot: that live D1's decoder is
// maximal-subpart (WHATWG) rather than per-byte. If D1 turned out to emit
// THREE replacements here, the byte half would have caught it and this test
// would still refuse — so a failure of the byte-equality assertion below is
// the interesting outcome, not a broken test.
func TestD1Verify_LengthPreservingRewriteIsRefused(t *testing.T) {
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

	exec(`CREATE TABLE mlen (id INTEGER PRIMARY KEY, c TEXT)`)
	exec(`INSERT INTO mlen (id, c) VALUES (1, 'plain')`)
	// The severed emoji. Stored: 4 bytes, 0 U+FFFD.
	exec(`INSERT INTO mlen (id, c) VALUES (2, CAST(x'41F09F98' AS TEXT))`)

	table := &ir.Table{
		Name: "mlen",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}

	// Ground-truth the premise FIRST, so a failure says which half moved.
	totals, err := (&D1RowReader{client: client}).countRows(ctx, table)
	if err != nil {
		t.Fatalf("countRows: %v", err)
	}
	if totals.textBytes != 9 || totals.replacements != 0 {
		t.Fatalf("live D1 reports the source as (bytes=%d, stored U+FFFD=%d); want (9, 0) for "+
			"'plain' plus a severed 4-byte sequence", totals.textBytes, totals.replacements)
	}

	reader := &D1RowReader{client: client}
	rows, err := reader.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var delivered d1DeliveredText
	for row := range rows {
		if s, ok := row["c"].(string); ok {
			delivered.add(deliveredText(s))
		}
	}
	readErr := reader.Err()

	if delivered.bytes != totals.textBytes {
		t.Errorf("PREMISE MOVED: live D1 delivered %d text bytes where it stores %d, so this rewrite is NOT "+
			"length-preserving on the current D1 and the byte half would have caught it. The count half is "+
			"still correct, but the case it exists for is no longer the one this fixture produces",
			delivered.bytes, totals.textBytes)
	}
	if delivered.replacements != 1 {
		t.Errorf("live D1 delivered %d U+FFFD for one severed 4-byte sequence; want 1 (a WHATWG "+
			"maximal-subpart decoder). A value of 3 means D1 replaces per BYTE", delivered.replacements)
	}
	if readErr == nil {
		t.Fatalf("the read returned NO error over a cell D1 rewrote without changing its byte count — "+
			"this is A0909-SLP-MEDIUM-2, and the copy would have persisted %q where the source holds "+
			"'A' || x'F09F98'", "A�")
	}
	var coded *sluicecode.CodedError
	if !errors.As(readErr, &coded) || coded.Code != sluicecode.CodeD1TextMangled {
		t.Fatalf("read failed with %v; want %s", readErr, sluicecode.CodeD1TextMangled)
	}
	if !strings.Contains(readErr.Error(), "U+FFFD where the source stores") {
		t.Errorf("the refusal does not name both counts, so an operator cannot tell which half fired: %v", readErr)
	}
	t.Logf("A0909-SLP-MEDIUM-2 refusal: %v", readErr)

	// The floor: with the severed cell removed the same table reads clean.
	exec(`DELETE FROM mlen WHERE id = 2`)
	reader2 := &D1RowReader{client: client}
	rows2, err := reader2.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows (clean): %v", err)
	}
	for range rows2 {
	}
	if err := reader2.Err(); err != nil {
		t.Fatalf("the clean table refused: %v — the count half is firing on legitimate data", err)
	}
}
