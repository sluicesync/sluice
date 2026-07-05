// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/redact"
)

// helper: build a *ir.Column with name + nullable; type defaults
// to Varchar(255) (the most common PII column shape).
func col(name string, nullable bool) *ir.Column {
	return &ir.Column{
		Name:     name,
		Type:     ir.Varchar{Length: 255},
		Nullable: nullable,
	}
}

// TestRedactRow_NilRegistry exercises the load-bearing fast path:
// the no-redactions case must be a zero-cost no-op so default
// operators pay nothing for the feature.
func TestRedactRow_NilRegistry(t *testing.T) {
	row := ir.Row{"id": int64(1), "email": "alice@example.com"}
	cols := []*ir.Column{col("id", false), col("email", false)}

	if err := migcore.RedactRow(nil, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("nil registry: unexpected error %v", err)
	}
	if row["email"] != "alice@example.com" {
		t.Errorf("nil registry should pass through; got %v", row["email"])
	}
}

// TestRedactRow_EmptyRegistry covers the other no-op path: a
// constructed-but-empty Registry. Same fast-path expectation.
func TestRedactRow_EmptyRegistry(t *testing.T) {
	row := ir.Row{"id": int64(1), "email": "alice@example.com"}
	cols := []*ir.Column{col("id", false), col("email", false)}

	r := redact.New()
	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("empty registry: unexpected error %v", err)
	}
	if row["email"] != "alice@example.com" {
		t.Errorf("empty registry should pass through; got %v", row["email"])
	}
}

// TestRedactRow_HashStrategy is the headline happy-path: a single
// hash:sha256 rule replaces one column's value with its hex digest;
// other columns pass through.
func TestRedactRow_HashStrategy(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})

	row := ir.Row{
		"id":    int64(1),
		"email": "alice@example.com",
		"name":  "Alice",
	}
	cols := []*ir.Column{col("id", false), col("email", true), col("name", true)}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	// Email should be replaced with the SHA-256 hex digest.
	want := sha256.Sum256([]byte("alice@example.com"))
	wantHex := hex.EncodeToString(want[:])
	if got := row["email"]; got != wantHex {
		t.Errorf("email: got %v; want %s", got, wantHex)
	}
	// Untouched columns pass through.
	if got := row["id"]; got != int64(1) {
		t.Errorf("id: got %v; want 1 (untouched)", got)
	}
	if got := row["name"]; got != "Alice" {
		t.Errorf("name: got %v; want 'Alice' (untouched)", got)
	}
}

// TestRedactRow_MultipleStrategies covers the realistic mix-of-
// strategies case: hash on email, truncate on phone, null on ssn,
// static on credit_card.
func TestRedactRow_MultipleStrategies(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
	r.Set("public", "users", "phone", redact.Truncate{N: 4})
	r.Set("public", "users", "ssn", redact.Null{})
	r.Set("public", "users", "credit_card", redact.Static{Value: "REDACTED"})

	row := ir.Row{
		"id":          int64(1),
		"email":       "alice@example.com",
		"phone":       "555-867-5309",
		"ssn":         "111-22-3333",
		"credit_card": "4111111111111111",
		"name":        "Alice",
	}
	cols := []*ir.Column{
		col("id", false),
		col("email", true),
		col("phone", true),
		col("ssn", true),         // nullable for Null strategy
		col("credit_card", true), // nullable
		col("name", true),
	}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	if s, ok := row["email"].(string); !ok || len(s) != 64 {
		t.Errorf("email should be 64-char hex; got %T %q", row["email"], row["email"])
	}
	if got := row["phone"]; got != "555-" {
		t.Errorf("phone: got %v; want '555-'", got)
	}
	if got := row["ssn"]; got != nil {
		t.Errorf("ssn: got %v; want nil", got)
	}
	if got := row["credit_card"]; got != "REDACTED" {
		t.Errorf("credit_card: got %v; want 'REDACTED'", got)
	}
	if got := row["name"]; got != "Alice" {
		t.Errorf("name: got %v; want 'Alice' (no rule, pass-through)", got)
	}
	if got := row["id"]; got != int64(1) {
		t.Errorf("id: got %v; want 1 (no rule, pass-through)", got)
	}
}

// TestRedactRow_CaseInsensitiveLookup confirms Registry's lowercase
// key policy works through migcore.RedactRow — operators on a case-folding
// engine like MySQL can declare rules in any case.
func TestRedactRow_CaseInsensitiveLookup(t *testing.T) {
	r := redact.New()
	r.Set("Public", "Users", "Email", redact.Hash{Algo: "sha256"})

	row := ir.Row{"email": "alice@example.com"}
	cols := []*ir.Column{col("email", true)}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if s, ok := row["email"].(string); !ok || len(s) != 64 {
		t.Errorf("case-insensitive lookup didn't match; row still has %v", row["email"])
	}
}

// TestRedactRow_RefusalWrapped covers the error-propagation path:
// a strategy that returns an error (e.g. Null on NOT NULL) gets
// wrapped with the schema.table.column identity for clear operator
// diagnostics.
func TestRedactRow_RefusalWrapped(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "ssn", redact.Null{})

	row := ir.Row{"ssn": "111-22-3333"}
	// ssn declared NOT NULL — Null strategy must refuse.
	cols := []*ir.Column{col("ssn", false)}

	err := migcore.RedactRow(r, "public", "users", row, cols, nil, "")
	if err == nil {
		t.Fatal("expected refusal error; got nil")
	}
	for _, want := range []string{"public", "users", "ssn", "null", "redact"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should contain %q", err.Error(), want)
		}
	}
}

// TestRedactRow_ColumnsNotInRow covers the defensive shape: a
// registered column might not appear in the row (e.g. the source
// row was constructed for a subset of columns). The strategy is
// still invoked with `nil` as the value; Hash/Truncate pass nil
// through; Null refuses on NOT NULL; Static replaces.
func TestRedactRow_ColumnsNotInRow(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})

	// Row is missing "email"; column list includes it.
	row := ir.Row{"id": int64(1)}
	cols := []*ir.Column{col("id", false), col("email", true)}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	// row["email"] was nil; Hash on nil passes through; final
	// row["email"] should be nil.
	if got, ok := row["email"]; ok && got != nil {
		t.Errorf("missing-column case: expected nil; got %v", got)
	}
}

// TestRedactRow_NoMatchingColumns covers a row+columns set where
// the registry has rules for OTHER tables. None of the row's
// columns should be touched.
func TestRedactRow_NoMatchingColumns(t *testing.T) {
	r := redact.New()
	r.Set("public", "accounts", "ssn", redact.Null{})

	row := ir.Row{"id": int64(1), "email": "alice@example.com"}
	cols := []*ir.Column{col("id", false), col("email", true)}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if got := row["email"]; got != "alice@example.com" {
		t.Errorf("no-matching-rule row should be unchanged; got %v", got)
	}
}

// TestRedactRow_NilColumnInList covers a defensive case: pipeline
// callers should never pass a nil column pointer, but the helper
// should skip rather than panic if one slips through.
func TestRedactRow_NilColumnInList(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})

	row := ir.Row{"email": "alice@example.com"}
	cols := []*ir.Column{nil, col("email", true), nil}

	if err := migcore.RedactRow(r, "public", "users", row, cols, nil, ""); err != nil {
		t.Fatalf("nil columns in list: unexpected error %v", err)
	}
	// email should still be hashed.
	if s, ok := row["email"].(string); !ok || len(s) != 64 {
		t.Errorf("email should be hashed despite nil entries; got %v", row["email"])
	}
}

// TestRedactRows_NilRegistryPassesThroughSrcVerbatim pins the
// zero-allocation fast path: nil/empty Registry returns the source
// channel verbatim (no goroutine, no wrapping).
func TestRedactRows_NilRegistryPassesThroughSrcVerbatim(t *testing.T) {
	src := make(chan ir.Row, 1)
	var srcRO <-chan ir.Row = src
	out, errFn := redactRows(context.Background(), src, nil, "public", "users", nil, nil, "")
	if out != srcRO {
		t.Errorf("nil registry: returned channel is not the input src")
	}
	if err := errFn(); err != nil {
		t.Errorf("nil registry: errFn() = %v; want nil", err)
	}

	r := redact.New()
	out2, errFn2 := redactRows(context.Background(), src, r, "public", "users", nil, nil, "")
	if out2 != srcRO {
		t.Errorf("empty registry: returned channel is not the input src")
	}
	if err := errFn2(); err != nil {
		t.Errorf("empty registry: errFn() = %v; want nil", err)
	}
}

// TestRedactRows_AppliesRedactionsToEveryRow covers the streaming
// happy path: multiple rows flow through, each gets redacted.
func TestRedactRows_AppliesRedactionsToEveryRow(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
	cols := []*ir.Column{col("email", true), col("name", true)}

	src := make(chan ir.Row, 3)
	src <- ir.Row{"email": "a@x.com", "name": "Alice"}
	src <- ir.Row{"email": "b@x.com", "name": "Bob"}
	src <- ir.Row{"email": "c@x.com", "name": "Carol"}
	close(src)

	out, errFn := redactRows(context.Background(), src, r, "public", "users", cols, nil, "")
	var received []ir.Row
	for row := range out {
		received = append(received, row)
	}
	if err := errFn(); err != nil {
		t.Fatalf("unexpected errFn() = %v", err)
	}
	if len(received) != 3 {
		t.Fatalf("got %d rows; want 3", len(received))
	}
	for i, row := range received {
		if s, ok := row["email"].(string); !ok || len(s) != 64 {
			t.Errorf("row %d: email not hashed; got %v", i, row["email"])
		}
	}
	// Sanity: different inputs → different hashes.
	if received[0]["email"] == received[1]["email"] {
		t.Errorf("different inputs produced identical hashes")
	}
}

// TestRedactRows_StrategyErrorClosesChannelAndExposesErr covers
// the refusal path: when a strategy returns an error, the output
// channel closes cleanly and errFn returns the wrapped error.
func TestRedactRows_StrategyErrorClosesChannelAndExposesErr(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "ssn", redact.Null{})
	cols := []*ir.Column{col("ssn", false)} // NOT NULL → Null refuses

	src := make(chan ir.Row, 1)
	src <- ir.Row{"ssn": "111-22-3333"}
	close(src)

	out, errFn := redactRows(context.Background(), src, r, "public", "users", cols, nil, "")
	var received []ir.Row
	for row := range out {
		received = append(received, row)
	}
	// The refusal occurred before the row could be sent — out closes
	// cleanly with zero rows received.
	if len(received) != 0 {
		t.Errorf("got %d rows; want 0 (refusal blocks forwarding)", len(received))
	}
	err := errFn()
	if err == nil {
		t.Fatal("errFn() = nil; want refusal error")
	}
	for _, want := range []string{"redact", "ssn", "NOT NULL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err %q should contain %q", err.Error(), want)
		}
	}
}

// TestRedactRows_CtxCancelExitsCleanly covers the cancellation
// path: cancelling ctx before src closes makes the goroutine exit
// and out close without errFn being set.
func TestRedactRows_CtxCancelExitsCleanly(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Hash{Algo: "sha256"})
	cols := []*ir.Column{col("email", true)}

	src := make(chan ir.Row) // unbuffered; sender never sends
	ctx, cancel := context.WithCancel(context.Background())
	out, errFn := redactRows(ctx, src, r, "public", "users", cols, nil, "")
	cancel()
	// out should close shortly after cancel.
	for range out {
		t.Errorf("ctx cancel: expected no rows; got one")
	}
	if err := errFn(); err != nil {
		t.Errorf("ctx cancel: errFn() = %v; want nil (ctx cancel is not a redact error)", err)
	}
	_ = src
}

// TestRedactRows_HexUseAvoidsUnusedImport ensures hex is still
// imported correctly by the test file. (Vacuous; serves as a
// reminder if the test imports get pruned later.)
func TestRedactRows_HexUseAvoidsUnusedImport(_ *testing.T) {
	_ = hex.EncodeToString([]byte{0})
	_ = sha256.Sum256
}

// TestRedactRow_RandomizeWithPK pins the v0.59.0 plumbing: when a
// randomize:* rule fires, pipeline.migcore.RedactRow derives a seed from
// the PK values + streamID and feeds it to the strategy.
// Replay-stable: same PK value across two calls produces the same
// randomized output.
func TestRedactRow_RandomizeWithPK(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "age", redact.RandomizeInt{Min: 18, Max: 90})

	cols := []*ir.Column{col("id", false), col("age", true)}
	pk := []string{"id"}

	row1 := ir.Row{"id": int64(7), "age": int64(35)}
	if err := migcore.RedactRow(r, "public", "users", row1, cols, pk, "stream-1"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	a := row1["age"].(int64)
	if a < 18 || a > 90 {
		t.Errorf("age %d outside [18, 90]", a)
	}

	// Same PK + streamID → same value.
	row2 := ir.Row{"id": int64(7), "age": int64(99)}
	if err := migcore.RedactRow(r, "public", "users", row2, cols, pk, "stream-1"); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if row2["age"] != a {
		t.Errorf("replay stability broken: %v != %v", row2["age"], a)
	}
}

// TestRedactRow_RandomizeNoPKRefuses pins the strategy-level
// refusal contract via migcore.RedactRow: a randomize:* rule on a no-PK
// table surfaces a clear error mentioning the strategy + column.
// Preflight should normally catch this earlier; this is the
// defense-in-depth check.
func TestRedactRow_RandomizeNoPKRefuses(t *testing.T) {
	r := redact.New()
	r.Set("public", "events", "rng", redact.RandomizeInt{Min: 0, Max: 100})
	cols := []*ir.Column{col("rng", true)}

	row := ir.Row{"rng": int64(0)}
	err := migcore.RedactRow(r, "public", "events", row, cols, nil, "stream-1")
	if err == nil {
		t.Fatal("expected refusal for randomize on no-PK table")
	}
	for _, want := range []string{"public", "events", "rng", "randomize:int", "primary key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %q", err.Error(), want)
		}
	}
}

// TestRedactRows_BufferedRelay pins the perf-parity matrix row-6 fix
// (gap 6): the redact tee's relay channel carries the standard
// rowChanBuffer so an engaged redaction never re-introduces an
// unbuffered rendezvous hop into the bulk-copy hot path.
func TestRedactRows_BufferedRelay(t *testing.T) {
	r := redact.New()
	r.Set("public", "users", "email", redact.Null{})
	src := make(chan ir.Row)
	close(src)
	out, _ := redactRows(context.Background(), src, r, "public", "users", nil, nil, "")
	if got := cap(out); got != rowChanBuffer {
		t.Errorf("redact relay channel cap = %d; want rowChanBuffer (%d)", got, rowChanBuffer)
	}
}
