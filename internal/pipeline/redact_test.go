// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
	"github.com/orware/sluice/internal/redact"
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

	if err := redactRow(nil, "public", "users", row, cols); err != nil {
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
	if err := redactRow(r, "public", "users", row, cols); err != nil {
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

	if err := redactRow(r, "public", "users", row, cols); err != nil {
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

	if err := redactRow(r, "public", "users", row, cols); err != nil {
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
// key policy works through redactRow — operators on a case-folding
// engine like MySQL can declare rules in any case.
func TestRedactRow_CaseInsensitiveLookup(t *testing.T) {
	r := redact.New()
	r.Set("Public", "Users", "Email", redact.Hash{Algo: "sha256"})

	row := ir.Row{"email": "alice@example.com"}
	cols := []*ir.Column{col("email", true)}

	if err := redactRow(r, "public", "users", row, cols); err != nil {
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

	err := redactRow(r, "public", "users", row, cols)
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

	if err := redactRow(r, "public", "users", row, cols); err != nil {
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

	if err := redactRow(r, "public", "users", row, cols); err != nil {
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

	if err := redactRow(r, "public", "users", row, cols); err != nil {
		t.Fatalf("nil columns in list: unexpected error %v", err)
	}
	// email should still be hashed.
	if s, ok := row["email"].(string); !ok || len(s) != 64 {
		t.Errorf("email should be hashed despite nil entries; got %v", row["email"])
	}
}
