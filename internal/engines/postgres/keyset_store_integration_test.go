//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres `db:` keyset store (PII Phase
// 4, ADR-0041). Mirrors control_table_integration_test.go's shape:
// boot a real Postgres via testcontainers, exercise the
// ensure-table idempotency + hand-written-row round-trip, and
// confirm two "streams" sharing one db: source resolve identical
// keyset bytes.

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/orware/sluice/internal/redact"
)

func TestKeysetStore_PG_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := openKeysetStore(ctx, dsn)
	if err != nil {
		t.Fatalf("openKeysetStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	// EnsureKeysetTable is idempotent — call it twice.
	if err := store.EnsureKeysetTable(ctx); err != nil {
		t.Fatalf("EnsureKeysetTable #1: %v", err)
	}
	if err := store.EnsureKeysetTable(ctx); err != nil {
		t.Fatalf("EnsureKeysetTable #2 (idempotent): %v", err)
	}

	// Hand-insert a two-generation keyset (the rotate/list CLI is
	// out of v1 scope — operators populate via SQL).
	gen1 := []byte("pg-keyset-gen1-secret-aaaaaaaa")
	gen2 := []byte("pg-keyset-gen2-secret-bbbbbbbb")
	applyPGApplier(t, dsn, `
		INSERT INTO "public"."sluice_keysets" (name, generation, bytes, active)
		VALUES
			('customer_pii', 1, decode('`+hexOf(gen1)+`','hex'), false),
			('customer_pii', 2, decode('`+hexOf(gen2)+`','hex'), true);
	`)

	ks, err := store.LoadKeyset(ctx)
	if err != nil {
		t.Fatalf("LoadKeyset: %v", err)
	}
	got, name, generation, err := ks.ResolveKey("customer_pii")
	if err != nil {
		t.Fatalf("ResolveKey: %v", err)
	}
	if name != "customer_pii" || generation != 2 || string(got) != string(gen2) {
		t.Errorf("resolve: got (%q,%q,%d); want customer_pii gen 2 bytes", got, name, generation)
	}

	// Two independent stores against the same DSN (== two streams
	// sharing one db: source) resolve identical bytes — the
	// cross-stream-stability primitive.
	store2, err := openKeysetStore(ctx, dsn)
	if err != nil {
		t.Fatalf("openKeysetStore #2: %v", err)
	}
	defer func() { _ = store2.Close() }()
	ks2, err := store2.LoadKeyset(ctx)
	if err != nil {
		t.Fatalf("LoadKeyset #2: %v", err)
	}
	got2, _, _, _ := ks2.ResolveKey("customer_pii")
	if string(got2) != string(got) {
		t.Errorf("cross-store mismatch: %q vs %q", got2, got)
	}

	// Two HMAC strategies built from the two stores produce the
	// same surrogate for the same input.
	h1 := redact.Hash{Algo: "hmac-sha256", Key: got}
	h2 := redact.Hash{Algo: "hmac-sha256", Key: got2}
	a, _ := h1.Redact(nil, "alice@example.com", nil)
	b, _ := h2.Redact(nil, "alice@example.com", nil)
	if a != b {
		t.Errorf("shared-keyset surrogates differ: %v vs %v", a, b)
	}
}

// hexOf returns the lowercase hex of b for the decode(...,'hex')
// INSERT (avoids driver-specific bytea literal escaping).
func hexOf(b []byte) string {
	const hexd = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexd[c>>4], hexd[c&0x0f])
	}
	return string(out)
}
