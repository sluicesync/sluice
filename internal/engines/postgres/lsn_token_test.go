// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLSNFromPositionToken_RoundTrip verifies the helper extracts
// the LSN from a canonical pgPos token, returns 0 on the empty-
// token case, and propagates parse errors on malformed tokens.
func TestLSNFromPositionToken_RoundTrip(t *testing.T) {
	pos, err := encodePGPos(pgPos{Slot: "sluice_slot", LSN: "0/16B7350"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	lsn, err := lsnFromPositionToken(pos.Token)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	wantLSN, err := pglogrepl.ParseLSN("0/16B7350")
	if err != nil {
		t.Fatalf("expected lsn parse: %v", err)
	}
	if lsn != wantLSN {
		t.Errorf("lsn = %v; want %v", lsn, wantLSN)
	}
}

func TestLSNFromPositionToken_EmptyTokenIsZero(t *testing.T) {
	lsn, err := lsnFromPositionToken("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lsn != 0 {
		t.Errorf("lsn = %v; want 0", lsn)
	}
}

func TestLSNFromPositionToken_MalformedReturnsError(t *testing.T) {
	if _, err := lsnFromPositionToken("not json"); err == nil {
		t.Error("expected error for malformed token")
	}
}

// TestLSNFromPositionToken_NonPGArrayTokenIsSilentZero pins soak finding
// F2: a MySQL / VStream source writes its CDC position as a JSON array of
// per-shard GTIDs. Such a token carries no LSN and must return 0 silently
// — not surface a parse error a caller then logs on every single apply.
// Leading whitespace before the '[' is tolerated.
func TestLSNFromPositionToken_NonPGArrayTokenIsSilentZero(t *testing.T) {
	for _, tok := range []string{
		`[{"keyspace":"commerce","shard":"-","gtid":"MySQL56/a1b2:1-100"}]`,
		`[]`,
		"  \n\t[{\"shard\":\"-80\"}]",
	} {
		lsn, err := lsnFromPositionToken(tok)
		if err != nil {
			t.Errorf("array token %q: unexpected error: %v", tok, err)
		}
		if lsn != 0 {
			t.Errorf("array token %q: lsn = %v; want 0", tok, lsn)
		}
	}
	// A malformed pgPos *object* token (not an array) still errors —
	// the guard is array-shape-specific, preserving genuine-corruption
	// diagnostics for a real Postgres source.
	if _, err := lsnFromPositionToken(`{"slot":"s","lsn":"nonsense"}`); err == nil {
		t.Error("expected error for malformed object token")
	}
}

// TestLSNFromPositionToken_PositionShape pins the helper to the
// canonical position shape so a future change to pgPos's wire
// format is caught here.
func TestLSNFromPositionToken_PositionShape(t *testing.T) {
	// Construct a position the same way encodePGPos does, then
	// pull the token out and feed it back in.
	encoded, err := encodePGPos(pgPos{Slot: "x", LSN: "1/2"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded.Engine != engineNamePostgres {
		t.Errorf("engine tag drifted: got %q; want %q", encoded.Engine, engineNamePostgres)
	}
	// And confirm an ir.Position with the right engine tag round-
	// trips through the helper.
	pos := ir.Position{Engine: engineNamePostgres, Token: encoded.Token}
	if _, err := lsnFromPositionToken(pos.Token); err != nil {
		t.Errorf("round-trip parse: %v", err)
	}
}
