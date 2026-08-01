// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// schemaBearingManifest is a signed-shape FULL manifest carrying a real
// source schema — the ordinary product of `backup full --encrypt --sign`,
// and the shape ADR-0183's demonstrated forgery targets (one segment, zero
// incrementals, so restore takes the single-manifest path and never
// recomputes the schema fingerprint).
func schemaBearingManifest() *irbackup.Manifest {
	return &irbackup.Manifest{
		FormatVersion: irbackup.FormatVersionSignedManifest,
		CreatedAt:     time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
		BackupID:      "abc0001",
		SchemaHash:    "recorded-hash-left-untouched",
		Schema: &ir.Schema{
			Tables: []*ir.Table{{
				Schema:     "public",
				Name:       "orders",
				Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 8}}},
				RLSEnabled: true,
				Indexes:    []*ir.Index{{Name: "orders_id_idx", Method: "btree"}},
			}},
		},
		Tables: []*irbackup.TableManifest{
			{Schema: "public", Name: "orders", RowCount: 1, Chunks: []*irbackup.ChunkInfo{
				{File: "chunks/public__orders/public__orders-0.jsonl.gz", RowCount: 1, SHA256: "s0"},
			}},
		},
	}
}

// writeV4ManifestSig writes a signature exactly as every binary from the
// SEC-F1 fix through v0.104.x did: v4 canonicalization (scheme token +
// row-chunk parent tables, NO schema bytes), hmac-kek, HMAC MAC.
// Reconstructed rather than produced by the current signer — which emits
// v5 — so the non-vacuity leg below exercises a REAL pre-ADR-0183
// signature, not v5 in disguise.
func writeV4ManifestSig(t *testing.T, ctx context.Context, store irbackup.Store, path string, m *irbackup.Manifest, seq int, key []byte) {
	t.Helper()
	payload, err := irbackup.CanonicalManifestBytesForVersion(m, seq, irbackup.ManifestCanonVersionV4, irbackup.SignatureSchemeHMACKEK)
	if err != nil {
		t.Fatal(err)
	}
	sig := &irbackup.ManifestSignature{
		CanonVersion: irbackup.ManifestCanonVersionV4,
		Scheme:       irbackup.SignatureSchemeHMACKEK,
		KeyID:        crypto.ManifestSigKeyID(key),
		Sequence:     seq,
		ChunkCount:   irbackup.ManifestChunkCount(m),
		MAC:          hex.EncodeToString(crypto.SignManifestHMAC(key, payload)),
	}
	body, err := irbackup.MarshalManifestSignature(sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, ManifestSigPath(path), bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

// plantHostileIndexMethod rewrites the manifest OBJECT ON THE STORE the
// way a store-level adversary would: edit the schema subtree in place,
// leave `schema_hash` exactly as recorded, touch nothing else. It works on
// the JSON document rather than the decoded struct so the tampered bytes
// are what a reader actually parses.
func plantHostileIndexMethod(t *testing.T, ctx context.Context, store irbackup.Store, path string) {
	t.Helper()
	rc, err := store.Get(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	const benign = `"Method": "btree"`
	const hostile = `"Method": "btree; CREATE ROLE attacker SUPERUSER; --"`
	doc := string(body)
	// Assert the plant lands before making it, so a JSON-tag rename on
	// ir.Index fails loudly here rather than silently tampering nothing and
	// leaving both legs of the pin green.
	if !strings.Contains(doc, benign) {
		t.Fatalf("fixture does not carry %s in its serialized schema — the plant would be a no-op:\n%s", benign, doc)
	}
	doc = strings.Replace(doc, benign, hostile, 1)
	if err := store.Put(ctx, path, strings.NewReader(doc)); err != nil {
		t.Fatal(err)
	}
	// The recorded fingerprint is deliberately NOT updated: the whole point
	// is that nothing on the bare-full path recomputes it.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal([]byte(doc), &probe); err != nil {
		t.Fatal(err)
	}
	if got := string(probe["schema_hash"]); got != `"recorded-hash-left-untouched"` {
		t.Fatalf("the plant disturbed schema_hash (%s); the forgery must leave it alone", got)
	}
}

// TestVerifyManifest_SchemaForgeryRefusedAtV5 is the ADR-0183 forgery pin.
//
// The audit's probe against unmodified code planted `Index.Method =
// "btree; CREATE ROLE attacker SUPERUSER; --"` into a SIGNED manifest's
// schema, left `schema_hash` untouched, and the HMAC signature still
// verified GREEN — because the canonical bytes covered the fingerprint
// STRING and zero bytes of the schema. Every Postgres DDL statement runs
// through ExecContext with no arguments, which pgx v5 forces onto the
// simple protocol, where `;` separates statements: the primitive is
// arbitrary SQL, not corruption.
//
// Two legs, and the second is the one that keeps the first honest:
//
//   - At v5 the tampered manifest must FAIL verification.
//   - At v4 — a real signature from a pre-ADR-0183 binary, reconstructed
//     byte-for-byte — the SAME tamper must still verify GREEN. That leg is
//     the permanent non-vacuity guard: it proves the v5 refusal comes from
//     the new schema binding rather than from the test disagreeing with
//     itself (if the plant ever stopped landing in the signed bytes, the
//     v5 leg would keep passing for the wrong reason and only this leg
//     would notice).
func TestVerifyManifest_SchemaForgeryRefusedAtV5(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)

	// --- v5 (this build): the tamper is refused. ---
	store := newMemStore()
	m := schemaBearingManifest()
	if err := WriteManifestAt(ctx, store, ManifestFileName, m); err != nil {
		t.Fatal(err)
	}
	if err := WriteManifestSig(ctx, store, ManifestFileName, m, 0, s); err != nil {
		t.Fatal(err)
	}
	// Control: the untampered chain verifies, read back off the store the
	// way restore reads it (a signature that could not verify its OWN
	// artifact would make the refusal below meaningless).
	clean, err := ReadManifestAt(ctx, store, ManifestFileName)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, clean, 0, s); err != nil {
		t.Fatalf("freshly signed manifest failed to verify on read-back: %v", err)
	}

	plantHostileIndexMethod(t, ctx, store, ManifestFileName)
	forged, err := ReadManifestAt(ctx, store, ManifestFileName)
	if err != nil {
		t.Fatal(err)
	}
	// The forgery is REAL: the decoded schema now carries the injected DDL,
	// and the recorded fingerprint still claims the original.
	if got := forged.Schema.Tables[0].Indexes[0].Method; !strings.Contains(got, "CREATE ROLE attacker") {
		t.Fatalf("plant did not reach the decoded schema (Method=%q) — the pin would be vacuous", got)
	}
	if err := VerifyManifest(ctx, store, ManifestFileName, forged, 0, s); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("SCHEMA FORGERY VERIFIED GREEN at v5: got %v, want ErrSignatureInvalid", err)
	}

	// --- v4 (pre-ADR-0183): the identical tamper verifies GREEN. ---
	old := newMemStore()
	om := schemaBearingManifest()
	if err := WriteManifestAt(ctx, old, ManifestFileName, om); err != nil {
		t.Fatal(err)
	}
	writeV4ManifestSig(t, ctx, old, ManifestFileName, om, 0, s.Key)
	plantHostileIndexMethod(t, ctx, old, ManifestFileName)
	oldForged, err := ReadManifestAt(ctx, old, ManifestFileName)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyManifest(ctx, old, ManifestFileName, oldForged, 0, s); err != nil {
		t.Fatalf("non-vacuity leg: the v4 signature should still verify over the tampered schema "+
			"(v4 covers zero bytes of it) — got %v. If this fails, the v5 leg above may be passing "+
			"for a reason unrelated to the schema binding", err)
	}
}

// TestSignManifestRefusesInProgressManifest pins the ordering invariant
// the canonical serialization's PartialState exemption rests on (see
// [refuseSigningInProgressManifest] and canonExempt in
// internal/ir/backup). Before this guard the invariant was read off three
// writers and asserted nowhere.
func TestSignManifestRefusesInProgressManifest(t *testing.T) {
	ctx := context.Background()
	s := testSigner(t)
	m := schemaBearingManifest()
	m.PartialState = irbackup.BackupStateInProgress
	if _, err := s.SignManifest(ctx, m, 0); err == nil {
		t.Fatal("signed an in_progress manifest — the PartialState exemption in the canonical bytes no longer holds")
	}
	// The complete and (backward-compatible) empty states both sign.
	for _, state := range []string{irbackup.BackupStateComplete, ""} {
		m.PartialState = state
		if _, err := s.SignManifest(ctx, m, 0); err != nil {
			t.Fatalf("partial_state=%q must sign: %v", state, err)
		}
	}
}
