// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 217: `backup verify` returned rc=0 `all chunks OK` over chains
// `restore` refuses, because the schema-fingerprint preflight was the one
// restore check verify never made.
//
// It was not only a release-skew story. Changing ONE hex digit of a
// manifest's own `schema_hash` was invisible to verify on every released
// binary, while every one of them refused to restore that chain — so the
// command an operator runs on a schedule specifically so they never learn
// about a bad backup during a recovery reported healthy on a chain that
// could not be recovered. Pre-existing back to v0.103.2; found by the
// v0.104.4 regression cycle, which asked the question v0.104.3's own
// headline had made the house rule: does verify AGREE with restore?
//
// The third test is the one that keeps the fix honest. Verify must never
// be STRICTER than restore either — a bare full crosses a fingerprint
// epoch fine, because the single-manifest restore path never walks links
// and never fingerprints them. A verify that refused it would report a
// chain broken that restores row-exact, which is the same defect
// inverted.

package backup

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// fingerprintProbeSchema is the schema both manifests below carry. Its
// shape is irrelevant to the pin — only that it fingerprints to something
// stable, so a mutated hash is the ONLY difference between the green and
// refused cases.
func fingerprintProbeSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:    "orders",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{
			Name: "orders_pkey", Unique: true, ConstraintNamed: true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}}}
}

// mustSchemaHash is the honest fingerprint of [fingerprintProbeSchema] —
// computed, never hardcoded, so this file cannot drift into pinning a
// stale constant the way item 104's golden did.
func mustSchemaHash(t *testing.T) string {
	t.Helper()
	h, err := irbackup.ComputeSchemaHash(fingerprintProbeSchema())
	if err != nil {
		t.Fatalf("ComputeSchemaHash: %v", err)
	}
	return h
}

// mutateHexDigit returns h with its first character changed — the
// smallest possible edit to a recorded fingerprint, and the one the
// regression cycle used against six released binaries.
func mutateHexDigit(h string) string {
	if h == "" {
		return h
	}
	if h[0] == 'a' {
		return "b" + h[1:]
	}
	return "a" + h[1:]
}

// writeFingerprintChain writes a full plus (optionally) one incremental,
// with each manifest's recorded SchemaHash supplied by the caller so a
// test can record an honest hash or a mutated one. Returns the store.
func writeFingerprintChain(t *testing.T, fullHash, incrHash string, withIncremental bool) irbackup.Store {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	full := &irbackup.Manifest{
		FormatVersion: irbackup.BackupFormatVersion,
		CreatedAt:     time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC),
		SourceEngine:  "postgres",
		SluiceVersion: "0.104.5",
		Kind:          irbackup.BackupKindFull,
		Schema:        fingerprintProbeSchema(),
		SchemaHash:    fullHash,
		Tables:        []*irbackup.TableManifest{{Name: "orders", RowCount: 0}},
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("write full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, full, lineage.ManifestFileName, blobcodec.CodecGzip)

	if !withIncremental {
		return store
	}

	incr := &irbackup.Manifest{
		FormatVersion:  irbackup.BackupFormatVersion,
		CreatedAt:      time.Date(2026, 7, 29, 1, 0, 0, 0, time.UTC),
		SourceEngine:   "postgres",
		SluiceVersion:  "0.104.5",
		Kind:           irbackup.BackupKindIncremental,
		ParentBackupID: full.BackupID,
		Schema:         fingerprintProbeSchema(),
		SchemaHash:     incrHash,
	}
	incr.BackupID = irbackup.ComputeBackupID(incr)
	path := lineage.IncrementalManifestPrefix + "incr-0001.json"
	if err := lineage.WriteManifestAt(ctx, store, path, incr); err != nil {
		t.Fatalf("write incremental: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(ctx, store, incr, path, blobcodec.CodecGzip)
	return store
}

func TestVerifyBackup_SchemaFingerprintMismatch_RefusedLikeRestore(t *testing.T) {
	ctx := context.Background()
	honest := mustSchemaHash(t)

	t.Run("control: a chain whose fingerprints reproduce verifies clean", func(t *testing.T) {
		store := writeFingerprintChain(t, honest, honest, true)
		if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{}); err != nil {
			t.Fatalf("an intact chain must verify clean, or the refusals below prove nothing: %v", err)
		}
	})

	for _, tc := range []struct {
		name               string
		fullHash, incrHash string
	}{
		{"the root full's fingerprint", mutateHexDigit(honest), honest},
		{"an incremental's fingerprint", honest, mutateHexDigit(honest)},
	} {
		t.Run("one hex digit changed in "+tc.name, func(t *testing.T) {
			store := writeFingerprintChain(t, tc.fullHash, tc.incrHash, true)
			_, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{})
			if err == nil {
				t.Fatal("verify returned clean over a chain whose recorded schema fingerprint does not reproduce — restore refuses this chain with rc=3 and 0 rows, so a green here is the verify/restore disagreement Bug 217 filed")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBackupManifestInvalid {
				t.Fatalf("want %s (the code restore reports for this shape, so a script branching on it sees the same thing from both commands), got %v",
					sluicecode.CodeBackupManifestInvalid, err)
			}
		})
	}
}

// TestVerifyBackup_BareFullFingerprintMismatch_StaysGreen is the
// never-stricter-than-restore half, and it is not a technicality: a bare
// full is exactly how a chain crosses a fingerprint epoch today
// (roadmap item 102's blast-radius note — the single-manifest restore
// path never walks links, so it never fingerprints them). Refusing it
// here would report a chain broken that restores row-exact.
func TestVerifyBackup_BareFullFingerprintMismatch_StaysGreen(t *testing.T) {
	ctx := context.Background()
	store := writeFingerprintChain(t, mutateHexDigit(mustSchemaHash(t)), "", false)
	if _, err := VerifyBackupCodedReport(ctx, store, VerifyOptions{}); err != nil {
		t.Fatalf("a BARE FULL whose fingerprint does not reproduce must still verify clean, because restore's single-manifest path never checks it — verify must never refuse what restore accepts: %v", err)
	}
}
