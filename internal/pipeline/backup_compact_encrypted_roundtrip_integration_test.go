//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 214 / roadmap item 94 — the encrypted `backup compact` → `restore`
// round trip, which had never existed.
//
// Compaction's whole bug history (Bugs 95, 139) is about REFUSALS TO
// MERGE, and every one of them was closed against a PLAINTEXT chain. That
// is exactly why this cell was empty, and why an orphan sweep that
// deleted the chain-root `manifest.json` — the ADR-0152 CEK-wrap identity
// and the only recorded copy of a passphrase chain's Argon2id salt —
// could ship: `compact` exited 0 with `groups_merged=1 segments_removed=3`
// while `verify` and `restore` both refused at `unwrap chain cek` with
// zero rows, minutes after that same chain restored 230/230 md5-exact.
//
// So this file restores. Asserting compaction's return value would
// reproduce the original mistake — the defect IS an operation reporting
// success over an unreadable artifact, so only a real restore into a real
// target can pin it. The matrix is the two encryption modes that fail
// DIFFERENTLY (per-chain refuses at the chain-CEK unwrap; per-chunk gets
// further and refuses with SLUICE-E-BACKUP-CHUNK-AUTH-FAILED on every
// chunk) plus the plaintext control that was unaffected all along.
//
// The restore-side envelope is built by [bug214ReadEnvelope], which
// reads the CHAIN-ROOT manifest for its Argon2id params exactly as the
// production CLI's buildReadEnvelope does, fresh-salt fallback included.
// That is deliberate: it is the read path the deletion breaks, so a pin
// that shortcut it (reusing the backup-side envelope object) would go
// green straight over the bug.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bug214ReadEnvelope builds the restore-side envelope the way the
// PRODUCTION CLI does — [EncryptionFlags.buildReadEnvelope]: read the
// chain-root manifest, derive the KEK from the Argon2id params recorded
// there, and FALL BACK TO FRESH DEFAULTS when the root manifest is absent
// or records none.
//
// That fallback is the load-bearing difference from the shared
// [envelopeFromManifest] helper, which t.Fatalf's on a missing root. The
// production CLI does not fatal — it silently builds an envelope with a
// fresh salt and hands it to the restore, which then fails at `unwrap
// chain cek` holding a perfectly good passphrase. Reproducing the
// fallback is what makes this a pin on the RESTORE rather than on a test
// helper's guard, and it is what makes the failure message match the one
// operators actually saw.
func bug214ReadEnvelope(t *testing.T, store irbackup.Store, passphrase string) crypto.EnvelopeEncryption {
	t.Helper()
	root, err := lineage.ReadRootManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadRootManifest: %v", err)
	}
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	if root != nil && root.ChainEncryption != nil && root.ChainEncryption.Argon2id != nil {
		p := root.ChainEncryption.Argon2id
		params = crypto.Argon2idParams{
			Salt:        p.Salt,
			Memory:      p.Memory,
			Iterations:  p.Iterations,
			Parallelism: p.Parallelism,
			KeyLen:      p.KeyLen,
		}
	}
	env, err := crypto.NewPassphraseEnvelope(passphrase, params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return env
}

// bug214Sums is a value fingerprint, not just a row count, so a restore
// that lands the right NUMBER of wrong rows still fails.
type bug214Sums struct {
	n, sumID, sumBalance int64
}

func bug214Read(t *testing.T, dsn string) bug214Sums {
	t.Helper()
	return bug214Sums{
		n:          pgQueryOne[int64](t, dsn, "SELECT COUNT(*) FROM ledger"),
		sumID:      pgQueryOne[int64](t, dsn, "SELECT COALESCE(SUM(id),0) FROM ledger"),
		sumBalance: pgQueryOne[int64](t, dsn, "SELECT COALESCE(SUM(balance),0) FROM ledger"),
	}
}

// bug214SeedEncryptedRotatedChain is the Bug-214 view of the shared
// chain-shaping harness ([seedRotatedChainFixture], in
// backup_chain_shaping_encrypted_matrix_integration_test.go): a PG
// source, a fresh target, and a multi-segment encrypted lineage built by
// a real rotating CDC stream. It was the original of that harness and is
// kept as a named projection rather than a second copy — the matrix
// generalises this file, it does not duplicate it.
//
// mode is "" for the plaintext control leg.
func bug214SeedEncryptedRotatedChain(
	t *testing.T,
	mode string,
	passphrase string,
) (sourceDSN, targetDSN string, store *blobcodec.LocalStore) {
	t.Helper()
	f := seedRotatedChainFixture(t, mode, passphrase)
	return f.sourceDSN, f.targetDSN, f.store
}

// TestBug214_EncryptedCompact_RestoresRowExact is THE pin: an encrypted
// multi-segment chain compacts and then RESTORES row-exact, in per-chain
// AND per-chunk mode, with the plaintext control alongside.
//
// Pre-fix, the two encrypted legs both fail — per-chain at `unwrap chain
// cek`, per-chunk at SLUICE-E-BACKUP-CHUNK-AUTH-FAILED — while the
// plaintext leg passes, which is precisely the asymmetry that kept the
// bug hidden.
func TestBug214_EncryptedCompact_RestoresRowExact(t *testing.T) {
	const passphrase = "compact-round-trip-passphrase"
	legs := []struct {
		name string
		mode string // "" = plaintext control
	}{
		{"per-chain", crypto.EncryptModePerChain},
		{"per-chunk", crypto.EncryptModePerChunk},
		{"plaintext-control", ""},
	}
	for _, leg := range legs {
		t.Run(leg.name, func(t *testing.T) {
			sourceDSN, targetDSN, store := bug214SeedEncryptedRotatedChain(t, leg.mode, passphrase)

			pre, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
			if err != nil || !ok {
				t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
			}
			if len(pre.Segments) < 3 {
				t.Fatalf("pre-compact segments = %d; want >= 3 (rotation didn't materialize)", len(pre.Segments))
			}

			// Oracle BEFORE compaction: the source's final state.
			oracle := bug214Read(t, sourceDSN)
			if oracle.n < 2 {
				t.Fatalf("source oracle has %d rows; the churn produced nothing", oracle.n)
			}

			// Compaction runs the way the CLI runs it for an operator who
			// did NOT pass --encrypt (the common case, and the one that
			// shipped the bug): no envelope, so the readability gate has
			// only the chain's identity to check.
			res, err := backup.CompactChain(context.Background(), store, backup.CompactOpts{
				MergeWindow: time.Hour,
			})
			if err != nil {
				t.Fatalf("CompactChain: %v", err)
			}
			if res.GroupsMerged < 1 {
				t.Fatalf("GroupsMerged = %d; want >= 1", res.GroupsMerged)
			}
			post, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
			if len(post.Segments) >= len(pre.Segments) {
				t.Fatalf("post-compact segments = %d; want < %d (no merge happened, so this pin would be vacuous)",
					len(post.Segments), len(pre.Segments))
			}

			// THE assertion: restore the compacted chain into a fresh
			// target through the production read path.
			var envRestore crypto.EnvelopeEncryption
			if leg.mode != "" {
				envRestore = bug214ReadEnvelope(t, store, passphrase)
			}
			eng, _ := engines.Get("postgres")
			if err := (&backup.ChainRestore{
				Target: eng, TargetDSN: targetDSN, Store: store, Envelope: envRestore,
			}).Run(context.Background()); err != nil {
				t.Fatalf("Bug 214: ChainRestore of the COMPACTED %s chain failed — compact reported success over an unreadable chain: %v", leg.name, err)
			}
			got := bug214Read(t, targetDSN)
			if got != oracle {
				t.Fatalf("restored compacted %s chain != source oracle: got %+v want %+v", leg.name, got, oracle)
			}
			t.Logf("Bug-214 %s pin PROVEN: %d-segment chain compacted to %d and restored row-exact (%+v)",
				leg.name, len(pre.Segments), len(post.Segments), oracle)
		})
	}
}

// TestBug214_EncryptedCompact_DryRunIsInert pins that `--dry-run` does
// not touch the chain identity (it never did — and it must not start
// to). A dry run followed by a real restore proves the chain is exactly
// as restorable as it was.
func TestBug214_EncryptedCompact_DryRunIsInert(t *testing.T) {
	const passphrase = "dry-run-inert-passphrase"
	sourceDSN, targetDSN, store := bug214SeedEncryptedRotatedChain(t, crypto.EncryptModePerChain, passphrase)

	pre, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	oracle := bug214Read(t, sourceDSN)

	res, err := backup.CompactChain(context.Background(), store, backup.CompactOpts{
		MergeWindow: time.Hour,
		DryRun:      true,
	})
	if err != nil {
		t.Fatalf("CompactChain --dry-run: %v", err)
	}
	if res.GroupsMerged < 1 {
		t.Fatalf("--dry-run planned %d merges; want >= 1 (otherwise this pin is vacuous)", res.GroupsMerged)
	}

	post, ok, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if !ok || len(post.Segments) != len(pre.Segments) {
		t.Errorf("--dry-run rewrote the catalog: %d segments, want %d", len(post.Segments), len(pre.Segments))
	}
	if ex, _ := store.Exists(context.Background(), lineage.ManifestFileName); !ex {
		t.Fatal("--dry-run deleted the chain-root manifest")
	}

	eng, _ := engines.Get("postgres")
	if err := (&backup.ChainRestore{
		Target: eng, TargetDSN: targetDSN, Store: store,
		Envelope: bug214ReadEnvelope(t, store, passphrase),
	}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore after --dry-run: %v", err)
	}
	if got := bug214Read(t, targetDSN); got != oracle {
		t.Errorf("restore after --dry-run != source oracle: got %+v want %+v", got, oracle)
	}
}
