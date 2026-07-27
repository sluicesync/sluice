// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0181 (audit SEC-2) pins that ride the REAL seal/open path.
//
// The unit pins in internal/ir/backup prove the renderer. These prove
// the renderer is REACHED: a real `Backup.Run --encrypt` seals chunks
// through the write-side gate, a real `Restore.Run` opens them through
// the read-side gate, and the version the MANIFEST RECORDS — not the one
// the reader prefers — is what selects the encoding on both sides. A
// test that computed an AAD itself and handed it to both halves would
// prove none of that.

package backup

import (
	"context"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// injectiveAADFixtureSchema is one table with a mixed column set, enough
// to make a wrong-payload restore visibly wrong rather than empty.
func injectiveAADFixtureSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public",
		Name:   "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "note", Type: ir.Text{}, Nullable: true},
		},
	}}}
}

func injectiveAADFixtureRows() map[string][]ir.Row {
	return map[string][]ir.Row{"orders": {
		{"id": int64(1), "note": "alpha"},
		{"id": int64(2), "note": "beta"},
		{"id": int64(3), "note": nil},
	}}
}

// newInjectiveAADEnvelope returns a cheap passphrase envelope. A fresh
// call yields the SAME KEK (fixed salt via the shared params), so the
// backup side and the restore side can be independent objects — which is
// what the CLI does.
func newInjectiveAADEnvelope(t *testing.T, params crypto.Argon2idParams) crypto.EnvelopeEncryption {
	t.Helper()
	env, err := crypto.NewPassphraseEnvelope("aad-path-pass", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return env
}

func injectiveAADParams(t *testing.T) crypto.Argon2idParams {
	t.Helper()
	p, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	p.Memory, p.Iterations, p.Parallelism = 1024, 1, 1
	return p
}

// restoreInjectiveAADFixture runs a real restore against store and
// returns the rows the target engine actually received.
func restoreInjectiveAADFixture(t *testing.T, store irbackup.Store, env crypto.EnvelopeEncryption) (map[string][]ir.Row, error) {
	t.Helper()
	tgt := newRestoreRecorderEngine("postgres")
	err := (&Restore{Target: tgt, TargetDSN: "tgt", Store: store, Envelope: env}).Run(context.Background())
	_, rows := tgt.snapshot()
	return rows, err
}

// relabelManifestFormatVersion rewrites ONLY the recorded FormatVersion
// of the chain's manifest — the store adversary's cheapest edit, and the
// exact move ADR-0154's downgrade-oracle pin exists to refuse.
func relabelManifestFormatVersion(t *testing.T, store irbackup.Store, fv int) {
	t.Helper()
	ctx := context.Background()
	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	m.FormatVersion = fv
	if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
		t.Fatalf("WriteManifestAt: %v", err)
	}
}

// TestBackupRestore_InjectiveChunkAAD_PathAndDowngradeOracle is the
// path pin. A real encrypted backup stamps FormatVersion 9 and seals its
// chunks with the injective AAD; a real restore opens them; and a
// manifest relabelled to v8 FAILS to open — proving the read side
// derives its AAD from the RECORDED version rather than from whatever
// this binary would prefer. Without that last assertion a green
// round-trip would be equally consistent with the reader ignoring the
// version entirely.
func TestBackupRestore_InjectiveChunkAAD_PathAndDowngradeOracle(t *testing.T) {
	ctx := context.Background()
	params := injectiveAADParams(t)
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := injectiveAADFixtureSchema()
	want := injectiveAADFixtureRows()

	b := &Backup{
		Source:     newBackupRecorderEngine("postgres", schema, want),
		SourceDSN:  "src",
		Store:      store,
		ChunkRows:  2, // two chunks, so the per-chunk AAD is exercised twice
		Encryption: &lineage.BackupEncryption{Envelope: newInjectiveAADEnvelope(t, params)},
	}
	if err := b.Run(ctx); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if m.FormatVersion != irbackup.FormatVersionInjectiveChunkAAD {
		t.Fatalf("fresh encrypted backup recorded FormatVersion %d; want %d — the rest of this test would be exercising the wrong tier",
			m.FormatVersion, irbackup.FormatVersionInjectiveChunkAAD)
	}
	if len(m.Tables) != 1 || len(m.Tables[0].Chunks) != 2 {
		t.Fatalf("expected 1 table / 2 chunks; got %+v", m.Tables)
	}

	t.Run("round-trips at the recorded version", func(t *testing.T) {
		got, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params))
		if err != nil {
			t.Fatalf("Restore.Run: %v", err)
		}
		rowSetEqual(t, "orders", got["orders"], want["orders"])
		for _, r := range got["orders"] {
			if r["id"].(int64) == 1 && r["note"] != "alpha" {
				t.Errorf("payload drift: id=1 note = %#v; want %q", r["note"], "alpha")
			}
		}
	})

	t.Run("relabelled to v8 it refuses to open", func(t *testing.T) {
		relabelManifestFormatVersion(t, store, irbackup.FormatVersionCDCPositionBinding)
		defer relabelManifestFormatVersion(t, store, irbackup.FormatVersionInjectiveChunkAAD)
		_, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params))
		if err == nil {
			t.Fatal("a v9-sealed chain opened under a v8 label — the read side is NOT deriving its AAD from the recorded version, so the dual-version path is itself an oracle")
		}
	})

	t.Run("restored to v9 it opens again", func(t *testing.T) {
		// The relabel is the ONLY thing that broke it — otherwise the
		// refusal above could be any incidental failure.
		got, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params))
		if err != nil {
			t.Fatalf("Restore.Run after restoring the label: %v", err)
		}
		rowSetEqual(t, "orders", got["orders"], want["orders"])
	})
}

// TestBackupRestore_RawConcatChain_PathAndUpgradeOracle is the OTHER
// direction, and the back-compat half of the contract: a chain sealed by
// the pre-v9 write path still restores under its own recorded version
// forever, and relabelling it UP to v9 fails.
//
// The pre-v9 chain is produced by the production resume ladder — a v7
// in-progress manifest on the store makes `Backup.Run` inherit that
// tier — rather than by hand-sealing chunks, so the tier the writer
// picks is under test too.
func TestBackupRestore_RawConcatChain_PathAndUpgradeOracle(t *testing.T) {
	ctx := context.Background()
	params := injectiveAADParams(t)
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	schema := injectiveAADFixtureSchema()
	want := injectiveAADFixtureRows()
	newBackup := func() *Backup {
		return &Backup{
			Source:     newBackupRecorderEngine("postgres", schema, want),
			SourceDSN:  "src",
			Store:      store,
			ChunkRows:  2,
			Encryption: &lineage.BackupEncryption{Envelope: newInjectiveAADEnvelope(t, params)},
		}
	}
	if err := newBackup().Run(ctx); err != nil {
		t.Fatalf("seed Run: %v", err)
	}

	// Rewind the chain to look like one an older binary left in progress:
	// recorded at v7, chain CEK re-wrapped under v7's CEK binding, no
	// completed tables so the resume re-seals every chunk.
	m, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	env := newInjectiveAADEnvelope(t, params)
	cek, err := lineage.UnwrapChainCEK(env, m.ChainEncryption.WrappedCEK, m)
	if err != nil {
		t.Fatalf("UnwrapChainCEK: %v", err)
	}
	m.FormatVersion = irbackup.FormatVersionChunkTableBinding
	m.PartialState = irbackup.BackupStateInProgress
	m.Tables = nil
	rewrapped, err := lineage.WrapChainCEK(env, cek, m)
	if err != nil {
		t.Fatalf("WrapChainCEK at v7: %v", err)
	}
	m.ChainEncryption.WrappedCEK = rewrapped
	if err := lineage.WriteManifest(ctx, store, m); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	if err := newBackup().Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	final, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest final: %v", err)
	}
	if final.FormatVersion != irbackup.FormatVersionChunkTableBinding {
		t.Fatalf("resumed pre-v9 chain recorded FormatVersion %d; want %d — a v9 stamp here would strand the chain's own chunks",
			final.FormatVersion, irbackup.FormatVersionChunkTableBinding)
	}

	t.Run("a v7 chain still restores on this binary", func(t *testing.T) {
		got, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params))
		if err != nil {
			t.Fatalf("Restore.Run: %v (newer sluice must always read older)", err)
		}
		rowSetEqual(t, "orders", got["orders"], want["orders"])
	})

	t.Run("relabelled UP to v9 it refuses to open", func(t *testing.T) {
		relabelManifestFormatVersion(t, store, irbackup.FormatVersionInjectiveChunkAAD)
		defer relabelManifestFormatVersion(t, store, irbackup.FormatVersionChunkTableBinding)
		if _, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params)); err == nil {
			t.Fatal("a v7-sealed chain opened under a v9 label — the reader is guessing the encoding instead of reading it")
		}
	})
}

// TestRestore_ParentTableForgery_ClosedAtV9 reproduces the audit's
// finding through the real restore path and pins the fix.
//
// The forgery: an adversary with source DDL creates a table whose NAME
// embeds `\nschema=public\ntable=orders`. Under raw concatenation its
// chunk seals to bytes that ALSO parse as "chunk <P'> of public.orders",
// so a store adversary who rewrites the manifest's File to P' and moves
// the chunk into public.orders' list gets the attacker's rows decrypted
// into the victim's table.
//
// The v7 sub-test asserts the forgery WORKS. That is not a bug being
// blessed — v5–v8 AAD bytes are frozen on-disk contract and cannot be
// repaired in place (ADR-0181's "why not migrate existing chains"), so
// the collision is a permanent property of those tiers. Asserting it is
// what makes the v9 sub-test meaningful: it proves the fixture really is
// the forgery and not some construction that never worked.
func TestRestore_ParentTableForgery_ClosedAtV9(t *testing.T) {
	// The victim table and the attacker's table share a column set — the
	// precondition that makes a cross-table chunk land silently.
	cols := []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "note", Type: ir.Text{}, Nullable: true},
	}
	const (
		attackerPath = "chunks/evil-0.jsonl.gz"
		attackerName = "evil\nschema=public\ntable=orders"
		// The File the adversary writes into the manifest so the victim's
		// read-side AAD reproduces the attacker's seal-side bytes.
		forgedPath = attackerPath + "\nschema=public\ntable=evil"
	)
	attackerRows := []ir.Row{{"id": int64(666), "note": "injected"}}

	for _, tier := range []struct {
		name        string
		fv          int
		wantForgery bool
	}{
		{"v7 raw concatenation: the forgery lands (frozen contract)", irbackup.FormatVersionChunkTableBinding, true},
		{"v9 injective encoding: the forgery is refused", irbackup.FormatVersionInjectiveChunkAAD, false},
	} {
		t.Run(tier.name, func(t *testing.T) {
			ctx := context.Background()
			params := injectiveAADParams(t)
			// An in-memory store: the forged path carries a newline, which a
			// real filesystem will not accept as a filename. Object stores
			// will, which is the deployment this attack targets.
			store := newMemStore()
			env := newInjectiveAADEnvelope(t, params)
			cek, err := crypto.GenerateCEK()
			if err != nil {
				t.Fatalf("GenerateCEK: %v", err)
			}

			m := &irbackup.Manifest{
				FormatVersion: tier.fv,
				SourceEngine:  "postgres",
				Kind:          irbackup.BackupKindFull,
				PartialState:  irbackup.BackupStateComplete,
				Schema:        &ir.Schema{Tables: []*ir.Table{{Schema: "public", Name: "orders", Columns: cols}}},
			}
			wrapped, err := lineage.WrapChainCEK(env, cek, m)
			if err != nil {
				t.Fatalf("WrapChainCEK: %v", err)
			}
			m.ChainEncryption = &irbackup.ChainEncryption{
				Algorithm:  crypto.AlgorithmAESGCM,
				Mode:       crypto.EncryptModePerChain,
				KEKMode:    env.Mode(),
				WrappedCEK: wrapped,
			}

			// SEAL under the attacker's own parent, through the production
			// write-side gate — this is what `backup full --encrypt` would
			// have produced for that table.
			aad := irbackup.ChunkAADForWrite(m, attackerPath, "public", attackerName, cek)
			info := writeExportChunk(t, store, attackerPath, cols, attackerRows, cek, aad, blobcodec.CodecZstd)

			// The store adversary's edit: relocate the ciphertext to the
			// forged path and list it under the LEGITIMATE public.orders.
			body, err := store.Get(ctx, attackerPath)
			if err != nil {
				t.Fatalf("read sealed chunk: %v", err)
			}
			if err := store.Put(ctx, forgedPath, body); err != nil {
				t.Fatalf("relocate sealed chunk: %v", err)
			}
			if err := body.Close(); err != nil {
				t.Fatalf("close sealed chunk: %v", err)
			}
			info.File = forgedPath
			m.Tables = []*irbackup.TableManifest{{
				Schema: "public", Name: "orders",
				RowCount: info.RowCount,
				Chunks:   []*irbackup.ChunkInfo{info},
			}}
			m.BackupID = irbackup.ComputeBackupID(m)
			if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
				t.Fatalf("WriteManifestAt: %v", err)
			}
			if err := lineage.UpdateLineageForManifestBestEffort(ctx, store, m, lineage.ManifestFileName, blobcodec.CodecZstd); err != nil {
				t.Fatalf("UpdateLineageForManifestBestEffort: %v", err)
			}

			got, err := restoreInjectiveAADFixture(t, store, newInjectiveAADEnvelope(t, params))
			landed := err == nil && len(got["orders"]) == 1 &&
				got["orders"][0]["id"] == int64(666) && got["orders"][0]["note"] == "injected"
			if landed != tier.wantForgery {
				t.Fatalf("attacker rows landed in public.orders = %v (want %v); restore err = %v, rows = %#v",
					landed, tier.wantForgery, err, got["orders"])
			}
			if !tier.wantForgery {
				t.Logf("v9 refusal surfaced as: %v", err)
			}
		})
	}
}
