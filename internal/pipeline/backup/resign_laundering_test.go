// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"strings"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestMaintenanceRefusesToLaunderATamperedChain is the behavioural half
// of audit 2026-09-06 S-1.
//
// THE ATTACK. An adversary who can write the backup store — the exact
// adversary ADR-0152/0154 are written against — edits a surviving
// manifest. That edit fails signature verification at restore, so the
// chain is "broken" from the operator's point of view but the DATA is
// the attacker's. Then the operator's scheduled `backup prune` runs with
// the chain's key material, as it must in order to re-sign the
// restructured result, and ResignLineage mints fresh valid signatures
// over the attacker's content. The next restore reports every signature
// verified.
//
// Nothing downstream catches it: verifySchemaHashes and verifyBackupIDs
// recompute KEYLESS hashes an adversary restates, and a chunk's GCM AAD
// binds it to manifest identity and its own path, not to the chunk LIST
// or the schema.
//
// The tamper here is a manifest field the signature covers — enough to
// make the existing signature fail, which is the precondition the whole
// attack rests on. The point of the cell is not which field: it is that
// maintenance REFUSES rather than re-signing.
func TestMaintenanceRefusesToLaunderATamperedChain(t *testing.T) {
	ctx := context.Background()

	build := func(t *testing.T) (irbackup.Store, *lineage.Signer) {
		t.Helper()
		store := newMemStore()
		base := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
		seedSegmentsWithGapsOpts(t, store, base, []time.Duration{time.Hour, time.Hour}, segmentSeedOpts{})
		signer := signChain(t, store)
		return store, signer
	}

	// The CONTROL runs first and is what makes the cells below mean
	// anything: an UNtampered signed chain must still prune. A door that
	// refuses everything would pass every tamper cell while having broken
	// routine maintenance for every operator.
	t.Run("control: an untampered signed chain still prunes", func(t *testing.T) {
		store, signer := build(t)
		if _, err := PruneChain(ctx, store, PruneOpts{
			KeepIncrementals: 1, Signer: signer,
		}); err != nil {
			t.Fatalf("a correctly-signed chain was refused by the anti-laundering door: %v", err)
		}
	})

	tamper := func(t *testing.T, store irbackup.Store) {
		t.Helper()
		m, err := lineage.ReadManifestAt(ctx, store, lineage.ManifestFileName)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		// A signed field, changed after signing. SourceEngine is folded
		// into the canonical bytes, so the recorded .sig no longer
		// verifies — which is precisely the state the attacker leaves
		// behind and the state maintenance must not re-sign away.
		m.SourceEngine = "tampered"
		if err := lineage.WriteManifestAt(ctx, store, lineage.ManifestFileName, m); err != nil {
			t.Fatalf("write tampered manifest: %v", err)
		}
	}

	assertRefused := func(t *testing.T, err error, op string) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s accepted a signed chain whose signatures do not verify — it would have "+
				"re-signed the tampered content, and the next restore would report every signature "+
				"verified", op)
		}
		if !strings.Contains(err.Error(), "laundering") {
			t.Errorf("%s refused, but not with the anti-laundering door's message (so it may have "+
				"refused for an unrelated reason and the door is untested): %v", op, err)
		}
	}

	t.Run("prune refuses a tampered signed chain", func(t *testing.T) {
		store, signer := build(t)
		tamper(t, store)
		_, err := PruneChain(ctx, store, PruneOpts{KeepIncrementals: 1, Signer: signer})
		assertRefused(t, err, "backup prune")
	})

	t.Run("compact refuses a tampered signed chain", func(t *testing.T) {
		store, signer := build(t)
		tamper(t, store)
		_, err := CompactChain(ctx, store, CompactOpts{MergeWindow: 24 * time.Hour, Signer: signer})
		assertRefused(t, err, "backup compact")
	})

	t.Run("a dry run still reports without refusing", func(t *testing.T) {
		// Dry runs change nothing and re-sign nothing, so they must stay
		// usable ON a tampered chain — that is how an operator inspects
		// what maintenance WOULD do while investigating.
		store, signer := build(t)
		tamper(t, store)
		if _, err := PruneChain(ctx, store, PruneOpts{
			KeepIncrementals: 1, Signer: signer, DryRun: true,
		}); err != nil && strings.Contains(err.Error(), "laundering") {
			t.Errorf("a --dry-run prune was refused by the anti-laundering door; a dry run re-signs "+
				"nothing and is exactly what an operator runs while investigating: %v", err)
		}
	})
}
