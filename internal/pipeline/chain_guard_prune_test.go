// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// ADR-0161 pin for the PRUNE writer: a concurrent chain writer landing
// between prune's catalog load and its catalog commit makes the run
// refuse with the coded SLUICE-E-BACKUP-CHAIN-CONFLICT — and, because
// prune now commits the catalog BEFORE its delete pass (the compaction
// ordering), the refused run leaves the chain byte-untouched: catalog
// unchanged AND every manifest/chunk still present, so the documented
// "re-run" remediation actually works.

import (
	"context"
	"fmt"
	"io"
	"testing"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// casHookStore adds the conditional-create capability to the pipeline
// test memStore, with a one-shot "another writer claims the slot first"
// injection armed by the test after seeding.
type casHookStore struct {
	*memStore
	armConflict bool
	raced       bool
}

func (s *casHookStore) PutIfAbsent(ctx context.Context, path string, r io.Reader) error {
	if s.armConflict && !s.raced {
		s.raced = true
		s.data[path] = []byte(`{"claimed_at":"2026-07-14T00:00:00Z","host":"other-cron"}`)
	}
	if _, taken := s.data[path]; taken {
		return fmt.Errorf("casmem: %q: %w", path, irbackup.ErrPathExists)
	}
	return s.Put(ctx, path, r)
}

func TestPruneChain_ConcurrentWriterConflictLeavesChainUntouched(t *testing.T) {
	ctx := context.Background()
	store := &casHookStore{memStore: newMemStore()}
	seedLineageChain(t, store, 5)
	store.armConflict = true

	res, err := backup.PruneChain(ctx, store, backup.PruneOpts{KeepIncrementals: 2})
	if err == nil {
		t.Fatalf("PruneChain = %+v, nil; want the concurrent-writer refusal", res)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeBackupChainConflict {
		t.Fatalf("PruneChain err = %v; want code %s", err, sluicecode.CodeBackupChainConflict)
	}

	// Commit-first ordering: the refused prune deleted NOTHING and the
	// catalog is unchanged — a plain re-run succeeds.
	cat, okCat, cerr := lineage.LoadLineageCatalog(ctx, store)
	if cerr != nil || !okCat {
		t.Fatalf("post-refusal LoadLineageCatalog: ok=%v err=%v", okCat, cerr)
	}
	if n := len(cat.Segments[0].Incrementals); n != 5 {
		t.Errorf("post-refusal catalog incrementals = %d; want the seeded 5 (untouched)", n)
	}
	for i := 1; i <= 5; i++ {
		p := fmt.Sprintf("manifests/incr-000%d.json", i)
		exists, eerr := store.Exists(ctx, p)
		if eerr != nil || !exists {
			t.Errorf("post-refusal %s: exists=%v err=%v; want present (deletes must not precede the catalog commit)", p, exists, eerr)
		}
	}

	res, err = backup.PruneChain(ctx, store, backup.PruneOpts{KeepIncrementals: 2})
	if err != nil {
		t.Fatalf("PruneChain re-run after refusal: %v; want success (the promised remediation)", err)
	}
	if len(res.Pruned) != 3 {
		t.Errorf("re-run Pruned = %d; want 3", len(res.Pruned))
	}
}
