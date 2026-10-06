// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// catalogWriteFailStore fails the lineage-catalog Put. ambiguous: the write
// lands and THEN errors (a timed-out PUT that succeeded); otherwise it is
// refused outright (a CAS conflict).
type catalogWriteFailStore struct {
	*memStore
	ambiguous bool
}

func (s *catalogWriteFailStore) Put(ctx context.Context, path string, r io.Reader) error {
	if path != lineage.LineageCatalogFileName {
		return s.memStore.Put(ctx, path, r)
	}
	if s.ambiguous {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if err := s.memStore.Put(ctx, path, bytes.NewReader(b)); err != nil {
			return err
		}
		return errors.New("catalog PUT timed out after the write landed")
	}
	return errors.New("catalog PUT refused: a concurrent writer moved the catalog")
}

// TestCompactChain_CatalogWriteFailure_CleansOrKeepsByTheStoredCatalog pins
// both sides of the merged-copy cleanup's trade-off. commitCompactedChain
// mutates the in-memory catalog before writing it, so the cleanup must not
// decide liveness from that catalog: on a REFUSED write the merged copy is
// garbage and is removed; on an AMBIGUOUS write (the Put landed) it is the
// chain's live segment and is kept, and the chain still reads.
func TestCompactChain_CatalogWriteFailure_CleansOrKeepsByTheStoredCatalog(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		name := "refused write: the merged copy is removed"
		if ambiguous {
			name = "ambiguous write: the merged copy is the live segment and is kept"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := &catalogWriteFailStore{memStore: newMemStore(), ambiguous: ambiguous}
			now := time.Date(2026, 5, 26, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store.memStore, now, usersSchema(), nil, framedRows)
			_, err := CompactChain(ctx, store, CompactOpts{
				MergeWindow: 2 * time.Hour, SmartCompaction: true, PKStrategy: PKStrategyPK,
				Now: func() time.Time { return now.Add(10 * time.Hour) },
			})
			if err == nil || !strings.Contains(err.Error(), "rewrite lineage catalog") {
				t.Fatalf("want the catalog-write failure, got %v", err)
			}
			merged := 0
			for k := range store.data {
				if strings.HasPrefix(k, mergedSegmentDirPrefix) {
					merged++
				}
			}
			if !ambiguous && merged != 0 {
				t.Fatalf("a refused catalog write leaked %d merged-copy files", merged)
			}
			if ambiguous && merged == 0 {
				t.Fatal("an ambiguous catalog write deleted the segment the stored catalog now references")
			}
			chain, err := lineage.BuildLineageChain(ctx, store, nil)
			if err != nil {
				t.Fatalf("the chain no longer walks: %v", err)
			}
			if err := NewSeveredTransactionDoor(store, nil, nil).Check(ctx, chain); err != nil {
				t.Fatalf("the chain no longer passes the door: %v", err)
			}
		})
	}
}
