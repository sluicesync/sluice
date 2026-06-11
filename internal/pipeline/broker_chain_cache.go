// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// brokerChainCache memoizes the broker's lineage-chain walk across
// ticks. [buildBrokerChain] costs one store GET + JSON decode PER
// MANIFEST in the chain; a long-running `backup stream` at a
// few-minute rollover grows the chain into the thousands, so an
// *idle* broker tick (default every 30s) was O(chain-length) GETs
// against blob storage. The cache keys the walked chain on the only
// two objects that can change while the chain's content changes:
//
//   - lineage.json's raw bytes. Every structural lineage change is a
//     [writeLineageCatalog] rewrite of this single object: rotation's
//     segment-append COMMIT, compaction's atomic catalog swap, prune's
//     restorable-floor advance, and each incremental append (which
//     also bumps the open segment's EndPosition + UpdatedAt).
//   - the tail (open) manifest's raw bytes. The per-chunk checkpoint
//     re-writes the SAME manifest path as chunks accumulate, and its
//     companion lineage.json update is BEST-EFFORT — so the tail
//     manifest is the one chain object that can change while the
//     catalog stays byte-identical. Every other manifest is immutable
//     once a child links off it (a re-rooted/compacted manifest set
//     always arrives with a catalog swap).
//
// A stale chain serving a replay decision is a correctness risk, so
// invalidation is conservative: identity bytes are read BEFORE the
// rebuild walk, meaning a writer racing the rebuild can only leave the
// cached key OLDER than the cached chain — the next tick then
// mismatches and rebuilds again. Any read/parse hiccup, and the
// catalog-less legacy shape (walk-discovered, no cheap head object),
// bypass the cache entirely and take the full walk, including its
// loud refusals.
//
// NOT goroutine-safe: confined to [SyncFromBackup.Run]'s single
// goroutine (same posture as the chainCEK field). One-shot restore
// paths keep calling [buildLineageChain] directly — they walk once
// and have nothing to amortize.
type brokerChainCache struct {
	chain        []segmentRecord
	catalogBytes []byte // raw lineage.json the chain was keyed on
	tailBytes    []byte // raw tail-manifest bytes the chain was keyed on
}

// get returns the lineage chain, reusing the cached walk when the
// chain identity (lineage.json + tail manifest, see the type comment)
// is byte-identical to the cached one. An idle hit costs exactly two
// store GETs regardless of chain length; any mismatch rebuilds the
// whole chain via [buildBrokerChain].
func (c *brokerChainCache) get(ctx context.Context, store irbackup.Store) ([]segmentRecord, error) {
	catalogBytes, tailBytes, identified := readChainIdentity(ctx, store)
	if identified && c.chain != nil &&
		bytes.Equal(catalogBytes, c.catalogBytes) && bytes.Equal(tailBytes, c.tailBytes) {
		return c.chain, nil
	}
	c.invalidate()
	chain, err := buildBrokerChain(ctx, store)
	if err != nil {
		return nil, err
	}
	if identified && len(chain) > 0 {
		c.chain, c.catalogBytes, c.tailBytes = chain, catalogBytes, tailBytes
	}
	return chain, nil
}

// invalidate drops the cached chain; the next get rebuilds.
func (c *brokerChainCache) invalidate() {
	c.chain, c.catalogBytes, c.tailBytes = nil, nil, nil
}

// readChainIdentity GETs the two chain-identity objects raw:
// lineage.json and the open segment's tail manifest (its last
// incremental, or its full when no incrementals exist yet).
// identified == false on any absence / parse / IO failure — "no cheap
// identity, bypass the cache": the legacy catalog-less shape is
// walk-discovered with no single head object, and an unreadable
// catalog must flow through [buildBrokerChain]'s loud refusal rather
// than a cache decision.
func readChainIdentity(ctx context.Context, store irbackup.Store) (catalogBytes, tailBytes []byte, identified bool) {
	catalogBytes, err := readAllAt(ctx, store, LineageCatalogFileName)
	if err != nil {
		return nil, nil, false
	}
	var cat LineageCatalog
	if err := json.Unmarshal(catalogBytes, &cat); err != nil || len(cat.Segments) == 0 {
		return nil, nil, false
	}
	seg := &cat.Segments[len(cat.Segments)-1]
	tailPath := seg.FullManifestPath
	if n := len(seg.Incrementals); n > 0 {
		tailPath = seg.Incrementals[n-1]
	}
	tailBytes, err = readAllAt(ctx, seg.store(store), tailPath)
	if err != nil {
		return nil, nil, false
	}
	return catalogBytes, tailBytes, true
}

// readAllAt reads the whole object at path. Callers here treat any
// failure (including a missing object) as "no identity available".
func readAllAt(ctx context.Context, store irbackup.Store, path string) ([]byte, error) {
	rc, err := store.Get(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}
