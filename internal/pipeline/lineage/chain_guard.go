// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// # The chain concurrent-writer guard (ADR-0160)
//
// lineage.json is the one read-modify-write object every chain writer
// shares: full-backup finalize, incremental/stream rollovers, the
// rotation FSM's COMMIT, compaction's catalog swap, prune's floor
// advance, and the reconcile/rebuild repair paths all load it, mutate
// in memory, and Put it back whole. Nothing in that cycle stopped a
// SECOND writer (a duplicate cron `backup incremental`, a backup
// racing a compact/prune, an operator double-start) from interleaving:
// the last Put won and the loser's structural update silently vanished.
//
// The guard turns the cycle into a compare-and-swap without needing a
// conditional OVERWRITE primitive (gocloud.dev exposes only create-if-
// absent portably): each catalog write first CLAIMS the chain's next
// write-generation by creating `lineage.gen/g-<N+1>` via the store's
// optional [irbackup.ConditionalPutter] capability. The generation was
// OBSERVED at load time ([LoadLineageCatalogForUpdate] lists the
// markers BEFORE reading the catalog), so a claim that finds the slot
// taken proves another writer advanced the chain in between — a loud,
// coded refusal ([sluicecode.CodeBackupChainConflict]); the losing
// write never touches the catalog.
//
// Liveness: a writer that claims a slot and crashes before its Put
// leaves an orphaned marker, NOT a stale lock — the next writer's
// observation lists markers (not catalog content), sees the orphan as
// the new base, and claims the slot after it. No lease, no TTL, no
// clock heuristics (the stale-lockfile problem this design rejects).
//
// Degradation: stores without the capability (and S3-compatibles whose
// conditional-PUT support fails at runtime) keep today's unguarded
// last-write-wins behavior — the guard WARNs and steps aside rather
// than bricking backups (see claimChainGen). Which backends are
// guarded is recorded in ADR-0160's support matrix.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// ChainGenPrefix is the store directory holding the chain's write-
// generation claim markers. Lives at the lineage root, sibling to
// lineage.json; never referenced by manifests, never swept by
// compaction (which deletes only manifest/chunk artifacts).
const ChainGenPrefix = "lineage.gen/"

// chainGenKeepTrailing is how many trailing claim markers survive the
// post-write GC. Keeping a window (rather than only the newest) means
// a re-opened slot — the one way a stale writer could slip a silent
// lost update past the guard — requires that writer to stall mid-cycle
// across this many COMPLETE competing catalog writes, which the
// seconds-long load→write window makes practically unreachable.
// Documented as ADR-0160's accepted residual.
const chainGenKeepTrailing = 8

// chainGenMarker is the claim marker's JSON body. Purely forensic —
// existence is the claim; the body tells the operator who the other
// writer was when a conflict fires.
type chainGenMarker struct {
	ClaimedAt     time.Time `json:"claimed_at"`
	SluiceVersion string    `json:"sluice_version,omitempty"`
	Host          string    `json:"host,omitempty"`
	PID           int       `json:"pid,omitempty"`
}

// chainGenMarkerPath returns the marker path for generation n. Zero-
// padded to uint64 width so the listing sorts lexically == numerically.
func chainGenMarkerPath(n uint64) string {
	return fmt.Sprintf(ChainGenPrefix+"g-%020d", n)
}

// parseChainGenMarker extracts the generation from a marker path.
// Malformed names are ignored (ok=false): under-observing can only
// produce a SPURIOUS conflict (safe direction), never a silent pass.
func parseChainGenMarker(p string) (uint64, bool) {
	name, found := strings.CutPrefix(p, ChainGenPrefix)
	if !found {
		return 0, false
	}
	digits, found := strings.CutPrefix(name, "g-")
	if !found {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// observeChainGen returns the chain's current write-generation (the
// max claimed marker; 0 when none — legacy chains and fresh stores)
// and whether the store supports the guard at all. MUST run BEFORE the
// catalog read it protects: observe-then-load means a competing write
// between the two steps surfaces as a (spurious but safe) conflict,
// whereas load-then-observe would let this writer claim PAST the
// competitor and silently clobber its update.
func observeChainGen(ctx context.Context, store irbackup.Store) (gen uint64, guarded bool, err error) {
	if _, ok := store.(irbackup.ConditionalPutter); !ok {
		return 0, false, nil
	}
	paths, err := store.List(ctx, ChainGenPrefix)
	if err != nil {
		return 0, false, fmt.Errorf("chain guard: list %q: %w", ChainGenPrefix, err)
	}
	var maxGen uint64
	for _, p := range paths {
		if n, ok := parseChainGenMarker(p); ok && n > maxGen {
			maxGen = n
		}
	}
	return maxGen, true, nil
}

// LoadLineageCatalogForUpdate is [LoadLineageCatalog] for a caller
// that intends to WRITE the catalog back: it additionally observes the
// chain's write-generation (before the read — see observeChainGen) and
// stamps it on the returned catalog, arming [WriteLineageCatalog]'s
// compare-and-swap. Read-only callers keep LoadLineageCatalog (no
// marker listing, no stamp).
//
// The absent case ((nil, false, nil)) carries no stamp; the in-package
// seed paths re-observe via loadCatalogForUpdate instead.
func LoadLineageCatalogForUpdate(ctx context.Context, store irbackup.Store) (*Catalog, bool, error) {
	cat, ok, _, err := loadCatalogForUpdate(ctx, store)
	return cat, ok, err
}

// loadCatalogForUpdate is the in-package variant that also returns the
// observation itself, for callers that seed a FRESH catalog when none
// exists yet (UpdateLineageForManifest's first write) and so need to
// stamp it by hand via stampChainGuard.
func loadCatalogForUpdate(ctx context.Context, store irbackup.Store) (cat *Catalog, ok bool, gen chainGuardStamp, err error) {
	n, guarded, err := observeChainGen(ctx, store)
	if err != nil {
		return nil, false, chainGuardStamp{}, err
	}
	gen = chainGuardStamp{gen: n, observed: guarded}
	cat, ok, err = LoadLineageCatalog(ctx, store)
	if err != nil || !ok {
		return cat, ok, gen, err
	}
	stampChainGuard(cat, gen)
	return cat, true, gen, nil
}

// chainGuardStamp is the observation a catalog carries from load-for-
// update to write: the write-generation seen before the read, and
// whether the store supports the guard. Zero value = unstamped =
// unguarded write (test fixtures, legacy callers).
type chainGuardStamp struct {
	gen      uint64
	observed bool
}

// stampChainGuard arms cat's next WriteLineageCatalog with the
// observed generation.
func stampChainGuard(cat *Catalog, s chainGuardStamp) {
	cat.guardGen = s.gen
	cat.guardObserved = s.observed
}

// claimChainGen claims write-generation `claim` by conditionally
// creating its marker. Returns:
//
//   - (true, nil) — claim won; the caller owns the transition and may
//     overwrite the catalog.
//   - (false, err) — the slot was already taken: another writer
//     advanced the chain since this caller observed it. err is the
//     coded [sluicecode.CodeBackupChainConflict] refusal.
//   - (false, nil) — the conditional write failed for a NON-precondition
//     reason (transport hiccup, or an S3-compatible that doesn't honor
//     `If-None-Match`). The guard DEGRADES with a WARN rather than
//     bricking backups against such providers: the catalog write
//     proceeds unguarded, exactly today's shipped behavior. Named wart;
//     see ADR-0160 "runtime degradation".
func claimChainGen(ctx context.Context, cp irbackup.ConditionalPutter, claim uint64, sluiceVersion string) (bool, error) {
	host, _ := os.Hostname()
	body, err := json.Marshal(chainGenMarker{
		ClaimedAt:     time.Now().UTC(),
		SluiceVersion: sluiceVersion,
		Host:          host,
		PID:           os.Getpid(),
	})
	if err != nil {
		return false, fmt.Errorf("chain guard: marshal claim marker: %w", err)
	}
	markerPath := chainGenMarkerPath(claim)
	putErr := cp.PutIfAbsent(ctx, markerPath, bytes.NewReader(body))
	if putErr == nil {
		return true, nil
	}
	if errors.Is(putErr, irbackup.ErrPathExists) {
		return false, chainConflictError(claim, markerPath)
	}
	slog.WarnContext(
		ctx, "chain guard: conditional claim write failed for a non-conflict reason; proceeding UNGUARDED for this write (the store may not support conditional PUTs — concurrent chain writers will not be detected on it)",
		slog.String("marker", markerPath),
		slog.String("err", putErr.Error()),
	)
	return false, nil
}

// chainConflictError builds the loud, coded concurrent-writer refusal.
func chainConflictError(claim uint64, markerPath string) error {
	return sluicecode.Wrap(
		sluicecode.CodeBackupChainConflict,
		"check for a duplicate scheduler/cron entry or a concurrent backup/compact/prune on this chain, let it finish, then re-run",
		fmt.Errorf(
			"backup chain conflict: another writer advanced this chain's lineage while this operation was in flight "+
				"(write-generation %d was already claimed; its marker %q records the other writer's host/pid/time): "+
				"two concurrent writers interleaving one chain's catalog can silently corrupt its structure, so this "+
				"operation wrote NO catalog change — verify the other writer (a duplicate cron `backup incremental`, "+
				"a backup racing a `backup compact`/`backup prune`, or a double-started run) has finished, then re-run",
			claim, markerPath,
		),
	)
}

// gcChainGenMarkers deletes claim markers older than the trailing
// window (see chainGenKeepTrailing) after a successful catalog write.
// Best-effort: a leaked marker is a few bytes of forensic JSON, so
// failures log at DEBUG and never affect the write's outcome.
func gcChainGenMarkers(ctx context.Context, store irbackup.Store, claimed uint64) {
	if claimed <= chainGenKeepTrailing {
		return
	}
	floor := claimed - chainGenKeepTrailing
	paths, err := store.List(ctx, ChainGenPrefix)
	if err != nil {
		slog.DebugContext(ctx, "chain guard: marker GC list failed", slog.String("err", err.Error()))
		return
	}
	for _, p := range paths {
		if n, ok := parseChainGenMarker(p); ok && n < floor {
			if err := store.Delete(ctx, p); err != nil {
				slog.DebugContext(ctx, "chain guard: marker GC delete failed", slog.String("path", p), slog.String("err", err.Error()))
			}
		}
	}
}
