// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

// # The chain-tip guard — refusing a FORK, not just a lost write
//
// ADR-0160's write-generation CAS makes the catalog read-modify-write
// cycle atomic. It does NOT ask whether the link being appended still
// belongs at the END of the chain, and that is a different question with
// a different answer.
//
// The shape, reproduced 2026-07-28 against real AWS S3 **and** a local
// directory: two `sluice backup incremental` runs from an overlapping
// cron. Each resolves its parent BEFORE opening its CDC window (that is
// where the parent has to be resolved — the window's StartPosition comes
// from it), so both record the same `parent_backup_id` minutes before
// either commits. The winner writes its manifest and appends to
// lineage.json. The loser then loads THE WINNER'S catalog, appends
// beside it, claims the next generation cleanly — no CAS conflict,
// nothing was clobbered — and exits 0. Both processes log "incremental
// backup complete". The lineage now records
//
//	segments[0].incrementals = [incr-…-18de509f, incr-…-8207acf8]
//
// with BOTH carrying parent `b47a805f`: a FORK. `backup verify` and
// `restore` refuse it from that moment on ("branching/mis-stitched
// lineage"), permanently — and a forked chain keeps ACCEPTING
// incrementals, each chaining off whichever sibling won the tail, so an
// operator banks weeks of green backups on a chain that died at the fork
// and finds out during a recovery. Postgres slot exclusivity does not
// prevent it either: the loser blocks on the slot, waits, acquires it,
// re-reads the SAME changes from the slot restart position, and commits
// a sibling reporting the same change count.
//
// So the guard the class needs is not more CAS — it is a precondition on
// the LINK: an incremental may only be appended when its parent is still
// the open segment's tip. Two places enforce it, deliberately:
//
//  1. [AssertExtendsChainTip], called by the incremental / stream
//     orchestrators immediately BEFORE the manifest write, so the loser
//     of a race writes nothing durable at all (its change chunks are
//     already staged, but nothing references them and nothing reads
//     them); and
//  2. the same check inside [UpdateLineageForManifest], against the
//     catalog that call just loaded under the ADR-0160 observation. That
//     is the durable gate — every chain writer funnels through it, so a
//     future writer that forgets (1) is still covered, and it narrows
//     (1)'s window from "before the manifest write" to "the catalog read
//     immediately preceding the Put".
//
// What is NOT closed, stated rather than implied: two writers landing
// inside ADR-0160's claim→Put window can both read the pre-append
// catalog and both pass the check, and the generation claim does not
// arbitrate them either — a writer that observes the OTHER's marker
// claims the generation after it and also "wins". The outcome there is a
// LOST UPDATE (one link orphaned, both runs exit 0), not a fork: because
// the siblings start at the SAME parent position, whichever one survives
// leaves the chain contiguous, and the next incremental re-reads
// whatever the dropped window covered. Closing it needs a CAS keyed on
// the catalog's own recorded generation, which reintroduces the
// stale-lock liveness problem ADR-0160 rejected. Pinned as an explicit
// weaker assertion by
// TestIncrementalBackup_SimultaneousExtenders_NeverForkTheChain.
//
// Refusals are coded [sluicecode.CodeBackupChainConflict]: same class,
// same operator action (find the other writer), and the code's existing
// remediation already names the duplicate-cron cause.
//
// The tip's identity is READ from the tip manifest rather than cached in
// the catalog. A cached tip id is one more field for compaction, prune,
// rotation and reconcile to keep in sync, and a drifted copy would
// either refuse a healthy chain or wave a fork through — the two
// failures this guard exists to prevent. One manifest GET per committed
// link is a rounding error against the CDC window that produced it.

import (
	"context"
	"fmt"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// AssertExtendsChainTip refuses when parentBackupID is no longer the
// open segment's chain tip — i.e. another writer extended this chain
// while the caller's CDC window was open. Callers run it immediately
// before their manifest write so the loser of a race leaves no manifest
// behind; [UpdateLineageForManifest] re-checks it as the durable gate.
//
// A caller with no parent link (a pre-Phase-3 manifest carrying no
// ParentBackupID) is not checked, exactly as [BuildLineageChain] skips
// the parent-link validation for those. An absent lineage.json is not
// checked either: there is no recorded tip to compare against, and the
// durable gate re-derives one from the catalog it seeds.
func AssertExtendsChainTip(ctx context.Context, rootStore irbackup.Store, parentBackupID string) error {
	if parentBackupID == "" {
		return nil
	}
	cat, ok, err := LoadLineageCatalog(ctx, rootStore)
	if err != nil || !ok {
		return err
	}
	seg := &cat.Segments[len(cat.Segments)-1] // the open segment
	return assertExtendsSegmentTip(ctx, rootStore, seg, parentBackupID, "")
}

// assertExtendsSegmentTip is the shared precondition: parentBackupID
// must be the BackupID of seg's current last link. orphanPath, when
// non-empty, names a manifest the caller already wrote durably before
// the fork was detected, so the refusal can tell the operator it is
// inert debris rather than leave them guessing.
func assertExtendsSegmentTip(ctx context.Context, rootStore irbackup.Store, seg *Segment, parentBackupID, orphanPath string) error {
	if parentBackupID == "" {
		return nil
	}
	tip, err := segmentTipID(ctx, rootStore, seg)
	if err != nil {
		return err
	}
	if tip == "" || tip == parentBackupID {
		return nil
	}
	return chainForkRefusal(parentBackupID, tip, orphanPath)
}

// segmentTipID returns the BackupID of seg's current chain tip: its last
// recorded incremental, or the segment's full when it has none yet.
// Returns ("", nil) only when the segment names no manifest at all (a
// catalog shape the chain walk refuses on its own terms) — never a
// silent pass over a manifest that exists but cannot be read, which
// would be the guard failing open on exactly the corruption it guards.
func segmentTipID(ctx context.Context, rootStore irbackup.Store, seg *Segment) (string, error) {
	path := seg.FullManifestPath
	if n := len(seg.Incrementals); n > 0 {
		path = seg.Incrementals[n-1]
	}
	if path == "" {
		return "", nil
	}
	ss := seg.Store(rootStore)
	exists, err := ss.Exists(ctx, path)
	if err != nil {
		return "", fmt.Errorf("chain tip guard: inspect %q: %w", path, err)
	}
	if !exists {
		// The catalog names a link that is not on disk. That is a broken
		// chain the restore walk will refuse; it is NOT evidence about
		// forking, so this guard declines to speak rather than
		// misattribute. (Reachable in practice only for a catalog whose
		// manifest write never landed.)
		return "", nil
	}
	m, err := ReadManifestAt(ctx, ss, path)
	if err != nil {
		return "", fmt.Errorf("chain tip guard: read chain tip manifest %q: %w", path, err)
	}
	return ManifestBackupID(m), nil
}

// chainForkRefusal builds the loud, coded fork refusal. It names the
// cause CONCRETELY (a second writer committed while this run's window
// was open) because the generic "mis-stitched lineage" wording sent
// operators hunting a prune/compact that never ran.
func chainForkRefusal(parentBackupID, tipID, orphanPath string) error {
	orphan := " Nothing durable was written for this run."
	if orphanPath != "" {
		orphan = fmt.Sprintf(
			" This run's manifest %q is already on disk but is NOT referenced by lineage.json — it is inert"+
				" debris, safe to delete, and restore will not read it.", orphanPath,
		)
	}
	return sluicecode.Wrap(
		sluicecode.CodeBackupChainConflict,
		"another `backup incremental`/`backup stream` extended this chain while this run's CDC window was open — "+
			"check for overlapping cron/scheduler entries on the same chain, let the other run finish, then re-run",
		fmt.Errorf(
			"backup chain fork refused: this link chains off %q, but the chain's tip is now %q — another writer "+
				"extended this chain while this run's CDC window was open. The parent is resolved BEFORE the window "+
				"opens, so two overlapping `backup incremental` runs both resolve the SAME parent and the second one "+
				"would commit a SIBLING of the first: two incrementals sharing one parent is a FORK, which `backup "+
				"verify` and `restore` refuse permanently while later incrementals keep chaining off whichever sibling "+
				"won the tail.%s Re-run to extend from %q",
			parentBackupID, tipID, orphan, tipID,
		),
	)
}
