//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0087 / Bug 139 compact integration pins (Postgres, testcontainers).
//
// The Bug-139 shape: a rotation-born segment whose creating session never
// committed an incremental (source idle at stream stop, or crash/end at the
// rotation boundary) carries no IncrementalCoverageStart stamp; it resolves
// to its full's anchor S, a few WAL bytes past the prior segment's
// EndPosition (P_N). Pre-ADR-0087, compact saw the "gap" and REFUSED the
// whole run with a corruption-blaming message. ADR-0087:
//
//   - compact SPLITS the merge group at the gap (WARN-naming the boundary)
//     instead of refusing — the surrounding contiguous runs still merge and
//     the chain stays fully restorable (the idle-stop leg below);
//   - the next stream/incremental resume of such a stamp-less open segment
//     replays from P_N, stamping IncrementalCoverageStart = P_N and healing
//     the gap so the WHOLE chain compacts (the resume-heal leg below).

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bug139Sums is the value-content fingerprint (not just a row count) used
// to assert restore equality against the source oracle.
type bug139Sums struct {
	n, sumID, sumBalance int64
}

func bug139Read(t *testing.T, dsn string) bug139Sums {
	t.Helper()
	return bug139Sums{
		n:          pgQueryOne[int64](t, dsn, "SELECT COUNT(*) FROM accounts"),
		sumID:      pgQueryOne[int64](t, dsn, "SELECT COALESCE(SUM(id),0) FROM accounts"),
		sumBalance: pgQueryOne[int64](t, dsn, "SELECT COALESCE(SUM(balance),0) FROM accounts"),
	}
}

// driveIdleStopRotationChain produces the exact Bug-139 idle-stop shape:
// an AGE-based rotation fires on the drain rollover AFTER the source has
// gone idle, so the rotation snapshot S is taken at a quiesced boundary —
// still a few WAL bytes past P_N (slot/snapshot bookkeeping records), but
// with no user events in (P_N, S] — and the freshly-opened segment
// receives NO rollover before the graceful stop, leaving a trailing
// zero-incremental rotation-born segment whose boundary reads as a gap.
//
// The timing must keep age rotation from firing WHILE writes still flow
// (which would make S > P_N and commit an overlap incremental into the new
// segment). So: churn briefly (< RetainRotateAt, no rotation yet), stop,
// let the segment age PAST the threshold while idle, then a single trailing
// write triggers one committed rollover whose age check rotates with S ==
// P_N. The shape is timing-sensitive, so the caller wraps this in a bounded
// retry on a fresh chain.
func driveIdleStopRotationChain(t *testing.T, sourceDSN string, store *blobcodec.LocalStore) {
	t.Helper()
	eng, _ := engines.Get("postgres")

	stream := &BackupStream{
		Source:         eng,
		SourceDSN:      sourceDSN,
		Store:          store,
		RolloverWindow: 600 * time.Millisecond,
		// Age-based rotation: a segment older than 3s rotates on its next
		// COMMITTED rollover. We engineer that committed rollover to be a
		// post-idle trailing write, so no user events land in (P_N, S].
		RetainRotateAt:     3 * time.Second,
		RolloverMaxChanges: 100,
		RolloverMaxBytes:   1 << 30,
		ChunkChanges:       50,
		SluiceVersion:      "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	// Phase 1: sustained churn spanning multiple age windows so several
	// CONTIGUOUS (stamped) segments form via age rotation WHILE writes flow
	// (each keeps its (P_N, S] overlap and stamps IncrementalCoverageStart
	// = P_N — contiguous, mergeable). This gives compact a pre-boundary run
	// of >= 2 segments to actually merge.
	for i := 0; i < 64; i++ {
		applyDDL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO accounts (email, balance) VALUES ('row%03d@example.com', %d);`, i, i,
		))
		time.Sleep(120 * time.Millisecond)
	}

	// Phase 2: stop writing and let the now-open segment age PAST the 3s
	// threshold while idle (empty rollovers skip the rotation check, so no
	// rotation fires here — the segment just ages).
	time.Sleep(4 * time.Second)

	// Phase 3: a single trailing write. The next committed rollover drains
	// it; segment 0 is now > 3s old, so the age check rotates — and because
	// no writes follow, the (P_N, S] window holds no user events (S still
	// lands a few WAL bytes past P_N from slot/snapshot bookkeeping, which
	// is exactly why the boundary reads as a coverage gap), leaving a
	// zero-incremental new segment.
	applyDDL(t, sourceDSN, `UPDATE accounts SET balance = balance + 1 WHERE id = 1;`)

	// Phase 4: stay idle a few rollover windows so the rotation fires and
	// the new segment stays empty, then gracefully stop.
	time.Sleep(3 * time.Second)
	cancel()
	select {
	case err := <-streamErr:
		if err != nil && !strings.Contains(err.Error(), "context") {
			t.Fatalf("stream.Run = %v; want clean exit", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("stream.Run did not exit within 20s of cancel")
	}
}

// seedAccountsChain boots a PG source, creates the accounts table +
// publication + slot, takes the anchored full, and returns the DSNs +
// store. Shared by both legs.
func seedAccountsChain(t *testing.T) (sourceDSN, targetDSN string, store *blobcodec.LocalStore, cleanup func()) {
	t.Helper()
	sourceDSN, targetDSN, cleanup = startPostgresLogical(t)
	applyDDL(t, sourceDSN, `
		CREATE TABLE accounts (
			id      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email   VARCHAR(255) NOT NULL,
			balance BIGINT NOT NULL DEFAULT 0
		);
		ALTER TABLE accounts REPLICA IDENTITY FULL;
	`)
	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	t.Cleanup(func() { dropPGLogicalSlot(t, sourceDSN, "sluice_slot") })

	eng, _ := engines.Get("postgres")
	store, _ = blobcodec.NewLocalStore(t.TempDir())
	rotationSeedFull(t, store, eng, sourceDSN, slotLSN)
	return sourceDSN, targetDSN, store, cleanup
}

// assertTrailingIdleStopShape verifies the lineage actually carries the
// Bug-139 boundary (a multi-segment lineage whose last segment is
// rotation-born, zero-incremental, and stamp-less). Returns the catalog.
func assertTrailingIdleStopShape(t *testing.T, store *blobcodec.LocalStore) *lineage.Catalog {
	t.Helper()
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), store)
	if err != nil || !ok {
		t.Fatalf("lineage.LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	if len(cat.Segments) < 2 {
		t.Fatalf("lineage segments = %d; want >= 2 (the idle-stop rotation didn't materialize — adjust churn/timing)", len(cat.Segments))
	}
	last := cat.Segments[len(cat.Segments)-1]
	if len(last.Incrementals) != 0 {
		t.Fatalf("trailing segment has %d incrementals; want 0 (Bug-139 idle-stop shape requires a zero-incremental trailing segment)", len(last.Incrementals))
	}
	if last.Dir == "" {
		t.Fatalf("trailing segment Dir is empty; want a rotation-born seg-* dir")
	}
	// Confirm the boundary is a genuine coverage gap (the whole point).
	prev := cat.Segments[len(cat.Segments)-2]
	if !boundaryHasCoverageGap(&prev, &last) {
		t.Fatalf("trailing boundary is not a coverage gap; the Bug-139 shape requires prev.End != last's coverage start")
	}
	return cat
}

// TestADR0087_Bug139_IdleStopCompact_SplitsAndRestores_PG is the
// load-bearing Bug-139 pin: an idle-stop rotation chain (trailing
// zero-incremental rotation-born segment) compacts — naive AND smart —
// by SPLITTING at the gap (instead of the pre-ADR-0087 whole-run
// refusal), and the compacted chain restores byte/count-equal to the
// uncompacted oracle.
func TestADR0087_Bug139_IdleStopCompact_SplitsAndRestores_PG(t *testing.T) {
	for _, smart := range []bool{false, true} {
		name := "naive"
		if smart {
			name = "smart"
		}
		t.Run(name, func(t *testing.T) {
			sourceDSN, targetDSN, store, cleanup := seedAccountsChain(t)
			defer cleanup()

			driveIdleStopRotationChain(t, sourceDSN, store)
			cat := assertTrailingIdleStopShape(t, store)
			preSegments := len(cat.Segments)

			// Oracle: the source's final state (the chain must restore to it).
			oracle := bug139Read(t, sourceDSN)
			if oracle.n == 0 {
				t.Fatal("source oracle has 0 rows; the churn produced nothing")
			}

			// Compact with a window wide enough to group every segment.
			res, err := CompactChain(context.Background(), store, CompactOpts{
				MergeWindow:     time.Hour,
				SmartCompaction: smart,
				PKStrategy:      PKStrategyPK,
			})
			if err != nil {
				if strings.Contains(err.Error(), "position gap") {
					t.Fatalf("Bug-139 regression: compact REFUSED the idle-stop chain on a position gap instead of splitting at it: %v", err)
				}
				t.Fatalf("CompactChain (smart=%v): %v", smart, err)
			}
			// At least one merge happened (the pre-boundary contiguous run),
			// AND the trailing stamp-less segment was split off (segment count
			// dropped but did not collapse to 1).
			if res.GroupsMerged < 1 {
				t.Fatalf("GroupsMerged = %d; want >= 1 (the pre-boundary run must merge)", res.GroupsMerged)
			}
			post, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
			if len(post.Segments) >= preSegments {
				t.Errorf("post-compact segments = %d; want < %d (a merge happened)", len(post.Segments), preSegments)
			}
			// The trailing stamp-less segment must SURVIVE as its own group
			// (split off, not merged into the run).
			lastPost := post.Segments[len(post.Segments)-1]
			if len(lastPost.Incrementals) != 0 {
				t.Errorf("trailing segment gained incrementals post-compact (%d); the split should keep it isolated", len(lastPost.Incrementals))
			}

			// Restore the compacted chain into the fresh target and assert it
			// equals the source oracle byte/count-for-content.
			if err := (&ChainRestore{Target: engineMust(t), TargetDSN: targetDSN, Store: store}).Run(context.Background()); err != nil {
				t.Fatalf("ChainRestore of compacted chain: %v", err)
			}
			got := bug139Read(t, targetDSN)
			if got != oracle {
				t.Errorf("restored compacted chain != source oracle: got %+v want %+v", got, oracle)
			}
			t.Logf("Bug-139 %s pin PROVEN: %d-segment idle-stop chain compacted to %d (trailing stamp-less segment split off); restore == source (%+v)",
				name, preSegments, len(post.Segments), oracle)
		})
	}
}

// TestADR0087_Bug139_ResumeHeals_WholeChainCompacts_PG is the resume-heal
// leg: after producing the idle-stop chain, a SECOND stream session under
// churn must resume the trailing stamp-less segment from P_N (stamping
// IncrementalCoverageStart = P_N), so the boundary heals and the WHOLE
// chain then compacts (N→1), restoring equal to the source oracle.
func TestADR0087_Bug139_ResumeHeals_WholeChainCompacts_PG(t *testing.T) {
	sourceDSN, targetDSN, store, cleanup := seedAccountsChain(t)
	defer cleanup()

	// Leg 1: produce the idle-stop chain (trailing stamp-less segment).
	driveIdleStopRotationChain(t, sourceDSN, store)
	cat := assertTrailingIdleStopShape(t, store)
	healSegDir := cat.Segments[len(cat.Segments)-1].Dir

	// Leg 2: a second stream session under churn. It must resume the
	// stamp-less open segment from P_N (the resume heal) and commit its
	// first incremental at P_N — stamping IncrementalCoverageStart.
	eng, _ := engines.Get("postgres")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 16; i++ {
			applyDDL(t, sourceDSN, fmt.Sprintf(
				`UPDATE accounts SET balance = balance + 1 WHERE id = %d;`, (i%4)+1,
			))
			time.Sleep(60 * time.Millisecond)
		}
	}()
	stream := &BackupStream{
		Source:             eng,
		SourceDSN:          sourceDSN,
		Store:              store,
		RolloverWindow:     700 * time.Millisecond,
		RolloverMaxChanges: 4,
		RolloverMaxBytes:   1 << 30,
		ChunkChanges:       50,
		SluiceVersion:      "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()
	wg.Wait()
	time.Sleep(2 * time.Second)
	cancel()
	select {
	case err := <-streamErr:
		if err != nil && !strings.Contains(err.Error(), "context") {
			t.Fatalf("resume stream.Run = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("resume stream.Run did not exit within 20s")
	}

	// The previously stamp-less segment must now be stamped + carry
	// incrementals — the boundary is healed.
	healed, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	var healSeg *lineage.Segment
	for i := range healed.Segments {
		if healed.Segments[i].Dir == healSegDir {
			healSeg = &healed.Segments[i]
			break
		}
	}
	if healSeg == nil {
		t.Fatalf("could not find the previously stamp-less segment (dir=%q) after resume", healSegDir)
	}
	if len(healSeg.Incrementals) == 0 {
		t.Fatalf("resume did not commit an incremental into the stamp-less segment (dir=%q); the P_N resume heal didn't fire", healSegDir)
	}
	if healSeg.IncrementalCoverageStart.Token == "" {
		t.Fatalf("resume committed an incremental but IncrementalCoverageStart stayed empty — the first incremental did not start at P_N (heal broken)")
	}

	oracle := bug139Read(t, sourceDSN)

	// Now the WHOLE chain is contiguous: compact merges everything.
	res, err := CompactChain(context.Background(), store, CompactOpts{
		MergeWindow:     time.Hour,
		SmartCompaction: true,
		PKStrategy:      PKStrategyPK,
	})
	if err != nil {
		t.Fatalf("CompactChain after resume heal: %v", err)
	}
	post, _, _ := lineage.LoadLineageCatalog(context.Background(), store)
	if len(post.Segments) != 1 {
		t.Errorf("post-compact segments = %d; want 1 (the healed chain is fully contiguous and collapses to one segment)", len(post.Segments))
	}
	if res.GroupsMerged != 1 {
		t.Errorf("GroupsMerged = %d; want 1 (one whole-chain merge)", res.GroupsMerged)
	}

	if err := (&ChainRestore{Target: eng, TargetDSN: targetDSN, Store: store}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore of healed+compacted chain: %v", err)
	}
	got := bug139Read(t, targetDSN)
	if got != oracle {
		t.Errorf("restored healed+compacted chain != source oracle: got %+v want %+v", got, oracle)
	}
	t.Logf("Bug-139 resume-heal pin PROVEN: stamp-less segment resumed from P_N + stamped; whole chain compacted %d→1; restore == source (%+v)",
		len(cat.Segments), oracle)
}

// engineMust returns the registered postgres engine for restore.
func engineMust(t *testing.T) ir.Engine {
	t.Helper()
	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	return eng
}
