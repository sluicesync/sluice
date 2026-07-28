//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The chain-SHAPING × encryption-mode round-trip matrix — the gate the
// class needs, built after the third instance rather than the fourth.
//
// Three separate CRITICALs have now had the same shape:
//
//   - Bug 214 — `backup compact` deleted the chain-root manifest.
//   - roadmap item 95 — `backup prune` did the same, on a path that runs
//     on a retention SCHEDULE rather than as occasional maintenance.
//   - Bug 215 — a `backup incremental` (or a RESTARTED `backup stream`)
//     extending a ROTATED chain sealed its chunks under the open
//     segment full's CEK, which the restore path never tries.
//
// Every one is a chain-shaping operation whose correctness was
// established against PLAINTEXT chains and never round-tripped through
// the encrypted READ path. Plaintext has no chain identity to delete, no
// CEK to pick wrongly, and no AAD to mis-bind, so a green plaintext test
// is not evidence for an encrypted one — and three operations shipped on
// exactly that assumption.
//
// So this file is a TABLE, not another point cell: every shaping
// operation × every encryption mode × the plaintext control, each cell
// writing a real encrypted chain, applying the operation, and then
// RESTORING IT AND COMPARING ROWS against the source oracle. Asserting
// an operation's return value is what let all three ship — the defect
// class IS an operation reporting success over an artifact it has made
// unreadable, so only a real restore into a real target can pin it.
//
// The second gate lives here too, and it is the one that turned Bug 215
// from a bug into a trap: **`verify` and `restore` must AGREE**. A
// fixture where `backup verify` returns rc=0 and `restore` then fails is
// itself a test failure, reported as such, because a green verify over an
// unrestorable chain is worse than no verify — it converts "I should
// check this" into "I checked this", and the discovery moment is a
// recovery.

package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// shapingFixture is one matrix cell's world: a live PG source, a fresh
// PG target, and the multi-segment chain under test.
type shapingFixture struct {
	sourceDSN string
	targetDSN string
	store     *blobcodec.LocalStore
	mode      string // "" = the plaintext control
	pass      string
	nextRow   int
}

// encrypted reports whether this cell writes an encrypted chain.
func (f *shapingFixture) encrypted() bool { return f.mode != "" }

// writerEncryption is the `--encrypt` posture a chain-EXTENDING writer
// gets from the CLI: a freshly-minted envelope (fresh salt — the CLI
// cannot know the chain's until it reads the root manifest), the rebind
// hook, and an EMPTY Mode, because an operator extending a chain omits
// `--encrypt-mode` and inherits (Bug 179/180). Nil for the control.
func (f *shapingFixture) writerEncryption(t *testing.T) *lineage.BackupEncryption {
	t.Helper()
	if !f.encrypted() {
		return nil
	}
	return &lineage.BackupEncryption{
		Envelope:        newTestPassphraseEnvelope(t, f.pass),
		RebuildForChain: passphraseRebuildHook(f.pass),
	}
}

// readEnvelope builds the restore/verify-side envelope the way the
// production CLI does — see [bug214ReadEnvelope], whose fresh-salt
// fallback is load-bearing.
func (f *shapingFixture) readEnvelope(t *testing.T) crypto.EnvelopeEncryption {
	t.Helper()
	if !f.encrypted() {
		return nil
	}
	return bug214ReadEnvelope(t, f.store, f.pass)
}

// insert adds n rows to the source and returns after they are committed.
func (f *shapingFixture) insert(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		f.nextRow++
		applyDDL(t, f.sourceDSN, fmt.Sprintf(
			`INSERT INTO ledger (memo, balance) VALUES ('post%03d', %d);`, f.nextRow, 1000+f.nextRow,
		))
	}
}

// segments returns the current lineage segment count.
func (f *shapingFixture) segments(t *testing.T) int {
	t.Helper()
	cat, ok, err := lineage.LoadLineageCatalog(context.Background(), f.store)
	if err != nil || !ok {
		t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
	}
	return len(cat.Segments)
}

// runIncremental extends the chain with a one-shot `backup incremental`
// resuming the OPEN segment — the exact Bug-215 operator action. Returns
// the manifest it wrote.
func (f *shapingFixture) runIncremental(t *testing.T) *irbackup.Manifest {
	t.Helper()
	eng, _ := engines.Get("postgres")
	before := f.manifestPaths(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	incr := &IncrementalBackup{
		Source:        eng,
		SourceDSN:     f.sourceDSN,
		Store:         f.store,
		Window:        12 * time.Second,
		MaxChanges:    200,
		ChunkChanges:  50,
		SluiceVersion: "test",
		Encryption:    f.writerEncryption(t),
	}
	if err := incr.Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	after := f.manifestPaths(t)
	for p, m := range after {
		if _, seen := before[p]; !seen {
			return m
		}
	}
	t.Fatal("backup incremental wrote no new manifest")
	return nil
}

// runStreamResume restarts `backup stream` against the ALREADY-ROTATED
// chain — the ordinary supervisor restart. The Bug-215 shape reaches it
// too, and far more often than the one-shot incremental does: a stream
// that CREATED the rotations resolves its CEK once at startup, when the
// open segment IS the root, and carries it across every rotation; a
// stream that STARTS on a rotated chain resolves against the open
// segment.
func (f *shapingFixture) runStreamResume(t *testing.T) {
	t.Helper()
	eng, _ := engines.Get("postgres")
	stream := &BackupStream{
		Source:                    eng,
		SourceDSN:                 f.sourceDSN,
		Store:                     f.store,
		RolloverWindow:            900 * time.Millisecond,
		RolloverMaxChanges:        4,
		RolloverMaxBytes:          1 << 30,
		ChunkChanges:              50,
		RetainRotateAtChainLength: 99, // no further rotation: isolate the RESUME
		SluiceVersion:             "test",
		Encryption:                f.writerEncryption(t),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- stream.Run(ctx) }()
	f.insert(t, 4)
	time.Sleep(6 * time.Second)
	cancel()
	select {
	case err := <-errCh:
		if err != nil && !strings.Contains(err.Error(), "context") {
			t.Fatalf("resumed stream.Run = %v; want clean exit", err)
		}
	case <-time.After(45 * time.Second):
		t.Fatal("resumed stream.Run did not exit within 45s of cancel")
	}
}

// manifestPaths indexes every manifest in the lineage by qualified path.
func (f *shapingFixture) manifestPaths(t *testing.T) map[string]*irbackup.Manifest {
	t.Helper()
	recs, err := lineage.ListAllSegmentManifests(context.Background(), f.store)
	if err != nil {
		t.Fatalf("ListAllSegmentManifests: %v", err)
	}
	out := make(map[string]*irbackup.Manifest, len(recs))
	for _, r := range recs {
		out[r.Segment.Dir+"/"+r.Path] = r.Manifest
	}
	return out
}

// oracle is the source's authoritative state — a value fingerprint, not
// just a count, so a restore that lands the right NUMBER of wrong rows
// still fails.
func (f *shapingFixture) oracle(t *testing.T) bug214Sums { return bug214Read(t, f.sourceDSN) }

// restored is the target's state after a chain restore.
func (f *shapingFixture) restored(t *testing.T) bug214Sums { return bug214Read(t, f.targetDSN) }

// shapingOp is one chain-shaping operation under test. apply runs it
// against the seeded rotated chain and returns a one-line description
// for the damage-map log.
//
// An operation carrying a CONFIRMED OPEN DEFECT asserts that defect
// inside its own apply — as `prune` did while roadmap item 100 was open
// — rather than through a harness-level "known broken" flag. That is
// deliberate: the two defects this matrix has caught surfaced at
// different LAYERS (one at restore, one as a coded refusal from the
// operation itself), and a single "the restore error must contain X"
// knob can only express one of them, which is exactly how the first
// version of the prune cell came to assert a surface the operation no
// longer had. What must NOT happen is deleting or skipping a cell to get
// green — a known-and-filed failing cell is worth far more than a matrix
// trimmed to what works — so a defect-carrying apply still has to fail
// when the defect changes shape or disappears (prune's did exactly that,
// and its assertion said so and named the promotion), and every cell
// still runs the full round trip and the verify/restore agreement gate
// below.
type shapingOp struct {
	name  string
	apply func(t *testing.T, f *shapingFixture) string
}

// shapingOps is the operation axis. Every one of these RESHAPES a chain
// — extends it, merges it, or trims it — which is the property that
// makes the encrypted read path a separate question from the plaintext
// one.
func shapingOps() []shapingOp {
	return []shapingOp{
		{
			// The control for the axis: rotation alone, no follow-up
			// operation. It must pass in every mode, and it is what
			// proves a failing sibling cell is about the OPERATION
			// rather than about rotation or about encryption.
			name: "rotate",
			apply: func(t *testing.T, f *shapingFixture) string {
				return fmt.Sprintf("rotation only, %d segments", f.segments(t))
			},
		},
		{
			// Bug 215's minimal repro.
			name: "rotate-then-resume",
			apply: func(t *testing.T, f *shapingFixture) string {
				f.insert(t, 3)
				m := f.runIncremental(t)
				if len(m.ChangeChunks) == 0 {
					t.Fatal("the resuming incremental captured no change chunks; this cell would be vacuous")
				}
				if f.encrypted() && m.ChangeChunks[0].Encryption == nil {
					t.Fatal("the resuming incremental wrote a PLAINTEXT chunk into an encrypted chain")
				}
				return fmt.Sprintf("one-shot incremental resumed the open segment, %d change chunk(s)", len(m.ChangeChunks))
			},
		},
		{
			// Bug 215's higher-traffic sibling: an ordinary restart.
			name: "rotate-then-stream-resume",
			apply: func(t *testing.T, f *shapingFixture) string {
				before := len(f.manifestPaths(t))
				f.runStreamResume(t)
				after := len(f.manifestPaths(t))
				if after <= before {
					t.Fatalf("the resumed stream committed no rollover (%d → %d manifests); this cell would be vacuous", before, after)
				}
				return fmt.Sprintf("stream restarted on the rotated chain, %d → %d manifests", before, after)
			},
		},
		{
			// Bug 214's cell.
			name: "compact",
			apply: func(t *testing.T, f *shapingFixture) string {
				before := f.segments(t)
				res, err := backup.CompactChain(context.Background(), f.store, backup.CompactOpts{MergeWindow: time.Hour})
				if err != nil {
					t.Fatalf("CompactChain: %v", err)
				}
				after := f.segments(t)
				if res.GroupsMerged < 1 || after >= before {
					t.Fatalf("compaction did nothing (groups=%d, %d → %d segments); this cell would be vacuous",
						res.GroupsMerged, before, after)
				}
				return fmt.Sprintf("compacted %d → %d segments", before, after)
			},
		},
		{
			// The combination: a chain a resume extended, then merged.
			name: "compact-after-rotate",
			apply: func(t *testing.T, f *shapingFixture) string {
				f.insert(t, 3)
				m := f.runIncremental(t)
				if len(m.ChangeChunks) == 0 {
					t.Fatal("the resuming incremental captured no change chunks; this cell would be vacuous")
				}
				before := f.segments(t)
				res, err := backup.CompactChain(context.Background(), f.store, backup.CompactOpts{MergeWindow: time.Hour})
				if err != nil {
					t.Fatalf("CompactChain: %v", err)
				}
				after := f.segments(t)
				if res.GroupsMerged < 1 || after >= before {
					t.Fatalf("compaction did nothing (groups=%d, %d → %d segments); this cell would be vacuous",
						res.GroupsMerged, before, after)
				}
				return fmt.Sprintf("resumed then compacted %d → %d segments", before, after)
			},
		},
		{
			// Roadmap item 100's cell — and the record of a decision this
			// matrix forced.
			//
			// It was a KNOWN-BROKEN cell: `backup prune` with a keep-count
			// landing INSIDE the floor segment dropped the incrementals the
			// first survivor parents on, so the survivors no longer formed
			// one chain (and the events in the gap were simply gone). Item
			// 95's readability gate caught that pre-commit and refused, so
			// what the cell pinned was a coded, inert REFUSAL — correct, but
			// it left the headline retention flag unable to succeed on the
			// most ordinary chain shapes. The repair had to be a retention-
			// CONTRACT decision, because re-stitching the parent pointer —
			// the tempting one-liner — would have converted a loud refusal
			// into a SILENT coverage gap.
			//
			// The decision: retention is SEGMENT-granular. A keep-count is
			// rounded UP to the nearest segment boundary, so prune retires
			// only whole leading segments — keeping MORE than asked, never
			// fewer. So this cell is now a normal round trip, exactly as the
			// old assertion's own failure message instructed whoever fixed
			// it, and what it pins is what an operator actually gets:
			//
			//  1. the ROUNDING happened and was reported — requested vs
			//     retained, both numbers, because silent over-retention is
			//     how "my disk did not shrink" starts;
			//  2. it is a real prune — whole segments gone, not a no-op
			//     dressed up as success;
			//  3. NO segment was trimmed in place — every surviving segment
			//     still holds every incremental it held before, which is the
			//     structural property that makes the result restorable;
			//  4. …and the harness then RESTORES the pruned chain and
			//     compares rows to the source oracle, in all three
			//     encryption modes. That is the assertion the refusal era
			//     could never make.
			name: "prune",
			apply: func(t *testing.T, f *shapingFixture) string {
				const keep = 1
				ctx := context.Background()
				pre, ok, err := lineage.LoadLineageCatalog(ctx, f.store)
				if err != nil || !ok {
					t.Fatalf("LoadLineageCatalog: ok=%v err=%v", ok, err)
				}
				before := len(pre.Segments)
				// Non-vacuity: keep=1 must NOT already land on a segment
				// boundary, or this cell exercises no rounding at all. The
				// segment that decides it is the NEWEST one holding any
				// incrementals — the open segment is routinely a
				// rotation-born full with none yet, which contributes no
				// boundary of its own.
				floor := -1
				for si := before - 1; si >= 0; si-- {
					if len(pre.Segments[si].Incrementals) > 0 {
						floor = si
						break
					}
				}
				if floor < 0 {
					t.Fatalf("the seeded %d-segment chain holds no incrementals at all; this cell would be vacuous", before)
				}
				if n := len(pre.Segments[floor].Incrementals); n < 2 {
					t.Fatalf("the newest incremental-bearing segment (index %d of %d) holds %d incremental(s), so "+
						"--keep-incrementals=%d already lands on a segment boundary and this cell would not exercise "+
						"item 100's rounding; retune the fixture's rollover/rotation thresholds", floor, before, n, keep)
				}

				// --dry-run must report the ROUNDED plan, not the requested
				// one: a dry run that lies about the real run is worse than
				// no dry run.
				plan, err := backup.PruneChain(ctx, f.store, backup.PruneOpts{
					KeepIncrementals: keep, DryRun: true,
				})
				if err != nil {
					t.Fatalf("PruneChain --dry-run: %v", err)
				}
				if plan.SegmentsDropped < 1 || len(plan.Pruned) == 0 {
					t.Fatalf("prune --keep-incrementals=%d planned nothing to drop on a %d-segment chain "+
						"(segments=%d manifests=%d); this cell would be vacuous",
						keep, before, plan.SegmentsDropped, len(plan.Pruned))
				}

				res, err := backup.PruneChain(ctx, f.store, backup.PruneOpts{KeepIncrementals: keep})
				if err != nil {
					t.Fatalf("`backup prune --keep-incrementals=%d` on a %d-segment chain REFUSED. Segment-granular "+
						"retention (roadmap item 100) is supposed to round this up to a whole-segment prune; a refusal "+
						"here means either the rounding regressed or the readability gate is refusing a chain that "+
						"reads: %v", keep, before, err)
				}
				if res.SegmentsDropped != plan.SegmentsDropped || len(res.Pruned) != len(plan.Pruned) {
					t.Errorf("the real run diverged from --dry-run: dropped %d segments / %d manifests, the plan said %d / %d",
						res.SegmentsDropped, len(res.Pruned), plan.SegmentsDropped, len(plan.Pruned))
				}
				// 1. Rounded UP, and said so.
				if res.RequestedIncrementals != keep {
					t.Errorf("RequestedIncrementals = %d; want the %d that was asked for", res.RequestedIncrementals, keep)
				}
				if res.IncrementalsRetained <= res.RequestedIncrementals {
					t.Errorf("retained %d incrementals for a requested %d — this cell is supposed to exercise the "+
						"round-UP, and an equal count means it did not",
						res.IncrementalsRetained, res.RequestedIncrementals)
				}
				// 2. A real prune.
				if res.SegmentsDropped < 1 {
					t.Fatalf("SegmentsDropped = %d; rounding up must not turn prune into a silent no-op", res.SegmentsDropped)
				}
				// 3. Nothing trimmed IN PLACE: every surviving segment kept
				//    every incremental it had. This is the structural
				//    property the restore below depends on.
				post, ok, err := lineage.LoadLineageCatalog(ctx, f.store)
				if err != nil || !ok {
					t.Fatalf("post-prune LoadLineageCatalog: ok=%v err=%v", ok, err)
				}
				if len(post.Segments) >= before {
					t.Fatalf("post-prune segments = %d; want fewer than the %d before", len(post.Segments), before)
				}
				held := make(map[string]int, len(pre.Segments))
				for _, seg := range pre.Segments {
					held[seg.SegmentID] = len(seg.Incrementals)
				}
				for _, seg := range post.Segments {
					if n, seen := held[seg.SegmentID]; !seen || len(seg.Incrementals) != n {
						t.Errorf("surviving segment %s (%s) holds %d incrementals, held %d before — a within-segment "+
							"trim is exactly what segment-granular retention exists to prevent",
							seg.SegmentID, seg.Dir, len(seg.Incrementals), n)
					}
				}
				// The Bug-214/item-95 file, still there after a whole-segment
				// prune that retired the root segment.
				if ex, _ := f.store.Exists(ctx, lineage.ManifestFileName); !ex {
					t.Error("prune deleted the chain-root manifest — the chain's identity, not segment 0's data")
				}
				return fmt.Sprintf("pruned --keep-incrementals=%d → retained %d (rounded up to a segment boundary); %d → %d segments, %d manifests dropped",
					keep, res.IncrementalsRetained, before, len(post.Segments), len(res.Pruned))
			},
		},
	}
}

// shapingModes is the encryption axis. The plaintext control is not
// decoration: it is what proves a red encrypted cell is about the
// ENCRYPTED read path rather than about the operation being broken
// outright, which is the exact distinction all three bugs turned on.
func shapingModes() []struct{ name, mode string } {
	return []struct{ name, mode string }{
		{"per-chain", crypto.EncryptModePerChain},
		{"per-chunk", crypto.EncryptModePerChunk},
		{"plaintext-control", ""},
	}
}

// TestChainShaping_EncryptedRoundTripMatrix is the matrix: for every
// {shaping operation} × {per-chain, per-chunk, plaintext}, write a chain,
// apply the operation, then VERIFY it, RESTORE it, and compare rows to
// the source.
//
// Damage map as of 2026-07-28 — all 18 cells round-trip row-exact:
//
//	rotate                     ×3   green
//	rotate-then-resume         ×3   green  (per-chain was Bug 215 — RED
//	                                        pre-fix with exactly the field
//	                                        error, mutation-verified)
//	rotate-then-stream-resume  ×3   green  (same defect, ordinary restart)
//	compact                    ×3   green  (Bug 214's cell)
//	compact-after-rotate       ×3   green
//	prune                      ×3   green  (was REFUSED + intact while
//	                                        roadmap item 100's retention
//	                                        CONTRACT was open — a keep-count
//	                                        landing inside the floor segment
//	                                        mis-stitched the lineage, so the
//	                                        readability gate refused it
//	                                        pre-commit. Retention is now
//	                                        SEGMENT-granular: the keep-count
//	                                        rounds UP to a segment boundary
//	                                        and the whole-segment prune that
//	                                        results restores row-exact — see
//	                                        the `prune` entry in
//	                                        [shapingOps])
//
// The mode axis is what makes each row readable at a glance: an
// encrypted-read-path defect shows as RED-encrypted / green-plaintext
// (Bug 215's signature), while a uniform result across all three modes
// says the behaviour is upstream of encryption entirely — which is what
// prune's row said while it refused, since the mis-stitch was structural
// and the plaintext control refused identically. A matrix without the
// plaintext control cannot tell those apart, which is how three of these
// shipped.
func TestChainShaping_EncryptedRoundTripMatrix(t *testing.T) {
	const passphrase = "chain-shaping-matrix-passphrase"
	for _, op := range shapingOps() {
		for _, m := range shapingModes() {
			t.Run(op.name+"/"+m.name, func(t *testing.T) {
				f := seedRotatedChainFixture(t, m.mode, passphrase)
				if segs := f.segments(t); segs < 3 {
					t.Fatalf("seeded segments = %d; want >= 3 (rotation didn't materialize)", segs)
				}
				what := op.apply(t, f)

				// The oracle is taken AFTER the operation, so it
				// includes anything the operation captured.
				oracle := f.oracle(t)
				if oracle.n < 2 {
					t.Fatalf("source oracle has %d rows; the fixture produced nothing", oracle.n)
				}

				verifyRep, verifyErr := f.verifyChain(t)
				restoreErr := f.restoreChain(t)
				var got bug214Sums
				if restoreErr == nil {
					got = f.restored(t)
				}
				rowsMatch := restoreErr == nil && got == oracle

				// GATE 2 — verify and restore must agree. This fires
				// BEFORE the round-trip assertion so the message names
				// the trap rather than the symptom, and it applies to
				// every cell without exception — including one whose
				// operation carries a filed defect: an operation that
				// produces an unrestorable chain does not get to keep a
				// green verify just because the breakage is filed.
				if verifyErr == nil && !rowsMatch {
					t.Errorf("VERIFY/RESTORE DISAGREEMENT — `backup verify` returned rc=0 (chunks=%d decrypted=%d) on a chain restore cannot read.\n"+
						"  cell:     %s / %s (%s)\n"+
						"  restore:  %v\n"+
						"  rows:     got %+v want %+v\n"+
						"A green verify over an unrestorable chain is worse than no verify: it is the signal operators are told to trust before they need the backup.",
						verifyRep.Chunks, verifyRep.Authenticated, op.name, m.name, what, restoreErr, got, oracle)
				}

				// GATE 1 — the round trip itself.
				if restoreErr != nil {
					t.Fatalf("DAMAGE [%s / %s]: %s — ChainRestore failed: %v", op.name, m.name, what, restoreErr)
				}
				if got != oracle {
					t.Fatalf("DAMAGE [%s / %s]: %s — restored rows != source oracle: got %+v want %+v",
						op.name, m.name, what, got, oracle)
				}
				if verifyErr != nil {
					t.Fatalf("`backup verify` failed on a chain that restored row-exact [%s / %s]: %v",
						op.name, m.name, verifyErr)
				}
				// Non-vacuity for the verify half: on an encrypted
				// chain the probe must have actually OPENED chunks.
				// Pre-Bug-215-fix this was 0 for every per-chain chain
				// while verify still logged decrypt_probe=true.
				if f.encrypted() && verifyRep.Authenticated == 0 {
					t.Errorf("`backup verify` authenticated 0 of %d chunks on an ENCRYPTED chain — the decrypt probe is vacuous again",
						verifyRep.Chunks)
				}
				t.Logf("OK [%s / %s]: %s; verify chunks=%d decrypted=%d; restored %+v",
					op.name, m.name, what, verifyRep.Chunks, verifyRep.Authenticated, oracle)
			})
		}
	}
}

// verifyChain runs `backup verify` exactly as the CLI does.
func (f *shapingFixture) verifyChain(t *testing.T) (backup.VerifyReport, error) {
	t.Helper()
	return backup.VerifyBackupCodedReport(context.Background(), f.store, backup.VerifyOptions{
		Envelope: f.readEnvelope(t),
	})
}

// restoreChain restores the whole chain into the fresh target through
// the production read path. Returns the error rather than failing, so
// the caller can compare it against verify's verdict.
func (f *shapingFixture) restoreChain(t *testing.T) error {
	t.Helper()
	eng, _ := engines.Get("postgres")
	return (&backup.ChainRestore{
		Target: eng, TargetDSN: f.targetDSN, Store: f.store, Envelope: f.readEnvelope(t),
	}).Run(context.Background())
}

// seedRotatedChainFixture boots a PG source + fresh target, takes an
// anchored (optionally encrypted) full, then drives a real CDC stream
// under continuous write load with a tight chain-length rotation
// threshold so a MULTI-SEGMENT lineage forms.
//
// The rotation is the point: every rotation-born segment full runs the
// ordinary `backup full` orchestrator against an empty provisional dir,
// so on an encrypted chain each one mints its OWN chain CEK. That is
// what makes "which manifest owns the key" a question with a wrong
// answer, and a single-segment chain can never ask it.
func seedRotatedChainFixture(t *testing.T, mode, passphrase string) *shapingFixture {
	t.Helper()
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)

	applyDDL(t, sourceDSN, `
		CREATE TABLE ledger (
			id      BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			memo    VARCHAR(255) NOT NULL,
			balance BIGINT NOT NULL DEFAULT 0
		);
		ALTER TABLE ledger REPLICA IDENTITY FULL;
		INSERT INTO ledger (memo, balance) VALUES ('seed', 1);
	`)
	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	t.Cleanup(func() { dropPGLogicalSlot(t, sourceDSN, "sluice_slot") })

	eng, _ := engines.Get("postgres")
	store, _ := blobcodec.NewLocalStore(t.TempDir())
	f := &shapingFixture{sourceDSN: sourceDSN, targetDSN: targetDSN, store: store, mode: mode, pass: passphrase}

	// Anchored seed full. Encryption (when requested) is stamped on the
	// chain root here — the manifest every later question is about.
	var seedEnc *lineage.BackupEncryption
	if mode != "" {
		seedEnc = &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            mode,
		}
	}
	if err := (&backup.Backup{
		Source: eng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChunkRows: 1, // several chunks, so per-chunk mode is exercised
		Encryption: seedEnc,
	}).Run(context.Background()); err != nil {
		t.Fatalf("seed Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("read seed full: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "postgres",
		Token:  fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, slotLSN),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite seed full: %v", err)
	}
	_ = lineage.UpdateLineageForManifestBestEffort(context.Background(), store, full, lineage.ManifestFileName, blobcodec.DefaultCodec)

	// Drive rotation: continuous writes + a chain-length threshold of 2.
	const churn = 24
	var wg sync.WaitGroup
	writeCtx, writeCancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < churn; i++ {
			select {
			case <-writeCtx.Done():
				return
			default:
			}
			applyDDL(t, sourceDSN, fmt.Sprintf(
				`INSERT INTO ledger (memo, balance) VALUES ('row%03d', %d);`, i, i,
			))
			time.Sleep(40 * time.Millisecond)
		}
	}()

	var streamEnc *lineage.BackupEncryption
	if mode != "" {
		streamEnc = &lineage.BackupEncryption{
			Envelope:        newTestPassphraseEnvelope(t, passphrase),
			RebuildForChain: passphraseRebuildHook(passphrase),
			Mode:            mode,
		}
	}
	stream := &BackupStream{
		Source:                    eng,
		SourceDSN:                 sourceDSN,
		Store:                     store,
		ParentRef:                 full.BackupID,
		RolloverWindow:            900 * time.Millisecond,
		RolloverMaxChanges:        6,
		RolloverMaxBytes:          1 << 30,
		ChunkChanges:              50,
		RetainRotateAtChainLength: 2,
		SluiceVersion:             "test",
		Encryption:                streamEnc,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	wg.Wait()
	writeCancel()
	time.Sleep(6 * time.Second)
	cancel()
	select {
	case err := <-streamErr:
		if err != nil && !strings.Contains(err.Error(), "context") {
			t.Fatalf("stream.Run = %v; want clean exit", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stream.Run did not exit within 30s of cancel")
	}
	f.nextRow = churn
	return f
}

// TestChainShaping_VerifyCatchesAWrongKeyChunk is the verify gate's own
// non-vacuity pin, and it is deliberately NOT dependent on any writer
// bug: it FORGES the Bug-215 artifact directly — a change chunk sealed
// under a segment full's CEK instead of the chain root's — and requires
// `backup verify` to refuse it.
//
// Without this, the verify fix would be pinned only by cells that also
// depend on the writer fix, so a later regression in EITHER would look
// the same. Here the writer is not involved at all.
func TestChainShaping_VerifyCatchesAWrongKeyChunk(t *testing.T) {
	const passphrase = "verify-catches-wrong-key"
	f := seedRotatedChainFixture(t, crypto.EncryptModePerChain, passphrase)

	// Sanity: the honest chain verifies AND restores.
	if _, err := f.verifyChain(t); err != nil {
		t.Fatalf("baseline verify of an untouched chain failed: %v", err)
	}

	// Find a change chunk in a NON-ROOT segment and re-seal it under
	// that segment full's own CEK — byte-for-byte the artifact a
	// pre-fix `backup incremental` wrote.
	recs, err := lineage.ListAllSegmentManifests(context.Background(), f.store)
	if err != nil {
		t.Fatalf("ListAllSegmentManifests: %v", err)
	}
	env := f.readEnvelope(t)
	rootCEK, err := lineage.UnwrapChainCEK(env, recs[0].Manifest.ChainEncryption.WrappedCEK, recs[0].Manifest)
	if err != nil {
		t.Fatalf("unwrap root chain cek: %v", err)
	}
	var resealed string
	for _, rec := range recs {
		if rec.Segment.Dir == "" || rec.Manifest.Kind != irbackup.BackupKindIncremental || len(rec.Manifest.ChangeChunks) == 0 {
			continue
		}
		segFull, ferr := lineage.ReadManifestAt(context.Background(), rec.Segment.Store(f.store), rec.Segment.FullManifestPath)
		if ferr != nil {
			t.Fatalf("read segment full: %v", ferr)
		}
		segCEK, uerr := lineage.UnwrapChainCEK(env, segFull.ChainEncryption.WrappedCEK, segFull)
		if uerr != nil {
			t.Fatalf("unwrap segment chain cek: %v", uerr)
		}
		if string(segCEK) == string(rootCEK) {
			t.Fatal("the segment full's CEK equals the chain root's; this fixture cannot forge the Bug-215 artifact " +
				"(if rotation started sharing the chain CEK, retarget this pin at that contract)")
		}
		if err := resealChangeChunk(t, f, rec, 0, rootCEK, segCEK); err != nil {
			t.Fatalf("reseal chunk: %v", err)
		}
		resealed = rec.Segment.Dir + "/" + rec.Manifest.ChangeChunks[0].File
		break
	}
	if resealed == "" {
		t.Fatal("no non-root-segment incremental with a change chunk; the forgery had nothing to target")
	}

	rep, verr := f.verifyChain(t)
	if verr == nil {
		t.Fatalf("`backup verify` returned rc=0 (chunks=%d decrypted=%d) on a chain carrying a chunk sealed under the WRONG CEK — "+
			"restore cannot read %s, and verify is the check operators run BEFORE they need the backup",
			rep.Chunks, rep.Authenticated, resealed)
	}
	// The CODE is the assertion, not the prose: operators script
	// `backup verify` against SLUICE-E-*, and a wrong-key chunk is the
	// GCM/AAD class, never the SHA/bit-rot one (the forgery re-stamped
	// the recorded digest precisely so the hash check cannot be what
	// fires).
	if ce, ok := sluicecode.FromError(verr); !ok || ce.Code != sluicecode.CodeBackupChunkAuthFailed {
		t.Errorf("verify error = %v; want a coded %s", verr, sluicecode.CodeBackupChunkAuthFailed)
	}
	if rep.Failed != 1 {
		t.Errorf("verify reported %d failed chunk(s); want exactly 1 (the resealed one)", rep.Failed)
	}
	// And the paired half: restore genuinely cannot read it, so verify's
	// new refusal is agreement rather than over-refusal.
	if rerr := f.restoreChain(t); rerr == nil {
		t.Error("ChainRestore SUCCEEDED on the resealed chain; the forgery did not land and this pin is vacuous")
	}
}

// resealChangeChunk decrypts the change chunk at index idx under oldCEK
// and re-encrypts it under newCEK with the SAME AAD and the same
// manifest-recorded SHA-256 updated in place — producing a chain that is
// byte-consistent everywhere except the key the chunk is sealed with.
func resealChangeChunk(t *testing.T, f *shapingFixture, rec lineage.SegmentRecord, idx int, oldCEK, newCEK []byte) error {
	t.Helper()
	ss := rec.Segment.Store(f.store)
	chunk := rec.Manifest.ChangeChunks[idx]
	rc, err := ss.Get(context.Background(), chunk.File)
	if err != nil {
		return fmt.Errorf("get chunk: %w", err)
	}
	ct, err := readAllAndClose(rc)
	if err != nil {
		return err
	}
	aad := irbackup.ChangeChunkAADFor(rec.Manifest, chunk, idx)
	pt, err := crypto.DecryptChunkWithAAD(ct, oldCEK, aad)
	if err != nil {
		return fmt.Errorf("decrypt under the root CEK: %w", err)
	}
	reCT, err := crypto.EncryptChunkWithAAD(pt, newCEK, aad)
	if err != nil {
		return fmt.Errorf("re-encrypt: %w", err)
	}
	if err := ss.Put(context.Background(), chunk.File, bytes.NewReader(reCT)); err != nil {
		return fmt.Errorf("put chunk: %w", err)
	}
	// Keep the manifest honest about the new ciphertext's digest, so the
	// SHA check cannot be what catches this — only the authenticated
	// open can.
	sum := sha256.Sum256(reCT)
	chunk.SHA256 = hex.EncodeToString(sum[:])
	if err := lineage.WriteManifestAt(context.Background(), ss, rec.Path, rec.Manifest); err != nil {
		return fmt.Errorf("rewrite manifest: %w", err)
	}
	t.Logf("resealed %s/%s under the segment CEK (SHA re-stamped)", rec.Segment.Dir, chunk.File)
	return nil
}

// readAllAndClose drains rc and closes it.
func readAllAndClose(rc io.ReadCloser) ([]byte, error) {
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read chunk body: %w", err)
	}
	return b, nil
}
