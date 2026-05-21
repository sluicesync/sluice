// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Inline backup-segment rotation FSM (ADR-0046 section 2). Driven by
// the rollover-loop goroutine that owns the CDC pump, over the SAME
// in-flight `cdc` handle:
//
//	STREAMING -> DRAIN -> SNAPSHOT -> BULKCOPY -> COMMIT -> STREAMING
//
// "Rotation" is not an exceptional event grafted onto an unbounded
// chain -- it is lineage.appendSegment. The correctness spine:
//
//   - DRAIN: the rollover loop only checks the rotation threshold
//     AFTER a successful rollover commit, which by construction ended
//     at a TxCommit boundary (ADR-0027). So P_N -- the prior segment's
//     last committed incremental EndPosition -- is always a
//     transaction-consistent position. There is nothing to drain
//     beyond that boundary.
//
//   - SNAPSHOT: open a backup-scoped snapshot anchored at S. Hard-
//     assert S >= P_N (ADR-0046 gotcha #1). The source is position-
//     monotonic and the snapshot opens at a quiesced rollover
//     boundary, so S >= P_N BY CONSTRUCTION; this is the defensive
//     assertion against a buggy/lying snapshot opener. A violation is
//     a LOUD abort that STAYS on the still-open prior segment -- never
//     a silent gap (the prior segment never lost position; the stream
//     just keeps streaming it).
//
//   - BULKCOPY: write the next segment's `backup full` under its
//     sub-dir (seg-<unix-millis>/) with the open segment's codec,
//     anchored at S.
//
//   - COMMIT (the single atomic linearization point): the next
//     segment's full is durable strictly BEFORE the ONE atomic
//     lineage.json write that appends the new segment AND caps the
//     prior one. That single Put flips authority; there is no window
//     where the lineage is non-authoritative.
//
//   - STREAMING: continue CDC from S on the SAME cdc handle (no slot
//     reopen); incrementals now append to the new open segment.
//
// rotation_state.json at the lineage root records the FSM phase + the
// provisional next-segment dir for crash recovery. On restart (see
// recoverRotationState): <=COMMIT (the lineage does NOT yet contain
// the provisional segment) -> discard the provisional segment, resume
// STREAMING on the still-open prior segment from its persisted
// position (no resume-the-FSM replay). >COMMIT (the lineage DOES
// contain it) -> the new segment is authoritative; the idempotent
// re-cap is a no-op. COMMIT -- the lineage.json write -- is the single
// linearization point.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/orware/sluice/internal/ir"
)

const (
	// rotationReasonAge is the lineage CapReason recorded when the
	// --retain-rotate-at age threshold fires. Stable string for
	// cross-version tooling / operator inspection.
	rotationReasonAge = "retain-rotate-at"

	// rotationReasonChainLength is the CapReason recorded when the
	// --retain-rotate-at-chain-length threshold fires.
	rotationReasonChainLength = "retain-rotate-at-chain-length"
)

// RotationStateFileName is the crash-recovery marker at the lineage
// root. Single small JSON object; rewritten at each FSM phase.
const RotationStateFileName = "rotation_state.json"

// rotationSegmentDirPrefix is the on-disk prefix of every
// rotation-opened segment sub-directory (`seg-<unix-millis>/`). Single
// source of truth shared by the producer ([performRotation]'s
// provisional-dir construction) and the consumer ([resolveLineage]'s
// missing-catalog multi-segment-evidence guard, Bug 66): if any
// `seg-*` path exists but lineage.json is absent, the backup is a
// rotated multi-segment lineage that cannot be reconstructed from a
// bare walk — a loud refusal, never a silent root-only partial.
const rotationSegmentDirPrefix = "seg-"

// rotationPhase enumerates the FSM phases recorded in
// rotation_state.json. The ordering matters for crash recovery: the
// authority flip is the lineage.json write, not the phase string --
// the phase only narrows which provisional dir to clean up.
type rotationPhase string

const (
	rotationPhaseDrain    rotationPhase = "drain"
	rotationPhaseSnapshot rotationPhase = "snapshot"
	rotationPhaseBulkCopy rotationPhase = "bulkcopy"
	rotationPhaseCommit   rotationPhase = "commit"
	rotationPhaseDone     rotationPhase = "done"
)

// rotationState is the on-disk crash-recovery marker.
type rotationState struct {
	Phase            rotationPhase `json:"phase"`
	Reason           string        `json:"reason"`
	ProvisionalDir   string        `json:"provisional_dir"`
	PriorSegmentEnd  ir.Position   `json:"prior_segment_end"`
	PriorSegmentDir  string        `json:"prior_segment_dir"`
	StartedAt        time.Time     `json:"started_at"`
	NewSegmentAnchor ir.Position   `json:"new_segment_anchor,omitempty"`
}

// errRotationAbortStayOpen is the sentinel the rollover loop checks
// via errors.Is when the S>=P_N hard-fail (or any pre-COMMIT failure)
// fires: the rotation is loudly abandoned and the stream STAYS on the
// still-open prior segment (no gap introduced).
var errRotationAbortStayOpen = errors.New("rotation aborted; staying on the open segment (no data gap introduced)")

// rotationAbortError wraps the underlying cause of a pre-COMMIT abort
// while matching errRotationAbortStayOpen via errors.Is, so the
// rollover loop can branch on "stay open" while still surfacing the
// real cause in logs / wrapped errors.
type rotationAbortError struct {
	what string
	err  error
}

func (e *rotationAbortError) Error() string {
	if e.err == nil {
		return "rotation aborted (stay open): " + e.what
	}
	return fmt.Sprintf("rotation aborted (stay open): %s: %v", e.what, e.err)
}
func (e *rotationAbortError) Unwrap() error { return e.err }
func (e *rotationAbortError) Is(target error) bool {
	return target == errRotationAbortStayOpen
}

// abortStayOpen builds a rotationAbortError (matches the sentinel
// under errors.Is; carries the cause).
func abortStayOpen(what string, cause error) error {
	return &rotationAbortError{what: what, err: cause}
}

// rotationCrashPoint is a test-only failpoint. The crash-injection
// integration matrix (ADR-0036-style proof-of-falsification) sets it
// to simulate a process kill at a specific FSM edge: the hook is
// called with the edge name AFTER that edge's durable effect and
// returns a non-nil error to abort the FSM there (mimicking a kill).
// Production builds never set it (nil = zero-cost no-op). Edges:
// "post-drain", "post-snapshot", "post-bulkcopy", "pre-commit-write",
// "post-commit-write".
var rotationCrashPoint func(edge string) error

func crashAt(edge string) error {
	if rotationCrashPoint == nil {
		return nil
	}
	return rotationCrashPoint(edge)
}

// rotateInputs bundles the rollover loop's state the FSM needs.
type rotateInputs struct {
	reason        string
	lastCommitted *ir.Manifest // the prior segment's last committed manifest (P_N source)
	// changesCh is the SAME in-flight CDC channel the rollover loop is
	// consuming. ADR-0046 step 5: CDC continues on the SAME handle --
	// the FSM does NOT re-open / re-position the pump (the engine's
	// CDC reader is single-stream; re-calling StreamChanges errors).
	// The pump is forward-only and already past ~P_N; the new
	// segment's full snapshot at S >= P_N covers the (P_N, S] window,
	// and the new segment's incrementals record StartPosition = S, so
	// the idempotent restore + the seg[i].end <= seg[i+1].start
	// boundary invariant absorb the overlap (the snapshot->CDC handoff
	// dedup sluice already proves for the initial full->stream
	// transition, replicated per segment boundary).
	changesCh <-chan ir.Change
	now       func() time.Time
	clockNow  func() time.Time
}

// rotateResult is what the rollover loop needs to continue on the new
// segment after a committed rotation.
type rotateResult struct {
	newSegStore ir.BackupStore
	newSegCodec Codec
	newSegDir   string
	newFull     *ir.Manifest
	resumePos   ir.Position // S -- where the SAME cdc handle resumes
	changesCh   <-chan ir.Change
}

// shouldRotate returns a non-empty rotation reason when a threshold
// has tripped against the OPEN segment. Length is checked first (no
// I/O); age reads the open segment's full CreatedAt from the lineage.
// Returns "" when no threshold is configured / none has fired.
func (b *BackupStream) shouldRotate(ctx context.Context, openSegRolloverSeq int, now time.Time) string {
	if b.RetainRotateAtChainLength > 0 && openSegRolloverSeq >= b.RetainRotateAtChainLength {
		return rotationReasonChainLength
	}
	if b.RetainRotateAt > 0 {
		// Age is measured from the OPEN segment's full CreatedAt -- the
		// stable anchor for "how old is this segment", robust across
		// stream restarts mid-segment.
		cat, ok, err := loadLineageCatalog(ctx, b.Store)
		if err != nil || !ok || len(cat.Segments) == 0 {
			return ""
		}
		seg := &cat.Segments[len(cat.Segments)-1]
		ss := seg.store(b.Store)
		fm, err := readManifestAt(ctx, ss, seg.FullManifestPath)
		if err != nil || fm.CreatedAt.IsZero() {
			return ""
		}
		if now.Sub(fm.CreatedAt) >= b.RetainRotateAt {
			return rotationReasonAge
		}
	}
	return ""
}

// performRotation drives the FSM over the same cdc handle. On success
// it returns the new open segment's store/codec + the SAME cdc
// handle's resumed change channel (continuing from S). On an S>=P_N
// hard-fail or any pre-COMMIT failure it returns a wrapped
// errRotationAbortStayOpen (the caller stays on the prior segment).
func (b *BackupStream) performRotation(ctx context.Context, in rotateInputs) (rotateResult, error) {
	var zero rotateResult

	cat, ok, err := loadLineageCatalog(ctx, b.Store)
	if err != nil {
		return zero, fmt.Errorf("load lineage: %w", err)
	}
	if !ok || len(cat.Segments) == 0 {
		// No lineage.json yet -- the open segment hasn't been
		// catalogued (no rollover committed). Nothing to rotate;
		// abort-stay-open (the next committed rollover seeds it).
		return zero, abortStayOpen("lineage not yet catalogued", nil)
	}
	priorIdx := len(cat.Segments) - 1
	prior := &cat.Segments[priorIdx]

	// P_N -- the prior segment's last committed incremental position.
	// The rollover loop only calls us right after a committed rollover
	// that ended at a TxCommit boundary, so this is transaction-
	// consistent (DRAIN is satisfied by construction).
	pN := in.lastCommitted.EndPosition
	if pN.Engine == "" && pN.Token == "" {
		return zero, abortStayOpen("prior segment has no committed EndPosition", nil)
	}

	now := in.now()
	provisionalDir := fmt.Sprintf(rotationSegmentDirPrefix+"%013d", now.UTC().UnixMilli())
	st := &rotationState{
		Phase:           rotationPhaseDrain,
		Reason:          in.reason,
		ProvisionalDir:  provisionalDir,
		PriorSegmentEnd: pN,
		PriorSegmentDir: prior.Dir,
		StartedAt:       now.UTC(),
	}
	if err := writeRotationState(ctx, b.Store, st); err != nil {
		return zero, abortStayOpen("write rotation_state (drain)", err)
	}
	if err := crashAt("post-drain"); err != nil {
		return zero, abortStayOpen("crash-injection post-drain", err)
	}

	// SNAPSHOT + BULKCOPY: write the next segment's `backup full` into
	// its sub-dir. The Backup orchestrator opens a backup-scoped
	// snapshot (anchor S) against the live source and sweeps it; the
	// source is position-monotonic and we're at a quiesced rollover
	// boundary, so S >= P_N by construction. We hard-assert it below.
	st.Phase = rotationPhaseSnapshot
	if err := writeRotationState(ctx, b.Store, st); err != nil {
		return zero, abortStayOpen("write rotation_state (snapshot)", err)
	}
	segStore := newPrefixedStore(b.Store, provisionalDir)
	segCodec := resolveCodec(b.Codec)

	if err := crashAt("post-snapshot"); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("crash-injection post-snapshot", err)
	}
	st.Phase = rotationPhaseBulkCopy
	if err := writeRotationState(ctx, b.Store, st); err != nil {
		return zero, abortStayOpen("write rotation_state (bulkcopy)", err)
	}
	full := &Backup{
		Source:        b.Source,
		SourceDSN:     b.SourceDSN,
		Store:         segStore,
		ChunkRows:     b.ChunkChanges,
		SluiceVersion: b.SluiceVersion,
		SlotName:      b.SlotName,
		Encryption:    b.Encryption,
		Codec:         segCodec,
		Now:           in.now,
	}
	if err := full.Run(ctx); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("bulk-copy next segment full", err)
	}
	newFull, err := readManifestAt(ctx, segStore, ManifestFileName)
	if err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("read new segment full manifest", err)
	}
	s := newFull.EndPosition

	if err := crashAt("post-bulkcopy"); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("crash-injection post-bulkcopy", err)
	}

	// S >= P_N HARD-FAIL ASSERTION (ADR-0046 section 2, gotcha #1).
	if err := assertAnchorMonotonic(b.Source, pN, s); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("S>=P_N assertion", err)
	}

	// CDC continues on the SAME handle (ADR-0046 step 5). We do NOT
	// re-call StreamChanges -- the engine's CDC reader is single-
	// stream and the pump is already flowing forward from ~P_N. The
	// rollover loop keeps consuming the SAME channel; the new
	// segment's incrementals record StartPosition = S, so the
	// (P_N, S] window the new full's snapshot also captured is
	// absorbed idempotently on restore (the per-segment-boundary
	// replica of the snapshot->CDC handoff dedup).
	changesCh := in.changesCh

	// COMMIT -- the single atomic linearization point. The new full is
	// durable (Backup.Run above). Now the ONE atomic lineage.json
	// write appends the new segment AND caps the prior. A crash BEFORE
	// this write -> <=COMMIT (recovery discards the provisional); a
	// crash AFTER -> >COMMIT (new segment authoritative). There is no
	// in-between: the Put is all-or-nothing at the storage layer.
	st.Phase = rotationPhaseCommit
	st.NewSegmentAnchor = s
	if err := writeRotationState(ctx, b.Store, st); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("write rotation_state (commit)", err)
	}
	// mid-COMMIT: rotation_state=commit is durable but the atomic
	// lineage.json write has NOT happened yet. A crash here is <=COMMIT
	// (recovery discards the provisional, resumes the prior segment).
	if err := crashAt("pre-commit-write"); err != nil {
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("crash-injection pre-commit-write", err)
	}

	cappedAt := now.UTC()
	prior.CappedAt = &cappedAt
	prior.CapReason = in.reason
	prior.EndPosition = pN
	cat.Segments = append(cat.Segments, LineageSegment{
		SegmentID:        manifestBackupID(newFull),
		Dir:              provisionalDir,
		FullManifestPath: ManifestFileName,
		StartPosition:    s,
		EndPosition:      s,
		Codec:            segCodec,
	})
	cat.UpdatedAt = cappedAt
	if err := writeLineageCatalog(ctx, b.Store, cat); err != nil {
		// The atomic authority-flip write failed -> still <=COMMIT.
		b.discardProvisional(ctx, provisionalDir)
		return zero, abortStayOpen("atomic lineage commit", err)
	}

	// post-COMMIT: the atomic lineage.json write succeeded; authority
	// has flipped. A crash HERE is >COMMIT and is NOT a stay-open
	// abort — the new segment is durable & authoritative. A real
	// process kill here just ends the process; the test failpoint
	// models that by returning a FATAL (non-abortStayOpen) error so
	// the rollover loop exits THIS run rather than continuing with a
	// stale open-segment pointer. The next run's recoverRotationState
	// sees the provisional dir in the lineage (>COMMIT) and resumes
	// on the new segment.
	if err := crashAt("post-commit-write"); err != nil {
		return zero, fmt.Errorf("rotation committed then crash-injected (>COMMIT; new segment durable): %w", err)
	}

	// >COMMIT: authority flipped. Clear the recovery marker.
	st.Phase = rotationPhaseDone
	_ = b.Store.Delete(ctx, RotationStateFileName)

	slog.InfoContext(
		ctx, "rotation: COMMIT -- new segment authoritative",
		slog.String("reason", in.reason),
		slog.String("new_segment_dir", provisionalDir),
		slog.String("prior_capped_at", cappedAt.Format(time.RFC3339)),
		slog.String("anchor_token", s.Token),
	)

	return rotateResult{
		newSegStore: segStore,
		newSegCodec: segCodec,
		newSegDir:   provisionalDir,
		newFull:     newFull,
		resumePos:   s,
		changesCh:   changesCh,
	}, nil
}

// assertAnchorMonotonic is the S>=P_N hard-fail. The non-empty +
// engine-match checks always fire. When the engine implements
// ir.PositionMonotonicChecker (PG does), a reported regression (P_N
// is NOT <= S) is a hard error. An engine without the surface falls
// back to the same-handle structural guarantee plus the non-empty /
// engine-match assertion.
func assertAnchorMonotonic(src ir.Engine, pN, s ir.Position) error {
	if s.Engine == "" && s.Token == "" {
		return errors.New("S>=P_N hard-fail: new segment snapshot anchor is empty")
	}
	if s.Engine != pN.Engine {
		return fmt.Errorf("S>=P_N hard-fail: anchor engine %q != prior segment engine %q", s.Engine, pN.Engine)
	}
	if chk, ok := src.(ir.PositionMonotonicChecker); ok {
		le, err := chk.PrecedesOrEqual(pN, s)
		if err != nil {
			return fmt.Errorf("S>=P_N hard-fail: cannot prove monotonic (P_N=%q S=%q): %w", pN.Token, s.Token, err)
		}
		if !le {
			return fmt.Errorf("S>=P_N hard-fail: new segment anchor S=%q regressed before prior segment end P_N=%q -- rotation would silently gap writes in (S, P_N)",
				s.Token, pN.Token)
		}
	}
	return nil
}

// discardProvisional best-effort removes the provisional segment dir's
// contents after a pre-COMMIT abort. The lineage was never written, so
// the provisional dir is unreferenced; leaving it would just waste
// space. Errors are WARN-logged -- they don't compound the abort.
func (b *BackupStream) discardProvisional(ctx context.Context, dir string) {
	ss := newPrefixedStore(b.Store, dir)
	paths, err := ss.List(ctx, "")
	if err != nil {
		slog.WarnContext(ctx, "rotation: could not list provisional segment for cleanup",
			slog.String("dir", dir), slog.String("err", err.Error()))
		return
	}
	for _, p := range paths {
		if derr := ss.Delete(ctx, p); derr != nil {
			slog.WarnContext(ctx, "rotation: could not delete provisional segment file",
				slog.String("dir", dir), slog.String("path", p), slog.String("err", derr.Error()))
		}
	}
	_ = b.Store.Delete(ctx, RotationStateFileName)
}

// recoverRotationState reconciles a rotation_state.json left by a
// crash mid-FSM. The single linearization point is the lineage.json
// write, NOT the phase string. Recovery decides <=COMMIT vs >COMMIT
// purely by whether the lineage already contains the provisional
// segment dir:
//
//   - lineage does NOT contain provisional dir -> <=COMMIT: discard
//     the provisional segment, resume STREAMING on the still-open
//     prior segment from its persisted position (no resume-the-FSM
//     replay -- the prior segment never lost position).
//   - lineage DOES contain provisional dir -> >COMMIT: the new
//     segment is authoritative; the idempotent re-cap is a no-op.
//
// Either way the marker is cleared. Called once at BackupStream.Run
// start, before streaming.
func recoverRotationState(ctx context.Context, store ir.BackupStore) error {
	exists, err := store.Exists(ctx, RotationStateFileName)
	if err != nil {
		return fmt.Errorf("inspect %q: %w", RotationStateFileName, err)
	}
	if !exists {
		return nil
	}
	rc, err := store.Get(ctx, RotationStateFileName)
	if err != nil {
		return fmt.Errorf("get %q: %w", RotationStateFileName, err)
	}
	body, rerr := io.ReadAll(rc)
	_ = rc.Close()
	if rerr != nil {
		return fmt.Errorf("read %q: %w", RotationStateFileName, rerr)
	}
	var st rotationState
	if err := json.Unmarshal(body, &st); err != nil {
		// A corrupt marker is itself a <=COMMIT signal (we never reach
		// the atomic write with a corrupt marker on disk): clear it,
		// resume on the prior open segment.
		slog.WarnContext(ctx, "rotation recovery: corrupt rotation_state.json; treating as pre-COMMIT, discarding any provisional segment",
			slog.String("err", err.Error()))
		_ = store.Delete(ctx, RotationStateFileName)
		return nil
	}
	if st.Phase == rotationPhaseDone || st.ProvisionalDir == "" {
		_ = store.Delete(ctx, RotationStateFileName)
		return nil
	}

	cat, ok, err := loadLineageCatalog(ctx, store)
	if err != nil {
		return fmt.Errorf("rotation recovery: load lineage: %w", err)
	}
	committed := false
	if ok {
		for i := range cat.Segments {
			if cat.Segments[i].Dir == st.ProvisionalDir {
				committed = true
				break
			}
		}
	}

	if committed {
		// >COMMIT: new segment is authoritative. The atomic write
		// already capped the prior segment; nothing to redo (idempotent
		// re-cap = no-op). Just clear the marker.
		slog.InfoContext(
			ctx, "rotation recovery: >COMMIT -- new segment is authoritative; clearing marker",
			slog.String("provisional_dir", st.ProvisionalDir),
			slog.String("phase_at_crash", string(st.Phase)),
		)
		_ = store.Delete(ctx, RotationStateFileName)
		return nil
	}

	// <=COMMIT: the authority flip never happened. Discard the
	// provisional segment; the stream resumes STREAMING on the still-
	// open prior segment from its persisted position (the prior
	// segment never lost position -- no FSM replay).
	slog.InfoContext(
		ctx, "rotation recovery: <=COMMIT -- discarding provisional segment, resuming the still-open prior segment",
		slog.String("provisional_dir", st.ProvisionalDir),
		slog.String("phase_at_crash", string(st.Phase)),
		slog.String("prior_segment_dir", st.PriorSegmentDir),
	)
	ps := newPrefixedStore(store, st.ProvisionalDir)
	if paths, lerr := ps.List(ctx, ""); lerr == nil {
		for _, p := range paths {
			_ = ps.Delete(ctx, p)
		}
	}
	_ = store.Delete(ctx, RotationStateFileName)
	return nil
}

// writeRotationState writes the crash-recovery marker via a single
// Put (atomic at the storage layer).
func writeRotationState(ctx context.Context, store ir.BackupStore, st *rotationState) error {
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal rotation_state: %w", err)
	}
	return store.Put(ctx, RotationStateFileName, bytes.NewReader(b))
}
