// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The VStream copy-completion door (VSTREAM-COPY-COMPLETION-UNPROVEN, Bug 327).
//
// # The harm
//
// Until v0.163.3 a PlanetScale/Vitess cold start persisted checkpoints
// during its copy (ADR-0072 Phase B). Bug 325 froze the checkpoint at the
// first one, written a few thousand rows in, and that first checkpoint
// often carried no copy cursor: a bare per-shard GTID list, byte for byte
// the shape of a finished copy's CDC anchor. v0.163.3 refuses a persisted
// position that carries a cursor (VSTREAM-COPY-RESUME-WITHDRAWN), but a
// cursor-less one reaches the warm resume, which copies nothing: the rest of
// the copy is never made and the stream reports healthy. The v0.163.3
// regression cycle measured 40,000–49,000 of 300,000 rows missing at exit 0.
//
// # The evidence
//
// The position cannot tell the two apart, so the door asks the target.
//
//   - The established-table record ([ir.EstablishedTablesStore]) is proof on
//     its own. Since v0.163.0 the cold start writes it in the CDC anchor's own
//     transaction ([writeColdStartAnchor]), after the copy, the float repair
//     and the FK verification; a mid-copy checkpoint never wrote it.
//   - The `sync-<stream-id>` header in sluice_migrate_state
//     ([syncMigrationID]) is NOT proof on its own, because it can say
//     `complete` about a target that does not hold the copy. Since v0.149.0
//     every cold-start lane flips it to complete as soon as the bulk copy
//     returns ([markRecordedColdStartComplete]) — before the VStream FLOAT
//     re-read, the FK verification and the anchor, so a ≤v0.163.1 stop in
//     those phases left a complete header beside the frozen pre-complete
//     checkpoint (F2). And [beginRecordedColdStart] clears it only inside
//     coldStartRunCopy, after --restart-from-scratch has already dropped the
//     target tables, and not at all when the recording context is inert (a
//     store that would not open, a failed EnsureControlTable, a stream id
//     over 250 characters) or ClearMigration fails; so a re-copy stopped in
//     that window leaves an old complete header beside an emptied target
//     (F1). A complete header therefore counts only with two more facts:
//     every in-scope table it records as having copied rows still holds rows
//     on the target, and the stream's sluice_cdc_state row was written after
//     the header (the anchor is written after complete, a frozen checkpoint
//     before), compared on one clock ([ir.ControlTimestampClock]).
//
// A header short of complete, a complete header whose copied table is empty
// on the target, or a position row older than the header is POSITIVE
// evidence that the copy did not finish, and --assert-copy-finished cannot
// override it. No header at all, or clocks that cannot be compared, is only
// absent evidence, and the flag can.
//
// UNVERIFIED PREMISES, written down rather than implied: (1) a ≤v0.163.1
// stream stopped in the F2 window and then warm-resumed by v0.163.2/v0.163.3
// (a Bug 327 victim already) has a position row rewritten since, which the
// clock comparison reads as finished; (2) a ≤v0.163.1 re-copy with inert
// recording stopped after writing some rows (F1) leaves every copied table
// non-empty, which the emptiness check reads as finished; (3) on Postgres the
// clock answer is this session's UTC offset, standing in for the offset of
// the session that wrote an older row.
//
// Who is refused: every stream whose last cold start finished on v0.99.5
// through v0.148.x, and every later one whose progress recording was
// disabled or whose evidence does not hold up as above. That false refusal
// was accepted by the operator (2026-10-21) as the price of refusing the
// silent loss; --assert-copy-finished is its escape where nothing contradicts
// it.
//
// # Fail closed
//
// The door runs exactly where persisted state is legacy or missing, so no
// read it makes is allowed to pass by failing: a record, header, position
// row, clock or target table that cannot be read refuses (restartable,
// through [controlReadFailure]); a control table this role cannot bring up to
// date refuses terminally with the `sluice control-tables ddl` remedy
// ([ir.ErrControlTableDDLRequired]: a restart meets the same refusal); a
// target that keeps neither surface proves nothing and refuses.
//
// # Where it runs, and who reaches it
//
// On both warm-resume branches of [Streamer.phaseOpenChangeStream], first,
// before the INCLUDE-TABLE-SCOPE-DRIFT door and the reader open, so a
// refusal has applied no change and changed no table in the stream's scope.
// Every caller that warm-resumes a VStream position from the target's control
// row goes through there: `sync start`, every `sync run` leg and supervisor
// restart (Streamer.Run), and the in-process retry ladder (runOnce again).
// The door is skipped, deliberately, for --position-from-manifest: that
// position is a backup chain's terminal manifest, never a sync cold start's
// copy checkpoint, and the copy under it is the restore's; that handoff
// records the restored tables once it starts ([Streamer.recordManifestHandoff]),
// so its later plain restarts pass on the record. `sync from-backup run` (the
// broker) has its own position format and never opens a VStream reader. A
// cold start, --restart-from-scratch and --reset-target-data never reach a
// warm-resume branch.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// vstreamCopyUnprovenMarker names the refusal of a VStream warm resume whose
// copy nothing on the target proves finished.
const vstreamCopyUnprovenMarker = "VSTREAM-COPY-COMPLETION-UNPROVEN"

// copyFinishedAssertedMarker names the WARN of a resume the operator let
// through with --assert-copy-finished.
const copyFinishedAssertedMarker = "COPY-FINISHED-ASSERTED"

// recopyRemedy is the re-copy, said with the two flags under which
// --restart-from-scratch does not clear the target.
const recopyRemedy = "the same `sluice sync start` command with --restart-from-scratch, which drops the in-scope " +
	"target tables and copies them again (with --schema-already-applied it cannot drop them and refuses " +
	"RESTART-FROM-SCRATCH-TARGET-NOT-CLEARED while they hold rows, so empty them yourself first; with " +
	"--inject-shard-column it refuses until this shard's rows are deleted by hand)"

// copyCompletion is what the target says about a stream's copy: proven, or
// unproven with the reason. contradicted marks positive evidence that the
// copy did not finish, which --assert-copy-finished cannot override.
type copyCompletion struct {
	proven       bool
	contradicted bool
	why          string
}

// vstreamCopyDoorApplies reports whether the copy-completion door judges this
// run: a source that copies through VStream (the only source whose cold start
// ever persisted a position during its copy), with the position read from the
// target's control row rather than a backup chain.
func (s *Streamer) vstreamCopyDoorApplies() bool {
	return s.Source.Capabilities().CDC == ir.CDCVStream && s.PositionFromManifestStore == nil
}

// phaseRefuseUnprovenVStreamCopy is the door. It returns assertRecord=true
// when it passed on the operator's --assert-copy-finished alone: the caller
// then records the stream's tables once the scope door has passed
// ([Streamer.recordAssertedCopy]), so later resumes pass on the record.
func (s *Streamer) phaseRefuseUnprovenVStreamCopy(ctx context.Context, applier ir.ChangeApplier, streamID string, persisted ir.Position) (assertRecord bool, err error) {
	if !s.vstreamCopyDoorApplies() {
		return false, nil
	}
	c, err := s.vstreamCopyCompletion(ctx, applier, streamID)
	if err != nil {
		return false, err
	}
	if c.proven {
		return false, nil
	}
	if s.AssertCopyFinished && !c.contradicted {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": nothing on the target proves this PlanetScale/Vitess stream's "+
			"cold-start copy finished ("+c.why+"); resuming because --assert-copy-finished says it did. If it did not, "+
			"the rows the copy never reached stay missing on the target; re-copy with --restart-from-scratch",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
		return true, nil
	}
	slog.ErrorContext(ctx, "resume refused: "+vstreamCopyUnprovenMarker,
		slog.String("stream_id", streamID),
		slog.String("reason", c.why),
		slog.Bool("assert_copy_finished", s.AssertCopyFinished),
		slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
	if c.contradicted && s.AssertCopyFinished {
		return false, assertContradictedError(streamID, c.why)
	}
	return false, vstreamCopyUnprovenError(streamID, c.why)
}

// vstreamCopyCompletion reads the evidence of a finished copy, the
// established-table record first (it is on the applier already open). A
// read that fails is an error, never "unproven" and never "proven".
func (s *Streamer) vstreamCopyCompletion(ctx context.Context, applier ir.ChangeApplier, streamID string) (copyCompletion, error) {
	var missing []string
	if store, ok := applier.(ir.EstablishedTablesStore); ok {
		rec, err := store.ReadEstablishedRecord(ctx, streamID)
		if err != nil {
			return copyCompletion{}, recordReadFailure(err)
		}
		// A record in progress is still a record: the scope door refuses it
		// COLD-START-INCOMPLETE next, which names the right remedy.
		if rec.Recorded {
			return copyCompletion{proven: true}, nil
		}
		missing = append(missing, "the stream has no established-table record")
	} else {
		missing = append(missing, "the target keeps no established-table record")
	}
	unproven := func(contradicted bool, why string) (copyCompletion, error) {
		return copyCompletion{contradicted: contradicted, why: strings.Join(append(missing, why), ", and ")}, nil
	}

	mstore, err := openMigrationStateStore(ctx, s.Target, s.TargetDSN, s.TargetSchema)
	if err != nil {
		return copyCompletion{}, controlReadFailure(fmt.Errorf("pipeline: %s: %w", vstreamCopyUnprovenMarker, err))
	}
	if mstore == nil {
		return unproven(false, "the target keeps no cold-start progress record")
	}
	defer migcore.CloseIf(mstore)
	id := syncMigrationID(streamID)
	state, found, err := readRecordedColdStart(ctx, mstore, id)
	if err != nil {
		if errors.Is(err, ir.ErrControlTableDDLRequired) {
			return copyCompletion{}, controlTableDDLRefusal(streamID, err)
		}
		return copyCompletion{}, controlReadFailure(fmt.Errorf("pipeline: %s: read the cold-start progress record %q: %w",
			vstreamCopyUnprovenMarker, id, err))
	}
	if !found {
		return unproven(false, fmt.Sprintf("no cold-start progress record (sluice_migrate_state %q)", id))
	}
	if state.Phase != ir.MigrationPhaseComplete {
		return unproven(true, fmt.Sprintf("its cold-start progress record (sluice_migrate_state %q) reads phase %q, "+
			"not %q: the copy it records did not finish", id, state.Phase, ir.MigrationPhaseComplete))
	}

	// A complete header alone is not proof (see the file comment).
	if why, err := s.headerCopyStillOnTarget(ctx, applier, streamID, state); err != nil {
		return copyCompletion{}, err
	} else if why != "" {
		return unproven(true, why)
	}
	verdict, why, err := positionRowAfterHeader(ctx, applier, streamID, state)
	if err != nil {
		return copyCompletion{}, err
	}
	switch verdict {
	case orderAfter:
		return copyCompletion{proven: true}, nil
	case orderBefore:
		return unproven(true, why)
	default:
		return unproven(false, why)
	}
}

// headerCopyStillOnTarget returns "" when every in-scope table the header
// records as having copied rows still holds rows on the target, and otherwise
// names the first that does not (F1: a re-copy that dropped the target and
// stopped before clearing the header).
func (s *Streamer) headerCopyStillOnTarget(ctx context.Context, applier ir.ChangeApplier, streamID string, state ir.MigrationState) (string, error) {
	scope, _, err := s.currentScopeKeys(ctx, applier, streamID)
	if err != nil {
		return "", controlReadFailure(fmt.Errorf("pipeline: %s: read the stream's scope: %w", vstreamCopyUnprovenMarker, err))
	}
	var expect []string
	for _, k := range scope {
		if state.TableProgress[bareTableOfKey(k)].RowsCopied > 0 {
			expect = append(expect, k)
		}
	}
	if len(expect) == 0 {
		return "", nil
	}
	_, holding, err := s.targetTableFacts(ctx, expect)
	if err != nil {
		return "", controlReadFailure(fmt.Errorf("pipeline: %s: check that the recorded copy is still on the target: %w",
			vstreamCopyUnprovenMarker, err))
	}
	for _, k := range expect {
		if !holding[k] {
			return fmt.Sprintf("its cold-start progress record reads complete with %d rows copied into %s, but that target "+
				"table is empty or absent: the target no longer holds the copy the record describes",
				state.TableProgress[bareTableOfKey(k)].RowsCopied, displayKey(k)), nil
		}
	}
	return "", nil
}

// writeOrder is how the stream's position row's last write stands against
// the header's.
type writeOrder int

const (
	orderUnknown writeOrder = iota
	orderBefore
	orderAfter
)

// positionRowAfterHeader compares the stream's sluice_cdc_state updated_at
// with the header's (F2): the CDC anchor is written after the header goes
// complete, a ≤v0.163.1 frozen checkpoint before it.
func positionRowAfterHeader(ctx context.Context, applier ir.ChangeApplier, streamID string, state ir.MigrationState) (writeOrder, string, error) {
	clock, ok := applier.(ir.ControlTimestampClock)
	if !ok {
		return orderUnknown, "this target cannot say whether its control-table timestamps share a clock, so the order of the " +
			"position and the progress record is unknown", nil
	}
	same, err := clock.ControlTimestampsComparable(ctx)
	if err != nil {
		return orderUnknown, "", controlReadFailure(fmt.Errorf("pipeline: %s: %w", vstreamCopyUnprovenMarker, err))
	}
	if !same {
		return orderUnknown, "this target's session is not on UTC, so a position row an older release wrote in local time " +
			"cannot be ordered against the UTC progress record", nil
	}
	streams, err := applier.ListStreams(ctx)
	if err != nil {
		return orderUnknown, "", controlReadFailure(fmt.Errorf("pipeline: %s: read the stream's position row: %w", vstreamCopyUnprovenMarker, err))
	}
	var row *ir.StreamStatus
	for i := range streams {
		if streams[i].StreamID == streamID {
			row = &streams[i]
			break
		}
	}
	switch {
	case row == nil || row.UpdatedAt.IsZero() || state.UpdatedAt.IsZero():
		return orderUnknown, "the position row's or the progress record's last write time is missing", nil
	case row.UpdatedAt.After(state.UpdatedAt):
		return orderAfter, "", nil
	case row.UpdatedAt.Before(state.UpdatedAt):
		return orderBefore, fmt.Sprintf("its position was last written at %s, before its cold-start progress record went "+
			"complete at %s: that position is a checkpoint from during the copy, not the anchor a finished copy writes",
			row.UpdatedAt.UTC().Format("2006-01-02 15:04:05.000000"), state.UpdatedAt.UTC().Format("2006-01-02 15:04:05.000000")), nil
	default:
		return orderUnknown, "the position row and the progress record were last written in the same instant, so their order is unknown", nil
	}
}

// vstreamCopyUnprovenError carries [ir.ErrIncludeTableScopeDrift], as
// VSTREAM-COPY-RESUME-WITHDRAWN does: a restart reads the same control rows
// back and refuses again, so the supervisor must not retry it
// ([refusalsARestartRepeats]).
func vstreamCopyUnprovenError(streamID, why string) error {
	return fmt.Errorf("pipeline: resume refused: %w: %s: sluice cannot tell whether PlanetScale/Vitess stream %q's "+
		"cold-start copy finished (%s). A cold start stopped during its copy by sluice v0.163.1 or earlier can have saved "+
		"a position that looks exactly like a finished copy's, and resuming from it would never copy the rest of the "+
		"tables, while the stream reported healthy. A copy that finished on v0.149.0 or later normally leaves a record "+
		"proving it (unless that run could not record its progress); this stream's does not hold up. No change has been "+
		"applied by this run and no table in the stream's scope was changed on the target. Re-copy the stream: %s. If you "+
		"have verified, table by table, that the target's row counts match the source's, re-run the same command once with "+
		"--assert-copy-finished: it records every in-scope table that exists on the target and holds rows as copied, so "+
		"later resumes pass without it (it is refused when the target shows the copy did not finish)",
		ir.ErrIncludeTableScopeDrift, vstreamCopyUnprovenMarker, streamID, why, recopyRemedy)
}

// assertContradictedError refuses --assert-copy-finished over positive
// evidence that the copy did not finish — the Bug 327 fingerprint.
func assertContradictedError(streamID, why string) error {
	return fmt.Errorf("pipeline: resume refused: %w: %s: --assert-copy-finished cannot be accepted for stream %q: the target "+
		"shows its cold-start copy did not finish (%s). Resuming would leave the rows the copy never reached missing. No "+
		"change has been applied by this run and no table in the stream's scope was changed on the target. Re-copy the "+
		"stream: %s",
		ir.ErrIncludeTableScopeDrift, vstreamCopyUnprovenMarker, streamID, why, recopyRemedy)
}

// controlTableDDLRefusal is the door's terminal refusal when the progress
// record cannot be read because this role cannot bring sluice_migrate_state up
// to date: a restart meets the same refusal, so it carries the no-restart
// sentinel instead of the restartable control-read wrap.
func controlTableDDLRefusal(streamID string, err error) error {
	return fmt.Errorf("pipeline: resume refused: %w: %s: stream %q's cold-start progress record cannot be read, because "+
		"the target's sluice_migrate_state control table is missing or out of date and this role cannot run the DDL that "+
		"fixes it. Have the statements `sluice control-tables ddl --engine <target engine>` prints applied by a role that "+
		"can (on a PlanetScale branch with safe migrations, ship each with `sluice deploy-ddl`), then re-run the same "+
		"command. No change has been applied by this run: %w",
		ir.ErrIncludeTableScopeDrift, vstreamCopyUnprovenMarker, streamID, err)
}

// recordAssertedCopy records, after a resume the operator let through with
// --assert-copy-finished, every in-scope table (live-added tables included)
// that exists on the target and holds rows, with each table's engine id where
// the source has one, through the same write a cold start makes
// ([recordEstablishedAfterCopy]). An in-scope table that is absent or empty
// on the target was not copied whatever the operator believes, and recording
// it would hide it from the INCLUDE-TABLE-SCOPE-DRIFT door: it is left out
// and named. It runs once the scope door has passed. A failure refuses:
// passing without the record would ask for the flag again on every resume
// while saying nothing.
func (s *Streamer) recordAssertedCopy(ctx context.Context, applier ir.ChangeApplier, streamID string) error {
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": this target keeps no established-table record, so the "+
			"acknowledgement cannot be recorded; every resume of this stream needs --assert-copy-finished",
			slog.String("stream_id", streamID))
		return nil
	}
	scope, _, err := s.currentScopeKeys(ctx, applier, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: %w", copyFinishedAssertedMarker, err))
	}
	_, holding, err := s.targetTableFacts(ctx, scope)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: read which in-scope tables hold rows on the target: %w",
			copyFinishedAssertedMarker, err))
	}
	var keys, names, skipped []string
	for _, k := range scope {
		if holding[k] {
			keys = append(keys, k)
			names = append(names, displayKey(k))
		} else {
			skipped = append(skipped, displayKey(k))
		}
	}
	if err := recordEstablishedAfterCopy(ctx, store, streamID, keys, s.establishedIDs(ctx, keys), s.sourceTableKeys); err != nil {
		if errors.Is(err, ir.ErrControlTableDDLRequired) {
			return fmt.Errorf("pipeline: resume refused: %w: %s: --assert-copy-finished could not record this stream's tables, "+
				"because this role cannot bring the sluice_cdc_state control table up to date. Have the statements `sluice "+
				"control-tables ddl --engine <target engine>` prints applied by a role that can (on a PlanetScale branch with "+
				"safe migrations, ship each with `sluice deploy-ddl`), then re-run the same command. No change has been "+
				"applied by this run: %w", ir.ErrIncludeTableScopeDrift, copyFinishedAssertedMarker, err)
		}
		return controlReadFailure(fmt.Errorf("pipeline: resume refused: %s: --assert-copy-finished could not record this "+
			"stream's tables as copied (no change has been applied by this run); fix what the error names and re-run the "+
			"same command: %w", copyFinishedAssertedMarker, err))
	}
	slog.WarnContext(ctx, copyFinishedAssertedMarker+": recorded the in-scope tables that hold rows on the target as copied, "+
		"on the operator's --assert-copy-finished; later resumes pass without the flag",
		slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	if len(skipped) > 0 {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": in-scope tables that are absent or empty on the target were NOT "+
			"recorded as copied; the next start judges each as never copied (INCLUDE-TABLE-SCOPE-DRIFT) — empty its target "+
			"table if it has one and bring it in with `sluice schema add-table`, or take it out of scope",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(skipped, ", ")))
	}
	return nil
}

// currentScopeKeys lists the stream's current scope as established-record
// keys, with display names: the set a cold start of this scope would copy
// and record, live-added tables included.
func (s *Streamer) currentScopeKeys(ctx context.Context, applier ir.ChangeApplier, streamID string) (keys, names []string, err error) {
	liveAdded, err := s.uniqueKeyDoorLiveAdded(ctx, applier, streamID)
	if err != nil {
		return nil, nil, err
	}
	scope, err := s.readStreamScope(ctx, func(table string) bool {
		return scopeAdmits(table, s.Filter, func(t string) bool { return liveAdded[t] })
	})
	if err != nil {
		return nil, nil, err
	}
	keys = make([]string, 0, len(scope.tables))
	names = make([]string, 0, len(scope.tables))
	for _, t := range scope.tables {
		keys = append(keys, t.key())
		names = append(names, t.display())
	}
	sort.Strings(names)
	return keys, names, nil
}

// targetTableFacts reports, for each established-record key, whether its
// target table exists (the target catalog, matched as the scope door matches
// it) and whether it holds rows ([ir.TableEmptyChecker]). A target that
// cannot answer the second question is an error, never "holds rows".
func (s *Streamer) targetTableFacts(ctx context.Context, keys []string) (exists, holding map[string]bool, err error) {
	exists, holding = map[string]bool{}, map[string]bool{}
	if len(keys) == 0 {
		return exists, holding, nil
	}
	pairs, err := s.uniqueKeyDoorScopes(ctx)
	if err != nil {
		return nil, nil, err
	}
	byNS := map[string]uniqueKeyDoorScope{}
	for _, p := range pairs {
		byNS[p.namespace] = p
	}
	grouped := map[string][]string{}
	for _, k := range keys {
		ns := ""
		if i := strings.LastIndexByte(k, 0); i >= 0 {
			ns = k[:i]
		}
		grouped[ns] = append(grouped[ns], k)
	}
	for ns, ks := range grouped {
		sc, ok := byNS[ns]
		if !ok {
			return nil, nil, fmt.Errorf("no target for namespace %q", ns)
		}
		names := map[string]bool{}
		for _, k := range ks {
			names[strings.ToLower(bareTableOfKey(k))] = true
		}
		tgt, err := s.readUniqueKeyDoorTarget(ctx, sc, names)
		if err != nil {
			if ns != "" && ir.IsDatabaseNotFound(err) {
				continue // no target database: no table exists there
			}
			return nil, nil, err
		}
		if err := s.markHoldingTables(ctx, sc, tgt, ks, exists, holding); err != nil {
			return nil, nil, err
		}
	}
	return exists, holding, nil
}

func (s *Streamer) markHoldingTables(ctx context.Context, sc uniqueKeyDoorScope, tgt []*ir.Table, keys []string, exists, holding map[string]bool) error {
	rw, err := s.Target.OpenRowWriter(ctx, sc.targetDSN)
	if err != nil {
		return connectHint(fmt.Errorf("open target row writer: %w", err))
	}
	defer migcore.CloseIf(rw)
	migcore.ApplyTargetSchema(rw, sc.targetSchema)
	checker, ok := rw.(ir.TableEmptyChecker)
	if !ok {
		return fmt.Errorf("target engine %q cannot report whether a table holds rows", s.Target.Name())
	}
	for _, k := range keys {
		t := matchTargetTable(tgt, bareTableOfKey(k))
		if t == nil {
			continue
		}
		exists[k] = true
		empty, err := checker.IsTableEmpty(ctx, t)
		if err != nil {
			return fmt.Errorf("probe target table %q: %w", t.Name, err)
		}
		holding[k] = !empty
	}
	return nil
}
