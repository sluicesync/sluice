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
//     on the target — or, where it does not, is empty on the SOURCE too (a
//     drained table; the source probe is the independent expected value,
//     [Streamer.headerCopyStillOnTarget]) — and the stream's sluice_cdc_state
//     row was written after the header (the anchor is written after
//     complete, a frozen checkpoint before), compared on one clock
//     ([ir.ControlTimestampClock]; Postgres sessions on UTC year-round only,
//     never a MySQL-family target, whose updated_at moves on non-position
//     writes too). A pass on that proof records the established tables, so
//     it is judged once.
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
// clock comparison reads as finished — and since a header-only pass now
// records the established tables, that verdict is permanent for the stream
// (its later starts pass on the record). The outcome is unchanged by that:
// the rows the stopped copy never reached were already lost by the earlier
// warm resume, and a re-copy is the remedy either way; (2) a ≤v0.163.1 re-copy with inert
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
	// headerProof: proven by the progress header and the target, with no
	// established-table record yet.
	headerProof bool
}

// recordAfterDoor is what the door asks its caller to record once the scope
// door has passed ([Streamer.recordAfterCopyDoor]).
type recordAfterDoor int

const (
	recordNothing recordAfterDoor = iota
	// recordAsserted: passed on --assert-copy-finished alone. Records the
	// in-scope tables that exist on the target and hold rows (F4).
	recordAsserted
	// recordHeaderProof: passed on the progress header and the target.
	// Records the in-scope tables that exist on the target, so later starts
	// and runOnce retries pass on the record and the header is judged once:
	// a table drained since (a queue, a replicated TRUNCATE) is legitimately
	// empty, and a later judgment of it must not depend on the source probe
	// again.
	recordHeaderProof
)

// vstreamCopyDoorApplies reports whether the copy-completion door judges this
// run: a source that copies through VStream (the only source whose cold start
// ever persisted a position during its copy), with the position read from the
// target's control row rather than a backup chain.
func (s *Streamer) vstreamCopyDoorApplies() bool {
	return s.Source.Capabilities().CDC == ir.CDCVStream && s.PositionFromManifestStore == nil
}

// phaseRefuseUnprovenVStreamCopy is the door. On a pass that rests on
// anything but an established-table record it says what the caller records
// once the scope door has passed ([Streamer.recordAfterCopyDoor]), so later
// resumes pass on the record.
func (s *Streamer) phaseRefuseUnprovenVStreamCopy(ctx context.Context, applier ir.ChangeApplier, streamID string, persisted ir.Position) (recordAfterDoor, error) {
	if !s.vstreamCopyDoorApplies() {
		return recordNothing, nil
	}
	s.copyDoorHeaderTables = nil
	c, err := s.vstreamCopyCompletion(ctx, applier, streamID)
	if err != nil {
		return recordNothing, err
	}
	if c.proven {
		if c.headerProof {
			return recordHeaderProof, nil
		}
		return recordNothing, nil
	}
	if s.AssertCopyFinished && !c.contradicted {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": nothing on the target proves this PlanetScale/Vitess stream's "+
			"cold-start copy finished ("+c.why+"); resuming because --assert-copy-finished says it did. If it did not, "+
			"the rows the copy never reached stay missing on the target; re-copy with --restart-from-scratch",
			slog.String("stream_id", streamID),
			slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
		return recordAsserted, nil
	}
	slog.ErrorContext(ctx, "resume refused: "+vstreamCopyUnprovenMarker,
		slog.String("stream_id", streamID),
		slog.String("reason", c.why),
		slog.Bool("assert_copy_finished", s.AssertCopyFinished),
		slog.String("position_token", truncateDryRunToken(persisted.Token, 60)))
	if c.contradicted && s.AssertCopyFinished {
		return recordNothing, assertContradictedError(streamID, c.why)
	}
	return recordNothing, vstreamCopyUnprovenError(streamID, c.why)
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
	if why, contradicted, err := s.headerCopyStillOnTarget(ctx, applier, streamID, state); err != nil {
		return copyCompletion{}, err
	} else if why != "" {
		return unproven(contradicted, why)
	}
	verdict, why, err := positionRowAfterHeader(ctx, applier, streamID, state)
	if err != nil {
		return copyCompletion{}, err
	}
	switch verdict {
	case orderAfter:
		tables := make(map[string]bool, len(state.TableProgress))
		for name := range state.TableProgress {
			tables[name] = true // RowsCopied == 0 entries included: the copy covered them
		}
		s.copyDoorHeaderTables = tables
		return copyCompletion{proven: true, headerProof: true}, nil
	case orderBefore:
		return unproven(true, why)
	default:
		return unproven(false, why)
	}
}

// headerCopyStillOnTarget returns "" when the copy the header describes is
// still on the target, and otherwise why not (F1: a re-copy that dropped the
// target and stopped before clearing the header).
//
// A table the header records as having copied rows that is now empty or
// absent on the target is not, by itself, evidence of F1: a healthy finished
// stream whose source table has been drained since (a queue, a session table,
// a replicated TRUNCATE) looks the same. The independent expected value is
// the SOURCE: a read-only probe for one row through the source's own row-read
// path ([Streamer.sourceTablesHoldRows]). Source empty too — the table was
// drained, and that is consistent. Source holding rows — the target lost the
// copy (contradicted, and --assert-copy-finished is refused). Probe that
// cannot run — unproven but NOT contradicted, so the flag stays an exit. A
// table under a `--where` row filter is never probed (the probe is
// unfiltered) and is unproven but NOT contradicted for the same reason.
func (s *Streamer) headerCopyStillOnTarget(ctx context.Context, applier ir.ChangeApplier, streamID string, state ir.MigrationState) (why string, contradicted bool, err error) {
	scope, _, err := s.currentScopeKeys(ctx, applier, streamID)
	if err != nil {
		return "", false, controlReadFailure(fmt.Errorf("pipeline: %s: read the stream's scope: %w", vstreamCopyUnprovenMarker, err))
	}
	var expect []string
	for _, k := range scope {
		if state.TableProgress[bareTableOfKey(k)].RowsCopied > 0 {
			expect = append(expect, k)
		}
	}
	if len(expect) == 0 {
		return "", false, nil
	}
	_, holding, err := s.targetTableFacts(ctx, expect)
	if err != nil {
		return "", false, controlReadFailure(fmt.Errorf("pipeline: %s: check that the recorded copy is still on the target: %w",
			vstreamCopyUnprovenMarker, err))
	}
	var emptied []string
	for _, k := range expect {
		if !holding[k] {
			emptied = append(emptied, k)
		}
	}
	if len(emptied) == 0 {
		return "", false, nil
	}
	// A table the stream filters (`--where`) is not probed: the probe reads
	// the source unfiltered, so it would find the rows the filter excludes and
	// call a stream whose matching rows have since gone a lost copy — refusing
	// the flag too, a dead end. Unproven, not contradicted, keeps the flag the
	// exit; plumbing the predicate into the probe would buy a sharper answer
	// for a narrow case at the cost of a second predicate path.
	//
	// The other emptied tables are still probed, and a contradiction on any of
	// them outranks an unproven answer on another: a filtered table, or a probe
	// that failed for one table, must not mask a lost copy the probe did see —
	// that would let --assert-copy-finished through over positive evidence.
	var filtered, probed []string
	for _, k := range emptied {
		if s.RowFilters[bareTableOfKey(k)] != "" {
			filtered = append(filtered, k)
		} else {
			probed = append(probed, k)
		}
	}
	var onSource map[string]bool
	var probeErr error
	if len(probed) > 0 {
		onSource, probeErr = s.sourceTablesHoldRows(ctx, probed)
	}
	for _, k := range probed {
		if onSource[k] {
			return fmt.Sprintf("its cold-start progress record reads complete with %d rows copied into %s, but that target "+
				"table is empty or absent while the source table holds rows: the target no longer holds the copy the record "+
				"describes", state.TableProgress[bareTableOfKey(k)].RowsCopied, displayKey(k)), true, nil
		}
	}
	if probeErr != nil {
		k := probed[0]
		return fmt.Sprintf("its cold-start progress record reads complete with %d rows copied into %s, that target table "+
			"is empty or absent, and the source could not be probed to tell a drained table from a lost copy (%v)",
			state.TableProgress[bareTableOfKey(k)].RowsCopied, displayKey(k), probeErr), false, nil
	}
	if len(filtered) > 0 {
		k := filtered[0]
		return fmt.Sprintf("its cold-start progress record reads complete with %d rows copied into %s, that target table "+
			"is empty or absent, and the table carries a --where row filter, so an unfiltered source probe cannot tell "+
			"a table whose matching rows have gone from a lost copy",
			state.TableProgress[bareTableOfKey(k)].RowsCopied, displayKey(k)), false, nil
	}
	return "", false, nil // every emptied table is empty on the source too: drained since
}

// sourceTablesHoldRows probes each key's source table for one row through
// the source's row-read path ([ir.BatchedRowReader], at most one row read):
// read-only, and independent of everything the target says. A table the
// source no longer has, a reader without the batched surface, or a table it
// cannot read in batches (no primary key) is an error, never an answer.
func (s *Streamer) sourceTablesHoldRows(ctx context.Context, keys []string) (map[string]bool, error) {
	names := map[string]string{} // bare name -> key
	for _, k := range keys {
		if strings.IndexByte(k, 0) >= 0 {
			return nil, fmt.Errorf("multi-namespace key %s is not probed", displayKey(k))
		}
		names[k] = k
	}
	sr, schema, err := s.openScopedSourceSchema(ctx, s.SourceDSN, func(n string) bool { _, ok := names[n]; return ok })
	if err != nil {
		return nil, err
	}
	migcore.CloseIf(sr)
	tables := map[string]*ir.Table{}
	if schema != nil {
		for _, t := range schema.Tables {
			if t != nil {
				tables[t.Name] = t
			}
		}
	}
	rr, err := s.Source.OpenRowReader(ctx, s.SourceDSN)
	if err != nil {
		return nil, connectHint(fmt.Errorf("open source row reader: %w", err))
	}
	defer migcore.CloseIf(rr)
	br, ok := rr.(ir.BatchedRowReader)
	if !ok {
		return nil, fmt.Errorf("source engine %q cannot read one row of a table", s.Source.Name())
	}
	out := map[string]bool{}
	for name, k := range names {
		t := tables[name]
		if t == nil {
			return nil, fmt.Errorf("source table %q not found", name)
		}
		rows, err := br.ReadRowsBatch(ctx, t, nil, 1)
		if err != nil {
			return nil, fmt.Errorf("probe source table %q: %w", name, err)
		}
		n := 0
		for range rows {
			n++
		}
		if err := rr.Err(); err != nil {
			return nil, fmt.Errorf("probe source table %q: %w", name, err)
		}
		out[k] = n > 0
	}
	return out, nil
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
		return orderUnknown, "this target's control-table timestamps cannot order the position against the progress record " +
			"(on a MySQL-family target sluice_cdc_state.updated_at also moves on stop requests and live-add writes; on " +
			"Postgres a session not on UTC year-round means older releases wrote it in local time)", nil
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
		"applied by this run and no table in the stream's scope was changed on the target. Re-copy the stream: %s. Row counts "+
		"taken now cannot settle it: this stream has applied none of the source's writes since it stopped, so they differ "+
		"even for a finished copy. To keep the stream instead, re-run the same command once with --assert-copy-finished "+
		"(it records every in-scope table that exists on the target and holds rows as copied, so later resumes pass without "+
		"it, and it is refused when the target shows the copy did not finish), let the stream catch up, pause writes on the "+
		"source, and run `sluice verify --depth count` against the same source and target: a table it reports short was "+
		"never fully copied, and the remedy is then the re-copy above. Do not skip that check: once the flag is accepted, "+
		"nothing refuses this stream again. On a `sluice sync run` "+
		"leg, take the leg out of the fleet and run `sluice sync start` with the flag: the fleet spec has no key for it", // remedy-partial: the operator's own invocation (the leg's flags) carries their DSNs
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
		"fixes it. %s, then re-run the same command. No change has been applied by this run: %w",
		ir.ErrIncludeTableScopeDrift, vstreamCopyUnprovenMarker, streamID, controlTableDDLRemedy(err), err)
}

// controlTableDDLRemedy is the operator action for a control-table statement
// this role cannot run: the engine's own ([ir.ControlTableDDLRemedy]), which
// knows whether the statement printed by `sluice control-tables ddl` fixes it
// (on a MySQL-family target it prints CREATE statements only, so a failed
// ALTER names the ALTER), or, when the engine attached none, the general
// recipe with that caveat.
func controlTableDDLRemedy(err error) string {
	if r := ir.ControlTableDDLRemedy(err); r != "" {
		return "Have " + r
	}
	return "Have the DDL the error names applied by a role that can run it (on Postgres, the statements `sluice " +
		"control-tables ddl --engine postgres` prints; on a MySQL-family target `sluice control-tables ddl` prints CREATE " +
		"statements only, so an existing table needs the ALTER itself, shipped with `sluice deploy-ddl` on a PlanetScale " +
		"branch with safe migrations)"
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
	return s.recordAfterCopyDoor(ctx, applier, streamID, recordAsserted)
}

// recordAfterCopyDoor writes the established-table record a door pass asked
// for, through the same write a cold start makes ([recordEstablishedAfterCopy]),
// with each table's engine id where the source has one:
//
//   - recordAsserted (--assert-copy-finished): the in-scope tables that exist
//     on the target AND hold rows. An absent or empty one was not copied
//     whatever the operator believes; recording it would hide it from the
//     INCLUDE-TABLE-SCOPE-DRIFT door, so it is left out and named (F4).
//   - recordHeaderProof (the progress header and the target proved the copy):
//     the in-scope tables that the header's copy covered (its TableProgress,
//     RowsCopied == 0 entries included) AND that exist on the target, empty
//     ones included — the door has already shown each empty copied table is
//     empty on the source too (drained), and recording it now is what keeps
//     a later start from judging the header again. An in-scope table the
//     header never covered (admitted to the scope after that cold start) is
//     left out and named, whatever the target holds.
//
// It runs once the scope door has passed. A failure refuses: passing without
// the record would leave the judgment to be repeated, or ask for the flag
// again, on every resume while saying nothing.
func (s *Streamer) recordAfterCopyDoor(ctx context.Context, applier ir.ChangeApplier, streamID string, kind recordAfterDoor) error {
	marker, why := copyFinishedAssertedMarker, "--assert-copy-finished"
	if kind == recordHeaderProof {
		marker, why = vstreamCopyUnprovenMarker, "the copy proven by its progress record"
	}
	store, ok := applier.(ir.EstablishedTablesStore)
	if !ok {
		slog.WarnContext(ctx, marker+": this target keeps no established-table record, so "+why+" cannot be recorded; "+
			"every resume of this stream is judged again", slog.String("stream_id", streamID))
		return nil
	}
	scope, _, err := s.currentScopeKeys(ctx, applier, streamID)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: %w", marker, err))
	}
	exists, holding, err := s.targetTableFacts(ctx, scope)
	if err != nil {
		return controlReadFailure(fmt.Errorf("pipeline: %s: read which in-scope tables exist and hold rows on the target: %w",
			marker, err))
	}
	var keys, names, skipped []string
	for _, k := range scope {
		if kind == recordHeaderProof {
			// Only a table the header's copy covered (its TableProgress,
			// RowsCopied == 0 included) is certified. A table admitted to the
			// scope since, which exists on the target (pre-created, or filled by
			// CDC on an older release), was never copied by that run; recording
			// it would hide it from the INCLUDE-TABLE-SCOPE-DRIFT door for good.
			if exists[k] && s.copyDoorHeaderTables[bareTableOfKey(k)] {
				keys = append(keys, k)
				names = append(names, displayKey(k))
			} else {
				skipped = append(skipped, displayKey(k))
			}
			continue
		}
		if holding[k] {
			keys = append(keys, k)
			names = append(names, displayKey(k))
		} else {
			skipped = append(skipped, displayKey(k))
		}
	}
	if err := recordEstablishedAfterCopy(ctx, store, streamID, keys, s.establishedIDs(ctx, keys), s.sourceTableKeys); err != nil {
		if errors.Is(err, ir.ErrControlTableDDLRequired) {
			return fmt.Errorf("pipeline: resume refused: %w: %s: %s could not record this stream's tables, because this role "+
				"cannot bring the sluice_cdc_state control table up to date. %s, then re-run the same command. No change has "+
				"been applied by this run: %w", ir.ErrIncludeTableScopeDrift, marker, why, controlTableDDLRemedy(err), err)
		}
		return controlReadFailure(fmt.Errorf("pipeline: resume refused: %s: %s could not record this stream's tables as "+
			"copied (no change has been applied by this run); fix what the error names and re-run the same command: %w",
			marker, why, err))
	}
	if kind == recordHeaderProof {
		slog.InfoContext(ctx, "copy proven by its progress record and the target; recorded the in-scope tables that exist "+
			"on the target as established, so later resumes pass on the record",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	} else {
		slog.WarnContext(ctx, copyFinishedAssertedMarker+": recorded the in-scope tables that hold rows on the target as copied, "+
			"on the operator's --assert-copy-finished; later resumes pass without the flag",
			slog.String("stream_id", streamID), slog.String("tables", strings.Join(names, ", ")))
	}
	if len(skipped) > 0 {
		slog.WarnContext(ctx, marker+": in-scope tables that are absent on the target, that are empty there (under "+
			"--assert-copy-finished), or that the progress record's copy did not cover were NOT recorded as copied; the next start judges each as never copied (INCLUDE-TABLE-SCOPE-DRIFT) — "+
			"empty its target table if it has one and bring it in with `sluice schema add-table`, or take it out of scope",
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
