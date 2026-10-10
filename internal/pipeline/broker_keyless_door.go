// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The F-E1 keyless door, judged per incremental (ADR-0191 §3.5).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// BrokerKeylessNoIdentityMarker is the grep-stable token on the keyless
// refusal for a change that carries no identity inside an incremental that
// records them (a VStream COPY row, an interleaved shard group, a MySQL
// transaction without a GTID or server identity, a capture-synthesized row
// such as an ADD COLUMN fill): the manifest's flag cannot see that per-change
// gap, so it is judged per change.
const BrokerKeylessNoIdentityMarker = "BROKER-KEYLESS-NO-IDENTITY"

// BrokerUnidentifiedChangesMarker is the grep-stable token of the WARN the
// broker logs once per incremental when changes to KEYED tables inside an
// incremental that records identities carry none: their re-run is not
// exactly-once (it converges by key, except for a key moved onto another row
// inside one interrupted transaction), so the incremental's exactly-once claim
// is qualified for them rather than refused — a VStream chain's COPY rows are
// keyed and routine (ADR-0191 review).
const BrokerUnidentifiedChangesMarker = "BROKER-UNIDENTIFIED-CHANGES"

// brokerKeylessHint is the remedy riding SLUICE-E-BROKER-KEYLESS-TABLE.
const brokerKeylessHint = "give each named table a PRIMARY KEY or a NOT NULL UNIQUE index on the SOURCE and take a new full " +
	"backup (or, for a table keyless only on the target, give the target table the source's key — one made of columns " +
	"the backup carries, not a serial, identity or defaulted surrogate); or, where the reason names the target's apply " +
	"marks, make them usable (a role that may use `sluice_cdc_apply_marks`, a target that commits them atomically) and " +
	"replay a chain this sluice wrote; or replicate the table with `sluice sync start` instead of through a backup chain"

// replayJudgement is one recorded table's replay-key judgment, cached by the
// fingerprint of the definition it was made against.
type replayJudgement struct {
	fp      string
	table   *ir.Table
	keyless *migcore.ReplayKeylessTable // nil: a re-applied row collides on a key
}

// incrementalTouch is what the door needs to know about one incremental's
// changes: the tables they touch and the tables touched by a change that
// carries no identity — both by bare table name.
type incrementalTouch struct {
	tables map[string]bool
	zeroID map[string]bool
}

// judgeReplayKeys judges every table the chain records, through
// [migcore.FindReplayKeylessTables], and returns the judgment of each by name.
// probeTarget adds the target-catalog half; only such a judgment is cached,
// keyed by [migcore.ReplayKeyFingerprint], so a later definition of the same
// table (an AlterTable delta that dropped or replaced its key) is judged again
// before the incremental that carries it is applied.
//
// Scope: every table any link of the chain records, in its Schema or in an
// AddTable/AlterTable delta. The broker has no table filter. Residual, stated:
// a change for a table no link records (a table created mid-chain that a scoped
// window-end schema read did not pick up) is not judged; the applier skips a
// table the target lacks, so those rows can land only on a table someone
// created on the target by hand. A key removed from the TARGET out of band
// while a broker runs is seen at the next run's start.
func (b *SyncFromBackup) judgeReplayKeys(ctx context.Context, probeTarget bool) (map[string]replayJudgement, error) {
	chain, err := b.brokerChain(ctx)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	out := map[string]replayJudgement{}
	var pending []*ir.Table
	for _, t := range backup.ChainRecordedTables(chain, migcore.TableFilter{}) {
		fp := migcore.ReplayKeyFingerprint(t)
		if j, ok := b.replayJudged[t.Name]; ok && probeTarget && j.fp == fp {
			out[t.Name] = j
			continue
		}
		pending = append(pending, t)
	}
	if len(pending) == 0 {
		return out, nil
	}
	var rw ir.RowWriter
	if probeTarget {
		rw, err = b.Target.OpenRowWriter(ctx, b.TargetDSN)
		if err != nil {
			return nil, migcore.WrapWithHint(migcore.PhaseConnect,
				fmt.Errorf("broker: keyless-table check: open target row writer: %w", migcore.ErrOrCancel(ctx, err)))
		}
		defer migcore.CloseIf(rw)
	}
	keyless, err := migcore.FindReplayKeylessTables(ctx, rw, pending, migcore.ReplayJudgeOptions{ProbeTarget: probeTarget})
	if err != nil {
		return nil, fmt.Errorf("broker: keyless-table check: %w", err)
	}
	for _, t := range pending {
		j := replayJudgement{fp: migcore.ReplayKeyFingerprint(t), table: t}
		for i := range keyless {
			if keyless[i].Name == t.Name {
				j.keyless = &keyless[i]
			}
		}
		out[t.Name] = j
		if probeTarget {
			if b.replayJudged == nil {
				b.replayJudged = map[string]replayJudgement{}
			}
			b.replayJudged[t.Name] = j
		}
	}
	return out, nil
}

// refuseKeylessIncremental is the F-E1 door for ONE incremental, run before
// anything of it is applied (ADR-0191 §3.5). A table a re-applied change could
// duplicate rows in is refused when the incremental touches it AND the replay
// cannot be made exactly-once: the incremental records no identities, a change
// to the table in it carries none, or the target's apply marks do not cover the
// table. A table the incremental does not touch is not this incremental's
// concern. The tables it lifts are recorded for the per-change backstop in the
// producer ([SyncFromBackup.liftedKeyless]).
func (b *SyncFromBackup) refuseKeylessIncremental(ctx context.Context, applier ir.ChangeApplier, link *lineage.SegmentRecord) error {
	b.liftedKeyless = nil
	judged, err := b.judgeReplayKeys(ctx, true)
	if err != nil {
		return err
	}
	var names []string
	for name, j := range judged {
		if j.keyless != nil {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil // the common case: every table collides on a key, and nothing is decoded
	}
	slices.Sort(names)
	touch, err := b.touchOf(ctx, link)
	if err != nil {
		return err
	}
	var offenders []migcore.ReplayKeylessTable
	lifted := map[string]bool{}
	for _, name := range names {
		if !touch.tables[name] {
			continue
		}
		why, err := b.exactlyOnceBlocker(ctx, applier, link, judged[name], touch)
		if err != nil {
			return fmt.Errorf("broker: keyless-table check for %q in incremental %s: %w", name, lineage.ManifestBackupID(link.Manifest), err)
		}
		if why != "" {
			offenders = append(offenders, migcore.ReplayKeylessTable{Name: name, Reason: migcore.ReplayKeylessReason(why)})
			continue
		}
		lifted[name] = true
	}
	if len(offenders) > 0 {
		scope := "incremental " + lineage.ManifestBackupID(link.Manifest)
		if hint, ok := b.classicResumeHint(offenders); ok {
			return errBrokerKeylessTablesHint(scope, hint, offenders)
		}
		return errBrokerKeylessTables(scope, offenders)
	}
	b.liftedKeyless = lifted
	return nil
}

// exactlyOnceBlocker is "" when an interrupted replay of the incremental is
// exactly-once for the judged table, and otherwise the reason it is not,
// prefixed by the reason the table needs it.
func (b *SyncFromBackup) exactlyOnceBlocker(ctx context.Context, applier ir.ChangeApplier, link *lineage.SegmentRecord, j replayJudgement, touch incrementalTouch) (string, error) {
	needs := string(j.keyless.Reason)
	if j.keyless.Reason == migcore.ReplayKeylessTargetUnjudged {
		return needs, nil
	}
	if b.classicSuspect != "" && b.classicSuspect == lineage.ManifestBackupID(link.Manifest) {
		return needs + "; and " + BrokerClassicResumeMarker + ": this broker resumes from a position written by sluice " +
			"v0.156.12 or older, which kept no apply marks, so if that run was interrupted inside this incremental, the " +
			"rows it committed cannot be told apart from the ones still to apply", nil
	}
	if !link.Manifest.ApplyIdentity {
		return needs + "; and this incremental records no change identities (it was written before ADR-0191, or rewritten by " +
			"smart compaction), so an interrupted replay of it cannot be made exactly-once", nil
	}
	if touch.zeroID[j.table.Name] {
		return needs + "; and " + BrokerKeylessNoIdentityMarker + ": a change to it in this incremental carries no identity " +
			"(a VStream COPY row, an interleaved shard group, a MySQL transaction without a GTID or server identity, or a " +
			"row the capture synthesized such as an ADD COLUMN fill), so apply marks cannot name it", nil
	}
	prober, ok := applier.(ir.ApplyMarksCoverageProber)
	if !ok {
		return needs + fmt.Sprintf("; and the target's change applier (%T) cannot report whether apply marks cover it", applier), nil
	}
	why, err := prober.MarksCoverReason(ctx, j.table)
	if err != nil {
		return "", migcore.ErrOrCancel(ctx, err)
	}
	if why != "" {
		return needs + "; and apply marks cannot make its replay exactly-once: " + why, nil
	}
	return "", nil
}

// touchOf decodes incremental link once (cached by path and id) and reports
// the tables its row changes touch and those touched by a change without an
// identity. It reads every chunk the way the replay will, so a chunk it cannot
// read refuses here, before anything of the incremental is applied.
func (b *SyncFromBackup) touchOf(ctx context.Context, link *lineage.SegmentRecord) (incrementalTouch, error) {
	key := link.Path + "\x00" + lineage.ManifestBackupID(link.Manifest)
	if t, ok := b.touched[key]; ok {
		return t, nil
	}
	touch := incrementalTouch{tables: map[string]bool{}, zeroID: map[string]bool{}}
	segStore := link.Segment.Store(b.Store)
	for idx, chunk := range link.Manifest.ChangeChunks {
		src, err := blobcodec.FetchChunkVerified(ctx, segStore, chunk.File, chunk.SHA256)
		if err != nil {
			return incrementalTouch{}, lineage.CodeChunkHashError(fmt.Errorf("keyless-table check: open chunk %s: %w", chunk.File, err))
		}
		cek, err := b.chunkCEK(chunk)
		if err != nil {
			_ = src.Close()
			return incrementalTouch{}, fmt.Errorf("keyless-table check: resolve chunk cek: %w", err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, cek, link.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(link.Manifest, chunk, idx))
		if err != nil {
			return incrementalTouch{}, lineage.CodeChunkAuthError(fmt.Errorf("keyless-table check: open chunk reader: %w", err))
		}
		if blobcodec.NumbersArePreserved(link.Manifest.SourceEngine) {
			cr.PreserveNumbers()
		}
		for {
			c, rerr := cr.ReadChange()
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				_ = cr.Close()
				return incrementalTouch{}, fmt.Errorf("keyless-table check: read change: %w", rerr)
			}
			name, ok := rowChangeTable(c)
			if !ok {
				continue
			}
			touch.tables[name] = true
			if ir.ApplyIDOf(c).IsZero() {
				touch.zeroID[name] = true
			}
		}
		if err := lineage.CodeChunkHashError(cr.Close()); err != nil {
			return incrementalTouch{}, err
		}
	}
	if b.touched == nil {
		b.touched = map[string]incrementalTouch{}
	}
	b.touched[key] = touch
	return touch, nil
}

// rowChangeTable is the bare table a row change targets.
func rowChangeTable(c ir.Change) (string, bool) {
	switch v := c.(type) {
	case ir.Insert:
		return ir.UnqualifiedTableName(v.Table), true
	case ir.Update:
		return ir.UnqualifiedTableName(v.Table), true
	case ir.Delete:
		return ir.UnqualifiedTableName(v.Table), true
	}
	return "", false
}

// refuseUnidentifiedLiftedChange is the per-change backstop of the door
// (ADR-0191 §3.5 row 3): a row change, about to be emitted, to a table the
// door lifted for this incremental must carry an identity. touchOf already
// refused such an incremental before anything was applied, so this firing
// means the stream changed between the scan and the replay — refused loudly,
// before the change is emitted, either way.
func (b *SyncFromBackup) refuseUnidentifiedLiftedChange(backupID string, c ir.Change) error {
	name, ok := rowChangeTable(c)
	if !ok || !b.liftedKeyless[name] || !ir.ApplyIDOf(c).IsZero() {
		return nil
	}
	return errBrokerKeylessTables("incremental "+backupID, []migcore.ReplayKeylessTable{{
		Name: name,
		Reason: migcore.ReplayKeylessReason(BrokerKeylessNoIdentityMarker + ": a change to it carries no identity, though " +
			"the incremental's scan found every change identified — the stream changed under the replay"),
	}})
}

// refuseKeylessAtStart runs the door at the broker's start for the next
// incremental it would apply after fromID — the one a warm resume is inside
// or about to start, or the first after an --at-chain-id assertion — so an
// operator restarting a broker learns of a refusal before anything is written.
// An idle chain (no incremental after fromID) has nothing to judge; an fromID
// the chain does not hold is left to the entry's own refusal.
func (b *SyncFromBackup) refuseKeylessAtStart(ctx context.Context, applier ir.ChangeApplier, fromID string) error {
	chain, err := b.brokerChain(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	idx, err := brokerAppliedPrefix(chain, fromID)
	if err != nil {
		return nil //nolint:nilerr // the entry's own check names the missing id (warm resume's tick, --at-chain-id's lookup)
	}
	for i := idx; i < len(chain); i++ {
		if lineage.CanonicalKind(chain[i].Manifest.Kind) == irbackup.BackupKindIncremental {
			return b.refuseKeylessIncremental(ctx, applier, &chain[i])
		}
	}
	return nil
}

// refuseKeylessAtColdStart is the audit F-E1 door BEFORE any cold-start leg —
// before --reset-target-data drops and restores, before --at-chain-id records
// a position. On --reset-target-data the current target tables are about to be
// dropped and recreated from the recorded schema, so only the recorded schema
// and the TARGET's ability to make any keyless replay exactly-once are judged;
// each incremental after the restore is judged by the tick. On --at-chain-id
// the first incremental after the asserted link is judged now, as on a warm
// resume. (A cold start with neither is the refusal coldStart returns.)
func (b *SyncFromBackup) refuseKeylessAtColdStart(ctx context.Context, applier ir.ChangeApplier) error {
	switch {
	case b.ResetTargetData:
		return b.refuseKeylessResetTarget(ctx, applier)
	case b.AtChainID != "":
		return b.refuseKeylessAtStart(ctx, applier, b.AtChainID)
	}
	return nil
}

// BrokerClassicResumeMarker is the grep-stable token on the keyless refusal
// for the one incremental a resumed classic-token broker may have been
// inside (see [SyncFromBackup.noteClassicResume]).
const BrokerClassicResumeMarker = "BROKER-CLASSIC-RESUME"

// noteClassicResume handles a warm resume over a CLASSIC `backup-broker`
// token — one written by v0.156.12 or older (ADR-0191 review, v0.157.0). That
// token proves the previous run kept no frontier and wrote no apply marks, so
// if it was interrupted inside the incremental after lastAppliedID, that
// incremental's committed prefix is on the target with nothing to skip it: a
// keyless table it touches would gain a duplicate of every row of the prefix
// on this run, identities or not. (Releases v0.156.11 and v0.156.12 refused
// keyless tables before applying anything, so in practice the exposure is a
// v0.156.10-or-older broker — but the token does not say which release wrote
// it, so the rule keys on the token.) The keyless lift is withheld for exactly
// that incremental — the next one after lastAppliedID that the chain already
// holds; one appended after this resume cannot have been started by the old
// binary — which restores v0.156.12's refusal for it and for nothing else.
//
// A classic token is ALWAYS at a boundary (no writer put a frontier in one,
// and decodeBrokerPosition refuses one that carries it), so "parked at a
// boundary" is no evidence: the old run advanced the token only when an
// incremental completed, and an interrupted one left the token exactly where
// a clean stop would. Nothing else on the target tells the two apart — the
// old run wrote no marks. The refusal therefore holds on every plain re-run:
// only applying the suspect incremental would rewrite the token (as v2), and
// the suspect is refused before anything of it is applied. Its remedy is its
// own ([SyncFromBackup.classicResumeHint]).
func (b *SyncFromBackup) noteClassicResume(ctx context.Context, lastAppliedID string) error {
	chain, err := b.brokerChain(ctx)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("broker: build chain: %w", err))
	}
	idx, err := brokerAppliedPrefix(chain, lastAppliedID)
	if err != nil {
		return nil //nolint:nilerr // the warm resume's own tick names the missing id
	}
	for i := idx; i < len(chain); i++ {
		if lineage.CanonicalKind(chain[i].Manifest.Kind) == irbackup.BackupKindIncremental {
			b.classicSuspect = lineage.ManifestBackupID(chain[i].Manifest)
			b.classicAfter = lastAppliedID
			return nil
		}
	}
	return nil
}

// refuseKeylessResetTarget is the door a --reset-target-data cold start runs
// BEFORE it drops anything. The target is about to be rebuilt from the
// recorded schema, so only the recorded half is judged, and only the
// TARGET-level half of exactly-once can be known now (marks usable, atomic
// commit): which later incrementals carry identities is decided per
// incremental once they arrive. A target that can never make a keyless replay
// exactly-once refuses here, while it still holds its data.
func (b *SyncFromBackup) refuseKeylessResetTarget(ctx context.Context, applier ir.ChangeApplier) error {
	judged, err := b.judgeReplayKeys(ctx, false)
	if err != nil {
		return err
	}
	var keyless []migcore.ReplayKeylessTable
	for _, j := range judged {
		if j.keyless != nil {
			keyless = append(keyless, *j.keyless)
		}
	}
	if len(keyless) == 0 {
		return nil
	}
	slices.SortFunc(keyless, func(a, c migcore.ReplayKeylessTable) int {
		switch {
		case a.Name < c.Name:
			return -1
		case a.Name > c.Name:
			return 1
		}
		return 0
	})
	why := ""
	if prober, ok := applier.(ir.ApplyMarksCoverageProber); !ok {
		why = fmt.Sprintf("the target's change applier (%T) cannot report whether apply marks cover it", applier)
	} else if why, err = prober.MarksCoverReason(ctx, nil); err != nil {
		return fmt.Errorf("broker: --reset-target-data: keyless-table check: %w", migcore.ErrOrCancel(ctx, err))
	}
	if why == "" {
		return nil
	}
	for i := range keyless {
		keyless[i].Reason += migcore.ReplayKeylessReason("; and no replay into this target can be made exactly-once: " + why)
	}
	return errBrokerKeylessTables("this chain", keyless)
}

// requireMarksForLifted closes the window between the door and the apply
// (ADR-0191 review): the door asked the target whether its marks cover each
// lifted table before the incremental, but the applier decides whether the
// mark table is usable only as its apply starts, and on "unusable" it used to
// disable the marks behind a WARN and apply anyway — the lifted keyless table
// then replayed unmarked. While this incremental lifted a table, the applier
// is told to refuse instead ([ir.ApplyMarksRequirer]); otherwise the WARN
// stands. An applier that answers the coverage question without honouring
// the requirement fails closed.
func (b *SyncFromBackup) requireMarksForLifted(applier ir.ChangeApplier, link *lineage.SegmentRecord) error {
	need := len(b.liftedKeyless) > 0
	req, ok := applier.(ir.ApplyMarksRequirer)
	if !ok {
		if !need {
			return nil
		}
		return errBrokerKeylessTables("incremental "+lineage.ManifestBackupID(link.Manifest), b.liftedTables(
			fmt.Sprintf("the target's change applier (%T) cannot be held to its apply marks for the apply", applier),
		))
	}
	req.RequireApplyMarks(need)
	return nil
}

// errMarksLostAtApply renders an apply the applier refused because the mark
// table became unusable between the door and the apply.
func (b *SyncFromBackup) errMarksLostAtApply(link *lineage.SegmentRecord, cause error) error {
	return errBrokerKeylessTables("incremental "+lineage.ManifestBackupID(link.Manifest), b.liftedTables(
		"the target's apply marks covered it when the incremental was judged, but were unusable when its apply "+
			"started: "+cause.Error(),
	))
}

// liftedTables renders the tables this incremental lifted, sorted, each with
// reason.
func (b *SyncFromBackup) liftedTables(reason string) []migcore.ReplayKeylessTable {
	names := make([]string, 0, len(b.liftedKeyless))
	for name := range b.liftedKeyless {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make([]migcore.ReplayKeylessTable, 0, len(names))
	for _, name := range names {
		out = append(out, migcore.ReplayKeylessTable{Name: name, Reason: migcore.ReplayKeylessReason(reason)})
	}
	return out
}

// classicResumeHint is the remedy for a refusal that names
// BROKER-CLASSIC-RESUME (Bug 298, v0.157.1); ok is false when no table is
// named for it. [brokerKeylessHint] misdirects there: the chain may already be
// one this sluice wrote, with identities and usable marks, and still refuse —
// what blocks the replay is that a classic token cannot say whether the old
// run left part of this incremental on the target, which no change to the
// source, the chain or the marks can answer. Two recoveries can, and they are
// the only two: rebuild the target, or the operator's assertion that the
// target holds the chain exactly through the token's link. (Giving the target
// table a key is not one: a table keyless on the source may legitimately hold
// duplicate rows, which a target key would merge.) A plain re-run refuses
// again ([SyncFromBackup.noteClassicResume]). The tables a mixed refusal
// names for another reason keep the general remedy.
func (b *SyncFromBackup) classicResumeHint(offenders []migcore.ReplayKeylessTable) (string, bool) {
	classic, other := 0, 0
	for _, o := range offenders {
		if strings.Contains(string(o.Reason), BrokerClassicResumeMarker) {
			classic++
		} else {
			other++
		}
	}
	if classic == 0 {
		return "", false
	}
	hint := fmt.Sprintf("for %s: re-running the same command refuses again, because this stream's position stays "+
		"the one sluice v0.156.12 or older wrote until an incremental is applied. Run it with --reset-target-data, which "+
		"drops the target's tables, restores the chain and records a position this sluice wrote (the target must allow "+
		"apply marks). Or, only if you know the run that wrote the position did NOT stop partway through incremental %s "+
		"(for example, a v0.156.11 or v0.156.12 broker refused it with SLUICE-E-BROKER-KEYLESS-TABLE before applying "+
		"any of it), delete stream %q's row from sluice_cdc_state and run again with --at-chain-id=%s; if that is "+
		"wrong, the rows the old run committed are duplicated in the keyless tables",
		BrokerClassicResumeMarker, b.classicSuspect, b.StreamID, b.classicAfter)
	if other > 0 {
		hint += ". For a table named for another reason: " + brokerKeylessHint
	}
	return hint, true
}

// errBrokerKeylessTables renders the coded refusal for scope (an incremental,
// or the chain), with the general remedy.
func errBrokerKeylessTables(scope string, tables []migcore.ReplayKeylessTable) error {
	return errBrokerKeylessTablesHint(scope, brokerKeylessHint, tables)
}

// errBrokerKeylessTablesHint is [errBrokerKeylessTables] with its remedy
// chosen by the caller.
func errBrokerKeylessTablesHint(scope, hint string, tables []migcore.ReplayKeylessTable) error {
	return sluicecode.Wrap(sluicecode.CodeBrokerKeylessTable, hint, fmt.Errorf(
		"broker: refusing to replay %s: %d table(s) could gain duplicate rows if the replay is interrupted: %s. "+
			"After an interruption (an error, a crash, SIGINT/SIGTERM) the broker re-applies the source transaction "+
			"that was in flight; a table the applier upserts into on a key the rows carry converges, but on these "+
			"tables the re-applied rows land a second time unless ADR-0190's apply marks skip them, and the marks can "+
			"for none of them here (audit F-E1, ADR-0191). Nothing of it has been applied",
		scope, len(tables), migcore.RenderReplayKeylessTables(tables),
	))
}
