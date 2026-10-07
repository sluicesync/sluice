// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The rotated-chain keyless door (Bug 297, the loud arm of audit
// F-E1-ROTATED-SEGMENT-OVERLAP).
//
// A chain that ROTATED (`backup stream --retain-rotate-at*`, ADR-0046) holds
// one full per segment. Chain restore applies the first as an ordinary
// restore and every later one DataOnly — OVER the rows the earlier segments
// already restored (ADR-0067) — through [ir.IdempotentRowWriter]. That only
// converges for a table on which a re-written row collides on a key:
//
//   - keyless on the RECORDED schema: both engines' idempotent writers refuse
//     the table outright (errKeylessIdempotent, Bug 125). Before this door
//     that refusal fired partway through the restore — the target already
//     holding the first segment's rows — with a message written for the
//     VStream cold-start copy;
//   - keyless on the TARGET only (a pre-created table keyed on a surrogate the
//     rows do not supply): the re-written rows land a second time, silently
//     on MySQL, whose ON DUPLICATE KEY UPDATE collides on any unique key
//     (measured before this door: a 14-segment chain into an AUTO_INCREMENT
//     -keyed table, 355 rows for the source's 32 at exit 0). On Postgres the
//     DataOnly upsert names the recorded key in ON CONFLICT and fails
//     partway with 42P10 instead — loud, but after writing.
//
// The judgment is the F-E1 one, [irbackup.JudgeReplayKey] through
// [migcore.FindReplayKeylessTables]; nothing here re-derives "keyed".
//
// SCOPE, stated because it is narrower than "a rotated chain with a keyless
// table": a later full is judged only for the tables it carries ROWS for.
// That is exactly the writer's reach (a table with no chunks never reaches
// the writer — restoreTable returns before it), and it is also the correct
// line: with no rows in the later snapshot nothing is re-written, and the
// (P_N, S] changes the next incremental replays land once. The other three
// in-run re-application sources F-E1-ROTATED-SEGMENT-OVERLAP lists — a
// resumed full, a compacted chain, restore's reparent reconcile — are not
// rotation and are not judged here.
//
// The independent expected value is the recorded schema of the segment full
// itself (and, for the target half, the target's live catalog): neither is
// derived from the restore attempt the door is protecting.

// isDataOnlyFull reports whether chain restore applies links[i] DataOnly: it
// is a full, and an earlier link is a full too. It is the ONE definition of a
// "later segment full", shared by [ChainRestore.Run] (which picks the write
// path with it) and the door (which judges what that path will write), so the
// two cannot disagree about which fulls are re-applied.
func isDataOnlyFull(links []lineage.SegmentRecord, i int) bool {
	if lineage.CanonicalKind(links[i].Manifest.Kind) != irbackup.BackupKindFull {
		return false
	}
	for j := 0; j < i; j++ {
		if lineage.CanonicalKind(links[j].Manifest.Kind) == irbackup.BackupKindFull {
			return true
		}
	}
	return false
}

// rotatedKeylessTable is one table a later segment full would re-write
// without a key to collide on.
type rotatedKeylessTable struct {
	migcore.ReplayKeylessTable
	// Fulls names each later segment full carrying rows for the table, as
	// "segment full <backup id> (directory <dir>)".
	Fulls []string
}

// laterFullTable is one distinct (table, definition) a later segment full
// carries rows for, with every full it appears in.
type laterFullTable struct {
	table *ir.Table
	fulls []string
}

// laterFullTables returns the tables the chain's later segment fulls carry
// rows for, limited to filter, one entry per distinct definition (name +
// [migcore.ReplayKeyFingerprint]) so a long chain judges each shape once.
func laterFullTables(links []lineage.SegmentRecord, filter migcore.TableFilter) []laterFullTable {
	var out []laterFullTable
	seen := map[string]int{}
	for i := range links {
		if !isDataOnlyFull(links, i) {
			continue
		}
		m := links[i].Manifest
		if m.Schema == nil {
			continue
		}
		label := "segment full " + lineage.ManifestBackupID(m)
		if links[i].Segment != nil && links[i].Segment.Dir != "" {
			label += " (directory " + links[i].Segment.Dir + ")"
		}
		entries := indexManifestTables(m.Tables)
		for _, t := range filteredSchemaView(m.Schema, filter).Tables {
			if t == nil {
				continue
			}
			entry, ok := entries[manifestTableKey(t.Schema, t.Name)]
			if !ok || len(entry.Chunks) == 0 {
				continue // nothing re-written: the writer is never reached
			}
			key := t.Name + "\x00" + migcore.ReplayKeyFingerprint(t)
			if k, ok := seen[key]; ok {
				out[k].fulls = append(out[k].fulls, label)
				continue
			}
			seen[key] = len(out)
			out = append(out, laterFullTable{table: t, fulls: []string{label}})
		}
	}
	return out
}

// findRotatedKeylessTables judges every table a later segment full re-writes.
// rw may be nil only when opts asks for no target probe.
func findRotatedKeylessTables(
	ctx context.Context,
	rw ir.RowWriter,
	links []lineage.SegmentRecord,
	filter migcore.TableFilter,
	opts migcore.ReplayJudgeOptions,
) ([]rotatedKeylessTable, error) {
	var out []rotatedKeylessTable
	for _, lt := range laterFullTables(links, filter) {
		// One table per call: two definitions of one name are judged
		// separately, and the verdict list is keyed by name.
		keyless, err := migcore.FindReplayKeylessTables(ctx, rw, []*ir.Table{lt.table}, opts)
		if err != nil {
			return nil, err
		}
		for _, k := range keyless {
			out = append(out, rotatedKeylessTable{ReplayKeylessTable: k, Fulls: lt.fulls})
		}
	}
	return out, nil
}

// rotatedKeylessHint is the remedy riding SLUICE-E-BACKUP-ROTATED-KEYLESS-TABLE
// on the read side (restore, chain restore, backup verify).
const rotatedKeylessHint = "the named tables' rows are in the chain, but this release cannot restore them from a rotated chain: " +
	"restore everything else with --exclude-table=<table> for each named table and copy those tables from the source another way " +
	"(for example `sluice migrate --include-table`); for a table keyless only on the TARGET, give the target table the source's key " +
	"(not a serial, identity or defaulted surrogate) or let sluice create it; to make future chains restorable, give each table a " +
	"PRIMARY KEY or NOT NULL UNIQUE index on the source and take a new full, or run `backup stream run` without --retain-rotate-at / " +
	"--retain-rotate-at-chain-length"

// errRotatedKeyless renders the read-side refusal. mode names the command;
// written says whether anything has been written yet (false for every door
// before the first segment; true only for the in-flight belt).
func errRotatedKeyless(mode string, found []rotatedKeylessTable, written bool) error {
	parts := make([]string, len(found))
	for i, f := range found {
		parts[i] = fmt.Sprintf("%q (%s; rows in %s)", f.Name, f.Reason, strings.Join(f.Fulls, ", "))
	}
	tail := "Nothing has been written"
	if written {
		tail = "The target holds the earlier segments' rows and has not been rolled back"
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRotatedKeylessTable, rotatedKeylessHint, fmt.Errorf(
		"%s: refusing a ROTATED chain: it re-applies every segment full after the first over the rows the earlier "+
			"segments already restored (ADR-0067), and %d table(s) it would re-write that way have no key a re-written "+
			"row collides on: %s. Re-writing such a snapshot appends a second copy of rows already on the target, so it "+
			"cannot be restored whole (Bug 297). %s",
		mode, len(found), strings.Join(parts, "; "), tail,
	))
}

// refuseRotatedKeylessRecorded is the recorded-schema half, a pure manifest
// judgment: a member of the shared pre-target door list (so the broker's
// --reset-target-data cold start runs it before its destructive drop) and of
// `backup verify`. filter scopes it exactly as the restore will.
func refuseRotatedKeylessRecorded(ctx context.Context, links []lineage.SegmentRecord, filter migcore.TableFilter, mode string) error {
	found, err := findRotatedKeylessTables(ctx, nil, links, filter, migcore.ReplayJudgeOptions{})
	if err != nil {
		return fmt.Errorf("%s: rotated-chain keyless check: %w", mode, err)
	}
	if len(found) == 0 {
		return nil
	}
	return errRotatedKeyless(mode, found, false)
}

// refuseRotatedKeylessTarget is the target-catalog half: a later segment
// full's table the target ALREADY holds, keyed only on columns the rows do not
// supply. It asks about target STATE, so like the re-run door it runs in
// [ChainRestore.Run] after the shared pre-target list (the broker's cold
// start drops the target tables before Run, which then finds them absent —
// they will be created from the recorded schema, key included).
//
// Only multi-full chains pay for the probe: a chain with no later full has
// nothing to judge and opens no writer.
func (r *ChainRestore) refuseRotatedKeylessTarget(ctx context.Context, links []lineage.SegmentRecord) error {
	if len(laterFullTables(links, r.Filter)) == 0 {
		return nil
	}
	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: open target row writer: %w", err))
	}
	defer migcore.CloseIf(rw)
	migcore.ApplyTargetSchema(rw, r.TargetSchema)
	found, err := findRotatedKeylessTables(ctx, rw, links, r.Filter, migcore.ReplayJudgeOptions{ProbeTarget: true})
	if err != nil {
		return fmt.Errorf("chain restore: rotated-chain keyless check: %w", err)
	}
	if len(found) == 0 {
		return nil
	}
	return errRotatedKeyless("chain restore", found, false)
}

// verifyChainShapeRefusals is `backup verify`'s prediction of the chain
// refusals restore makes from the chain's SHAPE, in restore's order: this
// door (unfiltered, which is what verify predicts — its hint names
// --exclude-table for the filtered restore that succeeds), then the
// severed-transaction door. Chain-path lineages only, as restore.
func verifyChainShapeRefusals(ctx context.Context, store irbackup.Store, chain []lineage.SegmentRecord, walk, encrypted, keyed bool, prober *chunkAuthProber) error {
	if walk {
		if err := refuseRotatedKeylessRecorded(ctx, chain, migcore.TableFilter{}, "verify"); err != nil {
			return err
		}
	}
	return verifySeveredTransactions(ctx, store, chain, walk, encrypted, keyed, prober)
}

// errDataOnlyKeyless is the in-flight belt in the DataOnly write dispatch: a
// later segment full reached the write with a table keyless on its recorded
// schema, which the up-front door should have refused. It exists so the
// refusal an operator sees here names rotation, not the engines' VStream
// cold-start wording, if the door is ever bypassed.
func errDataOnlyKeyless(table *ir.Table) error {
	return errRotatedKeyless("chain restore", []rotatedKeylessTable{{
		ReplayKeylessTable: migcore.ReplayKeylessTable{Name: table.Name, Reason: migcore.ReplayKeylessRecorded},
		Fulls:              []string{"the segment full being applied"},
	}}, true)
}
