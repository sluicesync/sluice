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
// converges when a re-written row collides on a key OF THE TABLE THE TARGET
// HOLDS:
//
//   - keyless on the later full's RECORDED schema: both engines' idempotent
//     writers refuse the table outright (errKeylessIdempotent, Bug 125).
//     Before this door that refusal fired partway through the restore, with a
//     message written for the VStream cold-start copy;
//   - keyless on the TARGET: the re-written rows land a second time, silently
//     on MySQL, whose ON DUPLICATE KEY UPDATE collides on any unique key
//     (measured before this door: a 14-segment chain into an AUTO_INCREMENT
//     -keyed table, 355 rows for the source's 32 at exit 0). On Postgres the
//     DataOnly upsert names the recorded key in ON CONFLICT and fails
//     partway with 42P10 instead — loud, but after writing.
//
// WHERE THE TARGET'S TABLE COMES FROM, which is the premise the first cut of
// this door got wrong (Bug 297 review, 2026-10-07; measured on MySQL at 170
// rows for 10): a DataOnly full never creates or alters a table. The table
// the later full is written into is the one segment 0's restore created, or
// an AddTable delta created (CREATE IF NOT EXISTS — a no-op over a table
// that already exists), as altered by the AlterTable deltas replayed since;
// a DropTable delta is not replayed. So a later full that RECORDS a key the
// chain never replayed — the key was added between the last rollover's
// schema refresh and the rotation full's read, or the table was dropped and
// re-created keyed — is written into the old keyless table. The first cut
// judged the later full's recorded key and passed exactly that. Three
// judgments now cover it:
//
//  1. [chainTargetProjection]: the table the chain's own replay holds on the
//     target at each later full, derived from the manifests alone, judged
//     before anything is written (and by `backup verify`);
//  2. [ChainRestore.refuseRotatedKeylessTarget]: the live target catalog
//     before anything is written, for a table the operator pre-created
//     (segment 0's CREATE IF NOT EXISTS keeps it, surrogate key and all);
//  3. [ChainRestore.refuseDataOnlyFullOnTarget]: the live target catalog
//     again just before each later full, a belt for any table the first two
//     mis-projected. It fires after earlier segments have landed, so it says
//     so.
//
// Every judgment goes through [irbackup.JudgeReplayKey]; the projection is a
// [ir.ReplayKeyProber] over the projected table, not a second definition of
// "keyed".
//
// SCOPE: a later full is judged only for the tables it carries ROWS for.
// That is exactly the writer's reach (a table with no chunks never reaches
// the writer — restoreTable returns before it), and it is also the correct
// line: with no rows in the later snapshot nothing is re-written, and the
// (P_N, S] changes the next incremental replays land once. The other in-run
// re-application sources F-E1-ROTATED-SEGMENT-OVERLAP lists — a resumed
// full, restore's reparent reconcile on a single-segment chain — are not
// rotation and are not judged here.
//
// Independent expected values: the chain's own schema history (root full and
// deltas) for judgment 1, the target's live catalog for 2 and 3 — none
// derived from the later full whose re-write is being judged.

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

// ReplayKeylessChainCreated is the projection's reason: the table the chain
// itself leaves on the target has no key the later full's rows collide on.
const ReplayKeylessChainCreated migcore.ReplayKeylessReason = "the table this chain's own replay leaves on the target " +
	"(created by segment 0's full or an AddTable delta and altered only by the deltas the chain records — a later " +
	"full never re-creates or re-keys it) has no PRIMARY KEY or NOT NULL UNIQUE index made of columns the rows carry"

// dataOnlyFullTables returns a later full's label and the tables (limited to
// filter) it carries rows for.
func dataOnlyFullTables(link *lineage.SegmentRecord, filter migcore.TableFilter) (string, []*ir.Table) {
	m := link.Manifest
	label := "segment full " + lineage.ManifestBackupID(m)
	if link.Segment != nil && link.Segment.Dir != "" {
		label += " (directory " + link.Segment.Dir + ")"
	}
	if m.Schema == nil {
		return label, nil
	}
	entries := indexManifestTables(m.Tables)
	var out []*ir.Table
	for _, t := range filteredSchemaView(m.Schema, filter).Tables {
		if t == nil {
			continue
		}
		if entry, ok := entries[manifestTableKey(t.Schema, t.Name)]; ok && len(entry.Chunks) > 0 {
			out = append(out, t) // a table with no chunks never reaches the writer
		}
	}
	return label, out
}

// rotatedFindings accumulates findings by (table, reason), so a table found
// in several fulls is named once with every full.
type rotatedFindings struct {
	order []string
	by    map[string]*rotatedKeylessTable
}

func (f *rotatedFindings) add(k migcore.ReplayKeylessTable, full string) {
	if f.by == nil {
		f.by = map[string]*rotatedKeylessTable{}
	}
	key := k.Name + "\x00" + string(k.Reason)
	if got, ok := f.by[key]; ok {
		got.Fulls = append(got.Fulls, full)
		return
	}
	f.by[key] = &rotatedKeylessTable{ReplayKeylessTable: k, Fulls: []string{full}}
	f.order = append(f.order, key)
}

func (f *rotatedFindings) list() []rotatedKeylessTable {
	out := make([]rotatedKeylessTable, 0, len(f.order))
	for _, k := range f.order {
		out = append(out, *f.by[k])
	}
	return out
}

// chainTargetProjection is the set of tables the chain's own replay holds on
// the target at a point in the walk, by name. It mirrors exactly what chain
// restore does to the target's TABLE SET and KEYS: the first full creates its
// (filtered) tables; an AddTable delta creates a table only if absent; an
// AlterTable delta re-shapes an existing one (every aspect restore cannot
// apply is refused up front by [refuseUnreplayableDeltas], so a delta that
// passes leaves the target matching its After); a DropTable delta and a later
// full change nothing.
type chainTargetProjection struct {
	started bool
	tables  map[string]*ir.Table
}

func (p *chainTargetProjection) apply(link *lineage.SegmentRecord, filter migcore.TableFilter) {
	m := link.Manifest
	switch lineage.CanonicalKind(m.Kind) {
	case irbackup.BackupKindFull:
		if p.started || m.Schema == nil {
			return
		}
		p.started = true
		p.tables = map[string]*ir.Table{}
		for _, t := range filteredSchemaView(m.Schema, filter).Tables {
			if t != nil {
				p.tables[t.Name] = t
			}
		}
	case irbackup.BackupKindIncremental:
		if p.tables == nil {
			p.tables = map[string]*ir.Table{}
		}
		for _, d := range m.SchemaDelta {
			if d == nil || d.After == nil || !filter.Allows(d.Table) {
				continue
			}
			_, exists := p.tables[d.Table]
			switch d.Kind {
			case irbackup.SchemaDeltaAddTable:
				if !exists {
					p.tables[d.Table] = d.After
				}
			case irbackup.SchemaDeltaAlterTable:
				if exists {
					p.tables[d.Table] = d.After
				}
			}
		}
	}
}

// ProbeReplayKey answers [ir.ReplayKeyProber] from the projection. keyed is
// conservative in one direction, stated: when the projected table has a
// PRIMARY KEY, that key must be supplied (Postgres's arbiter is the primary
// key first); a MySQL target whose unsupplied primary key sits beside a
// supplied NOT NULL UNIQUE would in fact collide on the UNIQUE. A projected
// table's primary key is unsupplied only when a later full no longer records
// one of its columns, so the false refusal needs a dropped key column.
func (p *chainTargetProjection) ProbeReplayKey(_ context.Context, table *ir.Table) (exists, keyed bool, err error) {
	pt := p.tables[table.Name]
	if pt == nil {
		return false, false, nil
	}
	return true, projectedKeySupplied(pt, irbackup.ReplaySuppliedColumns(table)), nil
}

// projectedKeySupplied is [irbackup.JudgeReplayKey]'s target half, answered
// from the projected target table rather than a live catalog. It applies the
// engine-neutral rules the live probes apply: a key needs a PRIMARY KEY or a
// NOT NULL, non-partial, column-only UNIQUE (TableReplayIdempotent), whose
// every column the later full supplies and the target does not generate.
//
// Residual, stated (v0.157.0 review item 3): the TARGET-dependent rules are
// not projected, because the projection serves `backup verify` too, which has
// no target. A DEFERRABLE key (a Postgres target refuses it as an ON CONFLICT
// arbiter; a MySQL target emits it immediate), a vtgate keyspace whose
// primary vindex the rows do not supply, and an index a cross-engine
// translation drops are judged only by the live probe chain restore runs at
// each later full (ChainRestore.applyFull) — loud, but after earlier links
// were written, and `backup verify` passes such a chain. Pinned as
// characterization by TestProjectedKeySupplied_ShapeMatrix.
func projectedKeySupplied(pt *ir.Table, supplied []string) bool {
	if !irbackup.TableReplayIdempotent(pt) {
		return false
	}
	have := make(map[string]bool, len(supplied))
	for _, c := range supplied {
		have[c] = true
	}
	// A key column the projected TARGET table generates is never supplied,
	// whatever the later full records: the live probes say the same
	// (postgres loadGeneratedColumns, the MySQL GENERATED check), and
	// without this the projection called such a key collidable and the
	// refusal arrived only at applyFull's live probe, after part-writing
	// (v0.157.0 review item 3).
	for _, c := range pt.Columns {
		if c != nil && c.IsGenerated() {
			delete(have, c.Name)
		}
	}
	allSupplied := func(idx *ir.Index) bool {
		for _, c := range idx.Columns {
			if c.Expression != "" || !have[c.Column] {
				return false
			}
		}
		return len(idx.Columns) > 0
	}
	if pt.PrimaryKey != nil && len(pt.PrimaryKey.Columns) > 0 {
		return allSupplied(pt.PrimaryKey)
	}
	// No PRIMARY KEY: TableReplayIdempotent found an eligible NOT NULL,
	// non-partial UNIQUE; one of those must be supplied.
	notNull := map[string]bool{}
	for _, c := range pt.Columns {
		if c != nil && !c.Nullable {
			notNull[c.Name] = true
		}
	}
	for _, idx := range pt.Indexes {
		if idx == nil || !idx.Unique || strings.TrimSpace(idx.Predicate) != "" || !allSupplied(idx) {
			continue
		}
		eligible := true
		for _, c := range idx.Columns {
			if !notNull[c.Column] {
				eligible = false
			}
		}
		if eligible {
			return true
		}
	}
	return false
}

// findRotatedKeylessRecorded walks the chain once, judging each later full's
// row-carrying tables against (a) its own recorded schema and (b) the
// projected target table, before anything is written.
func findRotatedKeylessRecorded(ctx context.Context, links []lineage.SegmentRecord, filter migcore.TableFilter) ([]rotatedKeylessTable, error) {
	var (
		proj  chainTargetProjection
		found rotatedFindings
	)
	for i := range links {
		if isDataOnlyFull(links, i) {
			label, tables := dataOnlyFullTables(&links[i], filter)
			for _, t := range tables {
				verdict, err := irbackup.JudgeReplayKey(ctx, &proj, t)
				if err != nil {
					return nil, err
				}
				switch verdict {
				case irbackup.ReplayKeylessRecorded:
					found.add(migcore.ReplayKeylessTable{Name: t.Name, Reason: migcore.ReplayKeylessRecorded}, label)
				case irbackup.ReplayKeylessTarget:
					found.add(migcore.ReplayKeylessTable{Name: t.Name, Reason: ReplayKeylessChainCreated}, label)
				}
				// ReplayTargetAbsent: the chain never created the table, so the
				// DataOnly write lands in whatever the target holds under that
				// name — judgments 2 and 3 read it live.
			}
		}
		proj.apply(&links[i], filter)
	}
	return found.list(), nil
}

// rotatedKeylessHint is the remedy riding SLUICE-E-BACKUP-ROTATED-KEYLESS-TABLE
// on the read side (restore, chain restore, backup verify).
const rotatedKeylessHint = "the named tables' rows are in the chain, but this release cannot restore them from it: " +
	"restore everything else with --exclude-table=<table> for each named table and copy those tables from the source another way " +
	"(for example `sluice migrate --include-table`); for a table keyless only on the TARGET, give the target table the source's key " +
	"(not a serial, identity or defaulted surrogate) or let sluice create it; for future chains, give each table a key on the source " +
	"BEFORE the chain's first full (a NOT NULL UNIQUE index added mid-chain replays; an ADD PRIMARY KEY mid-chain does not), or run " +
	"`backup stream run` without --retain-rotate-at / --retain-rotate-at-chain-length"

// errRotatedKeyless renders the read-side refusal. mode names the command;
// written says whether earlier segments have already been written (false for
// every door before the first segment; true for the in-flight belts).
func errRotatedKeyless(mode string, found []rotatedKeylessTable, written bool) error {
	parts := make([]string, len(found))
	for i, f := range found {
		parts[i] = fmt.Sprintf("%q (%s; rows in %s)", f.Name, f.Reason, strings.Join(f.Fulls, ", "))
	}
	tail := "Nothing has been written"
	if written {
		tail = "The target holds the earlier segments' rows and has not been rolled back; nothing of the refused full was written"
	}
	return sluicecode.Wrap(sluicecode.CodeBackupRotatedKeylessTable, rotatedKeylessHint, fmt.Errorf(
		"%s: refusing a ROTATED chain: it re-applies every segment full after the first over the rows the earlier "+
			"segments already restored (ADR-0067), and %d table(s) it would re-write that way have no key a re-written "+
			"row collides on: %s. Re-writing such a snapshot appends a second copy of rows already on the target, so it "+
			"cannot be restored whole (Bug 297). %s",
		mode, len(found), strings.Join(parts, "; "), tail,
	))
}

// refuseRotatedKeylessRecorded is judgment 1, a pure manifest walk: a member
// of the shared pre-target door list (so the broker's --reset-target-data
// cold start runs it before its destructive drop) and of `backup verify`.
// filter scopes it exactly as the restore will.
func refuseRotatedKeylessRecorded(ctx context.Context, links []lineage.SegmentRecord, filter migcore.TableFilter, mode string) error {
	found, err := findRotatedKeylessRecorded(ctx, links, filter)
	if err != nil {
		return fmt.Errorf("%s: rotated-chain keyless check: %w", mode, err)
	}
	if len(found) == 0 {
		return nil
	}
	return errRotatedKeyless(mode, found, false)
}

// probeLaterFulls judges the given later fulls' row-carrying tables against
// the LIVE target catalog through rw.
func probeLaterFulls(ctx context.Context, rw ir.RowWriter, fulls []*lineage.SegmentRecord, filter migcore.TableFilter) ([]rotatedKeylessTable, error) {
	var found rotatedFindings
	for _, link := range fulls {
		label, tables := dataOnlyFullTables(link, filter)
		if len(tables) == 0 {
			continue
		}
		keyless, err := migcore.FindReplayKeylessTables(ctx, rw, tables, migcore.ReplayJudgeOptions{ProbeTarget: true})
		if err != nil {
			return nil, err
		}
		for _, k := range keyless {
			found.add(k, label)
		}
	}
	return found.list(), nil
}

// laterFulls returns the chain's DataOnly fulls.
func laterFulls(links []lineage.SegmentRecord) []*lineage.SegmentRecord {
	var out []*lineage.SegmentRecord
	for i := range links {
		if isDataOnlyFull(links, i) {
			out = append(out, &links[i])
		}
	}
	return out
}

// openProbeWriter opens a target row writer routed exactly like the
// restore's own (--target-schema applied).
func (r *ChainRestore) openProbeWriter(ctx context.Context) (ir.RowWriter, error) {
	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return nil, migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: open target row writer: %w", migcore.ProbeErrOrCancel(ctx, err)))
	}
	migcore.ApplyTargetSchema(rw, r.TargetSchema)
	return rw, nil
}

// refuseRotatedKeylessTarget is judgment 2: a later full's table the target
// ALREADY holds (pre-created by the operator — segment 0's CREATE IF NOT
// EXISTS keeps it), keyed only on columns the rows do not supply. It asks
// about target STATE, so like the re-run door it runs in [ChainRestore.Run]
// after the shared pre-target list. A table absent here is judgment 1's: the
// chain creates it, and the projection says with which key.
//
// Only multi-full chains pay for the probe.
func (r *ChainRestore) refuseRotatedKeylessTarget(ctx context.Context, links []lineage.SegmentRecord) error {
	fulls := laterFulls(links)
	if len(fulls) == 0 {
		return nil
	}
	rw, err := r.openProbeWriter(ctx)
	if err != nil {
		return err
	}
	defer migcore.CloseIf(rw)
	found, err := probeLaterFulls(ctx, rw, fulls, r.Filter)
	if err != nil {
		return fmt.Errorf("chain restore: rotated-chain keyless check: %w", err)
	}
	if len(found) == 0 {
		return nil
	}
	return errRotatedKeyless("chain restore", found, false)
}

// refuseDataOnlyFullOnTarget is judgment 3, the belt: the live target, just
// before one later full is applied, when the tables it writes into exist.
// Called by [ChainRestore.applyFull] for every DataOnly full.
func (r *ChainRestore) refuseDataOnlyFullOnTarget(ctx context.Context, full *lineage.SegmentRecord) error {
	if _, tables := dataOnlyFullTables(full, r.Filter); len(tables) == 0 {
		return nil
	}
	rw, err := r.openProbeWriter(ctx)
	if err != nil {
		return err
	}
	defer migcore.CloseIf(rw)
	found, err := probeLaterFulls(ctx, rw, []*lineage.SegmentRecord{full}, r.Filter)
	if err != nil {
		return fmt.Errorf("chain restore: rotated-chain keyless check: %w", err)
	}
	if len(found) == 0 {
		return nil
	}
	return errRotatedKeyless("chain restore", found, true)
}

// verifyChainShapeRefusals is `backup verify`'s prediction of the chain
// refusals restore makes from the chain's SHAPE, in restore's order: the
// schema-delta preflight, this door (unfiltered, which is what verify
// predicts — its hint names --exclude-table for the filtered restore that
// succeeds), then the severed-transaction door. Chain-path lineages only, as
// restore.
func verifyChainShapeRefusals(ctx context.Context, store irbackup.Store, chain []lineage.SegmentRecord, walk, encrypted, keyed bool, prober *chunkAuthProber) error {
	if walk {
		if err := refuseUnreplayableDeltas(chain, migcore.TableFilter{}, "verify"); err != nil {
			return err
		}
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
