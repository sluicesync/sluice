// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The restore re-run door (audit F-E1, sibling of the broker's keyless
// refusal).
//
// A restore writes rows it does not check for: the full's bulk load is a
// plain COPY / LOAD DATA, and an incremental's INSERTs upsert only where the
// target table has a key. Re-running a restore that failed partway onto the
// same target therefore re-writes everything the earlier attempt wrote. On a
// keyed table that is loud (the load collides on the key and fails). On a
// keyless table it is silent: every row lands twice, at exit 0 (measured on
// Postgres at 2,001 rows for 1,001 in a chain restore).
//
// The door is deliberately narrower than "refuse a non-empty target", which
// is the obvious rule and the wrong one. Restoring into a target that already
// holds OTHER data — tables out of the restore's scope, or empty tables an
// operator pre-created — is an ordinary use, and a keyed in-scope table with
// rows already fails loudly without help. The silent class is exactly "an
// in-scope keyless table that already holds rows", so that is all it refuses.
// `restore` has no --reset-target-data, so there is no override flag: the
// remedy is to empty or exclude the named tables.
//
// The independent expected value is the TARGET's own row count — the earlier
// attempt's rows are on the target, not in any manifest.

// restoreKeylessHint is the remedy riding SLUICE-E-RESTORE-KEYLESS-TABLE-NOT-EMPTY.
const restoreKeylessHint = "empty the named target tables (TRUNCATE) or drop them and re-run the restore, or re-run " +
	"with --exclude-table for each; if those rows were loaded deliberately, decide first whether the restore should add to them"

// refuseKeylessPopulatedTargets refuses when any of tables is keyless (on its
// recorded definition or on the target) AND the target already holds rows in
// it. mode names the door in the message ("restore", "chain restore").
func refuseKeylessPopulatedTargets(ctx context.Context, rw ir.RowWriter, tables []*ir.Table, mode string) error {
	keyless, err := migcore.FindReplayKeylessTables(ctx, rw, tables, migcore.ReplayJudgeOptions{
		ProbeTarget:  true,
		OnlyNonEmpty: true,
	})
	if err != nil {
		return fmt.Errorf("%s: keyless re-run check: %w", mode, err)
	}
	if len(keyless) == 0 {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeRestoreKeylessTableNotEmpty, restoreKeylessHint, fmt.Errorf(
		"%s: refusing to write: %d target table(s) already hold rows and have no key a restored row can collide on: %s. "+
			"Restoring would append this backup's rows next to the ones already there, silently — which, after a restore "+
			"that failed partway, duplicates everything the earlier attempt wrote (audit F-E1). Nothing has been written",
		mode, len(keyless), migcore.RenderReplayKeylessTables(keyless),
	))
}

// ChainRecordedTables returns every table any link of chain records — in
// its recorded Schema or as the After shape of an AddTable/AlterTable delta —
// once per name (the newest definition wins, first-seen order kept), limited
// to filter. The recorded tables are used as-is, not retargeted: the
// cross-engine retarget the replay paths run before creating a table
// rewrites column TYPES only, never a table's name, key or nullability, which
// are all the doors read.
//
// It is the in-scope table set of both replay-duplication doors: the chain
// restore's re-run door and the `sync from-backup` keyless refusal.
func ChainRecordedTables(chain []lineage.SegmentRecord, filter migcore.TableFilter) []*ir.Table {
	if len(chain) == 0 {
		return nil
	}
	byName := map[string]int{}
	var tables []*ir.Table
	put := func(t *ir.Table) {
		if t == nil || !filter.Allows(t.Name) {
			return
		}
		if i, ok := byName[t.Name]; ok {
			tables[i] = t
			return
		}
		byName[t.Name] = len(tables)
		tables = append(tables, t)
	}
	for i := range chain {
		m := chain[i].Manifest
		if m == nil {
			continue
		}
		if m.Schema != nil {
			for _, t := range m.Schema.Tables {
				put(t)
			}
		}
		for _, d := range m.SchemaDelta {
			if d != nil && (d.Kind == irbackup.SchemaDeltaAddTable || d.Kind == irbackup.SchemaDeltaAlterTable) {
				put(d.After)
			}
		}
	}
	return tables
}

// refuseKeylessRerun is the chain-restore half of the re-run door, over
// every table the chain records (filtered). It opens a row
// writer routed exactly like the restore's own (--target-schema applied) so
// the emptiness and key probes read the tables the restore would write.
func (r *ChainRestore) refuseKeylessRerun(ctx context.Context, links []lineage.SegmentRecord) error {
	tables := ChainRecordedTables(links, r.Filter)
	if len(tables) == 0 {
		return nil
	}
	rw, err := r.Target.OpenRowWriter(ctx, r.TargetDSN)
	if err != nil {
		return migcore.WrapWithHint(migcore.PhaseConnect, fmt.Errorf("chain restore: open target row writer: %w", err))
	}
	defer migcore.CloseIf(rw)
	migcore.ApplyTargetSchema(rw, r.TargetSchema)
	return refuseKeylessPopulatedTargets(ctx, rw, tables, "chain restore")
}
