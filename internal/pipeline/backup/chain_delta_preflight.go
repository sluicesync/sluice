// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"fmt"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// refuseUnreplayableDeltas predicts, before anything is written, the two
// schema-delta refusals [ChainRestore.applySchemaDeltas] makes mid-replay:
// an ambiguous delta set ([lineage.DetectAmbiguousDeltas]) and an alter
// aspect with no faithful replay ([migcore.PreflightAlterDelta] — the same
// disposition table [migcore.ApplyAlterDelta] uses, so the two cannot
// disagree). Filter-aware exactly as the replay is (Bug 244).
//
// Why it exists (Bug 297 review): the remedy "give the table a key" sends
// an operator to ALTER TABLE … ADD PRIMARY KEY, which a running chain
// records as a primary-key delta that replay always refuses. Restore found
// that out at the delta, after writing every earlier link (measured: 11 of
// 51 keyed rows), and `backup verify` passed the chain. The class is every
// refused aspect, not only the primary key.
//
// A member of the shared pre-target list (the broker's --reset-target-data
// cold start replays the same deltas through ApplyAlterDelta) and of
// `backup verify`.
func refuseUnreplayableDeltas(links []lineage.SegmentRecord, filter migcore.TableFilter, mode string) error {
	for i := range links {
		m := links[i].Manifest
		if m == nil || lineage.CanonicalKind(m.Kind) != irbackup.BackupKindIncremental || len(m.SchemaDelta) == 0 {
			continue
		}
		var deltas []*irbackup.SchemaDeltaEntry
		for _, d := range m.SchemaDelta {
			if d != nil && filter.Allows(d.Table) {
				deltas = append(deltas, d)
			}
		}
		id := lineage.ManifestBackupID(m)
		if err := lineage.DetectAmbiguousDeltas(deltas); err != nil {
			return fmt.Errorf("%s: unsupportable schema delta in incremental %s: %w. "+
				"Force a fresh full + new chain to recover. Nothing has been written", mode, id, err)
		}
		for _, d := range deltas {
			if d.Kind != irbackup.SchemaDeltaAlterTable {
				continue
			}
			if err := migcore.PreflightAlterDelta(d, migcore.AlterDeltaContext{
				SourceEngine: m.SourceEngine,
				BackupID:     id,
				Origin:       mode + " (refused before writing anything)",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
