// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// KeyCollationPre01592Marker is the grep-stable token on the WARN a
// chain-reading command logs for an incremental whose key collations no
// release can check (KEY-COLLATION-PRE0159-2-UNDETECTABLE, Bug 307).
const KeyCollationPre01592Marker = "KEY-COLLATION-PRE0159-2-UNVERIFIABLE"

// WarnUnverifiableKeyCollationIncrement names the character key columns of
// an incremental that a sluice before v0.159.2 captured from a MySQL-family
// source. Such a capture recorded each table's shape as of its window's
// end and no per-row collation, so a key changed and changed back inside
// the window (`_ci` → `_bin`, a row 'A' beside 'a', moved away, back to
// `_ci`) left only the reverted shape and no delta — and v0.159.2 and later
// restore and replay it with the two rows merged at exit 0 (measured by the
// v0.159.2 cycle, MySQL 8.4). The same holds, by code-reading, for a
// `backup stream` window whose reader lagged the source past the revert and
// for a change run with `sql_log_bin=0`. Nothing in the chain says whether
// a window did this, so nothing can refuse it.
//
// It WARNs rather than refuses for the reason [WarnLegacyCharsetIncrement]
// gives: the evidence is not in the chain, and a refusal would refuse every
// such chain whether or not a key ever moved. The WARN names the table and
// its character key columns and the check that can tell — `sluice verify`
// against the source — and the remedy, a fresh full backup.
//
// Scope: an incremental (a full copies rows the server converted, and
// carries no window), a MySQL-family source (a Postgres stream reads its
// collations from the catalog and has its own stated residuals,
// KEY-COLLATION-PG-LOOSENED-REPLAY), SluiceVersion below v0.159.2 (an
// unparseable version is not assumed either way), and a primary or unique
// key with a character column in the segment's recorded schema (no other
// key's identity depends on a collation).
//
// The chain-reading surfaces, each classified the way the legacy-charset
// WARN's are: chain restore and sync from-backup WARN per incremental,
// backup verify too (its "all chunks OK" would otherwise say the chain is
// sound); restore of a bare full and export-as-parquet read no incremental.
func WarnUnverifiableKeyCollationIncrement(ctx context.Context, origin string, m *irbackup.Manifest) {
	if m == nil || m.Schema == nil || !strings.EqualFold(strings.TrimSpace(m.Kind), irbackup.BackupKindIncremental) ||
		!IsMySQLFamilyEngine(m.SourceEngine) || !sluiceVersionBefore(m.SluiceVersion, 159, 2) {
		return
	}
	for _, t := range m.Schema.Tables {
		cols := characterKeyColumns(t)
		if len(cols) == 0 {
			continue
		}
		slog.WarnContext(ctx, origin+": "+KeyCollationPre01592Marker+
			" — this incremental was captured by sluice "+strings.TrimSpace(m.SluiceVersion)+
			", which recorded these key columns' collation only as of the window's end. A key collation changed and "+
			"changed back inside the window left no evidence, so a restore or replay of it cannot refuse two source rows "+
			"the reverted collation merges. Check the restored target against the source with `sluice verify`, and take "+
			"a fresh full backup with sluice v0.159.2 or later",
			"table", t.Name, "columns", strings.Join(cols, ", "))
	}
}

// characterKeyColumns lists t's columns that are character-typed and part
// of its primary key or a unique index, in column order — a functional key
// part's column included ([isKeyColumn]).
func characterKeyColumns(t *ir.Table) []string {
	if t == nil {
		return nil
	}
	var cols []string
	for _, c := range t.Columns {
		if c == nil || !isKeyColumn(t, c.Name) {
			continue
		}
		switch ir.UnwrapDomain(c.Type).(type) {
		case ir.Char, ir.Varchar, ir.Text:
			cols = append(cols, c.Name)
		}
	}
	return cols
}
