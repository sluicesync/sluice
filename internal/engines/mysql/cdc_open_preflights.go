// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "context"

// preflightBinlogCDCOpen is the SINGLE binlog CDC-open preflight set:
// every chokepoint that opens (or hands off to) a binlog CDC stream —
// [CDCReader.StreamChanges] and both snapshot openers — runs exactly
// this, so a preflight added here reaches every open path at once and
// none can adopt a subset (the moved/narrowed-door shape; roster-gated
// by TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights, which
// also refuses any OTHER function calling an individual preflight
// directly). The set, in order:
//
//	preflightBinlogRowImage   Bug 193   partial row images / PARTIAL_JSON
//	preflightBinlogFormat     item 68e  STATEMENT/MIXED binlog_format
//	preflightReplicaSource    M2 G5     replica source, log_replica_updates=OFF
//	preflightBinlogDBFilter   M2 G6     --binlog-ignore-db / --binlog-do-db
//
// scope names the databases the stream will read (the G6 refusal is
// scope-limited per the Bug 246 discipline); see each preflight's own
// file for its mechanism and ground truth. Bulk-only runs (migrate,
// backup full) never read the binlog and are deliberately not gated.
// snapshotFilterScope names a snapshot opener's synced databases for
// the G6 filter preflight: the DSN's database in single-database mode,
// the selected set in multi-database mode.
func snapshotFilterScope(multiDatabase bool, dbName string, databases []string) binlogFilterScope {
	if !multiDatabase {
		return binlogFilterScope{databases: []string{dbName}}
	}
	return binlogFilterScope{databases: databases}
}

func preflightBinlogCDCOpen(ctx context.Context, db dbQuerier, scope binlogFilterScope) error {
	if err := preflightBinlogRowImage(ctx, db); err != nil {
		return err
	}
	if err := preflightBinlogFormat(ctx, db); err != nil {
		return err
	}
	if err := preflightReplicaSource(ctx, db); err != nil {
		return err
	}
	return preflightBinlogDBFilter(ctx, db, scope)
}
