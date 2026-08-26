// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// M2 capture-completeness sweep G6 — server-side binlog-filter preflight.
//
// A mysqld started with --binlog-ignore-db / --binlog-do-db excludes
// whole databases from the binlog: writes to a filtered database are
// applied and SQL-visible while never being logged. Ground-truthed on
// real mysql:8.0.46 (2026-08-26, capture-completeness matrix §binlog):
// under ROW format the filter keys on the CHANGED TABLE's actual
// database (proven with no default database selected), so every write
// to a filtered synced database vanishes from the log while the server
// stays healthy — cold copy complete, live CDC tail quietly empty,
// exit 0. Same terminal shape as the STATEMENT-format gap the 68e door
// closed, reached by server config instead of a variable. Sharper: the
// evidence columns ride the very SHOW MASTER STATUS row sluice already
// scans on every CDC start — scanMasterStatus deliberately discards
// them (its backup-position caller legitimately doesn't care), so this
// is a DEDICATED preflight beside preflightBinlogFormat rather than an
// overload of that shared scan.
//
// The refusal is SCOPED to the databases the sync actually reads (the
// Bug 246 lesson — a blanket refusal on any filter presence would fire
// on working configurations whose filters cover unrelated databases):
//
//   - Binlog_Do_DB non-empty: only listed databases are logged, so a
//     synced database absent from the list is blind → refuse naming it.
//     Requires the CONCRETE synced set; when the scope is only a
//     predicate (a caller that never supplied the list), the subset
//     relation is unprovable and the check fails CLOSED with its own
//     message rather than passing on ignorance.
//   - Binlog_Ignore_DB (consulted, matching the server's own rule, only
//     when the do-list is empty — with any --binlog-do-db present the
//     server ignores the ignore list entirely): a listed database that
//     is in the sync's scope is blind → refuse naming it.
//
// The filters are STARTUP OPTIONS — verified live: SET GLOBAL errors
// 1193 and no binlog_*_db system variable exists — so the preflight is
// authoritative for the life of the server process, and re-running at
// every CDC (re)open (the same chokepoints as preflightBinlogFormat;
// roster TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights)
// covers a filter ADDED across a mysqld restart mid-sync. The same
// columns exist on SHOW BINARY LOG STATUS (8.4+) and on MariaDB, which
// the masterStatusSpellings fallback already covers. Database-name
// matching is case-insensitive: on lower_case_table_names!=0 servers
// (the Windows/macOS defaults) a case-mismatched filter names the same
// database, and on a case-sensitive server the mismatch is at minimum a
// misconfiguration worth a loud stop — the over-refusal wart is
// accepted in the silent-loss direction's favor.

// binlogFilterScope names the databases a CDC start will read, for the
// filter preflight's scope-limited refusal. databases is the concrete
// synced set when known (single-database: the bound schema; the
// multi-database paths thread the selected set — snapshot opener via
// OpenMultiDatabaseSnapshotStream's databases, warm resume via
// [ir.CDCDatabaseListSetter]); inScope is the reader's event-allow
// predicate, used to test the SERVER's ignore-list entries when the
// concrete set alone would under-match.
type binlogFilterScope struct {
	databases []string
	inScope   func(database string) bool

	// tableAllowed is the stream's effective TABLE scope when one
	// exists (Bug 246: the reader's pipeline-supplied scope predicate;
	// the snapshot openers' table allowlist) — consulted only by the
	// G9 FK referential-action census, which must stay silent for a
	// cascade-carrying table the sync filters out. nil means every
	// table in the scoped databases is in scope. The G6 refusal never
	// reads it (server-side binlog filters are database-grained).
	tableAllowed func(schema, table string) bool
}

// admits reports whether db is part of the sync's scope, by concrete
// list or by predicate.
func (s binlogFilterScope) admits(db string) bool {
	for _, d := range s.databases {
		if strings.EqualFold(d, db) {
			return true
		}
	}
	return s.inScope != nil && s.inScope(db)
}

// binlogDBFilterRemedyHint is the machine-readable remedy carried on
// the coded refusal, mirroring the prose in the error message.
const binlogDBFilterRemedyHint = "remove --binlog-ignore-db / --binlog-do-db from the source mysqld's " +
	"startup options and restart it (the filters are not settable at runtime), or take the filtered " +
	"database out of the sync's scope, then re-run"

// preflightBinlogDBFilter reads the Binlog_Do_DB / Binlog_Ignore_DB
// columns of the master-status row and returns a coded refusal
// ([sluicecode.CodeCDCBinlogDBFiltered]) when the server-side binlog
// filters exclude a database in scope. See the file comment.
func preflightBinlogDBFilter(ctx context.Context, q dbQuerier, scope binlogFilterScope) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()

	doList, ignoreList, ok, err := readBinlogDBFilters(pctx, q)
	if err != nil {
		// The same statement must succeed moments later for the CDC anchor,
		// and every account that can read the binlog can run it — a failure
		// here is a broken connection, not evidence either way. Loud,
		// uncoded (the format preflight's posture).
		return fmt.Errorf("mysql: cdc: read binlog filter columns: %w", err)
	}
	if !ok {
		// No master-status row: binlog disabled. Not this preflight's
		// refusal — scanMasterStatus raises the existing loud "binlog
		// disabled?" error at anchor time.
		return nil
	}

	if len(doList) > 0 {
		if len(scope.databases) == 0 {
			// Fail closed: with only a predicate there is no way to prove
			// the synced set is inside the do-list, and passing on
			// ignorance is exactly the silent-empty-tail this door exists
			// to close. Reached only by callers that never supplied the
			// concrete set — both pipeline paths do.
			return sluicecode.Wrap(
				sluicecode.CodeCDCBinlogDBFiltered,
				binlogDBFilterRemedyHint,
				fmt.Errorf(
					"mysql: cdc: the source logs only the databases in --binlog-do-db (%s), and sluice cannot "+
						"enumerate this stream's synced database set to prove it is covered — refusing rather "+
						"than risk a silently empty CDC tail for an unlisted database",
					strings.Join(doList, ","),
				),
			)
		}
		for _, db := range scope.databases {
			if !containsFold(doList, db) {
				return sluicecode.Wrap(
					sluicecode.CodeCDCBinlogDBFiltered,
					binlogDBFilterRemedyHint,
					fmt.Errorf(
						"mysql: cdc: synced database %q is not in the source's --binlog-do-db list (%s): its "+
							"writes are applied but never written to the binlog, so the cold copy would complete "+
							"and the live CDC tail would be silently empty for it — the stream stays green while "+
							"the target freezes at the snapshot (ground-truthed on real mysql:8.0, 2026-08-26). "+
							"The filters are mysqld startup options: remove --binlog-do-db and restart the "+
							"server, or take %q out of the sync's scope. Then re-run",
						db, strings.Join(doList, ","), db,
					),
				)
			}
		}
		// Matching the server: with a non-empty do-list the ignore list is
		// never consulted, so a scope database also present there is still
		// logged — checking it would refuse a working configuration.
		return nil
	}

	for _, db := range ignoreList {
		if scope.admits(db) {
			return sluicecode.Wrap(
				sluicecode.CodeCDCBinlogDBFiltered,
				binlogDBFilterRemedyHint,
				fmt.Errorf(
					"mysql: cdc: synced database %q is in the source's --binlog-ignore-db list (%s): its writes "+
						"are applied but never written to the binlog, so the cold copy would complete and the "+
						"live CDC tail would be silently empty for it — the stream stays green while the target "+
						"freezes at the snapshot (ground-truthed on real mysql:8.0, 2026-08-26). The filters are "+
						"mysqld startup options: remove --binlog-ignore-db and restart the server, or take %q "+
						"out of the sync's scope. Then re-run",
					db, strings.Join(ignoreList, ","), db,
				),
			)
		}
	}
	return nil
}

// readBinlogDBFilters scans the master-status row (whichever of the
// masterStatusSpellings this server speaks) for the Binlog_Do_DB /
// Binlog_Ignore_DB columns, each a comma-separated database list.
// ok=false with a nil error means the binlog is disabled (no row).
// Missing columns read as empty lists — every known server (MySQL 5.x
// through 8.4's renamed statement, MariaDB through 11.x) carries both
// columns; a future rename would degrade this preflight to a pass, and
// the columns' presence is pinned by the real-server integration test.
func readBinlogDBFilters(ctx context.Context, q dbQuerier) (doList, ignoreList []string, ok bool, err error) {
	var lastErr error
	for _, stmt := range masterStatusSpellings {
		doList, ignoreList, ok, err = readBinlogDBFiltersVia(ctx, q, stmt)
		if err != nil {
			lastErr = err
			continue
		}
		return doList, ignoreList, ok, nil
	}
	return nil, nil, false, lastErr
}

// readBinlogDBFiltersVia runs one master-status spelling and scans its
// filter columns.
func readBinlogDBFiltersVia(ctx context.Context, q dbQuerier, stmt string) (doList, ignoreList []string, ok bool, err error) {
	rows, err := q.QueryContext(ctx, stmt)
	if err != nil {
		return nil, nil, false, err
	}
	defer func() { _ = rows.Close() }()
	return scanBinlogDBFilterRow(rows)
}

// scanBinlogDBFilterRow pulls the two filter columns out of the first
// (only) master-status row, by column NAME rather than position so the
// 8.4 statement rename or a column reorder cannot silently misread.
func scanBinlogDBFilterRow(rows *sql.Rows) (doList, ignoreList []string, ok bool, err error) {
	if !rows.Next() {
		return nil, nil, false, rows.Err()
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, false, err
	}
	dest := make([]any, len(cols))
	holders := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return nil, nil, false, err
	}
	for i, name := range cols {
		v, isStr := scanString(dest[i])
		if !isStr {
			continue
		}
		switch {
		case strings.EqualFold(name, "Binlog_Do_DB"):
			doList = splitDBList(v)
		case strings.EqualFold(name, "Binlog_Ignore_DB"):
			ignoreList = splitDBList(v)
		}
	}
	return doList, ignoreList, true, nil
}

// splitDBList splits the server's comma-separated filter rendering.
// Multiple --binlog-do-db flags render comma-separated; a database name
// CONTAINING a comma is pathological and splits wrong here — the
// conservative failure direction (an over-split fragment can only
// over-refuse, never silently pass a filtered database).
func splitDBList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// containsFold reports whether list contains s case-insensitively.
func containsFold(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
