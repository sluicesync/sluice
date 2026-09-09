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
// the masterStatusSpellings fallback already covers.
//
// # Database-name equality is the SERVER's, not a fold of our choosing
//
// Audit 2026-09-09 RC-1 (HIGH, observed end-to-end on real mysql:8.0.46):
// this door compared filter entries case-INSENSITIVELY on every server,
// and the comment defending that choice argued the fold erred "in the
// silent-loss direction's favor". It does — on the IGNORE arm only. On
// the DO arm a fold errs the other way: `--binlog-do-db=T` with database
// `t` on a case-sensitive server logs NOTHING for `t`, the fold said the
// list covered it, and the sync ran green with an empty tail (source 4
// rows, target 2, zero ERROR lines). The enumerate-the-harms miss, on
// the door built for exactly this loss.
//
// So the equality is now the server's own, read at preflight time and
// MEASURED per engine (2026-09-09, real containers, uppercase filter
// entry `SOURCE_DB` against database `source_db`, both arms; the
// integration pins in cdc_binlog_db_filter_case_integration_test.go
// re-derive every cell from `SHOW BINLOG EVENTS` rather than from this
// table, so a server that changes its rule fails the pin):
//
//	engine         lct   do-list logs source_db?   ignore-list skips it?   rule
//	MySQL 8.0.46    0    no                        no                      byte-exact
//	MySQL 8.0.46    1    yes                       yes                     case-fold
//	MariaDB 11.4    0    no                        no                      byte-exact
//	MariaDB 11.4    1    no (lowercase entry: yes) no                      byte-exact against the STORED name
//
// MySQL follows `lower_case_table_names` for its filter compare. MariaDB
// does not: it compares the entry as typed against the name as stored,
// and at lct=1 the stored name is the lowercase fold — so a mixed-case
// MariaDB entry matches nothing, at any setting. The rule is one
// function, [binlogFilterCaseRule.match], so both arms cannot disagree
// about one server. UNVERIFIED PREMISE (named, not assumed away): at
// lct=2 MariaDB stores the name as CREATEd rather than lowercased, so
// "the stored name is the fold" can be false there; lct=2 is only
// honoured on a case-insensitive filesystem (Windows/macOS), which is
// not where a production MariaDB source runs, and no Linux container
// can measure it. On that cell the do arm can over-refuse and the
// ignore arm can under-refuse; the comment says so instead of the
// code pretending otherwise. The same applies to MySQL at lct=2: the
// unit matrix pins the fold there because every predicate in this
// engine tests `!= 0` (see [Engine.lowerCaseTableNames]), not because
// a server was measured. UNVERIFIED PREMISE, non-ASCII names: the fold
// is [foldMySQLIdentifier], sluice's imitation of the server's, and
// table_name_fold.go names the non-ASCII residual it carries; where Go
// and the server disagree on a letter's case the do arm can pass while
// the server logs nothing (silent) and the ignore arm can refuse a
// working configuration — neither measured, both stated.

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

// admits reports whether the server's filter entry is part of the sync's
// scope, by concrete list (under the server's own equality) or by
// predicate, and names the synced database as SLUICE spells it — the
// refusal must say "app" when the operator's scope is `app`, even when
// the server's entry reads `APP`, or the two spellings side by side look
// like a match the door is wrongly refusing.
func (s binlogFilterScope) admits(rule binlogFilterCaseRule, entry string) (synced string, ok bool) {
	for _, d := range s.databases {
		if rule.match(entry, d) {
			return d, true
		}
	}
	if s.inScope != nil {
		// The predicate is the READER's scope: it answers for names as
		// the server STORES them (on a folding server it folds, RC-1b).
		// A filter entry is not a stored name, so first ask the server's
		// rule which stored spelling — if any — the entry would match,
		// and consult the predicate with that. Handing it the raw entry
		// let a folding MariaDB refuse `--binlog-ignore-db=CDC_SRC` for
		// scope `cdc_src`, which that server does not apply (caught by
		// the real-server pin on the release commit, 2026-09-09).
		if stored, ok := rule.storedSpelling(entry); ok && s.inScope(stored) {
			return stored, true
		}
	}
	return "", false
}

// binlogFilterCaseRule is the equality a source server applies between a
// binlog-filter entry and a database name. See the file comment's
// measured table for where each arm of match comes from.
type binlogFilterCaseRule struct {
	lct     int
	mariadb bool
}

// match reports whether the filter entry names database db under the
// server's rule.
func (r binlogFilterCaseRule) match(entry, db string) bool {
	switch {
	case r.lct == 0:
		return entry == db
	case r.mariadb:
		return entry == foldMySQLIdentifier(db)
	default:
		// One fold for the whole engine (foldMySQLIdentifier), not a
		// second Go imitation: EqualFold and ToLower disagree on some
		// non-ASCII letters, and two rules in one file is how the two
		// arms drift apart.
		return foldMySQLIdentifier(entry) == foldMySQLIdentifier(db)
	}
}

// storedSpelling reports the stored database name a filter entry would
// match under the server's rule, or ok=false when it can match none:
// byte-exact servers store what was typed; a folding MySQL matches the
// entry to its lowercase form; a folding MariaDB compares the entry as
// typed against the lowercase stored name, so only an already-lowercase
// entry matches anything.
func (r binlogFilterCaseRule) storedSpelling(entry string) (stored string, ok bool) {
	switch {
	case r.lct == 0:
		return entry, true
	case r.mariadb:
		if entry != foldMySQLIdentifier(entry) {
			return "", false
		}
		return entry, true
	default:
		return foldMySQLIdentifier(entry), true
	}
}

// readBinlogFilterCaseRule reads the two server facts the equality
// depends on. A failure is loud and uncoded, the posture of the status
// read below: both are one-row reads any account that can open a CDC
// stream can make, so a failure here is a broken connection rather
// than evidence about the filters. The reader's own flavor is OR'd in
// with the version sniff: a MariaDB whose VERSION() string lacks the
// word (a proxy's server_version) would otherwise take the MySQL fold
// branch, which on a folding MariaDB is the silent direction.
func readBinlogFilterCaseRule(ctx context.Context, q dbQuerier, flavor Flavor) (binlogFilterCaseRule, error) {
	lct, err := readLowerCaseTableNames(ctx, q)
	if err != nil {
		return binlogFilterCaseRule{}, fmt.Errorf("mysql: cdc: binlog filter case rule: %w", err)
	}
	var version string
	if err := q.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return binlogFilterCaseRule{}, fmt.Errorf("mysql: cdc: binlog filter case rule: read server version: %w", err)
	}
	_, _, mariadb := parseMariaDBVersion(version)
	return binlogFilterCaseRule{lct: lct, mariadb: mariadb || flavor == FlavorMariaDB}, nil
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
func preflightBinlogDBFilter(ctx context.Context, q dbQuerier, scope binlogFilterScope, flavor Flavor) error {
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
	if len(doList) == 0 && len(ignoreList) == 0 {
		// No filters: nothing to compare, so the case rule is not read.
		// (The common configuration pays for nothing here.)
		return nil
	}

	rule, err := readBinlogFilterCaseRule(pctx, q, flavor)
	if err != nil {
		return err
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
			if !containsUnderRule(rule, doList, db) {
				return sluicecode.Wrap(
					sluicecode.CodeCDCBinlogDBFiltered,
					binlogDBFilterRemedyHint,
					fmt.Errorf(
						"mysql: cdc: synced database %q is not in the source's --binlog-do-db list (%s) under the "+
							"server's own name comparison (%s): its writes are applied but never written to the "+
							"binlog, so the cold copy would complete and the live CDC tail would be silently empty "+
							"for it — the stream stays green while the target freezes at the snapshot "+
							"(ground-truthed on real mysql:8.0, 2026-08-26; the case rule on real MySQL and "+
							"MariaDB, 2026-09-09). The filters are mysqld startup options: remove --binlog-do-db "+
							"and restart the server, or take %q out of the sync's scope. Then re-run",
						db, strings.Join(doList, ","), rule.describe(), db,
					),
				)
			}
		}
		// Matching the server: with a non-empty do-list the ignore list is
		// never consulted, so a scope database also present there is still
		// logged — checking it would refuse a working configuration.
		return nil
	}

	for _, entry := range ignoreList {
		if db, ok := scope.admits(rule, entry); ok {
			return sluicecode.Wrap(
				sluicecode.CodeCDCBinlogDBFiltered,
				binlogDBFilterRemedyHint,
				fmt.Errorf(
					"mysql: cdc: synced database %q is in the source's --binlog-ignore-db list (%s) under the "+
						"server's own name comparison (%s): its writes are applied but never written to the "+
						"binlog, so the cold copy would complete and the live CDC tail would be silently empty "+
						"for it — the stream stays green while the target freezes at the snapshot "+
						"(ground-truthed on real mysql:8.0, 2026-08-26; the case rule on real MySQL and "+
						"MariaDB, 2026-09-09). The filters are mysqld startup options: remove "+
						"--binlog-ignore-db and restart the server, or take %q out of the sync's scope. "+
						"Then re-run",
					db, strings.Join(ignoreList, ","), rule.describe(), db,
				),
			)
		}
	}
	return nil
}

// describe names the rule in the refusal so an operator reading
// "`SOURCE_DB` is not in the list (SOURCE_DB)" can see WHY two spellings
// that look alike did not match.
func (r binlogFilterCaseRule) describe() string {
	switch {
	case r.lct == 0:
		return fmt.Sprintf("byte-exact, lower_case_table_names=%d", r.lct)
	case r.mariadb:
		return fmt.Sprintf("MariaDB: entry as typed against the stored lowercase name, lower_case_table_names=%d", r.lct)
	default:
		return fmt.Sprintf("case-insensitive, lower_case_table_names=%d", r.lct)
	}
}

// containsUnderRule reports whether list contains db under the server's
// equality.
func containsUnderRule(rule binlogFilterCaseRule, list []string, db string) bool {
	for _, entry := range list {
		if rule.match(entry, db) {
			return true
		}
	}
	return false
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
// CONTAINING a comma is pathological and splits wrong here, and the
// failure direction is ASYMMETRIC per arm (the honest statement — the
// earlier "can only over-refuse" claim was false for the ignore arm):
// on the DO arm a synced comma-carrying name never matches its own
// over-split fragments, so the check over-refuses (safe); on the IGNORE
// arm the same mismatch means a genuinely FILTERED comma-carrying synced
// database fails to match any fragment and the preflight passes — the
// under-refusal direction. Accepted as-is: a comma in a database name
// requires backtick quoting to even create, the server's own rendering
// is ambiguous for it (no escaping), and no parse of the joined string
// can recover the boundary.
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
