// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// M2 capture-completeness sweep G9 — FK referential-action capture WARN.
//
// Cascaded child changes (ON DELETE CASCADE / ON UPDATE CASCADE /
// ON DELETE SET NULL / SET DEFAULT) are NOT written to the binlog:
// ground-truthed on real mysql:8.0.46 (2026-08-26, capture-completeness
// matrix §binlog) the wire carries parent-table events ONLY, in all
// three action shapes. MySQL's design expects the replica to re-run the
// cascade locally through its own FK definitions — and sluice's appliers
// deliberately suppress exactly that (Bug 164's ordering bypass): MySQL
// targets set foreign_key_checks=0, privileged-PG targets apply under
// session_replication_role=replica, SQLite/D1 open with
// _pragma=foreign_keys(0). So a parent-key DELETE/UPDATE leaves the
// target's child rows silently surviving (or holding stale FK values)
// at exit 0; only an unprivileged-PG target — where enforcement stays on
// and the translated FK carries the action — converges. `sluice verify`
// is the only independent catch.
//
// The remedy here is a WARN, not a refusal: an FK-carrying schema is
// routine, divergence needs a parent-key DELETE/UPDATE on a
// referential-action FK to actually occur, and RESTRICT / NO ACTION FKs
// cause no invisible writes at all (they block or no-op on the source,
// so nothing cascades). Re-enabling target-side enforcement instead
// would re-open Bug 164's ordering failures, and applier-side cascade
// emulation is the wrong altitude — both rejected in the matrix row.
//
// The census is scoped per the Bug 246 discipline to the tables the
// stream will actually sync: the divergence lives on the CHILD table
// (the one whose rows silently survive), so a filter that excludes the
// child keeps the WARN silent — and a child in scope WARNs even when
// its parent is filtered out, because the source-side cascade fires
// regardless of what sluice syncs.
//
// BOTH MySQL CDC lanes carry the identical mechanism (the vstreamer
// re-serves the tablet mysqld's binlog, where the cascaded rows are
// equally absent — the matrix's cross-engine CLASS sibling), so the
// WARN is wired at both lanes' open chokepoints and roster-bound by
// TestFKReferentialActionWarnRoster_BothLanes:
//
//   - binlog lane: [preflightFKReferentialActions], a member of the
//     combined [preflightBinlogCDCOpen] set (all three chokepoints, via
//     TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights);
//   - vstream lane: [warnVStreamFKReferentialActions] at
//     [vstreamCDCReader.StreamChanges] and
//     [Engine.openVStreamSnapshotStreamFrom] (the seedable core every
//     vstream snapshot opener — fresh, filtered, resume, backup —
//     funnels through).
//
// Failure posture: this is a detector, never a gate — a probe error
// WARNs ("cannot rule the blindness out") and passes rather than
// silently skipping, the SL-1 discipline.

// fkReferentialActionMarker is the grep-stable marker every G9 WARN
// carries (the firing WARN, the degraded probe-error WARN, and the
// unenumerable-scope WARN); the tests and mutation runs key on it.
const fkReferentialActionMarker = "FK-REFERENTIAL-ACTION-CAPTURE-GAP"

// fkActionCensusQuery lists every FK in one database with its
// referential rules. Shape deliberately mirrors the schema reader's
// populateForeignKeys join (key_column_usage ⋈ referential_constraints
// on constraint schema+name, driven by k.table_schema = ?), which is
// the proven-through-vtgate form — Vitess rewrites the table_schema
// comparison to the backing vt_* database, so the same query serves
// both lanes. DISTINCT collapses key_column_usage's per-column rows to
// one row per constraint; the action filter runs in Go
// ([fkActionInvisible]) so RESTRICT/NO ACTION staying silent is a
// pinned decision, not a SQL clause nobody tests.
const fkActionCensusQuery = `
	SELECT DISTINCT
		k.table_name,
		k.constraint_name,
		IFNULL(k.referenced_table_name, ''),
		IFNULL(rc.update_rule, 'NO ACTION'),
		IFNULL(rc.delete_rule, 'NO ACTION')
	FROM   information_schema.key_column_usage k
	JOIN   information_schema.referential_constraints rc
	  ON   rc.constraint_schema = k.constraint_schema
	 AND   rc.constraint_name   = k.constraint_name
	WHERE  k.table_schema = ?
	  AND  k.referenced_table_name IS NOT NULL
	ORDER  BY k.table_name, k.constraint_name`

// fkActionInvisible reports whether a referential rule produces
// source-side child writes that never enter the binlog. CASCADE,
// SET NULL and SET DEFAULT rewrite/remove child rows invisibly;
// RESTRICT and NO ACTION only block or admit the parent statement —
// nothing cascades, so they must NOT trip the WARN. (InnoDB rejects
// SET DEFAULT at DDL time, but the rule string is matched anyway so a
// MariaDB or future-engine source is covered.)
func fkActionInvisible(rule string) bool {
	switch strings.ToUpper(strings.TrimSpace(rule)) {
	case "CASCADE", "SET NULL", "SET DEFAULT":
		return true
	}
	return false
}

// fkReferentialActionRemedy is the shared remedy sentence both lanes'
// WARNs carry.
const fkReferentialActionRemedy = "run `sluice verify` after parent-key DELETEs/UPDATEs (it compares real rows, " +
	"not binlog state), or drop the referential actions in favor of application-level deletes for the " +
	"sync's duration"

// preflightFKReferentialActions is the binlog-lane member of the
// combined CDC-open preflight set ([preflightBinlogCDCOpen]). WARN-only:
// it never fails the open, so it returns nothing. See the file comment
// for the mechanism and scope semantics.
func preflightFKReferentialActions(ctx context.Context, q dbQuerier, scope binlogFilterScope) {
	warnFKReferentialActions(ctx, q, scope, "mysql: cdc")
}

// warnVStreamFKReferentialActions is the vstream-lane sibling: it opens
// a short-lived SQL handle against the vtgate (openDB strips the
// vstream_* DSN params, Bug 126 — the same path shard discovery uses)
// and runs the shared census scoped to the keyspace. tables is the
// COPY/table allowlist when the caller has one (the snapshot openers);
// nil censuses the whole keyspace (the standalone reader carries no
// table scope). A nil cfg is skipped: [openVStreamReader] and
// [Engine.openVStreamSnapshotStreamFrom] always parse one, so nil is
// reachable only by in-package test constructions of the reader struct.
func warnVStreamFKReferentialActions(ctx context.Context, cfg *gomysql.Config, keyspace string, tables []string) {
	if cfg == nil {
		return
	}
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()
	db, err := openDB(pctx, cfg, nil)
	if err != nil {
		slog.WarnContext(ctx, "mysql/vstream: cdc: "+fkReferentialActionMarker+": could not open a SQL "+
			"connection to the vtgate to census FK referential actions; if any in-scope table's FK declares "+
			"ON DELETE/UPDATE CASCADE, SET NULL or SET DEFAULT, cascaded child changes never enter the binlog "+
			"the vstream re-serves and this open could not rule that out",
			slog.String("keyspace", keyspace),
			slog.String("error", err.Error()))
		return
	}
	defer func() { _ = db.Close() }()
	scope := binlogFilterScope{databases: []string{keyspace}, tableAllowed: tableAllowlist(tables)}
	warnFKReferentialActions(ctx, db, scope, "mysql/vstream: cdc")
}

// tableAllowlist builds a (schema, table) scope predicate from an
// unqualified table allowlist (the snapshot openers' scope shape,
// case-insensitive like the dispatch filter). nil/empty means
// "no table filter" and returns nil so the census covers every table
// in the scoped databases.
func tableAllowlist(tables []string) func(schema, table string) bool {
	if len(tables) == 0 {
		return nil
	}
	set := make(map[string]bool, len(tables))
	for _, t := range tables {
		set[strings.ToLower(t)] = true
	}
	return func(_, table string) bool { return set[strings.ToLower(table)] }
}

// fkActionFinding is one referential-action FK the census surfaced.
type fkActionFinding struct {
	database, child, constraint, parent string
	updateRule, deleteRule              string
}

// describe renders the finding for the WARN, naming only the rules that
// are actually invisible (a CASCADE-delete/RESTRICT-update FK prints
// its delete rule alone).
func (f fkActionFinding) describe() string {
	var rules []string
	if fkActionInvisible(f.deleteRule) {
		rules = append(rules, "ON DELETE "+strings.ToUpper(f.deleteRule))
	}
	if fkActionInvisible(f.updateRule) {
		rules = append(rules, "ON UPDATE "+strings.ToUpper(f.updateRule))
	}
	return fmt.Sprintf("%s.%s (%s → %s: %s)", f.database, f.child, f.constraint, f.parent, strings.Join(rules, ", "))
}

// fkActionWarnListCap bounds how many FKs one WARN line enumerates; the
// count in the message always names the full total.
const fkActionWarnListCap = 20

// warnFKReferentialActions runs the census over scope's databases and
// emits ONE aggregated WARN when any in-scope table's FK declares an
// invisible referential action. lane prefixes the log line
// ("mysql: cdc" / "mysql/vstream: cdc"); the marker and message body
// are lane-identical so the two lanes cannot drift apart. Never fails
// the caller; a probe error (or an unenumerable database scope) WARNs
// instead of silently skipping.
func warnFKReferentialActions(ctx context.Context, q dbQuerier, scope binlogFilterScope, lane string) {
	if len(scope.databases) == 0 {
		// Predicate-only scope: the census cannot enumerate the synced
		// set. Both pipeline paths supply the concrete list (the same
		// guarantee the G6 filter preflight leans on), so this is a
		// direct-API shape — say so rather than silently skipping.
		slog.WarnContext(ctx, lane+": "+fkReferentialActionMarker+": the synced database set was not "+
			"enumerated for this stream, so FK referential actions could not be censused; if any in-scope "+
			"table's FK declares ON DELETE/UPDATE CASCADE, SET NULL or SET DEFAULT, cascaded child changes "+
			"never enter the binlog and this open could not rule that out")
		return
	}

	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()

	var findings []fkActionFinding
	for _, db := range scope.databases {
		// rows crosses into appendFKActionFindings, which closes it and
		// checks rows.Err on every path; the linter cannot track that
		// through the call boundary (same posture as the streaming
		// readers' suppressions).
		rows, err := q.QueryContext(pctx, fkActionCensusQuery, db) //nolint:rowserrcheck
		if err != nil {
			slog.WarnContext(ctx, lane+": "+fkReferentialActionMarker+": could not census FK referential "+
				"actions from information_schema; if any in-scope table's FK declares ON DELETE/UPDATE "+
				"CASCADE, SET NULL or SET DEFAULT, cascaded child changes never enter the binlog and this "+
				"open could not rule that out",
				slog.String("database", db),
				slog.String("error", err.Error()))
			return
		}
		findings, err = appendFKActionFindings(findings, rows, db, scope.tableAllowed)
		if err != nil {
			slog.WarnContext(ctx, lane+": "+fkReferentialActionMarker+": could not read the FK "+
				"referential-action census rows; the census is incomplete and this open could not rule the "+
				"capture gap out",
				slog.String("database", db),
				slog.String("error", err.Error()))
			return
		}
	}
	if len(findings) == 0 {
		return
	}

	descs := make([]string, 0, min(len(findings), fkActionWarnListCap))
	for i, f := range findings {
		if i == fkActionWarnListCap {
			descs = append(descs, fmt.Sprintf("… and %d more", len(findings)-fkActionWarnListCap))
			break
		}
		descs = append(descs, f.describe())
	}
	slog.WarnContext(ctx, lane+": "+fkReferentialActionMarker+": in-scope tables declare FK referential "+
		"actions whose cascaded child changes NEVER enter the binlog (MySQL logs parent-table events only "+
		"and expects the replica to re-run the cascade locally — which sluice's applier deliberately "+
		"suppresses, Bug 164), so a parent-key DELETE/UPDATE leaves the target's child rows silently stale "+
		"while the sync exits 0. Affected: "+strings.Join(descs, "; ")+". Remedy: "+fkReferentialActionRemedy,
		slog.Int("fk_count", len(findings)))
}

// appendFKActionFindings scans one database's census rows, keeping only
// constraints with an invisible rule on an in-scope child table. rows
// is always closed.
func appendFKActionFindings(
	findings []fkActionFinding,
	rows sqlRows,
	db string,
	tableAllowed func(schema, table string) bool,
) ([]fkActionFinding, error) {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var f fkActionFinding
		f.database = db
		if err := rows.Scan(&f.child, &f.constraint, &f.parent, &f.updateRule, &f.deleteRule); err != nil {
			return findings, err
		}
		if !fkActionInvisible(f.deleteRule) && !fkActionInvisible(f.updateRule) {
			continue
		}
		if tableAllowed != nil && !tableAllowed(db, f.child) {
			continue
		}
		findings = append(findings, f)
	}
	return findings, rows.Err()
}

// sqlRows is the slice of *sql.Rows the census scanner needs; an
// interface so the scan loop stays testable without a driver.
type sqlRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}
