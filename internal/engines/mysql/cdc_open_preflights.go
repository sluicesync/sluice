// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"log/slog"
)

// preflightBinlogCDCOpen is the SINGLE binlog CDC-open preflight set:
// every chokepoint that opens (or hands off to) a binlog CDC stream —
// [CDCReader.StreamChanges] and both snapshot openers — runs exactly
// this, so a preflight added here reaches every open path at once and
// none can adopt a subset (the moved/narrowed-door shape; roster-gated
// by TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights, which
// also refuses any OTHER function calling an individual preflight
// directly). The set, in order:
//
//	preflightBinlogRowImage         Bug 193   partial row images / PARTIAL_JSON
//	preflightBinlogFormat           item 68e  STATEMENT/MIXED binlog_format
//	preflightReplicaSource          M2 G5     replica source, log_replica_updates=OFF
//	preflightBinlogDBFilter         M2 G6     --binlog-ignore-db / --binlog-do-db
//	preflightFKReferentialActions   M2 G9     FK referential-action capture WARN
//	advisePositionMode              2026-09-01 gtid_mode=OFF resume-mode INFO
//
// The last member is ADVISORY, not a preflight: it names no defect and
// returns nothing. It is here rather than at a call site so the three
// chokepoints cannot diverge on it, and it is deliberately NOT named
// `preflight…` so it does not join the derived preflight roster, whose
// members are all refusals.
//
// scope names the databases (and, when known, the table filter) the
// stream will read — the G6 refusal and the G9 census are scope-limited
// per the Bug 246 discipline; see each preflight's own file for its
// mechanism and ground truth. The G9 member is WARN-only and runs
// LAST, after every refusal, so a refused open never also warns.
// Bulk-only runs (migrate, backup full) never read the binlog and are
// deliberately not gated.
//
// (Ordering note: this block sat ABOVE snapshotFilterScope until
// 2026-09-01, which meant `go doc` attached the whole thing — the roster
// claim included — to that helper, and preflightBinlogCDCOpen had no doc
// at all. Pre-existing, found while fixing the same misattachment one
// file over. The chokepoint now follows its own doc.)
func preflightBinlogCDCOpen(ctx context.Context, db dbQuerier, scope binlogFilterScope, flavor Flavor) error {
	if err := preflightBinlogRowImage(ctx, db); err != nil {
		return err
	}
	if err := preflightBinlogFormat(ctx, db); err != nil {
		return err
	}
	if err := preflightReplicaSource(ctx, db); err != nil {
		return err
	}
	if err := preflightBinlogDBFilter(ctx, db, scope); err != nil {
		return err
	}
	preflightFKReferentialActions(ctx, db, scope)
	advisePositionMode(ctx, db, flavor)
	return nil
}

// snapshotFilterScope names a snapshot opener's synced databases for
// the G6 filter preflight — the DSN's database in single-database mode,
// the selected set in multi-database mode — plus the opener's table
// allowlist (nil = whole database) for the G9 census.
func snapshotFilterScope(multiDatabase bool, dbName string, databases, tables []string) binlogFilterScope {
	scope := binlogFilterScope{databases: databases, tableAllowed: tableAllowlist(tables)}
	if !multiDatabase {
		scope.databases = []string{dbName}
	}
	return scope
}

// positionModeAdvisoryMarker is the grep-stable prefix [advisePositionMode]
// carries.
const positionModeAdvisoryMarker = "POSITION-MODE"

// advisePositionMode reports, once per CDC open, which resume mode this
// source's `gtid_mode` selects — and says plainly that the two are not
// equally strong.
//
// sluice has never required GTID either way and still does not: there is
// no gtid_mode preflight, [gtidModeOnFor] simply detects the setting and
// picks an arm. That was defensible while the arms looked equivalent. It
// stopped being defensible on 2026-09-01, when the file/pos arm turned
// out to be the one carrying an instance-identity hazard: binlog
// filenames and offsets are instance-local, so a resume against a
// replaced source is only caught by the @@server_uuid stamp (v0.137.2),
// whereas a GTID set is instance-bound by construction and was always
// checked. Choosing the weaker arm silently, on MySQL 8's DEFAULT, left
// operators unable to know they were on it.
//
// INFO, not a WARN and emphatically not a refusal: file/pos is a
// supported, correct configuration, and PlanetScale requires gtid_mode=ON
// for imports anyway, so the population that lands here is self-hosted
// MySQL left at the default. A WARN on a working configuration is the
// noise that trains operators to ignore WARNs.
//
// Failure to read the mode is silent by design — this is advisory, and
// the preflights above already refuse loudly on a source they cannot
// interrogate.
func advisePositionMode(ctx context.Context, q rowQuerier, flavor Flavor) {
	if !positionModeAdvisoryApplies(flavor) {
		return
	}
	// Bounded like every other read on this chokepoint (audit 2026-08-27
	// A5), and the roster gate caught this one unbounded on first write.
	// The cap matters MORE here than for the refusals above, not less: an
	// advisory that hangs the CDC open it is merely commenting on would
	// be the worst possible trade. On expiry the read errors and the
	// advisory stays silent, which is the correct degrade for something
	// that names no defect.
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()
	on, err := gtidModeOnFor(pctx, q, flavor)
	if err != nil || on {
		return
	}
	slog.InfoContext(ctx, "mysql: cdc: "+positionModeAdvisoryMarker+": "+positionModeAdvisoryText)
}

// positionModeAdvisoryApplies is the flavor half of [advisePositionMode]'s
// decision, split out because it is the half worth pinning: MariaDB is
// always in GTID mode, and the vtgate flavors resume on VStream positions
// and reach neither binlog arm, so neither has a choice to advise about.
// Split from the read rather than tested through it — the read needs a
// real server, and this decision does not.
func positionModeAdvisoryApplies(flavor Flavor) bool {
	return flavor != FlavorMariaDB && !flavor.usesVStream()
}

const positionModeAdvisoryText = "this source has gtid_mode=OFF, so sluice resumes from a binlog FILE and " +
	"OFFSET. That is supported and correct, and it is the weaker of the two resume modes: binlog filenames " +
	"are instance-local, so if this source is ever replaced, rebuilt or failed over onto a different server, " +
	"the position's meaning does not carry over. sluice stamps the source's @@server_uuid onto such positions " +
	"and refuses a mismatch, but a GTID set is instance-bound by construction and needs no such stamp. " +
	"Enabling GTID mode on the source is the stronger configuration if you have the choice"
