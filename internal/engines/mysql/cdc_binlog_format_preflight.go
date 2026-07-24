// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Roadmap item 68e — binlog_format=ROW upfront gate.
//
// sluice's binlog CDC replays ROW-format rows-events. Under
// `binlog_format=STATEMENT` the server logs DML as SQL text
// (QueryEvents), which the dispatcher's DDL arm treats as generic DDL:
// cache invalidation, nothing applied. Ground-truthed 2026-07-23
// (Phase A, real mysql:8.0 with --binlog-format=STATEMENT): the cold
// copy lands, then every live INSERT/UPDATE/DELETE is SILENTLY LOST —
// the target freezes at the copy snapshot, the persisted position never
// advances, Run keeps running with no error, and shutdown exits 0. A
// quietly EMPTY CDC stream, the worst silent-loss shape. `MIXED` is the
// same class: the server picks STATEMENT logging for every deterministic
// statement, so most DML is still lost (and MIXED is MariaDB's DEFAULT,
// so an un-tuned MariaDB source hits this out of the box).
//
// [preflightBinlogFormat] therefore refuses anything but ROW, loudly and
// coded, at every binlog CDC start — the same three chokepoints as the
// Bug 193 row-image preflight (StreamChanges for cold-start handoff /
// warm resume / backup incremental, plus both snapshot openers so a
// cold start refuses BEFORE the bulk copy). Scope: the binlog reader
// flavors only (vanilla MySQL, MariaDB — the variable exists on both).
// VStream flavors never reach these paths — vtgate owns the row-event
// contract there. Bulk-only runs (migrate, backup full) never read the
// binlog and are deliberately not gated.
//
// The GLOBAL scope is the right signal for the same reason as the
// row-image preflight: each session copies its binlog_format from the
// global at connect, so the global governs what new transactions write.
// A session-level STATEMENT override on an individual writer (SUPER
// only) slips past this gate — those statements arrive as QueryEvents
// the dispatcher treats as DDL and does not apply. A real replica
// would EXECUTE the SQL text; sluice deliberately never does (replaying
// arbitrary SQL against a possibly-different-engine target is not
// faithful CDC), so that residue stays a documented limit rather than
// a dispatch-time belt.

// binlogFormatRemedyHint is the machine-readable remedy carried on the
// coded refusal, mirroring the prose in the error message.
const binlogFormatRemedyHint = "SET GLOBAL binlog_format=ROW on the source (dynamic, no restart; " +
	"applies to sessions opened after the change — or the provider console's binlog_format parameter), then re-run"

// preflightBinlogFormat reads @@GLOBAL.binlog_format and returns a
// coded refusal ([sluicecode.CodeCDCBinlogFormatNotRow]) when it is
// anything but ROW. Shares [rowQuerier] and the timeout discipline with
// [preflightBinlogRowImage] (see that file for why the read is bounded
// and why a failed read is a plain, uncoded error).
func preflightBinlogFormat(ctx context.Context, q rowQuerier) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()
	var format string
	if err := q.QueryRowContext(pctx, "SELECT @@GLOBAL.binlog_format").Scan(&format); err != nil {
		return fmt.Errorf("mysql: cdc: read @@GLOBAL.binlog_format: %w", err)
	}
	if strings.EqualFold(format, "ROW") {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCBinlogFormatNotRow,
		binlogFormatRemedyHint,
		fmt.Errorf(
			"mysql: cdc: the source logs statements, not rows (@@GLOBAL.binlog_format=%s): sluice's CDC replays "+
				"ROW-format binlog row events, and under STATEMENT (or MIXED, which statement-logs every "+
				"deterministic write — and is MariaDB's default) DML arrives as SQL text the reader cannot apply — "+
				"the stream would run GREEN while silently applying NOTHING: the target freezes at the cold-copy "+
				"snapshot, the resume position never advances, and no error is ever raised (ground-truthed on "+
				"mysql:8.0, 2026-07-23). Set the source to row logging before starting CDC: "+
				"SET GLOBAL binlog_format=ROW (dynamic, no restart; applies to sessions opened after the change; "+
				"on managed MySQL use the provider console's binlog_format parameter). Binlog segments already "+
				"written under STATEMENT/MIXED stay statement-logged — when in doubt, start the sync fresh after "+
				"the flip. Then re-run",
			format,
		),
	)
}
