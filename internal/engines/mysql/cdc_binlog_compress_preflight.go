// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// M2 capture-completeness sweep G8 — MariaDB log_bin_compress.
//
// With log_bin_compress=ON a MariaDB server writes any row image ≥
// log_bin_compress_min_len (256 B default) as a COMPRESSED row event
// (MARIADB_{WRITE,UPDATE,DELETE}_ROWS_COMPRESSED_EVENT_V1). Ground-
// truthed on real mariadb:11.4.12 + sluice's exact go-mysql v1.16.0
// (2026-08-26, capture-completeness matrix §binlog): the binlog is
// SIZE-CONDITIONAL — a small row logs as Write_rows_v1 while a ≥256 B
// row on the SAME table in the SAME session logs compressed, across all
// three DML verbs (a big row's DELETE compresses too, via its before-
// image). go-mysql decompresses and fully decodes these events, but
// dispatchRows' switch did not enumerate the three types, so they fell
// into the default arm's silent `return nil` — and the surrounding
// GTID/XID events ARE handled, so the resume position advanced cleanly
// past the loss: exit 0, restart-safe skip, size-conditional silent row
// loss. Backup incrementals ride the same reader and were equally
// affected. DDL is NOT affected (MARIADB_QUERY_COMPRESSED_EVENT is
// transparently decompressed into a normal QueryEvent upstream).
//
// Until sluice decodes the compressed variants, the class is refused
// loudly at two layers (the Bug 193 preflight+belt pattern):
//
//   - [preflightBinlogCompress] refuses log_bin_compress=ON at every
//     binlog CDC open (the [preflightBinlogCDCOpen] set, roster
//     TestCDCOpenPreflightRoster_EveryChokepointRunsAllPreflights).
//   - the dispatchRows belt ([compressedRowsBeltError]) refuses the
//     three event types at dispatch. The variable is GLOBAL-only but
//     DYNAMIC, and segments recorded while it was ON still replay
//     compressed events after it is turned off — so the belt, not the
//     preflight, is the load-bearing half. The event types exist only
//     when compression produced them: zero false refusals.
//
// The variable exists ONLY on MariaDB. On MySQL the probe errors 1193
// (ER_UNKNOWN_SYSTEM_VARIABLE), which is a PASS — a server without the
// variable cannot write compressed events. Any other read failure is
// loud and uncoded (the format preflight's posture: a broken read is
// not evidence either way, and the refusal's remedy would be wrong
// advice). Scope: binlog lane only — the VStream lane's vstreamer runs
// its own tablet-side decode and Vitess tablets run MySQL, and the
// mydumper/flatfile lanes never read the binlog.

// binlogCompressRemedyHint is the machine-readable remedy carried on
// the coded refusal, mirroring the prose in the error message.
const binlogCompressRemedyHint = "SET GLOBAL log_bin_compress=OFF on the source (dynamic, no restart), " +
	"then rotate past the already-compressed segments: binlog segments written while it was ON still " +
	"carry compressed events, so run FLUSH BINARY LOGS and restart the sync from a fresh position " +
	"(sync start --restart-from-scratch), or wait until retention purges them, then re-run"

// preflightBinlogCompress reads @@GLOBAL.log_bin_compress and returns a
// coded refusal ([sluicecode.CodeCDCBinlogCompressed]) when it is ON.
// See the file comment for the mechanism, the MySQL absent-variable
// PASS, and why the dispatch belt — not this preflight — is the
// load-bearing half.
func preflightBinlogCompress(ctx context.Context, q rowQuerier) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()
	var raw string
	if err := q.QueryRowContext(pctx, "SELECT @@GLOBAL.log_bin_compress").Scan(&raw); err != nil {
		if isUnknownSystemVariable(err) {
			// MySQL (every version): the variable does not exist, so the
			// server cannot write compressed row events.
			return nil
		}
		return fmt.Errorf("mysql: cdc: read @@GLOBAL.log_bin_compress: %w", err)
	}
	if v := strings.TrimSpace(raw); v == "0" || strings.EqualFold(v, "OFF") {
		return nil
	}
	// Anything else ("1", "ON", a future spelling) refuses — the
	// conservative direction is the silent-loss direction's opposite.
	return sluicecode.Wrap(
		sluicecode.CodeCDCBinlogCompressed,
		binlogCompressRemedyHint,
		errors.New(
			"mysql: cdc: the MariaDB source writes COMPRESSED binlog row events (@@GLOBAL.log_bin_compress=ON), "+
				"which sluice does not decode: every row image ≥ log_bin_compress_min_len (256 B default) — "+
				"INSERT, UPDATE, and DELETE alike (a big row's DELETE compresses via its before-image) — would be "+
				"silently dropped while small rows stream normally and the resume position advances past the loss "+
				"(ground-truthed on mariadb:11.4, 2026-08-26). SET GLOBAL log_bin_compress=OFF (dynamic, no "+
				"restart), then FLUSH BINARY LOGS and start the sync fresh: segments already written while it was "+
				"ON still carry compressed events, so a resume over them would deterministically re-refuse. Then re-run",
		),
	)
}

// isUnknownSystemVariable reports whether err is MySQL error 1193
// (ER_UNKNOWN_SYSTEM_VARIABLE) — the server does not know the variable
// at all, which for a MariaDB-only variable is proof the source is not
// MariaDB.
func isUnknownSystemVariable(err error) bool {
	var mysqlErr *gomysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1193
}

// compressedRowsBeltError is the dispatch-time belt for the three
// MariaDB compressed row-event types (see the file comment for why the
// belt is load-bearing: the variable is dynamic, and old segments
// replay compressed events after it is turned off). schema/table name
// the row the refusal is protecting.
func compressedRowsBeltError(et replication.EventType, schema, table string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCBinlogCompressed,
		binlogCompressRemedyHint,
		fmt.Errorf(
			"mysql: cdc: a MariaDB compressed row event (%s) for %s.%s reached the stream: the source wrote "+
				"this row image compressed (log_bin_compress=ON at write time — the variable is dynamic, so a "+
				"mid-stream SET GLOBAL, or a resume replaying segments recorded while it was ON, delivers these "+
				"even after a passing preflight), and sluice does not decode compressed row events — stopping "+
				"loudly instead of silently dropping the row. SET GLOBAL log_bin_compress=OFF, FLUSH BINARY LOGS "+
				"to rotate past the compressed segments (or wait out retention), then restart the sync from a "+
				"fresh position (sync start --restart-from-scratch)",
			et, schema, table,
		),
	)
}
