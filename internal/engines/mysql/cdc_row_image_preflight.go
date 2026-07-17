// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Bug 193 — binlog_row_image preflight + partial-row-image belt.
//
// Under `binlog_row_image=MINIMAL` the binlog UPDATE rows-event carries
// a before-image with ONLY the primary-key columns and an after-image
// with ONLY the changed columns; under `NOBLOB` both images omit
// unchanged BLOB/TEXT columns. sluice's UPDATE apply needs a usable
// identity (the before-image WHERE) AND a complete after-image (the
// SET clause writes every column it carries) — a partial image
// therefore either matches zero rows (the WHERE gets `col IS NULL`
// predicates for absent columns; the miss is absorbed for resume
// idempotency) or, PK-narrowed, would NULL out every unchanged column.
// Either way: silent UPDATE loss/corruption while the stream stays
// green. Live-proven on Azure Database for MySQL Flexible Server,
// whose platform DEFAULT is MINIMAL (12 source UPDATEs → zero target
// changes, DEBUG-only footprint). Bug 88 closed exactly this class for
// DELETE ([filterBeforeToPK]); the UPDATE arm cannot be fixed the same
// way because the after-image is also partial — applying MINIMAL
// updates correctly is replica-semantics work sluice deliberately does
// not attempt.
//
// The fix is therefore two layers:
//
//  1. [preflightBinlogRowImage] — refuse loudly (coded) at every CDC
//     start (sync cold-start anchor, warm resume, backup incremental —
//     all flow through [CDCReader.StreamChanges]; the snapshot openers
//     also preflight so a cold start refuses BEFORE the bulk copy, not
//     after it). Bulk-only runs (migrate, backup full) never read the
//     binlog row images and are deliberately not gated.
//  2. [refusePartialRowImage] — the defense-in-depth belt on the
//     INSERT/UPDATE dispatch arms: if a partial image reaches the
//     reader anyway (a writing session with a session-level
//     binlog_row_image override, or a resume replaying a binlog
//     segment recorded before the global was flipped to FULL), the
//     stream stops loudly instead of silently skipping/corrupting the
//     row. The DELETE arm keeps working on partial images by
//     construction (Bug 88's PK narrowing needs only the PK, which
//     every row image carries), so it takes no belt — refusing there
//     would regress the working partial-image DELETE replay.

// rowImagePreflightTimeout bounds the @@GLOBAL.binlog_row_image read so
// a half-dead pooled connection can't hang the stream startup (the
// Track-D lesson behind [positionVerifyTimeout]). The query is a
// single-variable metadata read; a healthy server answers in
// milliseconds.
const rowImagePreflightTimeout = 30 * time.Second

// rowImageRemedyHint is the machine-readable remedy carried on the
// coded refusal, mirroring the prose in the error message.
const rowImageRemedyHint = "SET GLOBAL binlog_row_image=FULL on the source " +
	"(Azure Database for MySQL Flexible Server: az mysql flexible-server parameter set " +
	"--name binlog_row_image --value FULL), then re-run"

// rowQuerier is the single-row query surface shared by *sql.DB and
// *sql.Conn, so the preflight runs identically on the CDC reader's
// pool and on a snapshot opener's handle.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// preflightBinlogRowImage reads @@GLOBAL.binlog_row_image and returns a
// coded refusal ([sluicecode.CodeCDCRowImagePartial]) when it is
// anything but FULL. See the file comment for why MINIMAL/NOBLOB are
// unstreamable rather than merely warn-worthy.
//
// The GLOBAL scope is the right signal: each writing session copies its
// session value from the global at connect, so the global governs what
// new transactions write to the binlog. A session-level override on an
// individual writer slips past this preflight by design — that residue
// is exactly what the [refusePartialRowImage] belt catches at dispatch.
//
// A failed read is a plain (uncoded) error: sluice cannot prove the
// full-image invariant it is about to depend on, and any real mysqld
// lets every account read a global system variable — a failure here
// means the connection itself is broken, which would fail the stream a
// moment later anyway.
func preflightBinlogRowImage(ctx context.Context, q rowQuerier) error {
	pctx, cancel := context.WithTimeout(ctx, rowImagePreflightTimeout)
	defer cancel()
	var image string
	if err := q.QueryRowContext(pctx, "SELECT @@GLOBAL.binlog_row_image").Scan(&image); err != nil {
		return fmt.Errorf("mysql: cdc: read @@GLOBAL.binlog_row_image: %w", err)
	}
	if strings.EqualFold(image, "FULL") {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCRowImagePartial,
		rowImageRemedyHint,
		fmt.Errorf(
			"mysql: cdc: the source streams partial binlog row images (@@GLOBAL.binlog_row_image=%s): "+
				"a partial UPDATE before-image omits non-key columns and its after-image omits unchanged "+
				"columns, so sluice's CDC would silently lose every UPDATE — the stream stays green and row "+
				"counts stay equal while row content diverges. Set the source to full row images before "+
				"starting CDC: SET GLOBAL binlog_row_image=FULL (dynamic, no restart; applies to sessions "+
				"opened after the change). On Azure Database for MySQL Flexible Server — whose platform "+
				"default is MINIMAL — run: az mysql flexible-server parameter set --resource-group <rg> "+
				"--server-name <server> --name binlog_row_image --value FULL (~20s, no restart). Then re-run",
			image,
		),
	)
}

// skippedColumnsFor returns the skipped-column index list for row image
// i of a rows event. go-mysql fills [replication.RowsEvent.SkippedColumns]
// parallel to Rows — one entry per image, empty under FULL — but a
// hand-built event (unit fixtures) may leave it shorter; treat a
// missing entry as "nothing skipped" so the belt never panics.
func skippedColumnsFor(ev *replication.RowsEvent, i int) []int {
	if i < len(ev.SkippedColumns) {
		return ev.SkippedColumns[i]
	}
	return nil
}

// refusePartialRowImage is the Bug 193 defense-in-depth belt: it
// returns a coded, stream-fatal error when a rows-event image omitted
// (skipped) a non-generated column — proof the image was written under
// a partial binlog_row_image that slipped past [preflightBinlogRowImage]
// (a session-level override on a writer, or a resume replaying a
// binlog segment recorded before the global was flipped to FULL).
//
// skipped is the binlog present-columns bitmap's complement for ONE row
// image (see [skippedColumnsFor]); op/img name the event and image for
// the message ("update"/"before", "insert"/"write", …). Absent columns
// are distinguished from present-but-NULL ones by the bitmap itself:
// a genuinely NULL value is present in the bitmap with its null bit
// set (decoded as nil), while a skipped column never made it into the
// image at all — only the latter is refused, so NULL values in a FULL
// image keep flowing exactly as before.
//
// Generated columns are exempt: the row decoder drops them anyway
// (their value is derived on the target), so a server that omits them
// from the image loses nothing.
func refusePartialRowImage(tbl *tableSchema, skipped []int, op, img string) error {
	for _, idx := range skipped {
		if idx >= 0 && idx < len(tbl.Columns) && tbl.Columns[idx].IsGenerated() {
			continue
		}
		colName := fmt.Sprintf("#%d", idx)
		if idx >= 0 && idx < len(tbl.Columns) {
			colName = tbl.Columns[idx].Name
		}
		return sluicecode.Wrap(
			sluicecode.CodeCDCRowImagePartial,
			rowImageRemedyHint,
			fmt.Errorf(
				"mysql: cdc: %s rows-event for %s.%s omits column %q from its %s-image — this binlog "+
					"segment was written under a partial binlog_row_image (MINIMAL/NOBLOB: a session-level "+
					"override on a writer, or a segment recorded before the global was set to FULL). "+
					"Applying a partial %s image would silently skip or corrupt the row, so the stream "+
					"stops here. Ensure @@GLOBAL.binlog_row_image=FULL (and no writing session overrides "+
					"it), then restart the sync; a fresh cold start (--restart-from-scratch) is the safe "+
					"recovery when the partial-image window's changes matter",
				op, tbl.Schema, tbl.Name, colName, img, op,
			),
		)
	}
	return nil
}
