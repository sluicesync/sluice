// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// # The ADD COLUMN fill (capture side)
//
// `ALTER TABLE t ADD COLUMN c … DEFAULT d` fills every row t already holds,
// and the fill writes no row event — so a backup chain holds nothing that
// describes it. Replay adds c from the window's recorded SchemaDelta, whose
// After is a catalog read taken at the END of the window, and the replayed
// DEFAULT is then the only thing that fills the pre-existing rows. It is the
// wrong thing whenever the window-end default is not the value the source
// filled with: Django's AddField (`ADD … DEFAULT 'v'` then `DROP DEFAULT`),
// a default changed later in the window, and every non-constant default
// (now(), clock_timestamp(), gen_random_uuid(), a sequence), which the
// target re-evaluates at restore time.
//
// So both capture lanes, once their window has closed and the delta is
// known, read the added columns' ACTUAL values back from the source and
// append them to the incremental as ordinary key-addressed updates
// (`UPDATE t SET c = <source value> WHERE pk = …`). They are change events
// like any other: no manifest format change, an older binary replays them
// the same way, and compaction treats them as the updates they are.
//
// # Which rows, and why the ordering is safe
//
// The fill covers every row the source holds when the read runs, keyed by
// the table's primary key, and is written AFTER every event of the window,
// at the window's end position, framed as one source transaction per
// table. At replay the ADD COLUMN delta applies first, then the window's
// events, then the fill. Case by case, for a row r of t:
//
//   - r pre-dates the window, or was inserted in it BEFORE the ALTER (its
//     INSERT carries no c): the target holds r with the replayed default;
//     the fill overwrites c with the source's value. This is the defect.
//   - r was written in the window AFTER the ALTER (INSERT or UPDATE): its
//     event carries c. The fill runs later and carries the value the source
//     holds at read time, which is that same value unless r changed again
//     after the window — see the next case. Either way r keeps the value
//     its last event wrote; the fill cannot resurrect an older one, because
//     it is read after every event it replays behind.
//   - r changed or was deleted AFTER the window's end but before the read:
//     the fill carries the newer value (or nothing, for a deleted row). The
//     NEXT link replays that change with its own image of c, so the chain as
//     a whole restores the source exactly; a restore that stops at THIS link
//     holds r's newer c a link early. Named, not hidden: a chain has no
//     point-in-time stop inside a link, and the restored tip converges the
//     moment the next link or a resumed sync replays.
//   - r was inserted after the window's end: not on the target when the fill
//     replays, so its update matches nothing — which every applier path
//     tolerates (the MySQL batch path routes a partial after-image to the
//     serial UPDATE, audit 2026-08-05 C-10, so it cannot fabricate the row
//     either) — and r's own INSERT arrives in the next link.
//
// RESIDUAL, stated rather than implied: the delta is attributed to the
// window whose end-of-window catalog read first SEES the column, which can
// be a DDL that ran after the window's last captured event. A row INSERTed
// between that event and the ALTER is then replayed by the NEXT link without
// c and holds the replayed default — the pre-existing attribution race, not
// something the fill introduces, and it needs an INSERT and the ALTER to both
// land in the milliseconds between window close and the catalog read.
//
// A table without a primary key has no address for its rows. Its fill is
// recorded as skipped, with a WARN here and the restore-side
// ADD-COLUMN-FILL-NOT-REPRODUCIBLE WARN naming every added column.
//
// COST, and an UNVERIFIED PREMISE on the stream lane: one full read of the
// key and the added columns per table, once per window that adds a column.
// On `backup stream` it runs inside the rollover, which stops draining the
// change stream until it returns — the same shape as an in-process
// rotation's bulk copy. On a busy source the CDC pump then blocks on its
// channel and sends no keepalive, so a fill that outlasts the source's
// replication idle timeout (PG wal_sender_timeout, MySQL net_write_timeout,
// 60s by default) should drop the connection and fail the stream loudly;
// nothing measures how large a table that takes.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// AddColumnFillNotCapturedMarker is the grep-stable token on the capture-side
// WARN for an ADD COLUMN whose fill could not be recorded.
const AddColumnFillNotCapturedMarker = "ADD-COLUMN-FILL-NOT-CAPTURED"

// changeChunkSealer is what writing a change chunk needs from the capture
// lane it belongs to: where to put it, how to compress it, and how to key
// it. [*BackupStream] and [*IncrementalBackup] both satisfy it.
type changeChunkSealer interface {
	changeChunkStore() irbackup.Store
	changeChunkCodec() blobcodec.Codec
	changeChunksEncrypted() bool
	resolveChunkCEK(chainCEK []byte) (cek, wrapped []byte, err error)
}

func (b *BackupStream) changeChunkStore() irbackup.Store       { return b.segStore }
func (b *BackupStream) changeChunkCodec() blobcodec.Codec      { return b.segCodec }
func (b *BackupStream) changeChunksEncrypted() bool            { return b.Encryption != nil }
func (b *IncrementalBackup) changeChunkStore() irbackup.Store  { return b.segStore }
func (b *IncrementalBackup) changeChunkCodec() blobcodec.Codec { return b.segCodec }
func (b *IncrementalBackup) changeChunksEncrypted() bool       { return b.Encryption != nil }

// newFillChunkBuffer returns a chunk buffer that appends to manifest's
// change-chunk list after the chunks its window already wrote — same run
// namespace, next ordinal, so the ADR-0152 binding orders the fill after the
// window's events.
func newFillChunkBuffer(sealer changeChunkSealer, manifest *irbackup.Manifest, chainCEK []byte) *changeChunkBuffer {
	return &changeChunkBuffer{
		sealer:       sealer,
		manifest:     manifest,
		runNamespace: changeChunkRunNamespace(manifest),
		chainCEK:     chainCEK,
		chunkIdx:     len(manifest.ChangeChunks),
	}
}

// appendChange writes c to the open chunk, opening one if none is, and
// rolls the chunk on the same two ceilings the capture windows use: the
// event count and the uncompressed byte ceiling.
func (cb *changeChunkBuffer) appendChange(ctx context.Context, c ir.Change, chunkSize int, out *captureOutcome) error {
	if cb.writer == nil {
		if err := cb.open(); err != nil {
			return err
		}
	}
	if err := cb.writer.WriteChange(c); err != nil {
		return err
	}
	if cb.writer.ChangeCount() >= int64(chunkSize) ||
		cb.writer.BytesWritten() >= backup.DefaultBackupChunkBytes {
		return cb.flushTo(ctx, out)
	}
	return nil
}

// captureAddColumnFill records, for every alter_table delta on manifest that
// adds a non-generated column, the source's current values of the added
// columns as key-addressed updates appended to cb, and stamps the delta's
// [irbackup.AddColumnFill]. It must run after the window has closed (the
// fill is ordered after every window event) and after manifest.SchemaDelta
// is set. Returns the stored bytes it added, for the rollover byte tally.
//
// A read failure fails the capture: the window is not committed, and the
// next attempt re-captures it from the same parent — by then a column that
// was dropped again is no longer an ADD, so a retry cannot wedge on it.
func captureAddColumnFill(ctx context.Context, src ir.Engine, dsn string, cb *changeChunkBuffer, chunkSize int) (int64, error) {
	pos := cb.manifest.EndPosition
	if pos == (ir.Position{}) {
		// A DDL-only `backup incremental` window records no end position
		// (see IncrementalBackup.Run). The fill then carries the position
		// the window resumed from, which is where the chain stands.
		pos = cb.manifest.StartPosition
	}
	var (
		rr  ir.RowReader
		out captureOutcome
	)
	defer func() { migcore.CloseIf(rr) }()
	for _, d := range cb.manifest.SchemaDelta {
		cols := fillColumns(d)
		if len(cols) == 0 {
			continue
		}
		fill := &irbackup.AddColumnFill{Columns: columnNames(cols)}
		d.AddColumnFill = fill
		key, skipped := fillKey(d.After, cols)
		if skipped != "" {
			fill.Skipped = skipped
			slog.WarnContext(ctx, "backup: "+AddColumnFillNotCapturedMarker+
				" — the values the source filled this ADD COLUMN's pre-existing rows with could not be recorded, "+
				"so a restore fills them with the column's window-end DEFAULT and names them again then",
				slog.String("table", d.Table),
				slog.Any("columns", fill.Columns),
				slog.String("reason", skipped))
			continue
		}
		if rr == nil {
			r, err := src.OpenRowReader(ctx, dsn)
			if err != nil {
				return 0, fmt.Errorf("add column fill: open source row reader: %w", err)
			}
			rr = r
		}
		started := time.Now()
		n, err := emitTableFill(ctx, rr, d, key, cols, pos, cb, chunkSize, &out)
		if err != nil {
			return 0, fmt.Errorf("add column fill for %s: %w", d.Table, err)
		}
		fill.Rows = n
		// The cost, logged per table so an operator can see what a
		// migration's ADD COLUMN made the backup read.
		slog.InfoContext(ctx, "backup: recorded the ADD COLUMN fill — the source's values for every row the column was added to",
			slog.String("table", d.Table),
			slog.Any("columns", fill.Columns),
			slog.Int64("rows", n),
			slog.Duration("read", time.Since(started)))
	}
	if err := cb.flushTo(ctx, &out); err != nil {
		return 0, err
	}
	return out.TotalBytes, nil
}

// emitTableFill reads key + cols for every row of d's table and appends one
// update per row, framed as a single source transaction at pos.
func emitTableFill(
	ctx context.Context,
	rr ir.RowReader,
	d *irbackup.SchemaDeltaEntry,
	key []string,
	cols []*ir.Column,
	pos ir.Position,
	cb *changeChunkBuffer,
	chunkSize int,
	out *captureOutcome,
) (int64, error) {
	proj := projectFillTable(d.After, key, cols)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel() // stops the reader's goroutine on an early return
	rows, err := rr.ReadRows(readCtx, proj)
	if err != nil {
		return 0, fmt.Errorf("read source rows: %w", err)
	}
	schema := d.After.Schema
	if schema == "" {
		schema = d.Schema
	}
	var n int64
	for row := range rows {
		if n == 0 {
			if err := cb.appendChange(ctx, ir.TxBegin{Position: pos}, chunkSize, out); err != nil {
				return 0, err
			}
		}
		before := make(ir.Row, len(key))
		for _, k := range key {
			before[k] = row[k]
		}
		// After carries the key as well: it is the shape every CDC update
		// has, and smart compaction refuses an event missing a key column.
		upd := ir.Update{Position: pos, Schema: schema, Table: d.Table, Before: before, After: row}
		if err := cb.appendChange(ctx, upd, chunkSize, out); err != nil {
			return 0, err
		}
		n++
	}
	// A cancelled read closes the channel early, and ReaderStreamErr
	// deliberately forgives cancellation — so a short fill must be refused
	// here, or it would be recorded as the whole table.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := migcore.ReaderStreamErr(rr, proj); err != nil {
		return 0, err
	}
	if n > 0 {
		if err := cb.appendChange(ctx, ir.TxCommit{Position: pos}, chunkSize, out); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// fillColumns returns the columns an alter_table delta adds that the fill
// must carry: every added column the source stores. A generated column is
// computed by the target from the columns it depends on.
func fillColumns(d *irbackup.SchemaDeltaEntry) []*ir.Column {
	if d == nil || d.Kind != irbackup.SchemaDeltaAlterTable || d.After == nil {
		return nil
	}
	var out []*ir.Column
	for _, c := range migcore.AddedColumns(d.Before, d.After) {
		if c != nil && !c.IsGenerated() {
			out = append(out, c)
		}
	}
	return out
}

// fillKey returns the primary-key columns that address the fill's rows, or
// the reason the rows cannot be addressed.
func fillKey(after *ir.Table, cols []*ir.Column) (key []string, skipped string) {
	if after.PrimaryKey == nil || len(after.PrimaryKey.Columns) == 0 {
		return nil, "the table has no primary key to address its rows by"
	}
	added := make(map[string]bool, len(cols))
	for _, c := range cols {
		added[c.Name] = true
	}
	for _, pc := range after.PrimaryKey.Columns {
		switch {
		case pc.Column == "":
			return nil, "the primary key has an expression entry"
		case added[pc.Column]:
			// The target's rows hold the replayed default in this column
			// until the fill lands, so it cannot address them.
			return nil, fmt.Sprintf("the primary key includes the added column %q", pc.Column)
		}
		key = append(key, pc.Column)
	}
	return key, ""
}

// projectFillTable is after narrowed to the key and the fill columns — the
// shape the row reader selects — with nothing else a reader could consult.
func projectFillTable(after *ir.Table, key []string, cols []*ir.Column) *ir.Table {
	want := make(map[string]bool, len(key)+len(cols))
	for _, k := range key {
		want[k] = true
	}
	for _, c := range cols {
		want[c.Name] = true
	}
	proj := &ir.Table{Schema: after.Schema, Name: after.Name, PrimaryKey: after.PrimaryKey}
	for _, c := range after.Columns {
		if c != nil && want[c.Name] {
			proj.Columns = append(proj.Columns, c)
		}
	}
	return proj
}
