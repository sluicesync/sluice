// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// backupReplayMismatchHint is the remedy a backup-chain capture lane gives a
// SLUICE-E-CDC-SCHEMA-REPLAY-MISMATCH refusal (GC-37 (j) third review).
//
// The MySQL reader's own hint is `sync --restart-from-scratch`, which does
// not exist for a backup and would not help if it did: the charset-DDL
// guard refuses AFTER the rows it replayed were emitted, and a
// `backup stream` rollover or an earlier `backup incremental` window that
// closed between those rows and the DDL event has already COMMITTED them to
// the chain, decoded by the post-DDL charset (the failing window itself is
// not committed — a non-transient capture error ends the stream before
// commitRollover, and the one-shot lane returns before writing its
// manifest). No restore of such a chain gives the values back; only a new
// full does.
const backupReplayMismatchHint = "take a fresh full backup of this source (sluice backup full), and start the chain from it; " +
	"windows this chain committed before the refusal may hold rows decoded by the wrong charset, and no restore of them recovers the values"

// backupCaptureReaderErr is how both backup capture lanes surface the CDC
// reader's terminal error: wrapped as before, and a replay-mismatch refusal
// re-hinted with [backupReplayMismatchHint] so the operator is not sent to a
// sync flag.
func backupCaptureReaderErr(e error) error {
	err := fmt.Errorf("cdc reader: %w", e)
	if ce, ok := sluicecode.FromError(e); ok && ce.Code == sluicecode.CodeCDCSchemaReplayMismatch {
		return sluicecode.Wrap(ce.Code, backupReplayMismatchHint, err)
	}
	return err
}

// charsetShapeReporter is implemented by a CDC reader that decodes some
// tables with no record of the charset their rows were written in (the
// MySQL-family reader on MariaDB binlog_row_metadata=NO_LOG, and VStream):
// per table (lower-cased), the column → {charset, collation} shape its rows
// were last decoded by, leaving out tables the session crossed an ALTER on.
type charsetShapeReporter interface {
	CharsetUnrecordedShapes() map[string]map[string][2]string
}

// refuseUnrecordedCharsetReplay is the capture lanes' check for a charset
// replay whose window ends BEFORE the stream reaches the ALTER (GC-37 (j)
// fourth review, item 2). The reader's own guard fires only at the ALTER,
// so without this a `backup incremental` run that stopped between the
// replayed rows and the ALTER committed them misdecoded and exited 0
// (MEASURED on MariaDB NO_LOG), and so could a `backup stream` rollover.
//
// The independent evidence is the source schema the lane reads at window
// start (before) and end (after). For a column whose CHARSET differs
// between the two, with a non-UTF-8 end charset: if the shape the reader
// decoded the table's rows by already carries the END charset and the
// session never crossed an ALTER on the table, the rows were decoded by the
// new charset — and every one of them precedes the ALTER in the stream, so
// was written in the old one. A shape still carrying the START charset was
// live and is not refused. A collation-only change is not refused: decoding
// is by charset, so it changes no value.
//
// It runs before the window commits and refuses with the replay-mismatch
// code and the fresh-full remedy. A reader that does not report shapes
// (Postgres, SQLite, a MySQL source with TABLE_MAP charsets, which decodes
// by the written charset) is not affected.
func refuseUnrecordedCharsetReplay(cdc any, before, after *ir.Schema) error {
	r, ok := cdc.(charsetShapeReporter)
	if !ok || before == nil || after == nil {
		return nil
	}
	var found []string
	for table, shape := range r.CharsetUnrecordedShapes() {
		bt, at := findSchemaTable(before, table), findSchemaTable(after, table)
		if bt == nil || at == nil {
			continue
		}
		for _, col := range at.Columns {
			afterCS, ok := textColumnCharset(col.Type)
			if !ok || !nonUTF8Charset(afterCS) {
				continue
			}
			bc := findTableColumn(bt, col.Name)
			if bc == nil {
				continue
			}
			beforeCS, ok := textColumnCharset(bc.Type)
			if !ok || strings.EqualFold(beforeCS, afterCS) {
				continue
			}
			if decodedBy, ok := shape[strings.ToLower(col.Name)]; ok && strings.EqualFold(decodedBy[0], afterCS) {
				found = append(found, fmt.Sprintf("%s.%s (%s → %s)", at.Name, col.Name, beforeCS, afterCS))
			}
		}
	}
	if len(found) == 0 {
		return nil
	}
	sort.Strings(found)
	return sluicecode.Wrap(sluicecode.CodeCDCSchemaReplayMismatch, backupReplayMismatchHint,
		fmt.Errorf("CHARSET-HISTORY-UNRECORDED: the source changed the charset of %s during this window, and this window "+
			"decoded those tables' rows by the NEW charset without the change stream reaching the ALTER — so they are rows "+
			"written in the old charset, decoded wrong. This source records no per-event charset (MariaDB "+
			"binlog_row_metadata=NO_LOG, PlanetScale/Vitess). Refusing before the window commits", strings.Join(found, ", ")))
}

// findSchemaTable finds a table by name, ignoring case.
func findSchemaTable(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t != nil && strings.EqualFold(t.Name, name) {
			return t
		}
	}
	return nil
}

// findTableColumn finds a column by name, ignoring case.
func findTableColumn(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c != nil && strings.EqualFold(c.Name, name) {
			return c
		}
	}
	return nil
}

// textColumnCharset is a string column's declared charset.
func textColumnCharset(t ir.Type) (string, bool) {
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Char:
		return v.Charset, v.Charset != ""
	case ir.Varchar:
		return v.Charset, v.Charset != ""
	case ir.Text:
		return v.Charset, v.Charset != ""
	}
	return "", false
}

// nonUTF8Charset reports whether a MySQL charset's stored bytes are not
// already UTF-8 — the only charsets a replay can misdecode.
func nonUTF8Charset(cs string) bool {
	switch strings.ToLower(cs) {
	case "", "utf8mb4", "utf8mb3", "utf8", "ascii", "binary":
		return false
	}
	return true
}
