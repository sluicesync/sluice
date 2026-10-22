// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// refuseSingleStreamMultiTable refuses `vstream_copy_single_stream=true`
// for a copy of more than one table (VSTREAM-SINGLE-STREAM-CATCHUP-DELETE-DROPPED).
//
// The parameter copies the whole scope in ONE VStream instead of one per
// table (ADR-0095's escape hatch). Before each table after the first,
// vttablet's uvstreamer (catchupAndCopy) catches up the tables already
// copied: it replays every change since the previous table's snapshot as
// ROW events inside the copy phase. The copy decodes only after images
// (bufferCopyRow), so a DELETE replayed there is dropped, and the CDC tail
// starts at the last table's snapshot, after it. A row deleted from a
// finished table while a later one was being copied stays on the target, at
// exit 0. Measured on Vitess 24 with no fault: 60 of 60 such deletes stale,
// every one received in the copy phase; updates exact. The default per-table
// copy opens each table as its own stream, has no catch-up, and was exact in
// the same cell.
//
// A one-table scope is unaffected (there is no later table to catch up
// before) and keeps the parameter; so does the keyspace-wide open with no
// table list, which ignores it (autoShard needs more than one listed table).
// Refused rather than repaired: applying the catch-up's deletes would need
// them ordered against the upsert queue they overtake, and the default copy
// already gives the same result with no catch-up at all (operator decision
// 2026-10-22).
func refuseSingleStreamMultiTable(cfg *gomysql.Config, tables []string) error {
	if len(tables) <= 1 || !vstreamCopySingleStreamFromDSN(cfg) {
		return nil
	}
	named := tables
	if len(named) > 5 {
		named = named[:5]
	}
	list := strings.Join(named, ", ")
	if len(tables) > len(named) {
		list += ", ..."
	}
	return fmt.Errorf("mysql/vstream: %w: refusing the source DSN parameter vstream_copy_single_stream=true for a copy "+
		"of %d tables (%s): one VStream copying several tables replays the changes made to the tables it has already "+
		"copied before it copies the next, and the copy keeps only inserted and updated rows from that replay, so a row "+
		"deleted from a copied table during the copy would stay on the target. Remove vstream_copy_single_stream from "+
		"the source DSN and re-run the same command: the default copies each table in its own stream, which replays "+
		"nothing. A copy of one table may keep the parameter. This run copied nothing",
		ir.ErrVStreamSingleStreamMultiTable, len(tables), list)
}

// CheckSnapshotScope implements [ir.SnapshotScopeChecker]: the
// [refuseSingleStreamMultiTable] refusal the VStream snapshot open raises,
// without opening anything. A DSN that does not parse returns nil: the
// open parses the same DSN with the same function and refuses it there.
// Every non-VStream flavor copies per table and has nothing to refuse.
func (e Engine) CheckSnapshotScope(dsn string, tables []string) error {
	if !e.Flavor.usesVStream() {
		return nil
	}
	cfg, err := parseDSNForFlavor(dsn, e.Flavor)
	if err != nil {
		return nil //nolint:nilerr // the open refuses the same parse error; see the doc comment
	}
	return refuseSingleStreamMultiTable(cfg, tables)
}
