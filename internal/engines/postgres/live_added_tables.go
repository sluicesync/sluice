// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// The Postgres half of the stream's live-added table set (Bug 321 R6 A).
//
// A completed `sluice schema add-table` makes its table part of the stream's
// scope, whatever the restart's --include-table says — the mechanism MySQL's
// --no-drain path already had (ADR-0034, `live_added_tables`). On Postgres
// the table joined the publication and nothing else, so a restart with the
// stream's original --include-table (staged-wave Mechanism A, as documented)
// dropped every change to it at the dispatch filter: silently, at exit 0.
// The streamer reads this set at startup and on its poll tick and admits
// its tables like the MySQL set's; only `schema add-table --abandon` takes
// one back out (an --exclude-table naming one is refused,
// LIVE-ADDED-TABLE-EXCLUDED).
//
// Stored in the table-set codec ([appliershared.EncodeTableSet]) rather than
// MySQL's comma list, so any table name round-trips.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// liveAddedTablesColumn is sluice_cdc_state's live-added set column, added
// on its first write like the table-set columns.
const liveAddedTablesColumn = "live_added_tables"

func (a *ChangeApplier) liveAddedQueries() appliershared.TableSetQueries {
	return a.tableSetQueries(liveAddedTablesColumn)
}

// ReadLiveAddedTables returns the stream's live-added set. No row, no
// column and NULL are the empty set; a value that does not decode is an
// error naming the recovery.
func (a *ChangeApplier) ReadLiveAddedTables(ctx context.Context, streamID string) ([]string, error) {
	if streamID == "" {
		return nil, errors.New("postgres: applier: ReadLiveAddedTables: streamID is empty")
	}
	tables, _, err := appliershared.ReadTableSet(ctx, a.db, controlCfg, a.liveAddedQueries(), streamID)
	return tables, classifyApplierError(err)
}

// RecordLiveAddedTable adds tableName to the stream's live-added set. The
// row must exist.
func (a *ChangeApplier) RecordLiveAddedTable(ctx context.Context, streamID, tableName string) error {
	if err := a.ensureLiveAddedColumn(ctx, streamID); err != nil {
		return err
	}
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.liveAddedQueries(), streamID, true,
		appliershared.TableSetEdit{Apply: func(set map[string]struct{}, _ bool, _ time.Time) (map[string]struct{}, bool) {
			set[tableName] = struct{}{}
			return set, true
		}}))
}

// RemoveLiveAddedTable takes tableName out of the stream's live-added set.
// No row, no column or no entry is nothing to remove.
func (a *ChangeApplier) RemoveLiveAddedTable(ctx context.Context, streamID, tableName string) error {
	return classifyApplierError(appliershared.UpdateTableSet(ctx, a.db, controlCfg, a.liveAddedQueries(), streamID, false,
		appliershared.TableSetEdit{Apply: func(set map[string]struct{}, _ bool, _ time.Time) (map[string]struct{}, bool) {
			if _, ok := set[tableName]; !ok {
				return nil, false
			}
			delete(set, tableName)
			if len(set) == 0 {
				return nil, true
			}
			return set, true
		}}))
}

func (a *ChangeApplier) ensureLiveAddedColumn(ctx context.Context, streamID string) error {
	if streamID == "" {
		return errors.New("postgres: applier: live-added tables: streamID is empty")
	}
	if err := cdcStateTable(a.controlSchema).ensureColumns(ctx, a.db, []controlColumn{tableSetColumn(liveAddedTablesColumn)}); err != nil {
		return fmt.Errorf("postgres: applier: record live-added table: %w", err)
	}
	// A pre-existing column of a shorter type is refused like the record's
	// columns (R9 S-4).
	return a.verifyTableSetColumnTypes(ctx)
}

var _ ir.LiveAddedTableRetirer = (*ChangeApplier)(nil)

// RetireLiveAddedTables implements [ir.LiveAddedTableRetirer]: the running
// stream's LIVE-ADDED-TABLE-EXCLUDED write ([appliershared.RetireLiveAddedTables]).
func (a *ChangeApplier) RetireLiveAddedTables(ctx context.Context, streamID string, tables []string) (string, error) {
	if err := a.ensureTableSetColumns(ctx, streamID); err != nil {
		return "", err
	}
	if err := a.ensureLiveAddedColumn(ctx, streamID); err != nil {
		return "", err
	}
	ref := controlTableRef(a.controlSchema)
	q := appliershared.RetireQueries{
		ReadForUpdate: "SELECT " + appliershared.PendingAddedTablesColumn + ", " + appliershared.EstablishedTablesColumn + ", " +
			appliershared.DroppedTablesColumn + ", " + liveAddedTablesColumn + " FROM " + ref + " WHERE stream_id = $1 FOR UPDATE",
		Update: "UPDATE " + ref + " SET " + appliershared.PendingAddedTablesColumn + " = $1, " + appliershared.EstablishedTablesColumn +
			" = $2, " + appliershared.DroppedTablesColumn + " = $3, " + liveAddedTablesColumn + " = $4 WHERE stream_id = $5",
	}
	codec := appliershared.LiveAddedCodec{
		Decode: appliershared.DecodeTableSet,
		Encode: func(tables []string) any {
			if len(tables) == 0 {
				return nil
			}
			return appliershared.EncodeTableSet(tables)
		},
	}
	unrecorded, err := appliershared.RetireLiveAddedTables(ctx, a.db, controlCfg, q, codec, streamID, tables)
	return unrecorded, classifyApplierError(err)
}
