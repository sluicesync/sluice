// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
)

// M2 G8: MariaDB compressed row events are CAPTURED, and captured
// IDENTICALLY to their uncompressed twins. go-mysql decompresses the
// event body in RowsEvent.DecodeData before any column decode and maps
// the compressed types to the same row-image kinds, so dispatchRows
// sees shape-identical Rows — the case labels route them through the
// exact same arms. This pin holds that equivalence at the dispatch
// layer; the family × verb matrix against a real compressing MariaDB
// lives in TestMariaDB_CDCReader_LogBinCompress_FamilyMatrix
// (cdc_binlog_compress_integration_test.go).

// TestDispatchRows_MariaDBCompressedRowEvents_CapturedIdentically
// dispatches each compressed type and its uncompressed v1 twin through
// two identically-primed readers and requires byte-identical emissions.
// All three verbs are pinned because all three occur on the wire (a big
// row's DELETE compresses via its before-image; ground-truthed
// mariadb:11.4.12). The anti-vacuity floor (emission count + concrete
// change type per verb) keeps a both-sides-dropped regression from
// greening the DeepEqual.
func TestDispatchRows_MariaDBCompressedRowEvents_CapturedIdentically(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		comp     replication.EventType
		uncomp   replication.EventType
		rows     [][]any
		wantN    int
		wantKind string
	}{
		{
			name:   "write",
			comp:   replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1,
			uncomp: replication.WRITE_ROWS_EVENTv1,
			rows:   [][]any{{int64(1)}},
			// TxBegin + Insert.
			wantN: 2, wantKind: "ir.Insert",
		},
		{
			name:   "update",
			comp:   replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1,
			uncomp: replication.UPDATE_ROWS_EVENTv1,
			rows:   [][]any{{int64(1)}, {int64(2)}},
			wantN:  2, wantKind: "ir.Update",
		},
		{
			name:   "delete",
			comp:   replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1,
			uncomp: replication.DELETE_ROWS_EVENTv1,
			rows:   [][]any{{int64(1)}},
			wantN:  2, wantKind: "ir.Delete",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := func(et replication.EventType) []ir.Change {
				r := newStagingReader(t, FlavorMariaDB, "0-1-1")
				ev := &replication.BinlogEvent{
					Header: hdr(et),
					Event:  &replication.RowsEvent{TableID: 7, Rows: tc.rows},
				}
				return dispatchAll(t, r, mariadbGTIDEvent(0, 2, false), ev)
			}
			compressed := run(tc.comp)
			plain := run(tc.uncomp)

			// Anti-vacuity floor: the row change must actually be there,
			// with the right concrete type — DeepEqual over two empty
			// slices proves nothing.
			if len(compressed) != tc.wantN {
				t.Fatalf("compressed %s dispatch emitted %d changes; want %d (TxBegin + the row change) — "+
					"the compressed event was dropped, the G8 silent-loss shape", tc.comp, len(compressed), tc.wantN)
			}
			if got := reflect.TypeOf(compressed[len(compressed)-1]).String(); got != tc.wantKind {
				t.Fatalf("compressed %s emitted a %s; want %s", tc.comp, got, tc.wantKind)
			}
			if !reflect.DeepEqual(compressed, plain) {
				t.Errorf("compressed %s and uncompressed %s dispatch DIVERGE:\ncompressed: %#v\nplain:      %#v",
					tc.comp, tc.uncomp, compressed, plain)
			}
		})
	}
}
