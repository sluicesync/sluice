// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// makeColBitmap packs a present-column bitmap the way Vitess does
// (RowChange.DataColumns / JsonPartialValues): Count is the full column
// count, Cols is little-endian bits (bit i in byte i/8, mask 1<<(i%8)).
// present[i]=true sets bit i. Mirrors vttablet's setBit so the fixtures
// exercise the exact wire encoding the belt reads.
func makeColBitmap(present []bool) *binlogdata.RowChange_Bitmap {
	cols := make([]byte, (len(present)+7)/8)
	for i, p := range present {
		if p {
			cols[i/8] |= 1 << (uint(i) & 0x7)
		}
	}
	return &binlogdata.RowChange_Bitmap{Count: int64(len(present)), Cols: cols}
}

// makeRowOmitting builds a query.Row for a partial after image: values[i]
// == nil encodes the omitted column exactly as Vitess does — a NULL cell
// (length -1) for the zero sqltypes.Value it leaves in place. Non-nil
// entries are ordinary present cells. This is what decodeVStreamRow would
// mis-read as a genuine NULL without the belt.
func makeRowOmitting(values []*string) *query.Row {
	row := &query.Row{Lengths: make([]int64, len(values))}
	for i, v := range values {
		if v == nil {
			row.Lengths[i] = -1
			continue
		}
		row.Lengths[i] = int64(len(*v))
		row.Values = append(row.Values, []byte(*v)...)
	}
	return row
}

func strptr(s string) *string { return &s }

// TestBitmapBitSet ground-truths the bit reader against Vitess's isBitSet
// convention (byte = index/8, mask = 1<<(index%8)) so a future proto/bit
// shift can't silently invert the belt's present/absent reading.
func TestBitmapBitSet(t *testing.T) {
	// bits 0,1,3 set within byte 0; bit 8 set in byte 1.
	cols := []byte{0b00001011, 0b00000001}
	want := map[int]bool{0: true, 1: true, 2: false, 3: true, 4: false, 7: false, 8: true, 9: false}
	for idx, w := range want {
		if got := bitmapBitSet(cols, idx); got != w {
			t.Errorf("bitmapBitSet(%d) = %v; want %v", idx, got, w)
		}
	}
	// Out-of-range byte reads UNSET (safe/loud direction).
	if bitmapBitSet(cols, 16) {
		t.Error("bitmapBitSet past end = true; want false")
	}
	if bitmapBitSet(nil, 0) {
		t.Error("bitmapBitSet(nil) = true; want false")
	}
}

func TestFirstUnsetAndSetBit(t *testing.T) {
	full := makeColBitmap([]bool{true, true, true})
	if idx, ok := firstUnsetBit(full.GetCols(), int(full.GetCount())); ok {
		t.Errorf("firstUnsetBit(all-set) = %d,true; want _,false", idx)
	}
	if idx, ok := firstSetBit(full.GetCols(), int(full.GetCount())); !ok || idx != 0 {
		t.Errorf("firstSetBit(all-set) = %d,%v; want 0,true", idx, ok)
	}

	partial := makeColBitmap([]bool{true, false, true}) // column 1 omitted
	if idx, ok := firstUnsetBit(partial.GetCols(), int(partial.GetCount())); !ok || idx != 1 {
		t.Errorf("firstUnsetBit(0,_,1) = %d,%v; want 1,true", idx, ok)
	}

	none := makeColBitmap([]bool{false, false})
	if idx, ok := firstSetBit(none.GetCols(), int(none.GetCount())); ok {
		t.Errorf("firstSetBit(all-unset) = %d,true; want _,false", idx)
	}
}

// TestRefuseVStreamPartialRowImage pins the belt directly across the shape
// matrix. The class here is the row-image posture (FULL vs partial vs
// partial-JSON), not a type family — every posture the RowChange bitmaps
// can express is exercised: no bitmap, full bitmap, one-omitted,
// leading/trailing-omitted, and partial-JSON. Each partial shape must
// REFUSE with CodeCDCRowImagePartial; each full shape must PASS (nil), or
// a FULL VStream regresses.
func TestRefuseVStreamPartialRowImage(t *testing.T) {
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT64},
		{Name: "email", Type: query.Type_VARCHAR},
		{Name: "bio", Type: query.Type_BLOB},
	}

	cases := []struct {
		name      string
		rc        *binlogdata.RowChange
		wantRefue bool
		wantCol   string // substring expected in the message when refusing
	}{
		{
			name: "no bitmap (FULL image) passes",
			rc: &binlogdata.RowChange{
				After: makeRowOmitting([]*string{strptr("7"), strptr("a@x"), strptr("hi")}),
			},
		},
		{
			name: "full DataColumns bitmap passes",
			rc: &binlogdata.RowChange{
				After:       makeRowOmitting([]*string{strptr("7"), strptr("a@x"), strptr("hi")}),
				DataColumns: makeColBitmap([]bool{true, true, true}),
			},
		},
		{
			name: "trailing blob omitted refuses (NOBLOB)",
			rc: &binlogdata.RowChange{
				Before:      makeRowOmitting([]*string{strptr("7"), strptr("a@x"), nil}),
				After:       makeRowOmitting([]*string{strptr("7"), strptr("a@y"), nil}),
				DataColumns: makeColBitmap([]bool{true, true, false}),
			},
			wantRefue: true,
			wantCol:   "bio",
		},
		{
			name: "middle column omitted refuses, names first omitted",
			rc: &binlogdata.RowChange{
				After:       makeRowOmitting([]*string{strptr("7"), nil, strptr("hi")}),
				DataColumns: makeColBitmap([]bool{true, false, true}),
			},
			wantRefue: true,
			wantCol:   "email",
		},
		{
			name: "leading column omitted refuses",
			rc: &binlogdata.RowChange{
				After:       makeRowOmitting([]*string{nil, strptr("a@x"), strptr("hi")}),
				DataColumns: makeColBitmap([]bool{false, true, true}),
			},
			wantRefue: true,
			wantCol:   "id",
		},
		{
			name: "partial-JSON refuses even with full DataColumns",
			rc: &binlogdata.RowChange{
				After:             makeRowOmitting([]*string{strptr("7"), strptr("a@x"), strptr("hi")}),
				DataColumns:       makeColBitmap([]bool{true, true, true}),
				JsonPartialValues: makeColBitmap([]bool{false, false, true}),
			},
			wantRefue: true,
			wantCol:   "bio",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := refuseVStreamPartialRowImage(c.rc, fields, "main", "users")
			if !c.wantRefue {
				if err != nil {
					t.Fatalf("want nil (FULL pass-through); got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("want coded refusal; got nil")
			}
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
				t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
			}
			if c.wantCol != "" && !strings.Contains(err.Error(), c.wantCol) {
				t.Errorf("message %q does not name column %q", err.Error(), c.wantCol)
			}
		})
	}
}

// TestVStreamPartialRowImage_DispatchMatrix drives the belt through the
// real dispatcher (dispatch → dispatchRow) so the guard is proven wired in
// at the call site, not just callable in isolation. INSERT/UPDATE/DELETE ×
// {FULL, partial} — the partial UPDATE must stop the stream loudly and
// emit NOTHING (no half-applied Insert/Update on the channel); the FULL
// shapes must flow unchanged.
func TestVStreamPartialRowImage_DispatchMatrix(t *testing.T) {
	newReader := func() *vstreamCDCReader {
		return &vstreamCDCReader{
			keyspace: "main",
			shards:   []string{"-"},
			fields:   make(map[string][]*query.Field),
		}
	}
	fieldEv := &binlogdata.VEvent{
		Type: binlogdata.VEventType_FIELD,
		FieldEvent: &binlogdata.FieldEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			Fields: []*query.Field{
				{Name: "id", Type: query.Type_INT64},
				{Name: "email", Type: query.Type_VARCHAR},
				{Name: "bio", Type: query.Type_BLOB},
			},
		},
	}
	rowEv := func(rc *binlogdata.RowChange) *binlogdata.VEvent {
		return &binlogdata.VEvent{
			Type: binlogdata.VEventType_ROW,
			RowEvent: &binlogdata.RowEvent{
				TableName:  "users",
				Keyspace:   "main",
				Shard:      "-",
				RowChanges: []*binlogdata.RowChange{rc},
			},
		}
	}
	full := func() *binlogdata.RowChange_Bitmap { return makeColBitmap([]bool{true, true, true}) }

	t.Run("partial UPDATE refuses and emits nothing", func(t *testing.T) {
		r := newReader()
		out := make(chan ir.Change, 4)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.dispatch(ctx, fieldEv, out); err != nil {
			t.Fatalf("field dispatch: %v", err)
		}
		partial := rowEv(&binlogdata.RowChange{
			Before:      makeRowOmitting([]*string{strptr("7"), strptr("a@x"), nil}),
			After:       makeRowOmitting([]*string{strptr("7"), strptr("a@y"), nil}),
			DataColumns: makeColBitmap([]bool{true, true, false}),
		})
		err := r.dispatch(ctx, partial, out)
		if err == nil {
			t.Fatal("partial UPDATE: want refusal; got nil")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
		}
		close(out)
		if got := drainChannel(out); len(got) != 0 {
			t.Fatalf("partial UPDATE emitted %d changes; want 0 (loud stop, no half-apply)", len(got))
		}
	})

	t.Run("FULL INSERT/UPDATE/DELETE flow unchanged", func(t *testing.T) {
		r := newReader()
		out := make(chan ir.Change, 8)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		evs := []*binlogdata.VEvent{
			fieldEv,
			// INSERT — After only, full bitmap.
			rowEv(&binlogdata.RowChange{
				After:       makeRowOmitting([]*string{strptr("7"), strptr("a@x"), strptr("hi")}),
				DataColumns: full(),
			}),
			// UPDATE — Before+After, full bitmap.
			rowEv(&binlogdata.RowChange{
				Before:      makeRowOmitting([]*string{strptr("7"), strptr("a@x"), strptr("hi")}),
				After:       makeRowOmitting([]*string{strptr("7"), strptr("a@y"), strptr("hi")}),
				DataColumns: full(),
			}),
			// DELETE — Before only, no bitmap (deletes log every column).
			rowEv(&binlogdata.RowChange{
				Before: makeRowOmitting([]*string{strptr("7"), strptr("a@y"), strptr("hi")}),
			}),
		}
		for _, ev := range evs {
			if err := r.dispatch(ctx, ev, out); err != nil {
				t.Fatalf("dispatch %v: %v", ev.GetType(), err)
			}
		}
		close(out)
		got := drainChannel(out)
		if len(got) != 3 {
			t.Fatalf("got %d changes; want 3 (insert, update, delete)", len(got))
		}
		if _, ok := got[0].(ir.Insert); !ok {
			t.Errorf("got[0] = %T; want ir.Insert", got[0])
		}
		upd, ok := got[1].(ir.Update)
		if !ok {
			t.Fatalf("got[1] = %T; want ir.Update", got[1])
		}
		// The unchanged blob must carry its real value in a FULL image —
		// proof the belt didn't over-fire and drop a present column. BLOB
		// decodes to []byte per the value contract.
		if bio, _ := upd.After["bio"].([]byte); string(bio) != "hi" {
			t.Errorf("update.After[bio] = %#v; want []byte(\"hi\")", upd.After["bio"])
		}
		if _, ok := got[2].(ir.Delete); !ok {
			t.Errorf("got[2] = %T; want ir.Delete", got[2])
		}
	})
}

// TestVStreamPartialRowImage_ColdStartDispatch pins the belt on the SECOND
// dispatch path — vstreamSnapshotStream.dispatchCDCRow, the cold-start
// snapshot->CDC catch-up that serves the DEFAULT first sync. Audit
// 2026-07-17 (A1, CRITICAL) found the item-74 belt was wired into
// dispatchRow but MISSED on its hand-mirrored twin dispatchCDCRow, so a
// self-hosted-Vitess NOBLOB source doing a cold start would silently write
// NULL over an unchanged BLOB/TEXT column (stream green, counts equal). A
// mirror method is a mirror obligation: a partial UPDATE here must refuse
// loudly, before any decode/send, exactly as it does on dispatchRow.
func TestVStreamPartialRowImage_ColdStartDispatch(t *testing.T) {
	fields := []*query.Field{
		{Name: "id", Type: query.Type_INT64},
		{Name: "email", Type: query.Type_VARCHAR},
		{Name: "bio", Type: query.Type_BLOB},
	}
	// currentVgtid empty -> positionFor() returns a zero position cleanly,
	// so the belt (which runs after the position resolve) is reachable.
	s := &vstreamSnapshotStream{
		fields:   map[string][]*query.Field{fieldCacheKey("-", "users"): fields},
		boolWarn: newBoolRangeWarner(),
	}
	partial := &binlogdata.VEvent{
		Type: binlogdata.VEventType_ROW,
		RowEvent: &binlogdata.RowEvent{
			TableName: "users",
			Keyspace:  "main",
			Shard:     "-",
			RowChanges: []*binlogdata.RowChange{{
				Before:      makeRowOmitting([]*string{strptr("7"), strptr("a@x"), nil}),
				After:       makeRowOmitting([]*string{strptr("7"), strptr("a@y"), nil}),
				DataColumns: makeColBitmap([]bool{true, true, false}), // bio omitted (NOBLOB)
			}},
		},
	}
	out := make(chan ir.Change, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.dispatchCDCRow(ctx, partial, out)
	if err == nil {
		t.Fatal("cold-start partial UPDATE: want refusal; got nil (the silent NULL-overwrite audit A1 caught)")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCRowImagePartial {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCRowImagePartial, err, err)
	}
	close(out)
	if got := drainChannel(out); len(got) != 0 {
		t.Fatalf("cold-start partial UPDATE emitted %d changes; want 0 (loud stop, no half-apply)", len(got))
	}
}
