// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"

	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/query"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Item 74 — VStream partial-row-image belt (the Bug-193 lesson through
// the Vitess/VStream door).
//
// The vanilla binlog reader refuses partial binlog row images at CDC
// start ([preflightBinlogRowImage]) and belts them at dispatch
// ([refusePartialRowImage]); see cdc_row_image_preflight.go for the why.
// VStream reaches the same silent-loss class by a different route and
// cannot use the same preflight: sluice talks to a vtgate, not to the
// underlying mysqlds, so there is no single @@GLOBAL.binlog_row_image to
// read — a self-hosted Vitess is a fleet of tablets, each with its own
// mysqld, and vtgate exposes no aggregate row-image posture. The faithful
// belt is therefore at decode time, on the wire signal Vitess itself
// carries: the RowChange.DataColumns bitmap.
//
// How the silent loss happens without the belt. Vitess populates
// [binlogdata.RowChange.DataColumns] ONLY when the AFTER image is genuinely
// partial — i.e. the source tablet runs binlog_row_image=NOBLOB AND has the
// experimental VReplicationExperimentalFlagAllowNoBlobBinlogRowImage flag on
// (Vitess 16+). (Without that flag a partial image makes the tablet's
// getValues abort the stream loudly on the Vitess side — "partial row image
// encountered: ensure binlog_row_image is set to 'full'" — so that case is
// already loud.) The bitmap's Count is the table's full column count and a
// bit is SET when that column is present in the after image; an UNSET bit
// marks a column omitted (an unchanged BLOB/TEXT under NOBLOB). Crucially,
// Vitess encodes an omitted column into the query.Row as a NULL cell
// (RowToProto3 writes length -1 for the zero sqltypes.Value it left in
// place), so [decodeVStreamRow] — which has no bitmap and reads a -1 length
// as SQL NULL — would emit the omitted, still-present column as though it
// had changed to NULL. On an UPDATE apply that writes NULL over the real
// value: silent corruption, stream green, row counts equal. Exactly the
// Bug-193 class, one door over. PlanetScale (the managed flavor) pins
// binlog_row_image=FULL, so DataColumns is never populated there and the
// belt never fires — only self-hosted Vitess reaches it.
//
// NOBLOB omits unchanged BLOB/TEXT from UPDATE images only (INSERT/DELETE
// log every column — there is no "unchanged" for them), so a partial
// after-image is always an UPDATE; refusing on the after image covers the
// whole NOBLOB class (the paired partial before-image, which Vitess does
// NOT hand us a bitmap for, is moot once we refuse the UPDATE outright).
//
// Why refuse and not carry-forward. To reconstruct the omitted value sluice
// would need the prior value, but NOBLOB omits the unchanged blob from the
// before image too, so the event does not carry it — recovering it would
// mean reading the target, replica-apply semantics sluice deliberately does
// not attempt (same reasoning as the binlog UPDATE arm in
// cdc_row_image_preflight.go). Loud refusal over a silent guess.
//
// The [binlogdata.RowChange.JsonPartialValues] bitmap is the same
// silent-loss class one variable over — binlog_row_value_options=PARTIAL_JSON
// makes Vitess carry a JSON column's after value as a JSON_[INSERT|REPLACE|
// REMOVE] diff expression, not the value; applying it verbatim corrupts the
// document. A SET bit there marks such a column, and the belt refuses it too
// (the vanilla path's [partialJSONUpdatesError] mirror). Note Vitess also
// populates DataColumns (full, all-bits-set) alongside JsonPartialValues, so
// the two arms are checked independently: a partial-JSON row has no UNSET
// DataColumns bit, and a NOBLOB row has no SET JsonPartialValues bit.

// bitmapBitSet reports whether the bit at index is set in a Vitess
// RowChange bitmap's packed bytes, mirroring vttablet's isBitSet
// (byte = index/8, mask = 1<<(index%8), little-endian within the byte).
// A short/absent byte (a malformed or hand-built bitmap) reads as UNSET
// so the belt errs toward refusing rather than passing a partial image.
func bitmapBitSet(cols []byte, index int) bool {
	byteIndex := index / 8
	if byteIndex < 0 || byteIndex >= len(cols) {
		return false
	}
	return cols[byteIndex]&(1<<(uint(index)&0x7)) != 0
}

// firstUnsetBit returns the lowest column index in [0,count) whose bit is
// unset in the bitmap, and true; or 0,false when every bit is set. Used
// for DataColumns, where an unset bit marks a column omitted from the
// after image.
func firstUnsetBit(cols []byte, count int) (int, bool) {
	for i := 0; i < count; i++ {
		if !bitmapBitSet(cols, i) {
			return i, true
		}
	}
	return 0, false
}

// firstSetBit returns the lowest column index in [0,count) whose bit is
// set in the bitmap, and true; or 0,false when every bit is unset. Used
// for JsonPartialValues, where a set bit marks a partial-JSON diff value.
func firstSetBit(cols []byte, count int) (int, bool) {
	for i := 0; i < count; i++ {
		if bitmapBitSet(cols, i) {
			return i, true
		}
	}
	return 0, false
}

// vstreamColumnName names column i for a refusal message, preferring the
// cached FIELD name and falling back to a positional token when the index
// is out of the field slice's range (defensive; a well-formed stream keeps
// DataColumns.Count aligned with the FIELD event's column count).
func vstreamColumnName(fields []*query.Field, i int) string {
	if i >= 0 && i < len(fields) {
		if name := fields[i].GetName(); name != "" {
			return name
		}
	}
	return fmt.Sprintf("#%d", i)
}

// refuseVStreamPartialRowImage is the item-74 belt: it returns a coded,
// stream-fatal refusal when a VStream RowChange carries a partial after
// image (a NOBLOB-omitted column, via DataColumns) or a partial-JSON diff
// value (via JsonPartialValues). Returns nil for the ordinary FULL-image
// case — the common path, where both bitmaps are nil/empty — so it is a
// cheap guard on every RowChange before decode. See the file comment for
// why a partial image is a refusal rather than a silent pass or a guess.
func refuseVStreamPartialRowImage(rc *binlogdata.RowChange, fields []*query.Field, schema, table string) error {
	if dc := rc.GetDataColumns(); dc.GetCount() > 0 {
		if idx, omitted := firstUnsetBit(dc.GetCols(), int(dc.GetCount())); omitted {
			return vstreamPartialRowImageError(
				schema, table, vstreamColumnName(fields, idx),
				"omits column %[3]q from its after image — the source tablet streams partial binlog row "+
					"images (binlog_row_image=NOBLOB with Vitess's AllowNoBlobBinlogRowImage experimental "+
					"flag: an unchanged BLOB/TEXT column is dropped from the UPDATE image). sluice cannot "+
					"tell an omitted column from one genuinely set to NULL, so applying this UPDATE would "+
					"silently write NULL over the column's real value",
				"Set binlog_row_image=FULL on the source Vitess cluster's mysqld tablets (self-hosted "+
					"Vitess only; PlanetScale is already FULL), then restart the sync; a fresh cold start "+
					"(--restart-from-scratch) is the safe recovery when the partial-image window's UPDATEs matter",
			)
		}
	}
	if jpv := rc.GetJsonPartialValues(); jpv.GetCount() > 0 {
		if idx, partial := firstSetBit(jpv.GetCols(), int(jpv.GetCount())); partial {
			return vstreamPartialRowImageError(
				schema, table, vstreamColumnName(fields, idx),
				"carries a partial-JSON diff value for column %[3]q — the source runs "+
					"binlog_row_value_options=PARTIAL_JSON, under which a JSON column's after image is a "+
					"JSON_INSERT/REPLACE/REMOVE diff expression, not the value; applying it verbatim would "+
					"silently corrupt or lose JSON content",
				"Set binlog_row_value_options='' on the source Vitess cluster's mysqld tablets, then "+
					"restart the sync",
			)
		}
	}
	return nil
}

// vstreamPartialRowImageError builds the shared coded refusal for both
// belt arms. detail is a fmt template whose positional args are, in order:
// [1] schema, [2] table, [3] column — so the caller can drop the column
// name into whatever prose reads best. It reuses
// [sluicecode.CodeCDCRowImagePartial] (the partial-binlog-row-image code)
// rather than minting a Vitess-specific one: this is the same silent-loss
// class as the vanilla binlog belt, and operators grepping the code find
// one entry covering both doors.
func vstreamPartialRowImageError(schema, table, column, detail, remedy string) error {
	return sluicecode.Wrap(
		sluicecode.CodeCDCRowImagePartial,
		remedy,
		fmt.Errorf(
			"mysql/vstream: cdc: row event for %[1]s.%[2]s "+detail+", so the stream stops here",
			schema, table, column,
		),
	)
}
