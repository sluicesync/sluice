// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

import "sluicesync.dev/sluice/internal/ir"

// KeyPredicate returns the part of an UPDATE's or DELETE's before-image that
// its WHERE compares: the target table's primary-key columns, plus any of
// alsoKeep the image carries, whenever the image names every key column and
// none of them is generated. Otherwise it returns before unchanged.
//
// # Why the WHERE stops at the key (Bug 339)
//
// A WHERE built from a whole before-image is a compare-and-set on every
// column: the write lands only if the target still holds exactly what the
// source held before the change. Anything that makes a single non-key column
// differ makes it match no row, and a miss is tolerated (zero rows affected
// is the ADR-0010 resume answer) — so the change is dropped, and so is every
// later UPDATE and DELETE of that row, because each before-image carries the
// values the target never received. The rows stay stale, and rows the source
// deletes stay on the target, at exit 0.
//
// sluice itself is one such writer. The forwarded ADD COLUMN backfill reads
// the source's CURRENT value for the added column, which is newer than the
// stream whenever a row changed between the ALTER and the read; the stream's
// later UPDATE of that row then carries the ALTER's DEFAULT in its
// before-image. Measured on a VStream source (whose reader passes the full
// row image through), every such UPDATE and every later change of the row
// was lost on Postgres targets and on the MySQL per-change path, while the
// MySQL batch path — which applies a keyed UPDATE as an upsert by primary
// key — was exact for the same stream.
//
// The binlog and pgoutput readers already narrow their before-images to the
// key (Bug 88/193, Bug 92), so for them this is the identity. Doing it here
// rather than in each reader is what makes it hold for every producer of a
// row image: the VStream readers, the trigger sources' full and changed
// capture modes, and changes replayed from a backup chain or a broker log
// that an older reader recorded.
//
// # When the image is kept whole
//
//   - The target has no primary key (keyless tables keep the whole-row,
//     one-row address of GC-42; see [WholeRowImage]).
//   - The image does not carry every key column — a source that narrowed to
//     a different identity (REPLICA IDENTITY USING INDEX, a target whose key
//     differs from the source's). Narrowing to the part it does carry would
//     address by a proper subset of the key, which can match rows the change
//     never named.
//   - A key column is generated: the WHERE skips generated columns, so the
//     narrowed predicate would again be a proper subset of the key.
//
// alsoKeep names columns the statement needs in its WHERE besides the key —
// a sharded Neki target's routing columns, which must stay so the statement
// still routes to the shard holding the row.
func KeyPredicate(before ir.Row, key []string, colTypes map[string]*ir.Column, alsoKeep []string) ir.Row {
	if len(key) == 0 || len(before) == 0 {
		return before
	}
	for _, k := range key {
		if _, ok := before[k]; !ok {
			return before
		}
		if col := colTypes[k]; col != nil && col.IsGenerated() {
			return before
		}
	}
	out := make(ir.Row, len(key)+len(alsoKeep))
	for _, k := range key {
		out[k] = before[k]
	}
	for _, k := range alsoKeep {
		if v, ok := before[k]; ok {
			out[k] = v
		}
	}
	return out
}
