// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// dropUnchangedKeyColumns returns after without the primary-key columns whose
// value the change did not alter, so the per-change UPDATE's SET list does not
// name them (GC-41 (e)).
//
// # Why
//
// On a sharded Vitess/PlanetScale keyspace whose primary vindex is the primary
// key, vtgate refuses ANY assignment to a primary-vindex column at plan time —
// Error 1235 VT12001 "you cannot UPDATE primary vindex columns" — even
// `SET id = 1 WHERE id = 1`. Before this, [buildUpdateSQL] SET every After
// column, so every UPDATE the per-change path sent to such a target was
// refused: `Apply` (the non-batched streamer, the broker, chain restore) and
// every change the batch loop hands back to it (a partial after-image
// including the default-on ADD COLUMN backfill, an empty before-image, a
// keyless table, a primary-key change). The batched path never had the
// problem, because its upsert already leaves key columns out of the
// ON DUPLICATE KEY UPDATE list ([onDuplicateKeyUpdateClause]).
//
// A REAL key change keeps the column in SET, so on a vindexed key vtgate still
// refuses it and the classifier marks it SHARDED-TARGET-VINDEX-UPDATE: a row
// cannot move between shards through vtgate, and nothing here pretends it can.
//
// # Why dropping the column cannot change the result
//
// The WHERE clause is built from the before-image, so it carries `k = ?`
// bound to before[k] for every key column the before-image holds. A column is
// dropped only when after[k] is [appliershared.SameBoundValue] to before[k] —
// the same Go value, so [prepareApplierValue] binds the same argument for the SET as for
// the WHERE. The statement therefore assigns k the very value its own WHERE
// pinned k to, and a matched row's k compares equal to that value under the
// column's collation. Two cases:
//
//   - The stored key is byte-identical to before[k] (a faithful target — the
//     ordinary case): the assignment is a no-op, so leaving it out changes no
//     stored byte.
//   - The stored key is collation-equal but not byte-identical (case, trailing
//     pad, accent under an insensitive collation): the target already
//     disagreed with the source before this change. The full SET would have
//     rewritten the key's bytes as a side effect of a change that, by the
//     source's own images, did not touch the key; the trimmed SET leaves them.
//     That is exactly what the batched path does for the same change, so the
//     trim makes the two paths agree rather than introducing an outcome
//     either lacked. It is not silent loss: no value the change carried is
//     dropped — the change carried "unchanged" for k.
//
// Nothing else observable depends on the SET list naming an unchanged key.
// MySQL fires ON UPDATE CURRENT_TIMESTAMP only when some column's value
// actually changes, and an equal assignment is not a change; UPDATE triggers
// fire per matched row whatever the SET list says; and the affected-row count
// counts changed rows (or matched rows under CLIENT_FOUND_ROWS), neither of
// which an equal assignment moves. So the trim applies on every flavor, not
// only on vtgate targets: one statement shape everywhere, matching the
// batched path's, instead of a flavor fork the tests would have to cover
// twice.
//
// # Direction of the comparison's error
//
// reflect.DeepEqual is deliberately strict, as in the Neki sibling
// (dropUnchangedShardKeys in the postgres engine). A false "changed" — two
// encodings of one logical value, a time.Time differing only by location —
// keeps the column in SET, which is the pre-GC-41 behaviour (on a vtgate
// target, a loud refusal). A looser equality could call a real key change
// "unchanged": time.Time.Equal, for one, treats one instant in two locations
// as equal while the DATETIME the two render to differs.
//
// DeepEqual alone was NOT strict enough for floats: it compares float64 and
// float32 with ==, under which -0.0 equals +0.0, and a MySQL DOUBLE/FLOAT
// key stores the two distinctly (verified on mysql:8.4: a DOUBLE PRIMARY KEY
// holds -0). A source `UPDATE … SET k = -0` therefore read as "unchanged",
// left SET, and the target kept +0 at exit 0 (GC-41 (e) review). So equality
// is [appliershared.SameBoundValue]: floats by bit pattern, everything else
// by DeepEqual; its doc carries the per-family audit. A false "unchanged"
// cannot arise from it, because equal values bind equal arguments.
//
// # What is left alone
//
//   - A key column missing from either image: nothing establishes whether it
//     changed, so it stays in SET when after carries it.
//   - Non-key columns, even unchanged ones: the argument above rests on the
//     WHERE pinning the column, which a key-narrowed before-image does only for
//     key columns. A non-PK vindex column is not trimmed here; a target that
//     routes on one is refused before anything is written instead
//     ([RowWriter.ShardKeyUpsertMismatch]).
//   - An after-image with nothing left to assign once its unchanged keys are
//     gone (a table whose every column is in the key, or a partial after-image
//     carrying only keys): it is returned unchanged, because an UPDATE needs a
//     SET list and skipping the statement would skip the UPDATE triggers and
//     the key-scoped row check a matched-but-unchanged row still gets. On a
//     vindexed key that one shape stays refused, loudly.
//
// Primary-key names match case-insensitively: MySQL column names are, and a
// cross-engine source may spell them differently from information_schema. A
// miss only keeps a column in SET.
func dropUnchangedKeyColumns(before, after ir.Row, pk []string, colTypes map[string]*ir.Column) ir.Row {
	if len(pk) == 0 || len(after) == 0 {
		return after
	}
	isKey := func(col string) bool {
		for _, k := range pk {
			if strings.EqualFold(k, col) {
				return true
			}
		}
		return false
	}
	var trimmed ir.Row
	for col, av := range after {
		if !isKey(col) {
			continue
		}
		bv, inBefore := before[col]
		if !inBefore || !appliershared.SameBoundValue(bv, av) {
			continue
		}
		if trimmed == nil {
			trimmed = make(ir.Row, len(after))
			for k, v := range after {
				trimmed[k] = v
			}
		}
		delete(trimmed, col)
	}
	if trimmed == nil || len(appliershared.NonGeneratedRowKeys(trimmed, colTypes)) == 0 {
		return after
	}
	return trimmed
}
