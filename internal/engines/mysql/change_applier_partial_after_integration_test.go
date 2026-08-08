//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-08-05 C-10: the ADR-0140 update-as-upsert must not FABRICATE a row.
//
// A keyed, non-PK-changing UPDATE is applied by the coalescing batch path as an
// INSERT(after-image) … ON DUPLICATE KEY UPDATE. That is sound only while the
// after-image is a COMPLETE row for the target table, because the INSERT branch
// writes the whole row. When the source omits a column from its after-image —
// Postgres pgoutput sends an unchanged out-of-line TOASTed column as the 'u'
// datum, and [decodeTuple] drops it — and the target row is ABSENT (drift, a
// resnapshot gap, a partial restore, an out-of-band target delete), the INSERT
// branch fires and synthesises a row that never existed at the source: the
// omitted column lands at the target column's DEFAULT (or NULL), exit 0.
//
// These pins ground-truth against the target's OWN values (a real MySQL
// SELECT), not against the applier's report. The oracle for "what should have
// happened" is the SERIAL path (batchSize=1), which builds a real UPDATE and
// simply misses — the same thing the Postgres applier does on every path.
package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// partialAfterDDL models the PG→MySQL TOAST shape: a big out-of-line column
// (`body`) that an UPDATE to a sibling column does not touch, plus a column
// carrying a non-NULL DEFAULT so a fabricated row is distinguishable from a
// real one by value and not only by NULL-ness.
const partialAfterDDL = `
	CREATE TABLE toasty (
		id   INT          NOT NULL,
		tag  VARCHAR(32)  NULL,
		body TEXT         NULL,
		note VARCHAR(32)  NOT NULL DEFAULT 'fabricated-default',
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// seedPartialAfter inserts the two rows the pins work with and then deletes
// row 2 out-of-band, so row 1 is PRESENT at the target and row 2 is ABSENT —
// the two cells of the reachability matrix.
const seedPartialAfter = `
	INSERT INTO toasty (id, tag, body, note) VALUES
		(1, 'orig1', 'the-big-out-of-line-body-1', 'real'),
		(2, 'orig2', 'the-big-out-of-line-body-2', 'real');
	DELETE FROM toasty WHERE id = 2;`

// partialAfterStream is the source stream: two UPDATEs whose after-image omits
// `body` and `note` exactly the way pgoutput omits an unchanged TOASTed column.
// Before is the key-only image both readers produce (PG's
// synthesizeKeyOnlyBefore / filterBeforeToKeyCols, MySQL's filterBeforeToPK).
func partialAfterStream() []ir.Change {
	return []ir.Change{
		ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: "pa-000001"},
			Schema:   "target_db", Table: "toasty",
			Before: ir.Row{"id": int64(1)},
			After:  ir.Row{"id": int64(1), "tag": "upd1"},
		},
		ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: "pa-000002"},
			Schema:   "target_db", Table: "toasty",
			Before: ir.Row{"id": int64(2)},
			After:  ir.Row{"id": int64(2), "tag": "upd2"},
		},
	}
}

// toastyRow reads one row back from the real target. present reports whether
// the row exists at all — the load-bearing observation for the absent-row cell.
func toastyRow(t *testing.T, dsn string, id int) (present bool, tag, body, note sql.NullString) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, "SELECT tag, body, note FROM toasty WHERE id = ?", id).
		Scan(&tag, &body, &note)
	if err == sql.ErrNoRows {
		return false, tag, body, note
	}
	if err != nil {
		t.Fatalf("select id=%d: %v", id, err)
	}
	return true, tag, body, note
}

// TestPartialAfterUpdate_AbsentRowIsNotFabricated is the C-10 pin. Run at every
// batch shape the applier has — serial (batchSize=1), single-lane coalescing,
// and the W=4 concurrent lanes — because the update-as-upsert lives on the
// coalescing handle and a pin at one shape says nothing about the others.
func TestPartialAfterUpdate_AbsentRowIsNotFabricated(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batchSize int
		lanes     int
	}{
		{"serial", 1, 1},
		{"single-lane-coalescing", 64, 1},
		{"concurrent-lanes", 64, concurrentLanesW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := startMySQLForApplier(t)
			defer cleanup()
			applyMySQLApplier(t, dsn, partialAfterDDL)
			applyMySQLApplier(t, dsn, seedPartialAfter)

			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			a := openConcurrentApplier(t, ctx, dsn, tc.lanes)
			defer closeApplier(a)

			pumpBatchedChangesPipelined(t, ctx, a, testStreamID, partialAfterStream(), tc.batchSize)

			// PRESENT row: the update lands and the omitted columns keep the
			// target's existing values. This is the half that already worked and
			// that the fix must not regress.
			present, tag, body, note := toastyRow(t, dsn, 1)
			if !present {
				t.Fatal("id=1 vanished; the present-row UPDATE should have landed")
			}
			if tag.String != "upd1" {
				t.Errorf("id=1 tag = %q; want %q (the UPDATE did not land)", tag.String, "upd1")
			}
			if body.String != "the-big-out-of-line-body-1" {
				t.Errorf("id=1 body = %q; want the seeded value — an omitted after-image column must PRESERVE the target's value", body.String)
			}
			if note.String != "real" {
				t.Errorf("id=1 note = %q; want %q", note.String, "real")
			}

			// ABSENT row: the source UPDATE cannot be applied, and the applier
			// must not invent one. Anything present here is a row that never
			// existed at the source.
			present, tag, body, note = toastyRow(t, dsn, 2)
			if present {
				t.Errorf("C-10: id=2 was FABRICATED — target row was absent and the "+
					"partial after-image was upserted as an INSERT: tag=%q body=%q(valid=%v) note=%q",
					tag.String, body.String, body.Valid, note.String)
			}
		})
	}
}

// notNullNoDefaultDDL is the same shape with the omitted column NOT NULL and
// without a DEFAULT. It is here to pin the OTHER half of the class: under
// MySQL's strict sql_mode the fabricating INSERT fails loudly (1364) instead
// of writing a wrong value, so the defect's visibility depends entirely on the
// target column's nullability. A fix that only made this case loud would have
// left the silent one open — this pin says both cells must reach the same
// verdict, "no row".
const notNullNoDefaultDDL = `
	CREATE TABLE toasty_nn (
		id   INT          NOT NULL,
		tag  VARCHAR(32)  NULL,
		body TEXT         NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// TestPartialAfterUpdate_AbsentRowNotNullColumn pins that the absent-row
// partial UPDATE is a clean no-op rather than a stream-killing 1364 — the
// applier must decide the row is unapplicable BEFORE it reaches a shape whose
// only outcome is a target-side constraint error.
func TestPartialAfterUpdate_AbsentRowNotNullColumn(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()
	applyMySQLApplier(t, dsn, notNullNoDefaultDDL)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a := openConcurrentApplier(t, ctx, dsn, 1)
	defer closeApplier(a)

	stream := []ir.Change{
		ir.Update{
			Position: ir.Position{Engine: engineNameMySQL, Token: "pann-000001"},
			Schema:   "target_db", Table: "toasty_nn",
			Before: ir.Row{"id": int64(7)},
			After:  ir.Row{"id": int64(7), "tag": "upd7"},
		},
	}
	pumpBatchedChangesPipelined(t, ctx, a, testStreamID, stream, 64)

	if present, _, _, _ := toastyRowNN(t, dsn, 7); present {
		t.Error("C-10: id=7 was fabricated into a table whose omitted column is NOT NULL")
	}
}

// toastyRowNN is [toastyRow] for the NOT NULL fixture (no `note` column).
func toastyRowNN(t *testing.T, dsn string, id int) (present bool, tag, body, _ sql.NullString) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, "SELECT tag, body FROM toasty_nn WHERE id = ?", id).Scan(&tag, &body)
	if err == sql.ErrNoRows {
		return false, tag, body, sql.NullString{}
	}
	if err != nil {
		t.Fatalf("select id=%d: %v", id, err)
	}
	return true, tag, body, sql.NullString{}
}
