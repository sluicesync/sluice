// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/ir"
)

// TestWriteRows_ChildBeforeParentIsAcceptedOnAPreExistingForeignKey is the
// GROUND TRUTH behind this engine's
// ir.Capabilities.BulkCopyBypassesForeignKeys = true, and therefore behind
// a SQLite target's exemption from the roadmap-item-140 pre-copy refusal
// (SLUICE-E-TARGET-PREEXISTING-FOREIGN-KEY).
//
// The safety argument the exemption rests on is an environmental fact —
// "every writable SQLite connection opens with _pragma=foreign_keys(0), so
// the copy cannot fail child-before-parent" — and a safety argument that
// cites a fact about the world owes that fact a check. Here it is: a target
// database that ALREADY carries the constraint (created outside sluice,
// the branched-from-an-existing-db shape), a child row written through the
// real RowWriter with NO parent row anywhere, and no error.
//
// The companion assertion is the one that keeps this from proving too
// much: the same file, opened with enforcement ON, rejects the identical
// row. So the acceptance above is the pragma doing work, not SQLite being
// indifferent to foreign keys.
func TestWriteRows_ChildBeforeParentIsAcceptedOnAPreExistingForeignKey(t *testing.T) {
	ctx := context.Background()
	eng := Engine{}
	path := filepath.Join(t.TempDir(), "fk.db")

	// Pre-create the target the way a branched database arrives: tables
	// plus their foreign keys, made by something other than this run.
	seed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed connection: %v", err)
	}
	if _, err := seed.ExecContext(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
		CREATE TABLE orders (
			id      INTEGER PRIMARY KEY,
			user_id INTEGER,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);
	`); err != nil {
		t.Fatalf("seed DDL: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("close seed connection: %v", err)
	}

	orders := &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "user_id", Type: ir.Integer{Width: 64}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Name: "pk", Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	// user_id 42 does not exist in `users` and never will in this test.
	orphan := []ir.Row{{"id": int64(1), "user_id": int64(42)}}

	if err := writeRows(t, ctx, eng, path, orders, orphan); err != nil {
		t.Fatalf("the SQLite writer rejected a child row whose parent is absent: %v\n\n"+
			"This engine declares ir.Capabilities.BulkCopyBypassesForeignKeys = true, which exempts a "+
			"SQLite target from the item-140 pre-copy refusal. If the writable-connection pragma set "+
			"(writePragmas / ADR-0134) no longer disables enforcement, that exemption is now WRONG and a "+
			"SQLite operator gets a mid-copy failure with no preflight — flip the capability and the "+
			"internal/docsync roster together.", err)
	}

	// The control: the identical row IS rejected on a connection with
	// enforcement on, so the acceptance above is the pragma, not SQLite
	// ignoring the constraint.
	strict, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open strict connection: %v", err)
	}
	defer func() { _ = strict.Close() }()
	if _, err := strict.ExecContext(ctx, `INSERT INTO orders (id, user_id) VALUES (2, 43)`); err == nil {
		t.Error("an orphan INSERT succeeded even with foreign_keys(1); this test cannot distinguish the " +
			"pragma from a constraint SQLite never enforces, so the pin above proves nothing")
	}
}
