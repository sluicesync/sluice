//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL DDL implicit-commit premise, proven rather than cited.
//
// `internal/appliershared`'s `TransactionalDDL=false`, this engine's batch
// wiring, and the shared batch loop all reason from one environmental fact:
// **MySQL commits implicitly before and after every DDL statement**, so a
// `TRUNCATE TABLE` issued on an open `*sql.Tx` ends that transaction and the
// subsequent `tx.Rollback()` undoes nothing. It is a fact about the server,
// which is exactly the class CLAUDE.md's premise-naming step says owes a
// runtime check or a named test — and it had neither. What it had instead was
// a cross-reference (change_applier_batch.go: "applyOne has the same
// implicit-commit rough edge — see the comment there") pointing at a comment
// that did not exist, in a file whose [ChangeApplier.Apply] doc asserted the
// OPPOSITE ("a crash between them rolls back both — progress and data can
// never diverge").
//
// Two directions, both read from the server, because pinning only the failing
// one would leave "MySQL is not atomic here" reading as a blanket disclaimer
// when it is true of exactly one change type:
//
//   - [TestApplyOne_TruncateSurvivesTheRollback] — the truncate is still
//     applied after the position write in the same tx fails and the applier
//     rolls back. That IS the implicit commit, observed.
//   - [TestApplyOne_SchemaSnapshotRollsBackWithItsPosition] — the same forced
//     failure on the pure-DML arm leaves nothing behind, so ADR-0007's
//     position-and-data atomicity is genuinely available everywhere except
//     the DDL arm.
//
// The independent expected value in both is the target server's own state
// (`SELECT COUNT(*)`), never anything the applier reports about itself.
//
// SCOPE, stated so the name cannot be read as broader than the truth: this
// grades the SERIAL per-change core ([ChangeApplier.applyOneImpl]), which is
// where the batch loop deliberately routes every schema event. The batched
// and lane cores never dispatch a schema event onto their own tx — that is a
// routing property of `appliershared.RunBatchLoop`, and
// [TestMySQLBatchLoopRoutesSchemaEventsOffTheBatchTx] below is what fails if
// the routing flag flips.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// breakPositionWrites drops the control table out from under an already-open
// applier, so the next position write inside [ChangeApplier.applyOneImpl]
// fails with errno 1146 AFTER dispatch has already run. That is the exact
// interleaving the atomicity claim is about, and it is reachable without a
// failpoint.
func breakPositionWrites(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx,
		"DROP TABLE `target_db`.`"+appliershared.ControlTableName+"`"); err != nil {
		t.Fatalf("drop control table: %v", err)
	}
}

func TestApplyOne_TruncateSurvivesTheRollback(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL,
			v  VARCHAR(32),
			PRIMARY KEY (id)
		);
		INSERT INTO widgets (id, v) VALUES (1,'a'), (2,'b'), (3,'c');
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	applier := opened.(*ChangeApplier)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	if got := countAllRows(t, dsn, "target_db", "widgets"); got != 3 {
		t.Fatalf("setup: widgets has %d rows; want 3", got)
	}

	breakPositionWrites(t, ctx, applier.db)

	err = applier.applyOne(ctx, "implicit-commit-stream", ir.Truncate{
		Position: ir.Position{Engine: engineNameMySQL, Token: "tok-truncate"},
		Schema:   "target_db",
		Table:    "widgets",
	})
	if err == nil {
		t.Fatal("applyOne returned nil with the control table dropped; the position write must fail for this " +
			"test to observe the rollback path at all")
	}

	got := countAllRows(t, dsn, "target_db", "widgets")
	if got == 3 {
		t.Fatalf("widgets still has 3 rows — the TRUNCATE was rolled back with the failed position write. "+
			"That would mean MySQL did NOT implicitly commit the DDL, which is the environmental premise "+
			"`TransactionalDDL=false`, the shared batch loop's schema-event routing, and applyOneImpl's "+
			"implicit-commit paragraph all rest on. If MySQL has genuinely changed, those three are now "+
			"pessimistic rather than wrong — but they are no longer describing the server. (rows=%d)", got)
	}
	if got != 0 {
		t.Fatalf("widgets has %d rows after the truncate; want 0", got)
	}
	t.Log("PROVEN: MySQL implicit-committed the TRUNCATE; the applier's tx.Rollback on the failed position " +
		"write did not undo it, so ADR-0007 position-and-data atomicity is unavailable on this arm")
}

func TestApplyOne_SchemaSnapshotRollsBackWithItsPosition(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	applyMySQLApplier(t, dsn, `
		CREATE TABLE gadgets (
			id BIGINT NOT NULL,
			v  VARCHAR(32),
			PRIMARY KEY (id)
		);
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	applier := opened.(*ChangeApplier)
	defer func() { _ = applier.Close() }()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	breakPositionWrites(t, ctx, applier.db)

	err = applier.applyOne(ctx, "implicit-commit-stream", ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "tok-snapshot"},
		Schema:   "target_db",
		Table:    "gadgets",
		IR: &ir.Table{
			Name: "gadgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "v", Type: ir.Varchar{Length: 32}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	})
	if err == nil {
		t.Fatal("applyOne returned nil with the control table dropped; the position write must fail for this " +
			"test to observe the rollback path at all")
	}

	// The schema-history row is the SchemaSnapshot arm's data write. It is
	// pure DML, so the failed position write in the same tx must have taken
	// it with it — which is what makes the Truncate finding a statement
	// about one change type rather than about the engine.
	if got := countAllRows(t, dsn, "target_db", schemaHistoryTableName); got != 0 {
		t.Fatalf("%s has %d rows after the position write failed; want 0. The SchemaSnapshot arm writes only "+
			"DML, so ADR-0049 locked decision #4a's \"a single commit makes them atomic. A failure rolls back "+
			"BOTH\" must hold here — if it does not, a position can outlive the schema version it names and a "+
			"resume takes a spurious cold start", schemaHistoryTableName, got)
	}
	t.Logf("PROVEN: the DML arm rolled back with its position (%s empty), so the implicit-commit exception is "+
		"scoped to ir.Truncate and not a general disclaimer", schemaHistoryTableName)
}

// TestMySQLDeclaresNonTransactionalDDL binds the engine half of the argument:
// applyOneImpl's rough edge is tolerable only because no batch tx ever
// carries a DDL statement, and the only thing routing it away is this flag.
// The shared-loop half — that its schema-event predicate covers EVERY change
// type MySQL renders as DDL, not just SchemaSnapshot — is
// TestIsSchemaEvent_CoversEveryDDLBearingChangeType in
// `internal/appliershared`, where the predicate lives. Two facts, each
// individually plausible; neither alone protects the batch tx.
func TestMySQLDeclaresNonTransactionalDDL(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opened, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	applier := opened.(*ChangeApplier)
	defer func() { _ = applier.Close() }()

	if applier.batchConfig().TransactionalDDL {
		t.Error("MySQL declares TransactionalDDL=true — a schema event would then be dispatched ONTO the batch " +
			"tx, and its DDL implicit commit would end that tx while other rows were still pending, leaving " +
			"them durable with no position write and tx.Rollback silently unable to undo them. MySQL DDL is " +
			"not transactional (TestApplyOne_TruncateSurvivesTheRollback proves it against the server); this " +
			"flag is the only thing routing DDL off the batch tx")
	}
}
