//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The documented PlanetScale recovery path, end to end on real
// databases: a `migrate --resume` whose recorded state marks every
// in-scope table complete must get PAST the create-tables phase and INTO
// the index phase EVEN WHEN THE TARGET REFUSES DIRECT DDL — because that
// is where the ADR-0148 deploy-request index-build fallback lives, and
// both index-phase hints (errno 3024's statement-time wall and the
// safe-migrations 1105) tell the operator to re-run with --resume plus a
// service token to finish just the indexes with no re-copy.
//
// Measured at 122 GB against real PlanetScale before this fix: safe
// migrations ON, token armed, `--resume` on a chain whose bulk copy had
// already completed (target counts exact-matched the source) →
// `rc=3` after one second, `SLUICE-E-PS-DIRECT-DDL-BLOCKED` on
// `create table "orders"`, `bulk copy progress lines: 0`. Nothing
// re-copied, nothing achieved, and the index phase never reached.
//
// Why the refusal is INJECTED at the SchemaWriter boundary rather than
// grown from a real safe-migrations branch: 1105 is a PlanetScale
// control-plane behaviour no container can reproduce, and the
// orchestrator only ever sees an opaque error returned from
// `sw.CreateTablesWithoutConstraints` — the exact seam where the field
// error surfaced. Everything else here is real: real PG source and
// target, a real first migration that records real progress rows in
// `sluice_migrate_state`, and the real PG SchemaWriter building the real
// secondary index during the resume (asserted by reading the target's
// own catalog, not by asking the double). The refusal is armed
// unconditionally, so a green run is a run where the statement was never
// issued.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// errIndexStatementTimeLimit stands in for PlanetScale's statement-time
// wall (errno 3024) that kills a large deferred `ALTER … ADD INDEX`
// AFTER the copy has completed — the failure that produces the state
// shape ("every table complete, header phase = indexes") whose hint
// tells the operator to re-run with --resume.
var errIndexStatementTimeLimit = errors.New(
	"Error 3024 (HY000): Query execution was interrupted, maximum statement execution time exceeded",
)

// ddlRefusedTargetEngine wraps a real target engine so a chosen DDL
// phase is refused the way a walled PlanetScale branch refuses it, while
// every other phase runs against the real target.
type ddlRefusedTargetEngine struct {
	ir.Engine
	writer *refusingCreateSchemaWriter
}

func (e *ddlRefusedTargetEngine) OpenSchemaWriter(ctx context.Context, dsn string) (ir.SchemaWriter, error) {
	inner, err := e.Engine.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		return nil, err
	}
	e.writer.inner = inner
	return e.writer, nil
}

// OpenMigrationStateStore forwards the optional resume-store surface the
// embedded ir.Engine interface doesn't carry — without it the resume
// state would be invisible and the test would be vacuous.
func (e *ddlRefusedTargetEngine) OpenMigrationStateStore(ctx context.Context, dsn string) (ir.MigrationStateStore, error) {
	opener, ok := e.Engine.(ir.MigrationStateStoreOpener)
	if !ok {
		return nil, errors.New("inner engine does not implement MigrationStateStoreOpener")
	}
	return opener.OpenMigrationStateStore(ctx, dsn)
}

// refusingCreateSchemaWriter delegates every DDL phase to the real
// writer except the ones the test arms with an error, and records the
// phase order. createErr arms the safe-migrations create-tables refusal;
// indexErr arms the index-phase wall that produces the field's state
// shape in the first place (every table complete, the header phase stuck
// at `indexes`).
//
// It deliberately does NOT re-expose the inner writer's optional
// surfaces (ir.IncrementalIndexBuilder, ir.IndexVerifier): that drives
// the orchestrator's non-overlapped copy → identity-sync → whole-schema
// CreateIndexes branch, which is the branch a MySQL/PlanetScale target
// takes — the engine shape the field failure came from — while the
// target underneath is still a real database.
type refusingCreateSchemaWriter struct {
	inner     ir.SchemaWriter
	createErr error
	indexErr  error

	phases      []string
	createCalls int
}

func (w *refusingCreateSchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "CreateTablesWithoutConstraints")
	w.createCalls++
	if w.createErr != nil {
		return w.createErr
	}
	return w.inner.CreateTablesWithoutConstraints(ctx, s)
}

func (w *refusingCreateSchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "CreateIndexes")
	if w.indexErr != nil {
		return w.indexErr
	}
	return w.inner.CreateIndexes(ctx, s)
}

func (w *refusingCreateSchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "CreateConstraints")
	return w.inner.CreateConstraints(ctx, s)
}

func (w *refusingCreateSchemaWriter) SyncIdentitySequences(ctx context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "SyncIdentitySequences")
	return w.inner.SyncIdentitySequences(ctx, s)
}

func (w *refusingCreateSchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	w.phases = append(w.phases, "CreateViews")
	return w.inner.CreateViews(ctx, s)
}

func (w *refusingCreateSchemaWriter) Close() error {
	if c, ok := w.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (w *refusingCreateSchemaWriter) reached(phase string) bool {
	for _, p := range w.phases {
		if p == phase {
			return true
		}
	}
	return false
}

// readTableProgress reads the recorded per-table progress map through
// the engine's own MigrationStateStore — the same surface the resume
// classifier reads, so the test's premise ("every table is recorded
// complete") is ground-truthed rather than assumed. (readState, shared
// with the other resume tests, projects only the header's phase column.)
func readTableProgress(t *testing.T, ctx context.Context, eng ir.Engine, dsn, migrationID string) map[string]ir.TableProgress {
	t.Helper()
	opener, ok := eng.(ir.MigrationStateStoreOpener)
	if !ok {
		t.Fatal("engine does not implement MigrationStateStoreOpener")
	}
	store, err := opener.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("open migration state store: %v", err)
	}
	if c, ok := store.(interface{ Close() error }); ok {
		defer func() { _ = c.Close() }()
	}
	state, found, err := store.Read(ctx, migrationID)
	if err != nil {
		t.Fatalf("read migration state: %v", err)
	}
	if !found {
		t.Fatalf("no migration state row for %q", migrationID)
	}
	return state.TableProgress
}

// TestMigrate_ResumeWithDirectDDLRefused_ReachesIndexPhase is the gate.
// It asserts on the PHASE REACHED — not on the exit code — because "the
// run exited 0" would also be satisfied by a run that skipped
// everything.
func TestMigrate_ResumeWithDirectDDLRefused_ReachesIndexPhase(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	// A secondary index on orders so the index phase has real work to do
	// on the resume — the phase whose reachability is the whole point.
	const seedDDL = `
		CREATE TABLE customers (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		CREATE TABLE orders (
			id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			customer_id BIGINT NOT NULL,
			amount      BIGINT NOT NULL
		);
		CREATE INDEX orders_customer_idx ON orders (customer_id);
		INSERT INTO customers (email) VALUES ('a@example.com'), ('b@example.com');
		INSERT INTO orders (customer_id, amount)
			SELECT 1 + (g % 2), g FROM generate_series(1, 40) g;
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Attempt 1 reproduces the field's PRECURSOR, not a clean run: the
	// copy completes for every table and the run then dies in the INDEX
	// phase, the way PlanetScale's statement-time wall (errno 3024) kills
	// a large deferred ALTER … ADD INDEX. That is the only state shape
	// the hint's advice applies to — every table recorded complete with
	// the header phase stuck at `indexes`. (A migration recorded
	// `complete` would short-circuit at "already complete; nothing to
	// do" and never exercise a DDL phase at all.)
	walled := &refusingCreateSchemaWriter{indexErr: errIndexStatementTimeLimit}
	first := &Migrator{
		Source:      pgEng,
		Target:      &ddlRefusedTargetEngine{Engine: pgEng, writer: walled},
		SourceDSN:   sourceDSN,
		TargetDSN:   targetDSN,
		MigrationID: "resume-ddl-refused",
	}
	if err := first.Run(ctx); err == nil {
		t.Fatal("test setup: expected the first attempt to fail in the index phase")
	} else if !errors.Is(err, errIndexStatementTimeLimit) {
		t.Fatalf("test setup: first attempt failed with %v; want the index-phase wall", err)
	}
	// Read the recorded progress through the SAME store surface the
	// resume classifier consults — readState() only projects the header's
	// phase column.
	progress := readTableProgress(t, ctx, pgEng, targetDSN, "resume-ddl-refused")
	for _, table := range []string{"customers", "orders"} {
		if got := progress[table].State; got != ir.TableProgressComplete {
			t.Fatalf("test setup: %s recorded %q; want complete (the index-phase wall must fire AFTER every copy)", table, got)
		}
	}
	if state := readState(t, targetDSN, "resume-ddl-refused"); state.Phase == ir.MigrationPhaseComplete {
		t.Fatal("test setup: the failed migration is recorded complete; the resume would short-circuit")
	}

	// Attempt 2: --resume against a target that refuses direct DDL —
	// which is what a safe-migrations branch does, and what the hint
	// tells the operator to run.
	refusing := &refusingCreateSchemaWriter{createErr: errDirectDDLDisabled}
	resumeMig := &Migrator{
		Source:      pgEng,
		Target:      &ddlRefusedTargetEngine{Engine: pgEng, writer: refusing},
		SourceDSN:   sourceDSN,
		TargetDSN:   targetDSN,
		MigrationID: "resume-ddl-refused",
		Resume:      true,
	}
	if err := resumeMig.Run(ctx); err != nil {
		t.Fatalf("resume with direct DDL refused failed: %v\nphases reached: %v\n"+
			"this is the 122 GB PlanetScale dead end: every in-scope table is recorded complete, "+
			"so the create-tables phase has nothing to do, yet its refused DDL killed the run "+
			"before the index phase where the ADR-0148 deploy-request fallback lives",
			err, refusing.phases)
	}
	if refusing.createCalls != 0 {
		t.Errorf("create-tables was issued %d time(s); want 0", refusing.createCalls)
	}
	if !refusing.reached("CreateIndexes") {
		t.Fatalf("the resume never reached the index phase (phases: %v)", refusing.phases)
	}

	// Ground truth, not the double: the real PG writer rebuilt the index
	// during the resume, and no row was re-copied or lost. The catalog
	// read goes through its own connection, never the writer under test.
	targetDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer targetDB.Close()
	if !pgIndexExists(t, ctx, targetDB, "orders", "orders_customer_idx") {
		t.Error("orders_customer_idx missing on the target after the resume — the index phase ran but built nothing")
	}
	if got := countRows(t, targetDSN, "customers"); got != 2 {
		t.Errorf("customers row count = %d; want 2", got)
	}
	if got := countRows(t, targetDSN, "orders"); got != 40 {
		t.Errorf("orders row count = %d; want 40", got)
	}
	if final := readState(t, targetDSN, "resume-ddl-refused"); final.Phase != ir.MigrationPhaseComplete {
		t.Errorf("final phase = %q; want complete", final.Phase)
	}
}

// TestMigrate_ResumeWithIncompleteTable_StillIssuesCreateTables pins the
// must-NOT-break direction on real databases: when a table is recorded
// in-progress (a genuine mid-copy crash) the resume still issues its
// DDL, so a refusing target fails LOUDLY at create-tables instead of
// quietly proceeding on an unverified assumption.
func TestMigrate_ResumeWithIncompleteTable_StillIssuesCreateTables(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE customers (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		CREATE TABLE orders (
			id     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			amount BIGINT NOT NULL
		);
		INSERT INTO customers (email) VALUES ('a@example.com');
		INSERT INTO orders (amount) SELECT g FROM generate_series(1, 50) g;
	`
	applyPGDDL(t, sourceDSN, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Attempt 1 dies mid-copy on orders, leaving it recorded in-progress.
	failing := &failingRowWriterEngine{Engine: pgEng, failTable: "orders", failAfterRows: 10}
	first := &Migrator{
		Source:      pgEng,
		Target:      failing,
		SourceDSN:   sourceDSN,
		TargetDSN:   targetDSN,
		MigrationID: "resume-ddl-refused-partial",
	}
	if err := first.Run(ctx); err == nil {
		t.Fatal("test setup: expected the first attempt to fail")
	}
	progress := readTableProgress(t, ctx, pgEng, targetDSN, "resume-ddl-refused-partial")
	if got := progress["orders"].State; got == ir.TableProgressComplete {
		t.Fatalf("test setup: orders recorded %q; want a non-complete state", got)
	}

	refusing := &refusingCreateSchemaWriter{createErr: errDirectDDLDisabled}
	resumeMig := &Migrator{
		Source:      pgEng,
		Target:      &ddlRefusedTargetEngine{Engine: pgEng, writer: refusing},
		SourceDSN:   sourceDSN,
		TargetDSN:   targetDSN,
		MigrationID: "resume-ddl-refused-partial",
		Resume:      true,
	}
	err := resumeMig.Run(ctx)
	if err == nil {
		t.Fatalf("resume with a non-complete table succeeded despite refused DDL; the skip must be scoped to "+
			"'every in-scope table recorded complete' (phases: %v)", refusing.phases)
	}
	if !errors.Is(err, errDirectDDLDisabled) {
		t.Errorf("err = %v; want the create-tables refusal", err)
	}
	if !strings.Contains(err.Error(), "create tables") {
		t.Errorf("err = %v; want the create-tables phase prefix", err)
	}
	if refusing.createCalls != 1 {
		t.Errorf("create-tables issued %d time(s); want exactly 1", refusing.createCalls)
	}
}
