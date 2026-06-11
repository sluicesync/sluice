//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for resume anchor adoption (task #42, ADR-0085)
// against real Postgres — THE chain-gap pin this fix exists for.
//
// The silent-loss shape: `backup full` crashes after ≥1 table
// completed; writes land on the COMPLETED table; the re-run resumed
// (keeping the completed table verbatim, exact as-of the FIRST
// attempt's anchor A1) but recorded its own fresh snapshot anchor A2 as
// EndPosition. The gap writes in (A1, A2] were then in neither the kept
// chunks nor the next incremental's window (which opened at A2) — the
// chain restored cleanly, exit 0, missing those writes. Worse, the
// --chain-slot recovery message advised `sluice slot drop` + retry,
// which released the very WAL that covered the gap.
//
// TestBackup_ResumeAnchorAdoption_NoChainGap pins the fixed end-to-end
// flow (interrupt → gap writes → resume ADOPTS A1 + the standing chain
// slot → incremental → chain restore → byte-equal content, gap writes
// present exactly once). The gap writes hit BOTH a kept table and a
// re-streamed table with INSERT+UPDATE+DELETE across the value
// families (native / string-leaf / temporal — Bug 74 doctrine), so the
// re-streamed table also exercises the overlap-replay convergence this
// fix leans on (ADR-0010 idempotent appliers).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// failOnPathMatchPutStore fails the FIRST Put whose path contains
// match — the crash seam: interrupting the backup deterministically at
// "second table's first chunk upload", after the first table's
// per-table checkpoint committed. Substring (not prefix) because chunk
// paths are schema-qualified (`chunks/public__late_t/…`).
type failOnPathMatchPutStore struct {
	*LocalStore

	match  string
	failed bool
}

func (s *failOnPathMatchPutStore) Put(ctx context.Context, path string, r io.Reader) error {
	if !s.failed && strings.Contains(path, s.match) {
		s.failed = true
		return fmt.Errorf("injected crash before Put(%s)", path)
	}
	return s.LocalStore.Put(ctx, path, r)
}

// anchorFamilyDDL builds two identical family-matrix tables: kept_t
// completes before the interrupt; late_t is re-streamed by the resume
// (schema order == sweep order on the serial pool; kept_t < late_t).
// Columns cover the value families — native (bigint/int/double/bool),
// string-leaf (text/varchar/numeric/uuid), temporal (timestamp/
// timestamptz/date) — with NULLs in the seed rows.
const anchorFamilyDDL = `
	CREATE TABLE kept_t (
		id   BIGINT PRIMARY KEY,
		n    INT NOT NULL,
		f    DOUBLE PRECISION,
		b    BOOLEAN,
		s    TEXT,
		vc   VARCHAR(64),
		dec  NUMERIC(12,4),
		u    UUID,
		ts   TIMESTAMP,
		tstz TIMESTAMPTZ,
		d    DATE
	);
	CREATE TABLE late_t (
		id   BIGINT PRIMARY KEY,
		n    INT NOT NULL,
		f    DOUBLE PRECISION,
		b    BOOLEAN,
		s    TEXT,
		vc   VARCHAR(64),
		dec  NUMERIC(12,4),
		u    UUID,
		ts   TIMESTAMP,
		tstz TIMESTAMPTZ,
		d    DATE
	);
	ALTER TABLE kept_t REPLICA IDENTITY FULL;
	ALTER TABLE late_t REPLICA IDENTITY FULL;
	INSERT INTO kept_t VALUES
		(1, 10, 1.5,  true,  'alpha', 'va', 12.3400, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2026-01-01 10:00:00', '2026-01-01 10:00:00+00', '2026-01-01'),
		(2, 20, NULL, false, NULL,    'vb', NULL,    NULL,                                   NULL,                  NULL,                       NULL),
		(3, 30, 3.5,  NULL,  'gamma', NULL, 99.0001, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', '2026-02-02 02:02:02', '2026-02-02 02:02:02+05',  '2026-02-02');
	INSERT INTO late_t SELECT * FROM kept_t;
`

// anchorGapDML writes the gap-window delta on one table: an INSERT
// touching every family, an UPDATE rewriting every family on a seed
// row, and a DELETE of a seed row.
func anchorGapDML(table, tag string) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]s VALUES
			(100, 1000, 10.25, true, 'gap-%[2]s', 'gap-vc-%[2]s', 555.5500, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a33', '2026-03-03 03:03:03', '2026-03-03 03:03:03+00', '2026-03-03');
		UPDATE %[1]s SET
			n = 99, f = 9.75, b = false, s = 'upd-%[2]s', vc = 'upd-vc-%[2]s',
			dec = 7.7700, u = 'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380a44',
			ts = '2026-04-04 04:04:04', tstz = '2026-04-04 04:04:04+02', d = '2026-04-04'
		WHERE id = 1;
		DELETE FROM %[1]s WHERE id = 2;
	`, table, tag)
}

// pgTableContentFingerprint returns (row count, md5 over the sorted
// full-row texts) for table at dsn — multiset equality, so a duplicate
// OR a missing row OR any value drift changes the fingerprint. The
// session timezone is pinned so timestamptz renders identically on
// source and target.
func pgTableContentFingerprint(t *testing.T, dsn, table string) (int64, string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(context.Background(), "SET TIME ZONE 'UTC'"); err != nil {
		t.Fatalf("set timezone: %v", err)
	}
	var n int64
	var sum string
	q := fmt.Sprintf("SELECT COUNT(*), COALESCE(md5(string_agg(t::text, '|' ORDER BY t::text)), '') FROM %s t", table)
	if err := conn.QueryRowContext(context.Background(), q).Scan(&n, &sum); err != nil {
		t.Fatalf("fingerprint %s: %v", table, err)
	}
	return n, sum
}

func assertPGTablesMatch(t *testing.T, sourceDSN, targetDSN string, tables ...string) {
	t.Helper()
	for _, table := range tables {
		srcN, srcSum := pgTableContentFingerprint(t, sourceDSN, table)
		dstN, dstSum := pgTableContentFingerprint(t, targetDSN, table)
		if srcN != dstN || srcSum != dstSum {
			t.Errorf("table %s diverged: source (%d rows, %s) != target (%d rows, %s) — gap writes lost or duplicated",
				table, srcN, srcSum, dstN, dstSum)
		}
	}
}

// runAnchoredResumeChainGapFlow drives the shared interrupt → gap-write
// → resume → incremental → chain-restore flow and asserts the adopted
// anchor + byte-equal content. chainSlot selects the --chain-slot shape
// (slot provisioned by the backup) vs the operator-pre-created-slot
// mirror (publication + slot created manually BEFORE the full).
func runAnchoredResumeChainGapFlow(t *testing.T, chainSlot bool) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, anchorFamilyDDL)

	if !chainSlot {
		// The documented manual chain shape: publication first (pgoutput
		// resolves membership with a historic catalog snapshot), then
		// the standing slot, both BEFORE the full backup's anchor.
		applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
		if _, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot"); err != nil {
			t.Fatalf("pre-create chain slot: %v", err)
		}
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 1. Interrupted full: kept_t completes, then the first late_t
	// chunk upload "crashes". TableParallelism=1 pins sweep order.
	crashing := &failOnPathMatchPutStore{LocalStore: store, match: "late_t"}
	err = (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: crashing,
		SluiceVersion: "test", ChainSlot: chainSlot, TableParallelism: 1,
	}).Run(context.Background())
	if err == nil {
		t.Fatal("interrupted Run: expected injected crash; got nil")
	}

	inProgress, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest (in-progress): %v", err)
	}
	if inProgress.PartialState != irbackup.BackupStateInProgress {
		t.Fatalf("PartialState = %q; want in_progress", inProgress.PartialState)
	}
	anchorA1 := inProgress.EndPosition
	if anchorA1.Engine == "" && anchorA1.Token == "" {
		t.Fatal("in-progress manifest carries no anchor; the crashed run lost it (fix step 1 broken)")
	}
	if chainSlot && !pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot dropped by the interrupted run; the resume has no WAL-retention guarantee to adopt (commit-timing broken)")
	}

	// 2. Gap writes, in (A1, A2]: INSERT+UPDATE+DELETE across the value
	// families on the COMPLETED table (the pre-fix silent-loss class —
	// in neither the kept chunks nor an A2-opened window) AND on the
	// to-be-re-streamed table (exercises overlap-replay convergence).
	applyDDL(t, sourceDSN, anchorGapDML("kept_t", "k"))
	applyDDL(t, sourceDSN, anchorGapDML("late_t", "l"))

	// 3. Resume: the SAME command. Must adopt — no already-exists
	// refusal, EndPosition == A1, chain slot untouched.
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChainSlot: chainSlot, TableParallelism: 1,
	}).Run(context.Background()); err != nil {
		t.Fatalf("resume Run: %v (a --chain-slot resume must ADOPT the standing slot, not refuse on already-exists)", err)
	}
	final, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest (final): %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete {
		t.Fatalf("PartialState = %q; want complete", final.PartialState)
	}
	if final.EndPosition != anchorA1 {
		t.Fatalf("final EndPosition = %+v; want the ADOPTED first-attempt anchor %+v — recording the resume's fresh anchor is the silent chain gap",
			final.EndPosition, anchorA1)
	}
	if !pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot missing after the resumed run")
	}

	// 4. Post-resume writes too, so the incremental window carries
	// changes on both sides of the resume's snapshot.
	applyDDL(t, sourceDSN, `
		INSERT INTO kept_t (id, n, s) VALUES (200, 2000, 'post-k');
		UPDATE late_t SET n = n + 1 WHERE id = 3;
		DELETE FROM late_t WHERE id = 100;
	`)

	// 5. Incremental off the adopted anchor, then chain-restore.
	ctx, c := context.WithTimeout(context.Background(), 120*time.Second)
	defer c()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		Window: 15 * time.Second, MaxChanges: 15, ChunkChanges: 100,
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}
	if err := (&ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}

	// 6. Multiset-exact content equality: every gap-window and
	// post-resume write present exactly once, across every family.
	assertPGTablesMatch(t, sourceDSN, targetDSN, "kept_t", "late_t")
}

// TestBackup_ResumeAnchorAdoption_NoChainGap is the load-bearing
// end-to-end pin for the --chain-slot shape. Revert the adoption (or
// the early anchor stamp, or the commit timing) and this fails: either
// the resume refuses on already-exists, or kept_t's gap writes are
// silently missing from the restored target.
func TestBackup_ResumeAnchorAdoption_NoChainGap(t *testing.T) {
	runAnchoredResumeChainGapFlow(t, true)
}

// TestBackup_ResumeAnchorAdoption_OperatorSlotMirror pins the same flow
// on the non-chain-slot shape: the operator created publication + slot
// BEFORE the full (the documented manual chain contract). The resume
// adopts the anchor identically; the standing slot serves the chain.
func TestBackup_ResumeAnchorAdoption_OperatorSlotMirror(t *testing.T) {
	runAnchoredResumeChainGapFlow(t, false)
}

// TestBackup_ResumeAnchorAdoption_KeylessRestreamRefused pins the
// keyless guard against real PG: a truly keyless table (no PK, no
// non-null UNIQUE index) that must be re-streamed on an anchored
// resume is refused loudly — its re-streamed chunks would overlap the
// chain's replay window and the keyless applier fallback (plain
// INSERT, ADR-0010) would duplicate the overlap.
func TestBackup_ResumeAnchorAdoption_KeylessRestreamRefused(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, `
		CREATE TABLE kept_t (id BIGINT PRIMARY KEY, s TEXT);
		CREATE TABLE keyless_t (v BIGINT, s TEXT);
		INSERT INTO kept_t VALUES (1, 'a'), (2, 'b');
		INSERT INTO keyless_t VALUES (1, 'a'), (2, 'b');
	`)

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	crashing := &failOnPathMatchPutStore{LocalStore: store, match: "keyless_t"}
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: crashing,
		SluiceVersion: "test", TableParallelism: 1,
	}).Run(context.Background()); err == nil {
		t.Fatal("interrupted Run: expected injected crash; got nil")
	}

	err = (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", TableParallelism: 1,
	}).Run(context.Background())
	if err == nil {
		t.Fatal("resume Run succeeded with a keyless re-stream; want loud refusal (silent-duplicate hazard)")
	}
	for _, want := range []string{"keyless_t", "PRIMARY KEY", "--force-overwrite"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}

	// The named escape hatch works: --force-overwrite starts fresh.
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", TableParallelism: 1, ForceOverwrite: true,
	}).Run(context.Background()); err != nil {
		t.Fatalf("force-overwrite Run: %v", err)
	}
}
