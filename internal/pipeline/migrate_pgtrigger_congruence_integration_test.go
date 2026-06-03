//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #74 — postgres-trigger CONGRUENCE-vs-parent integration test
// (Phase 2 readiness gate).
//
// postgres-trigger Phase 1 (ADR-0066) shipped the trigger-based capture
// engine for managed PG tiers that lock down logical-replication slots
// (Heroku Essential / Render Basic / Supabase free, …). It has had one
// happy-path regression cycle. Before Phase 2 (cross-engine) work begins
// we need a DURABLE, CI-resident correctness signal that the trigger-
// based capture produces the SAME target state as the proven slot-based
// `postgres` engine for an identical workload. That is a CONGRUENCE
// (differential) test: run the same source workload through both engines
// into two separate targets and assert the targets are byte-identical.
//
// ---- Why a TWO-SOURCE differential (not one source, two engines) ----
//
// postgres-trigger Phase 1 only supports the SAME-engine shape
// (`postgres-trigger` → `postgres-trigger`); cross-engine targets are
// deferred to a follow-up phase (engine.go doc-comment, §"Cross-engine
// targets … are deferred"). The trigger engine also requires source-side
// setup (`pgtrigger.Setup` installs the `sluice_change_log` capture
// table + per-table triggers) that the slot-based engine must NOT see.
// So the two CDC paths cannot share one source DB. The harness therefore
// seeds TWO identical source DBs from the SAME DDL+seed string:
//
//   - LEG A (parent, slot-based): `postgres` → `postgres`, driven by the
//     full [Streamer] orchestrator (snapshot bulk-copy + pgoutput logical
//     replication CDC). The Streamer manages the publication + slot
//     itself.
//   - LEG B (trigger-based): `postgres-trigger` → `postgres-trigger`,
//     driven by the SAME manual path the Phase-1 e2e test uses
//     (migrate_pgtrigger_integration_test.go): pgtrigger.Setup →
//     Migrator bulk-copy → OpenCDCReader (trigger poll) → OpenChangeApplier.
//     This path must NOT go through the Streamer — the Streamer's
//     coldStart opens the snapshot stream via OpenSnapshotStream, which
//     the trigger engine DELEGATES to the composed postgres engine's
//     SLOT-based pgoutput path (engine.go OpenSnapshotStream doc). Going
//     through the Streamer would silently exercise the slot path, not
//     the trigger-based capture under test — defeating the differential.
//
// Both legs apply the IDENTICAL deterministic DML sequence (INSERT /
// same-column UPDATE / multi-column UPDATE / DELETE) so the only variable
// between targets is the capture mechanism. After quiescence the test
// asserts the two targets are byte-identical.
//
// ---- The Bug-74 pin discipline (value-type matrix) ----
//
// Per CLAUDE.md "pin the class, not the representative": the congruence
// checksum exercises EVERY CDC-fidelity-relevant value family, not one
// representative. The trigger capture log is JSONB built via to_jsonb()
// and decoded with UseNumber (ADR-0066 §3/§4), so NUMERIC + JSONB round-
// trips through the trigger path are a real precision-loss risk worth
// pinning. The seed schema spans: int4, int8, numeric(high-precision),
// text, varchar, boolean, timestamp, timestamptz, bytea, jsonb. The
// congruence assertion folds EVERY column into the ordered checksum +
// performs an explicit per-row column-text diff so a single-family
// silent divergence (the Bug-74 failure shape) fails LOUDLY with the
// offending column named.

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"

	// Side-effect import registers the postgres engine (looked up by
	// name via engines.Get); pgtrigger self-registers through the named
	// import above.
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// congruenceTable is the single table both legs migrate. ~8-12 columns
// spanning the value families that matter for CDC fidelity. REPLICA
// IDENTITY FULL is set on the slot-based source so the pgoutput OLD-tuple
// carries every column (the parent engine's standard same-engine shape);
// the trigger engine captures the full OLD/NEW via to_jsonb regardless.
const congruenceTable = "ledger"

// congruenceSeedDDL is applied IDENTICALLY to both source DBs. Keeping it
// in one const guarantees the two legs start from byte-identical source
// state — the differential's whole premise. 20 seed rows; the value
// families are deliberately mixed across rows (NULLs in the nullable
// columns on some rows, high-precision numerics, multi-byte text, a
// non-trivial JSONB document, raw bytea).
const congruenceSeedDDL = `
	CREATE TABLE ` + congruenceTable + ` (
		id           BIGINT PRIMARY KEY,
		seq          INTEGER NOT NULL,
		amount       NUMERIC(30,12) NOT NULL,
		label        TEXT,
		code         VARCHAR(32) NOT NULL,
		active       BOOLEAN NOT NULL,
		created_at   TIMESTAMP(6) NOT NULL,
		observed_at  TIMESTAMPTZ,
		blob         BYTEA,
		doc          JSONB
	);
	ALTER TABLE ` + congruenceTable + ` REPLICA IDENTITY FULL;
`

// congruenceSeedRows generates the 20-row INSERT applied identically to
// both sources. Spelled out as a generator so the per-row value families
// are visible and the NULL / precision / multi-byte / JSONB shapes are
// deterministic.
func congruenceSeedRows() string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + congruenceTable +
		" (id, seq, amount, label, code, active, created_at, observed_at, blob, doc) VALUES\n")
	for i := 1; i <= 20; i++ {
		if i > 1 {
			b.WriteString(",\n")
		}
		// High-precision numeric: 12 fractional digits exercised; the
		// trigger path's JSONB UseNumber decode must not truncate.
		amount := fmt.Sprintf("%d.%012d", 1000+i, int64(i)*123456789)
		// Multi-byte label on every 3rd row, NULL on every 5th.
		label := fmt.Sprintf("'row-%d-éü中'", i)
		if i%5 == 0 {
			label = "NULL"
		}
		// observed_at NULL on every 4th row (nullable timestamptz family).
		observed := fmt.Sprintf("'2026-05-%02d 12:34:56.123456+00'", (i%28)+1)
		if i%4 == 0 {
			observed = "NULL"
		}
		// bytea NULL on every 6th row.
		blob := fmt.Sprintf(`'\x%02x%02x%02x'`, i, i*2%256, i*3%256)
		if i%6 == 0 {
			blob = "NULL"
		}
		// JSONB document with a high-precision numeric leaf + nested
		// array + bool — the UseNumber-sensitive shape.
		doc := fmt.Sprintf(
			`'{"k": %d, "ratio": %d.%09d, "tags": ["a","b",%d], "ok": %t}'`,
			i, i, int64(i)*987654321, i*10, i%2 == 0,
		)
		fmt.Fprintf(
			&b,
			"(%d, %d, %s, %s, '%s', %t, '2026-01-%02d 00:00:00.%06d', %s, %s, %s)",
			i, i*7, amount, label, fmt.Sprintf("CODE-%04d", i), i%2 == 0,
			(i%28)+1, i*111%1_000_000, observed, blob, doc,
		)
	}
	b.WriteString(";")
	return b.String()
}

// congruenceCDCDML is the deterministic post-bulk-copy change sequence
// applied IDENTICALLY to both sources after each leg's CDC reader is
// streaming "from now". Covers every shape the task calls for:
//
//	INSERT (new id=21, id=22)
//	single-column UPDATE (id=3: bump seq only)
//	multi-column UPDATE  (id=7: amount + label + doc together)
//	DELETE (id=12, and id=22 which was just inserted — insert-then-delete)
//
// The expected stable target state after this sequence is 20 - 1 (id=12
// deleted) + 2 (21,22 inserted) - 1 (22 deleted) = 20 rows, with id=21
// present and id=12, id=22 absent.
const congruenceCDCDML = `
	INSERT INTO ` + congruenceTable + `
		(id, seq, amount, label, code, active, created_at, observed_at, blob, doc)
	VALUES
		(21, 147, 99999.999999999999, 'cdc-insert-é', 'CODE-0021', true,
		 '2026-02-02 02:02:02.020202', '2026-02-02 02:02:02.020202+00',
		 '\xdeadbeef', '{"k": 21, "ratio": 21.123456789, "tags": ["x"], "ok": true}'),
		(22, 154, 11111.111111111111, 'cdc-temp', 'CODE-0022', false,
		 '2026-03-03 03:03:03.030303', NULL, NULL,
		 '{"k": 22, "ratio": 0.000000001, "tags": [], "ok": false}');

	UPDATE ` + congruenceTable + ` SET seq = 30000 WHERE id = 3;

	UPDATE ` + congruenceTable + `
	   SET amount = 271828.182845904523,
	       label  = 'multi-col-update-中',
	       doc    = '{"k": 7, "ratio": 7.777777777, "tags": ["u","p","d"], "ok": true}'
	 WHERE id = 7;

	DELETE FROM ` + congruenceTable + ` WHERE id = 12;
	DELETE FROM ` + congruenceTable + ` WHERE id = 22;
`

// congruenceExpectedRows is the stable target row count after the seed +
// CDC sequence settles. Used as the quiescence gate on both legs.
const congruenceExpectedRows = 20

// TestMigratePGTrigger_CongruenceVsParent is the differential test. It
// proves the postgres-trigger engine's trigger-based capture lands the
// SAME target state as the slot-based parent `postgres` engine for an
// identical bulk-copy + CDC workload spanning the full value-type matrix.
func TestMigratePGTrigger_CongruenceVsParent(t *testing.T) {
	srcSlot, tgtSlot, srcTrig, tgtTrig, cleanup := startPGTrigCongruenceContainer(t)
	defer cleanup()

	seed := congruenceSeedRows()

	// Seed BOTH sources byte-identically from the same DDL + rows.
	for _, dsn := range []string{srcSlot, srcTrig} {
		pgTrigCongruenceExec(t, dsn, congruenceSeedDDL)
		pgTrigCongruenceExec(t, dsn, seed)
	}

	// ---- LEG A: parent slot-based postgres -> postgres via Streamer ----
	stopA := runCongruenceSlotLeg(t, srcSlot, tgtSlot)
	defer stopA()

	// ---- LEG B: trigger-based postgres-trigger -> postgres-trigger ----
	stopB := runCongruenceTriggerLeg(t, srcTrig, tgtTrig)
	defer stopB()

	// Quiesce with a CONTENT-AWARE drain predicate, not a bare row count.
	//
	// The stable post-CDC row count is ALSO 20 (seed 20 - del id=12 -
	// del id=22 + ins id=21 + ins id=22 = 20), identical to the seed
	// count. A `count == 20` gate is therefore ambiguous: it's satisfied
	// the instant the bulk copy lands the 20 seed rows — BEFORE any CDC
	// mutation applies — so the test could snapshot mid-drain and pass or
	// fail on timing. (This ambiguity is exactly how the Bug 92 silent
	// UPDATE loss could have slipped past a count-only gate: the dropped
	// UPDATEs don't change the row count.) Instead, poll until the target
	// reflects EVERY applied CDC mutation: the two deletes are gone, the
	// insert is present, the single-column UPDATE landed, and the
	// multi-column RICH UPDATE (id=7 amount — the Bug 92 shape) landed.
	if !waitForCongruenceDrained(tgtSlot, 90*time.Second) {
		t.Fatalf("slot-based target never reflected the full CDC sequence: %s",
			congruenceDrainDiag(tgtSlot))
	}
	if !waitForCongruenceDrained(tgtTrig, 90*time.Second) {
		t.Fatalf("trigger-based target never reflected the full CDC sequence: %s",
			congruenceDrainDiag(tgtTrig))
	}

	// ---- CONGRUENCE assertion ----
	assertPGTrigCongruent(t, tgtSlot, tgtTrig)
}

// waitForCongruenceDrained polls a target until it reflects the ENTIRE
// congruenceCDCDML sequence — not just a row count. The predicate is
// satisfied only when every CDC mutation has landed simultaneously:
//
//	id=12  absent  (DELETE)
//	id=22  absent  (INSERT then DELETE)
//	id=21  present (INSERT)
//	id=3   seq    = 30000               (single-column UPDATE)
//	id=7   amount = 271828.182845904523 (multi-column RICH UPDATE — the
//	                                     Bug 92 shape: a numeric WHERE-
//	                                     poisoning family)
//	row count = congruenceExpectedRows  (belt-and-suspenders)
//
// At seed time NONE of the per-id predicates hold; they can only all
// become true once the CDC stream has fully drained. That makes this a
// deterministic quiescence gate immune to the count==seed-count==20
// ambiguity (see the call site).
func waitForCongruenceDrained(dsn string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if congruenceFullyDrained(dsn) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// congruenceFullyDrained is the single-shot predicate behind
// waitForCongruenceDrained. Returns false (not an error) on any read
// failure so the poll loop keeps trying until the deadline.
func congruenceFullyDrained(dsn string) bool {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// One query folds every marker into a single boolean so the read is
	// atomic w.r.t. the target's MVCC snapshot — no chance of observing a
	// half-applied tx across multiple round-trips.
	var drained bool
	q := fmt.Sprintf(`
		SELECT
			NOT EXISTS (SELECT 1 FROM %[1]s WHERE id = 12)
			AND NOT EXISTS (SELECT 1 FROM %[1]s WHERE id = 22)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 21)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 3 AND seq = 30000)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 7 AND amount = 271828.182845904523)
			AND (SELECT count(*) FROM %[1]s) = %[2]d
	`, congruenceTable, congruenceExpectedRows)
	if err := db.QueryRowContext(ctx, q).Scan(&drained); err != nil {
		return false
	}
	return drained
}

// congruenceDrainDiag renders a compact human-readable snapshot of the
// drain markers for a failure message — so a never-drained target tells
// the operator WHICH mutation is missing rather than just "timed out".
func congruenceDrainDiag(dsn string) string {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Sprintf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		rows   int
		has12  bool
		has22  bool
		has21  bool
		seq3OK bool
		amt7OK bool
	)
	q := fmt.Sprintf(`
		SELECT
			(SELECT count(*) FROM %[1]s),
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 12),
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 22),
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 21),
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 3 AND seq = 30000),
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 7 AND amount = 271828.182845904523)
	`, congruenceTable)
	if err := db.QueryRowContext(ctx, q).Scan(&rows, &has12, &has22, &has21, &seq3OK, &amt7OK); err != nil {
		return fmt.Sprintf("diag query: %v", err)
	}
	return fmt.Sprintf(
		"rows=%d (want %d) id12_gone=%v id22_gone=%v id21_present=%v id3_seq_updated=%v id7_amount_updated=%v",
		rows, congruenceExpectedRows, !has12, !has22, has21, seq3OK, amt7OK,
	)
}

// runCongruenceSlotLeg drives the parent slot-based postgres -> postgres
// migration via the full Streamer (snapshot bulk-copy + pgoutput CDC),
// then applies the deterministic CDC DML on the source. Returns a stop
// closure the caller defers.
func runCongruenceSlotLeg(t *testing.T, srcDSN, tgtDSN string) func() {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  "pgtrig-congruence-slot",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for the snapshot bulk-copy to deliver all 20 seed rows before
	// driving CDC, so the post-snapshot DML is captured by the slot.
	if !waitForExactRowCount(tgtDSN, congruenceTable, 20, 90*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("slot-based bulk copy never delivered 20 seed rows; got %d",
			pollRowCount(tgtDSN, congruenceTable))
	}

	// Drive the identical CDC sequence on the slot source.
	pgTrigCongruenceExec(t, srcDSN, congruenceCDCDML)

	return func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(15 * time.Second):
			t.Error("slot-based Streamer.Run did not return after ctx cancel")
		}
	}
}

// runCongruenceTriggerLeg drives the trigger-based postgres-trigger ->
// postgres-trigger migration via the MANUAL path (Setup -> Migrator
// bulk-copy -> OpenCDCReader -> OpenChangeApplier), mirroring the Phase-1
// e2e test. It must not go through the Streamer (see file doc). Returns a
// stop closure the caller defers.
func runCongruenceTriggerLeg(t *testing.T, srcDSN, tgtDSN string) func() {
	t.Helper()
	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}

	ctx := context.Background()

	// Step 1: install the trigger-engine capture state on the source.
	if _, err := pgtrigger.Setup(ctx, srcDSN, pgtrigger.SetupOptions{
		Tables: []string{congruenceTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	// Step 2: bulk-copy via Migrator, excluding the sluice-managed
	// change-log tables (source-side capture state, not user data).
	mig := &Migrator{
		Source:    trigEng,
		Target:    trigEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}
	migCtx, migCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer migCancel()
	if err := mig.Run(migCtx); err != nil {
		t.Fatalf("trigger-leg Migrator.Run: %v", err)
	}
	if got := pollRowCount(tgtDSN, congruenceTable); got != 20 {
		t.Fatalf("trigger-based bulk copy delivered %d rows; want 20", got)
	}

	// Step 3: open the trigger-engine CDC reader (anchors "from now")
	// BEFORE driving the CDC DML so post-Setup changes are captured.
	reader, err := trigEng.OpenCDCReader(ctx, srcDSN)
	if err != nil {
		t.Fatalf("trigger OpenCDCReader: %v", err)
	}

	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("trigger StreamChanges: %v", err)
	}

	// Step 4: open the target-side change applier and tail the channel.
	applier, err := trigEng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("trigger OpenChangeApplier: %v", err)
	}
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("trigger EnsureControlTable: %v", err)
	}

	applyCtx, applyCancel := context.WithCancel(ctx)
	const streamID = "pgtrig-congruence-trigger"
	applyDone := make(chan error, 1)
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		applyDone <- applier.Apply(applyCtx, streamID, out)
	}()

	// Step 5: drive the IDENTICAL CDC sequence on the trigger source.
	pgTrigCongruenceExec(t, srcDSN, congruenceCDCDML)

	return func() {
		applyCancel()
		applyWG.Wait()
		select {
		case err := <-applyDone:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Errorf("trigger applier.Apply returned non-cancel error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("trigger applier did not exit after ctx cancel")
		}
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
}

// assertPGTrigCongruent is the differential oracle. It compares the two
// targets THREE ways so a silent single-family divergence (the Bug-74
// failure shape) cannot pass:
//
//  1. Ordered full-table MD5 over every column rendered to text
//     (md5(string_agg(row::text, '\n' ORDER BY id))). Folds EVERY value
//     family into one digest; a difference anywhere fails.
//  2. Per-column ordered MD5 for EACH column individually, so when (1)
//     fails the offending FAMILY is named (numeric vs jsonb vs bytea …)
//     rather than just "the tables differ".
//  3. Row-count equality (belt-and-suspenders; the quiescence gate
//     already pins both at congruenceExpectedRows).
func assertPGTrigCongruent(t *testing.T, slotDSN, trigDSN string) {
	t.Helper()

	// (3) row counts.
	nSlot := pollRowCount(slotDSN, congruenceTable)
	nTrig := pollRowCount(trigDSN, congruenceTable)
	if nSlot != nTrig {
		t.Fatalf("row-count divergence: slot-based=%d trigger-based=%d", nSlot, nTrig)
	}

	// (2) per-column digests — named family on mismatch. The order
	// mirrors congruenceSeedDDL so the value matrix is explicit here too.
	columns := []string{
		"id", "seq", "amount", "label", "code", "active",
		"created_at", "observed_at", "blob", "doc",
	}
	var mismatches []string
	for _, col := range columns {
		// COALESCE(col::text,'<NULL>') so NULLs are folded deterministically
		// and a NULL-vs-empty-string divergence is caught.
		q := fmt.Sprintf(
			"SELECT md5(COALESCE(string_agg(COALESCE(%s::text,'<NULL>'), E'\\n' ORDER BY id), '')) FROM %s",
			pgTrigQuoteIdent(col), congruenceTable,
		)
		slotDigest := pgTrigCongruenceScalar(t, slotDSN, q)
		trigDigest := pgTrigCongruenceScalar(t, trigDSN, q)
		if slotDigest != trigDigest {
			mismatches = append(mismatches, fmt.Sprintf(
				"column %q: slot-based md5=%s trigger-based md5=%s", col, slotDigest, trigDigest,
			))
		}
	}
	if len(mismatches) > 0 {
		t.Fatalf("CONGRUENCE FAILURE — trigger-based capture diverged from the "+
			"slot-based parent on %d column-family digest(s) (Bug-74-class silent "+
			"loss):\n  - %s", len(mismatches), strings.Join(mismatches, "\n  - "))
	}

	// (1) whole-row digest. Redundant if (2) passed, but it also catches
	// any column the per-column list above missed (defence against the
	// columns slice drifting out of sync with the schema).
	wholeRow := fmt.Sprintf(
		"SELECT md5(COALESCE(string_agg(t::text, E'\\n' ORDER BY t.id), '')) FROM %s t",
		congruenceTable,
	)
	slotWhole := pgTrigCongruenceScalar(t, slotDSN, wholeRow)
	trigWhole := pgTrigCongruenceScalar(t, trigDSN, wholeRow)
	if slotWhole != trigWhole {
		t.Fatalf("CONGRUENCE FAILURE — whole-row digest diverged "+
			"(slot-based=%s trigger-based=%s) despite per-column digests matching; "+
			"a column outside the per-column list differs", slotWhole, trigWhole)
	}

	t.Logf("CONGRUENT — trigger-based capture is byte-identical to the "+
		"slot-based parent across %d rows and %d value-family columns "+
		"(whole-row md5=%s)", nSlot, len(columns), slotWhole)
}

// startPGTrigCongruenceContainer boots ONE pre-baked wal_level=logical
// container and creates the four databases the two legs need (src_slot,
// tgt_slot, src_trig, tgt_trig). wal_level=logical is required for the
// slot-based leg's pgoutput CDC; it's a strict superset of what the
// trigger leg needs (plain replica), so both legs share the container.
//
// Mirrors startPostgresLogical's boot shape but creates four DBs instead
// of one source+target pair. Local helper (not a shared-fixture reuse)
// because the shared per-database reset fixture lives in the
// engines/postgres package and isn't importable here.
func startPGTrigCongruenceContainer(t *testing.T) (srcSlot, tgtSlot, srcTrig, tgtTrig string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		pgPrebakedImage,
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_wal_senders=8",
					"-c", "max_replication_slots=8",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	baseConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("pgx", baseConn)
	if err != nil {
		terminate()
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	dbNames := []string{"src_slot", "tgt_slot", "src_trig", "tgt_trig"}
	for _, name := range dbNames {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create database %q: %v", name, err)
		}
	}

	dsnFor := func(name string) string {
		dsn, derr := pgTrigSwapDB(baseConn, name)
		if derr != nil {
			terminate()
			t.Fatalf("build DSN for %q: %v", name, derr)
		}
		return dsn
	}

	return dsnFor("src_slot"), dsnFor("tgt_slot"),
		dsnFor("src_trig"), dsnFor("tgt_trig"), terminate
}

// pgTrigSwapDB replaces the database-name component of a Postgres URI
// DSN. Local helper so this file is self-contained on the build-tag
// isolation front (mirrors swapDSNDatabase / buildPGDSN; redeclared with
// a file-unique name to avoid collision in the package's test binary).
func pgTrigSwapDB(orig, newDB string) (string, error) {
	u, err := url.Parse(orig)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if u.Path == "" || u.Path == "/" {
		return "", fmt.Errorf("DSN has no db-name path: %q", orig)
	}
	u.Path = "/" + strings.TrimPrefix(newDB, "/")
	return u.String(), nil
}

// pgTrigCongruenceExec runs a (possibly multi-statement) DDL/DML block.
func pgTrigCongruenceExec(t *testing.T, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec: %v\nstmt: %s", err, pgTrigFirstLine(stmt))
	}
}

// pgTrigCongruenceScalar runs a query expected to return a single string
// scalar (the md5 digests). FAILs on error.
func pgTrigCongruenceScalar(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var s sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("scalar query: %v\nquery: %s", err, query)
	}
	return s.String
}

// pgTrigQuoteIdent quotes a PG identifier (file-unique name; the package
// already has quoteIdent variants in non-test code that aren't visible
// to this test file's build-tag scope without import churn).
func pgTrigQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// pgTrigFirstLine returns s up to the first newline for compact error
// messages.
func pgTrigFirstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
