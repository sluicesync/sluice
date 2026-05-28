//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #72 — postgres-trigger PHASE 2 cross-engine CONGRUENCE test
// (postgres-trigger -> mysql), with the full Bug-74 value-family pin.
//
// Phase 1 (v0.85.0) shipped the trigger-based capture engine SAME-engine
// only (postgres-trigger -> postgres-trigger). Phase 2 is the cross-engine
// step: postgres-trigger -> mysql / planetscale. This engine's entire
// reason to exist is TRUST on slot-less managed PG (Heroku Essential /
// Render Basic / Supabase free), so a silent cross-engine value drop is
// the cardinal sin. This test is the durable, CI-resident correctness
// signal that the trigger capture lands the SAME MySQL target state as the
// proven slot-based `postgres` engine for an identical cross-engine
// workload spanning every CDC-fidelity value family.
//
// ---- Why a TWO-SOURCE differential (not one source, two engines) ----
//
// Same rationale as the same-engine congruence test
// (migrate_pgtrigger_congruence_integration_test.go): the trigger engine
// requires source-side Setup (`pgtrigger.Setup` installs sluice_change_log
// + per-table triggers) that the slot-based engine must NOT see, so the
// two CDC paths cannot share one source DB. The harness seeds TWO
// identical PG sources from the SAME DDL+seed string and points each leg
// at its OWN MySQL target database:
//
//   - LEG A (reference, proven): `postgres` -> `mysql` cross-engine via
//     the full [Streamer] (snapshot bulk-copy + pgoutput logical-
//     replication CDC). This is the path v0.68.x validated; the
//     pgoutput tuple decode produces IR-canonical value shapes
//     (numeric->string, bytea->raw []byte, timestamp->time.Time,
//     jsonb->[]byte) via the postgres engine's value_decode.go.
//   - LEG B (under test): `postgres-trigger` -> `mysql` cross-engine via
//     the MANUAL path (pgtrigger.Setup -> Migrator bulk-copy excluding
//     the sluice change-log tables -> OpenCDCReader (trigger poll) ->
//     OpenChangeApplier on the MySQL target). It must NOT go through the
//     Streamer: the Streamer's coldStart opens OpenSnapshotStream, which
//     pgtrigger DELEGATES to the slot-based pgoutput path (engine.go
//     doc) — going through the Streamer would silently exercise the slot
//     path, defeating the differential.
//
// The trigger CDC reader decodes the JSONB capture log (cdc_reader.go,
// decodeJSONBRow) into a DIFFERENT value shape than pgoutput: integers ->
// int64, non-integer numerics -> json.Number, and everything else as the
// JSON-scalar string (bytea as PG's `\x`-hex TEXT, timestamps as ISO
// strings, jsonb as a nested map/array, bool as Go bool). THAT shape is
// what flows into the MySQL ChangeApplier's value-prepare path — a path
// the already-shipped pgoutput->MySQL leg never exercises. The difference
// is the Bug-74 risk this phase exists to pin.
//
// ---- The Bug-74 family matrix (pin the class, not the representative) ----
//
// Per CLAUDE.md "pin the class, not the representative": the seed schema +
// CDC DML exercise EVERY CDC-fidelity value family x shape:
//
//	int4                     seq          scalar + single-col UPDATE bump
//	int8                     id (PK)      scalar
//	numeric(30,12)           amount       scalar + rich UPDATE + unchanged
//	text                     label        scalar + NULL + rich UPDATE
//	varchar(32)              code         scalar + unchanged-rich UPDATE
//	boolean                  active       scalar + unchanged-rich UPDATE
//	timestamp                created_at   scalar + unchanged-rich UPDATE
//	timestamptz              observed_at  scalar + NULL + unchanged-rich
//	bytea (DANGER)           blob         scalar + NULL + unchanged-rich
//	jsonb                    doc          scalar + rich UPDATE
//
// The "unchanged-rich UPDATE" column is the Bug-92 shape: the multi-column
// UPDATE on id=7 re-binds amount/label/doc but leaves code/active/
// created_at/observed_at/blob unchanged. The MySQL applier emits a
// full-row SET (every column in After), so every unchanged column ALSO
// flows through the value-prepare path on that UPDATE — exercising the
// silent-drop shape for bytea / timestamptz / varchar / bool / timestamp.
//
// Cross-engine the families map: jsonb->JSON, numeric->DECIMAL,
// bytea->LONGBLOB, timestamptz->TIMESTAMP, timestamp->DATETIME,
// bool->TINYINT(1). The DANGER families for the trigger path are:
//   - bytea: the trigger emits `\x`-hex TEXT; it must land as the correct
//     raw bytes in MySQL, NOT the literal ASCII of the hex string (Bug 92
//     class).
//   - numeric/DECIMAL precision (json.Number round-trip).
//   - timestamptz timezone normalization (ISO string with offset).
//
// ---- The oracle ----
//
// Quiescence is a CONTENT-AWARE drain predicate on each MySQL target
// (mirrors waitForCongruenceDrained — every mutation must have landed:
// deletes gone, insert present, both UPDATEs reflected). The congruence
// assertion folds EVERY column into an ordered MySQL-side checksum AND a
// per-column digest, so a single-family divergence (the Bug-74 failure
// shape) fails LOUDLY with the offending column named.

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

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/engines/pgtrigger"
	"github.com/orware/sluice/internal/ir"

	// Side-effect imports register the engines looked up by name via
	// engines.Get; pgtrigger self-registers through the named import.
	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// crossCongruenceTable is the single table both legs migrate. REPLICA
// IDENTITY FULL on each PG source so the slot-based leg's pgoutput OLD-
// tuple carries every column (the parent engine's standard same-setting
// shape); the trigger engine captures full OLD/NEW via to_jsonb anyway.
const crossCongruenceTable = "ledger"

// crossCongruenceSeedDDL is applied IDENTICALLY to both PG sources. One
// const guarantees byte-identical source state — the differential's whole
// premise. The column set spans every CDC-fidelity value family the
// Bug-74 matrix requires.
const crossCongruenceSeedDDL = `
	CREATE TABLE ` + crossCongruenceTable + ` (
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
	ALTER TABLE ` + crossCongruenceTable + ` REPLICA IDENTITY FULL;
`

// crossCongruenceSeedRows generates the 20-row INSERT applied identically
// to both sources. Spelled out as a generator so the per-row value
// families are visible and the NULL / precision / multi-byte / JSONB
// shapes are deterministic. Mirrors the same-engine congruence seed.
func crossCongruenceSeedRows() string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + crossCongruenceTable +
		" (id, seq, amount, label, code, active, created_at, observed_at, blob, doc) VALUES\n")
	for i := 1; i <= 20; i++ {
		if i > 1 {
			b.WriteString(",\n")
		}
		// High-precision numeric: 12 fractional digits exercised; the
		// trigger path's JSONB UseNumber decode must not truncate, and
		// the json.Number must round-trip cleanly to MySQL DECIMAL(30,12).
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
		// JSONB document with a high-precision numeric leaf + nested array
		// + bool — the UseNumber-sensitive shape.
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

// crossCongruenceCDCDML is the deterministic post-bulk-copy change
// sequence applied IDENTICALLY to both sources after each leg's CDC reader
// is streaming "from now". Covers every shape:
//
//	INSERT (new id=21, id=22)
//	single-column UPDATE (id=3: bump seq only)
//	multi-column RICH UPDATE (id=7: amount + label + doc together; the
//	                          other 6 columns stay unchanged — the Bug-92
//	                          full-row-SET shape)
//	DELETE (id=12, and id=22 which was just inserted — insert-then-delete)
//
// Stable target row count after this sequence: 20 - 1 (id=12) + 2 (21,22)
// - 1 (id=22) = 20 rows, with id=21 present and id=12, id=22 absent.
const crossCongruenceCDCDML = `
	INSERT INTO ` + crossCongruenceTable + `
		(id, seq, amount, label, code, active, created_at, observed_at, blob, doc)
	VALUES
		(21, 147, 99999.999999999999, 'cdc-insert-é', 'CODE-0021', true,
		 '2026-02-02 02:02:02.020202', '2026-02-02 02:02:02.020202+00',
		 '\xdeadbeef', '{"k": 21, "ratio": 21.123456789, "tags": ["x"], "ok": true}'),
		(22, 154, 11111.111111111111, 'cdc-temp', 'CODE-0022', false,
		 '2026-03-03 03:03:03.030303', NULL, NULL,
		 '{"k": 22, "ratio": 0.000000001, "tags": [], "ok": false}');

	UPDATE ` + crossCongruenceTable + ` SET seq = 30000 WHERE id = 3;

	UPDATE ` + crossCongruenceTable + `
	   SET amount = 271828.182845904523,
	       label  = 'multi-col-update-中',
	       doc    = '{"k": 7, "ratio": 7.777777777, "tags": ["u","p","d"], "ok": true}'
	 WHERE id = 7;

	DELETE FROM ` + crossCongruenceTable + ` WHERE id = 12;
	DELETE FROM ` + crossCongruenceTable + ` WHERE id = 22;
`

// crossCongruenceExpectedRows is the stable target row count after the
// seed + CDC sequence settles. Belt-and-suspenders inside the content-
// aware drain predicate.
const crossCongruenceExpectedRows = 20

// TestMigratePGTrigger_CrossCongruenceVsParent is the cross-engine
// differential. It proves the postgres-trigger engine's trigger-based
// capture lands the SAME MySQL target state as the slot-based parent
// `postgres` engine for an identical cross-engine bulk-copy + CDC
// workload spanning the full Bug-74 value-type matrix.
func TestMigratePGTrigger_CrossCongruenceVsParent(t *testing.T) {
	srcSlot, srcTrig, pgCleanup := startPGCrossCongruencePG(t)
	defer pgCleanup()
	tgtSlot, tgtTrig, myCleanup := startPGCrossCongruenceMySQL(t)
	defer myCleanup()

	seed := crossCongruenceSeedRows()

	// Seed BOTH PG sources byte-identically from the same DDL + rows.
	for _, dsn := range []string{srcSlot, srcTrig} {
		pgCrossCongruenceExecPG(t, dsn, crossCongruenceSeedDDL)
		pgCrossCongruenceExecPG(t, dsn, seed)
	}

	// ---- LEG A: parent slot-based postgres -> mysql via Streamer ----
	stopA := runCrossCongruenceSlotLeg(t, srcSlot, tgtSlot)
	defer stopA()

	// ---- LEG B: trigger-based postgres-trigger -> mysql (manual path) --
	stopB := runCrossCongruenceTriggerLeg(t, srcTrig, tgtTrig)
	defer stopB()

	// Quiesce with a CONTENT-AWARE drain predicate, not a bare row count.
	// The stable post-CDC count is ALSO 20 (identical to the seed count),
	// so a count==20 gate is satisfied the instant the bulk copy lands —
	// BEFORE any CDC mutation applies. (That ambiguity is exactly how a
	// silent UPDATE loss could slip past a count-only gate; the dropped
	// UPDATE doesn't change the row count.) Poll until every CDC mutation
	// has landed: deletes gone, insert present, single-col UPDATE landed,
	// multi-col RICH UPDATE landed.
	if !waitForCrossCongruenceDrained(tgtSlot, 120*time.Second) {
		t.Fatalf("slot-based MySQL target never reflected the full CDC sequence: %s",
			crossCongruenceDrainDiag(tgtSlot))
	}
	if !waitForCrossCongruenceDrained(tgtTrig, 120*time.Second) {
		t.Fatalf("trigger-based MySQL target never reflected the full CDC sequence: %s",
			crossCongruenceDrainDiag(tgtTrig))
	}

	// ---- CONGRUENCE assertion ----
	assertPGTrigCrossCongruent(t, tgtSlot, tgtTrig)
}

// waitForCrossCongruenceDrained polls a MySQL target until it reflects the
// ENTIRE crossCongruenceCDCDML sequence — not just a row count. Satisfied
// only when every CDC mutation has landed simultaneously (see the same-
// engine congruence test for the count==seed-count ambiguity rationale).
func waitForCrossCongruenceDrained(dsn string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if crossCongruenceFullyDrained(dsn) {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

// crossCongruenceFullyDrained is the single-shot predicate behind
// waitForCrossCongruenceDrained. Returns false (not an error) on any read
// failure so the poll loop keeps trying until the deadline. One query
// folds every marker into a single boolean so the read is atomic w.r.t.
// the target's snapshot.
func crossCongruenceFullyDrained(dsn string) bool {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var drained bool
	q := fmt.Sprintf(`
		SELECT
			NOT EXISTS (SELECT 1 FROM %[1]s WHERE id = 12)
			AND NOT EXISTS (SELECT 1 FROM %[1]s WHERE id = 22)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 21)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 3 AND seq = 30000)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 7 AND amount = 271828.182845904523)
			AND (SELECT count(*) FROM %[1]s) = %[2]d
	`, crossCongruenceQuoteIdent(crossCongruenceTable), crossCongruenceExpectedRows)
	if err := db.QueryRowContext(ctx, q).Scan(&drained); err != nil {
		return false
	}
	return drained
}

// crossCongruenceDrainDiag renders a compact snapshot of the drain markers
// for a failure message — so a never-drained target names WHICH mutation
// is missing rather than just "timed out".
func crossCongruenceDrainDiag(dsn string) string {
	db, err := sql.Open("mysql", dsn)
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
	`, crossCongruenceQuoteIdent(crossCongruenceTable))
	if err := db.QueryRowContext(ctx, q).Scan(&rows, &has12, &has22, &has21, &seq3OK, &amt7OK); err != nil {
		return fmt.Sprintf("diag query: %v", err)
	}
	return fmt.Sprintf(
		"rows=%d (want %d) id12_gone=%v id22_gone=%v id21_present=%v id3_seq_updated=%v id7_amount_updated=%v",
		rows, crossCongruenceExpectedRows, !has12, !has22, has21, seq3OK, amt7OK,
	)
}

// runCrossCongruenceSlotLeg drives the parent slot-based postgres -> mysql
// migration via the full Streamer (snapshot bulk-copy + pgoutput CDC),
// then applies the deterministic CDC DML on the PG source. Returns a stop
// closure the caller defers.
func runCrossCongruenceSlotLeg(t *testing.T, srcDSN, tgtDSN string) func() {
	t.Helper()
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	streamer := &Streamer{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		StreamID:  "pgtrig-cross-congruence-slot",
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Wait for the snapshot bulk-copy to deliver all 20 seed rows before
	// driving CDC, so the post-snapshot DML is captured by the slot.
	if !waitForExactRowCountMySQL(tgtDSN, crossCongruenceTable, 20, 120*time.Second) {
		streamCancel()
		<-runErr
		t.Fatalf("slot-based bulk copy never delivered 20 seed rows to MySQL; got %d",
			pollRowCountMySQL(tgtDSN, crossCongruenceTable))
	}

	// Drive the identical CDC sequence on the slot source.
	pgCrossCongruenceExecPG(t, srcDSN, crossCongruenceCDCDML)

	return func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("slot-based Streamer.Run did not return after ctx cancel")
		}
	}
}

// runCrossCongruenceTriggerLeg drives the trigger-based postgres-trigger
// -> mysql migration via the MANUAL path (Setup -> Migrator bulk-copy ->
// OpenCDCReader -> OpenChangeApplier), mirroring the Phase-1 e2e test. It
// must NOT go through the Streamer (see file doc — the Streamer's coldStart
// delegates to the slot-based snapshot path). Returns a stop closure.
func runCrossCongruenceTriggerLeg(t *testing.T, srcDSN, tgtDSN string) func() {
	t.Helper()
	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	ctx := context.Background()

	// Step 1: install the trigger-engine capture state on the PG source.
	if _, err := pgtrigger.Setup(ctx, srcDSN, pgtrigger.SetupOptions{
		Tables: []string{crossCongruenceTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	// Step 2: cross-engine bulk-copy via Migrator (postgres-trigger source,
	// mysql target), excluding the sluice-managed change-log tables.
	mig := &Migrator{
		Source:    trigEng,
		Target:    mysqlEng,
		SourceDSN: srcDSN,
		TargetDSN: tgtDSN,
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}
	migCtx, migCancel := context.WithTimeout(ctx, 3*time.Minute)
	defer migCancel()
	if err := mig.Run(migCtx); err != nil {
		t.Fatalf("trigger-leg cross-engine Migrator.Run: %v", err)
	}
	if got := pollRowCountMySQL(tgtDSN, crossCongruenceTable); got != 20 {
		t.Fatalf("trigger-based cross-engine bulk copy delivered %d rows; want 20", got)
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

	// Step 4: open the MySQL target-side change applier and tail the
	// channel. The applier is the cross-engine surface under test — it
	// receives the trigger reader's JSON-scalar value shapes.
	applier, err := mysqlEng.OpenChangeApplier(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("trigger OpenChangeApplier (mysql target): %v", err)
	}
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("trigger EnsureControlTable: %v", err)
	}

	applyCtx, applyCancel := context.WithCancel(ctx)
	const streamID = "pgtrig-cross-congruence-trigger"
	applyDone := make(chan error, 1)
	var applyWG sync.WaitGroup
	applyWG.Add(1)
	go func() {
		defer applyWG.Done()
		applyDone <- applier.Apply(applyCtx, streamID, out)
	}()

	// Step 5: drive the IDENTICAL CDC sequence on the trigger source.
	pgCrossCongruenceExecPG(t, srcDSN, crossCongruenceCDCDML)

	return func() {
		applyCancel()
		applyWG.Wait()
		select {
		case err := <-applyDone:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Errorf("trigger applier.Apply returned non-cancel error: %v", err)
			}
		case <-time.After(15 * time.Second):
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

// assertPGTrigCrossCongruent is the cross-engine differential oracle. It
// compares the two MySQL targets THREE ways so a silent single-family
// divergence (the Bug-74 failure shape) cannot pass:
//
//  1. Per-column ordered MD5 for EACH column individually, so a mismatch
//     names the offending FAMILY (numeric vs jsonb vs bytea …). The bytea
//     digest folds HEX(blob) so a `\x`-hex-string-as-ASCII corruption
//     (the Bug 92 class) diverges loudly from the raw-bytes parent.
//  2. Whole-row ordered MD5 over every column rendered to text, catching
//     any column the per-column list missed (drift guard).
//  3. Row-count equality (belt-and-suspenders; the quiescence gate already
//     pins both at crossCongruenceExpectedRows).
//
// MySQL's GROUP_CONCAT is bounded by group_concat_max_len (default 1024),
// which would silently TRUNCATE the digest input and mask a divergence
// past the cap. We raise it to 1 GiB per session before every digest read.
func assertPGTrigCrossCongruent(t *testing.T, slotDSN, trigDSN string) {
	t.Helper()

	// (3) row counts.
	nSlot := pollRowCountMySQL(slotDSN, crossCongruenceTable)
	nTrig := pollRowCountMySQL(trigDSN, crossCongruenceTable)
	if nSlot != nTrig {
		t.Fatalf("row-count divergence: slot-based=%d trigger-based=%d", nSlot, nTrig)
	}

	// (1) per-column digests — named family on mismatch. The order mirrors
	// crossCongruenceSeedDDL so the value matrix is explicit here too.
	// `blob` is rendered via HEX() so the digest reflects the raw bytes,
	// not their (possibly corrupt) textual form — this is the load-bearing
	// bytea pin.
	columns := []struct{ name, expr string }{
		{"id", "CAST(id AS CHAR)"},
		{"seq", "CAST(seq AS CHAR)"},
		{"amount", "CAST(amount AS CHAR)"},
		{"label", "label"},
		{"code", "code"},
		{"active", "CAST(active AS CHAR)"},
		{"created_at", "CAST(created_at AS CHAR)"},
		{"observed_at", "CAST(observed_at AS CHAR)"},
		{"blob", "HEX(`blob`)"},
		{"doc", "CAST(doc AS CHAR)"},
	}
	var mismatches []string
	for _, col := range columns {
		// COALESCE(expr,'<NULL>') folds NULLs deterministically so a
		// NULL-vs-empty-string divergence is caught. GROUP_CONCAT ...
		// ORDER BY id gives a stable per-column digest.
		q := fmt.Sprintf(
			"SELECT MD5(COALESCE(GROUP_CONCAT(COALESCE(%s,'<NULL>') ORDER BY id SEPARATOR '\\n'),'')) FROM %s",
			col.expr, crossCongruenceQuoteIdent(crossCongruenceTable),
		)
		slotDigest := pgCrossCongruenceScalarMySQL(t, slotDSN, q)
		trigDigest := pgCrossCongruenceScalarMySQL(t, trigDSN, q)
		if slotDigest != trigDigest {
			mismatches = append(mismatches, fmt.Sprintf(
				"column %q: slot-based md5=%s trigger-based md5=%s", col.name, slotDigest, trigDigest,
			))
		}
	}
	if len(mismatches) > 0 {
		t.Fatalf("CROSS-ENGINE CONGRUENCE FAILURE — trigger-based capture "+
			"diverged from the slot-based parent on %d column-family digest(s) "+
			"(Bug-74-class silent loss):\n  - %s",
			len(mismatches), strings.Join(mismatches, "\n  - "))
	}

	// (2) whole-row digest. Redundant if (2) passed, but it also catches
	// any column the per-column list above missed (defence against the
	// columns slice drifting out of sync with the schema). CONCAT_WS folds
	// every column into one row string; HEX(blob) keeps the byte rendering.
	wholeRow := fmt.Sprintf(
		"SELECT MD5(COALESCE(GROUP_CONCAT("+
			"CONCAT_WS('|', "+
			"COALESCE(CAST(id AS CHAR),'<N>'), COALESCE(CAST(seq AS CHAR),'<N>'), "+
			"COALESCE(CAST(amount AS CHAR),'<N>'), COALESCE(label,'<N>'), "+
			"COALESCE(code,'<N>'), COALESCE(CAST(active AS CHAR),'<N>'), "+
			"COALESCE(CAST(created_at AS CHAR),'<N>'), COALESCE(CAST(observed_at AS CHAR),'<N>'), "+
			"COALESCE(HEX(`blob`),'<N>'), COALESCE(CAST(doc AS CHAR),'<N>')"+
			") ORDER BY id SEPARATOR '\\n'),'')) FROM %s",
		crossCongruenceQuoteIdent(crossCongruenceTable),
	)
	slotWhole := pgCrossCongruenceScalarMySQL(t, slotDSN, wholeRow)
	trigWhole := pgCrossCongruenceScalarMySQL(t, trigDSN, wholeRow)
	if slotWhole != trigWhole {
		t.Fatalf("CROSS-ENGINE CONGRUENCE FAILURE — whole-row digest diverged "+
			"(slot-based=%s trigger-based=%s) despite per-column digests matching; "+
			"a column outside the per-column list differs", slotWhole, trigWhole)
	}

	t.Logf("CROSS-ENGINE CONGRUENT — trigger-based capture is byte-identical "+
		"to the slot-based parent across %d rows and %d value-family columns "+
		"(whole-row md5=%s)", nSlot, len(columns), slotWhole)
}

// startPGCrossCongruencePG boots ONE wal_level=logical PG container and
// creates the two source databases (src_slot, src_trig). wal_level=logical
// is required for the slot-based leg's pgoutput CDC; it's a strict superset
// of what the trigger leg needs (plain replica), so both legs share the
// container.
func startPGCrossCongruencePG(t *testing.T) (srcSlot, srcTrig string, cleanup func()) {
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
		t.Fatalf("start PG container: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	baseConn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		terminate()
		t.Fatalf("PG connection string: %v", err)
	}

	db, err := sql.Open("pgx", baseConn)
	if err != nil {
		terminate()
		t.Fatalf("open PG: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, name := range []string{"src_slot", "src_trig"} {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create database %q: %v", name, err)
		}
	}

	dsnFor := func(name string) string {
		dsn, derr := pgCrossCongruenceSwapPGDB(baseConn, name)
		if derr != nil {
			terminate()
			t.Fatalf("build PG DSN for %q: %v", name, derr)
		}
		return dsn
	}
	return dsnFor("src_slot"), dsnFor("src_trig"), terminate
}

// startPGCrossCongruenceMySQL boots ONE MySQL container and creates the two
// target databases (tgt_slot, tgt_trig). Mirrors startMySQL's boot shape
// (runMySQLWithRetry) but creates two targets instead of one source+target
// pair.
func startPGCrossCongruenceMySQL(t *testing.T) (tgtSlot, tgtTrig string, cleanup func()) {
	t.Helper()

	container := runMySQLWithRetry(
		t,
		mysqltc.WithDatabase("source_db"),
		mysqltc.WithUsername("root"),
		mysqltc.WithPassword("rootpw"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	srcConn, err := container.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		terminate()
		t.Fatalf("MySQL connection string: %v", err)
	}

	db, err := sql.Open("mysql", srcConn+"&multiStatements=true")
	if err != nil {
		terminate()
		t.Fatalf("open MySQL: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, name := range []string{"tgt_slot", "tgt_trig"} {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create MySQL database %q: %v", name, err)
		}
	}

	dsnFor := func(name string) string {
		dsn, derr := buildMySQLDSN(srcConn, name)
		if derr != nil {
			terminate()
			t.Fatalf("build MySQL DSN for %q: %v", name, derr)
		}
		return dsn
	}
	return dsnFor("tgt_slot"), dsnFor("tgt_trig"), terminate
}

// pgCrossCongruenceSwapPGDB replaces the database-name component of a
// Postgres URI DSN. File-unique name for build-tag isolation.
func pgCrossCongruenceSwapPGDB(orig, newDB string) (string, error) {
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

// pgCrossCongruenceExecPG runs a (possibly multi-statement) DDL/DML block
// against a Postgres DSN.
func pgCrossCongruenceExecPG(t *testing.T, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PG: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("PG exec: %v\nstmt: %s", err, pgCrossCongruenceFirstLine(stmt))
	}
}

// pgCrossCongruenceScalarMySQL runs a single-scalar query against MySQL.
// It raises group_concat_max_len to 1 GiB on the same session first so the
// digest GROUP_CONCAT can't silently truncate (default 1024 bytes would
// mask a divergence past the cap). FAILs on error.
func pgCrossCongruenceScalarMySQL(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open MySQL: %v", err)
	}
	defer func() { _ = db.Close() }()
	// Pin the pool to a single connection so the SET SESSION and the
	// digest query run on the SAME session (a pooled query could otherwise
	// land on a connection without the raised cap).
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "SET SESSION group_concat_max_len = 1073741824"); err != nil {
		t.Fatalf("raise group_concat_max_len: %v", err)
	}
	var s sql.NullString
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("MySQL scalar query: %v\nquery: %s", err, query)
	}
	return s.String
}

// crossCongruenceQuoteIdent backtick-quotes a MySQL identifier.
func crossCongruenceQuoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// pgCrossCongruenceFirstLine returns s up to the first newline for compact
// error messages.
func pgCrossCongruenceFirstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
