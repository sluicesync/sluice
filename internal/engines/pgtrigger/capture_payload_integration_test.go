//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0068 — capture-payload mode end-to-end integration test.
//
// Proves the "trigger-only change; reader/applier are payload-shape-
// agnostic" claim empirically: for EACH of the three capture-payload
// modes (full / changed / minimal) the test installs the source-side
// trigger via `trigger setup --capture-payload=<mode>`, drives a
// deterministic INSERT/UPDATE/DELETE workload, replays the captured
// change-log through the (unmodified) CDC reader + change applier into a
// target DB, and asserts the target is BYTE-IDENTICAL to the post-
// workload source.
//
// The apply path is the manual reader→applier path the Phase-1/congruence
// tests use (Setup → OpenCDCReader → OpenChangeApplier), NOT the Streamer
// — going through the Streamer would exercise the bulk-copy snapshot
// anchor, whereas here the target is built entirely from the change-log
// replay so a partial `after` (changed/minimal) or partial `before`
// (minimal) is the ONLY thing constructing the target row. That is the
// strongest form of the payload-agnostic proof: if the applier needed a
// full image it would fail to produce a byte-identical target.
//
// Shape variants exercised (the class for the changed-set computation,
// per CLAUDE.md "pin the class"):
//   - narrow update on a wide table (one non-PK column changed)
//   - all-columns-changed update
//   - zero-non-PK-columns-changed update (SET a = a — the PK-only-after
//     no-op case; must still apply harmlessly)
//   - NULL ↔ value transition (both directions)
//   - DELETE
//
// For the `changed` mode the workload runs against the rich-type matrix
// (numeric / jsonb / bytea / timestamptz / text[] / boolean) and asserts
// exact round-trip — this confirms the changed-set diff + JSONB round-
// trip don't corrupt or drop typed values (ADR-0066 §4 / Bug-74 rule).

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	"github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// startPGTrigSrcTgtPair boots ONE PG container and creates a source +
// target DB pair, returning both DSNs and a cleanup. The trigger engine
// needs no special wal_level (the whole point), so the upstream image is
// used directly.
func startPGTrigSrcTgtPair(t *testing.T) (src, tgt string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	container, err := pgtc.Run(
		ctx,
		"postgres:16",
		pgtc.WithDatabase("source_db"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
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

	for _, name := range []string{"src_db", "tgt_db"} {
		if _, err := db.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
			terminate()
			t.Fatalf("create database %q: %v", name, err)
		}
	}
	dsnFor := func(name string) string {
		u, perr := url.Parse(baseConn)
		if perr != nil {
			terminate()
			t.Fatalf("parse base DSN: %v", perr)
		}
		u.Path = "/" + name
		return u.String()
	}
	return dsnFor("src_db"), dsnFor("tgt_db"), terminate
}

// payloadWideTable is a wide table whose value families exercise the
// changed-set diff + JSONB round-trip (ADR-0066 §4). The PK is `id`.
const payloadWideTable = "wide"

const payloadWideSeedDDL = `
	CREATE TABLE ` + payloadWideTable + ` (
		id           BIGINT PRIMARY KEY,
		seq          INTEGER NOT NULL,
		amount       NUMERIC(30,12),
		label        TEXT,
		code         VARCHAR(32) NOT NULL,
		active       BOOLEAN NOT NULL,
		observed_at  TIMESTAMPTZ,
		blob         BYTEA,
		tags         TEXT[],
		doc          JSONB
	);
`

// payloadWideSeedRows seeds 12 deterministic rows spanning the value
// families (NULLs on some rows, high-precision numerics, multi-byte
// text, arrays, JSONB).
func payloadWideSeedRows() string {
	var b strings.Builder
	b.WriteString("INSERT INTO " + payloadWideTable +
		" (id, seq, amount, label, code, active, observed_at, blob, tags, doc) VALUES\n")
	for i := 1; i <= 12; i++ {
		if i > 1 {
			b.WriteString(",\n")
		}
		amount := fmt.Sprintf("%d.%012d", 1000+i, int64(i)*123456789)
		label := fmt.Sprintf("'row-%d-éü中'", i)
		if i%5 == 0 {
			label = "NULL"
		}
		observed := fmt.Sprintf("'2026-05-%02d 12:34:56.123456+00'", (i%28)+1)
		if i%4 == 0 {
			observed = "NULL"
		}
		blob := fmt.Sprintf(`'\x%02x%02x%02x'`, i, i*2%256, i*3%256)
		if i%6 == 0 {
			blob = "NULL"
		}
		tags := fmt.Sprintf(`'{"t%d","t%d",null}'`, i, i*2)
		if i%3 == 0 {
			tags = "NULL"
		}
		doc := fmt.Sprintf(
			`'{"k": %d, "ratio": %d.%09d, "tags": ["a","b",%d], "ok": %t}'`,
			i, i, int64(i)*987654321, i*10, i%2 == 0,
		)
		fmt.Fprintf(
			&b,
			"(%d, %d, %s, %s, '%s', %t, %s, %s, %s, %s)",
			i, i*7, amount, label, fmt.Sprintf("CODE-%04d", i), i%2 == 0,
			observed, blob, tags, doc,
		)
	}
	b.WriteString(";")
	return b.String()
}

// payloadWideCDCDML is the deterministic post-setup change sequence,
// applied IDENTICALLY for each mode. It exercises every shape in the
// changed-set computation class:
//
//	INSERT (id=21)
//	narrow update     (id=3:  one non-PK column — seq)
//	all-columns update(id=7:  every non-PK column at once)
//	zero-column update(id=8:  SET seq = seq — PK-only after; harmless no-op)
//	NULL → value      (id=5:  label was NULL, set to a value)
//	value → NULL      (id=2:  observed_at had a value, set to NULL)
//	rich multi-col    (id=9:  numeric + jsonb + bytea + array together)
//	DELETE (id=12)
const payloadWideCDCDML = `
	INSERT INTO ` + payloadWideTable + `
		(id, seq, amount, label, code, active, observed_at, blob, tags, doc)
	VALUES
		(21, 147, 99999.999999999999, 'cdc-insert-é', 'CODE-0021', true,
		 '2026-02-02 02:02:02.020202+00', '\xdeadbeef', '{"x","y"}',
		 '{"k": 21, "ratio": 21.123456789, "tags": ["x"], "ok": true}');

	UPDATE ` + payloadWideTable + ` SET seq = 30000 WHERE id = 3;

	UPDATE ` + payloadWideTable + `
	   SET seq = 31000, amount = 271828.182845904523, label = 'all-cols-中',
	       code = 'CODE-7777', active = false,
	       observed_at = '2026-07-07 07:07:07.070707+00', blob = '\xc0ffee',
	       tags = '{"p","q","r"}',
	       doc = '{"k": 7, "ratio": 7.777777777, "tags": ["u","p","d"], "ok": true}'
	 WHERE id = 7;

	UPDATE ` + payloadWideTable + ` SET seq = seq WHERE id = 8;

	UPDATE ` + payloadWideTable + ` SET label = 'was-null-now-set' WHERE id = 5;

	UPDATE ` + payloadWideTable + ` SET observed_at = NULL WHERE id = 2;

	UPDATE ` + payloadWideTable + `
	   SET amount = 161803.398874989484,
	       doc = '{"k": 9, "ratio": 9.999999999, "nested": {"deep": [1,2,3]}, "ok": false}',
	       blob = '\xbadc0de5',
	       tags = '{"a",null,"c"}'
	 WHERE id = 9;

	DELETE FROM ` + payloadWideTable + ` WHERE id = 12;
`

// TestCapturePayload_EndToEnd_AllModes is the ADR-0068 per-mode
// byte-identical-apply pin. For each mode it builds a fresh source +
// target DB pair, installs the trigger in that mode, runs the identical
// workload, replays the change-log into the target via the unmodified
// reader+applier, and asserts the target equals the source byte-for-byte
// across every column (the rich-type matrix is folded into the digest).
func TestCapturePayload_EndToEnd_AllModes(t *testing.T) {
	for _, mode := range []CapturePayload{CapturePayloadFull, CapturePayloadChanged, CapturePayloadMinimal} {
		mode := mode
		t.Run(string(mode), func(t *testing.T) {
			src, tgt, cleanup := startPGTrigSrcTgtPair(t)
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			// Create the schema on BOTH source and target (DDL only).
			// The target's rows arrive PURELY via the change-log replay
			// (no bulk copy), so a partial `after` (changed/minimal) or
			// partial `before` (minimal) is the only thing constructing
			// each target row — the strongest payload-agnostic proof.
			payloadExec(t, src, payloadWideSeedDDL)
			payloadExec(t, tgt, payloadWideSeedDDL)

			// Install the trigger in this mode BEFORE seeding the rows,
			// so the seed INSERTs are themselves captured as change-log
			// INSERT events (full new-row image in every mode) and the
			// target is built entirely from the stream.
			if _, err := Setup(ctx, src, SetupOptions{
				Tables:         []string{payloadWideTable},
				Schema:         "public",
				CapturePayload: mode,
			}); err != nil {
				t.Fatalf("Setup(%s): %v", mode, err)
			}

			// Open the reader BEFORE any DML so the stream anchors at the
			// current MAX(id) (the empty change-log) and captures every
			// subsequent INSERT/UPDATE/DELETE.
			e := Engine{}
			reader, err := e.OpenCDCReader(ctx, src)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() {
				if c, ok := reader.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			}()
			out, err := reader.StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}

			// Open the applier and tail the channel.
			applier, err := e.OpenChangeApplier(ctx, tgt)
			if err != nil {
				t.Fatalf("OpenChangeApplier: %v", err)
			}
			if err := applier.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}
			applyCtx, applyCancel := context.WithCancel(ctx)
			const streamID = "capture-payload-e2e"
			applyDone := make(chan error, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				applyDone <- applier.Apply(applyCtx, streamID, out)
			}()

			// Seed the rows (captured as INSERT events), then drive the
			// identical UPDATE/DELETE workload — all after the reader
			// anchored, so every change flows through the change-log.
			payloadExec(t, src, payloadWideSeedRows())
			payloadExec(t, src, payloadWideCDCDML)

			// Wait until the target reflects the full workload (id=21
			// present, id=12 gone, the narrow + all-col + rich updates
			// applied), then assert byte-identity.
			if !waitForPayloadDrained(tgt, 90*time.Second) {
				applyCancel()
				wg.Wait()
				t.Fatalf("mode %s: target never drained: %s", mode, payloadDrainDiag(tgt))
			}

			assertPayloadByteIdentical(t, src, tgt, mode)

			applyCancel()
			wg.Wait()
			select {
			case err := <-applyDone:
				if err != nil && !strings.Contains(err.Error(), "context canceled") {
					t.Errorf("applier.Apply returned non-cancel error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("applier did not exit after ctx cancel")
			}
			if c, ok := applier.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		})
	}
}

// waitForPayloadDrained polls the target until it reflects EVERY CDC
// mutation (not just a row count): the insert is present, the delete is
// gone, the narrow / all-column / rich updates landed, and the NULL
// transitions applied in both directions.
func waitForPayloadDrained(dsn string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if payloadFullyDrained(dsn) {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func payloadFullyDrained(dsn string) bool {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var drained bool
	q := fmt.Sprintf(`
		SELECT
			EXISTS (SELECT 1 FROM %[1]s WHERE id = 21)
			AND NOT EXISTS (SELECT 1 FROM %[1]s WHERE id = 12)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 3 AND seq = 30000)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 7 AND amount = 271828.182845904523 AND code = 'CODE-7777')
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 9 AND amount = 161803.398874989484)
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 5 AND label = 'was-null-now-set')
			AND EXISTS (SELECT 1 FROM %[1]s WHERE id = 2 AND observed_at IS NULL)
	`, payloadWideTable)
	if err := db.QueryRowContext(ctx, q).Scan(&drained); err != nil {
		return false
	}
	return drained
}

func payloadDrainDiag(dsn string) string {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Sprintf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+payloadWideTable).Scan(&rows); err != nil {
		return fmt.Sprintf("diag: %v", err)
	}
	return fmt.Sprintf("rows=%d", rows)
}

// assertPayloadByteIdentical compares the source and target tables three
// ways (whole-row digest, per-column digest naming the offending family
// on mismatch, row count) so a Bug-74-class single-family silent
// divergence in the changed-set / JSONB-round-trip path fails loudly.
func assertPayloadByteIdentical(t *testing.T, src, tgt string, mode CapturePayload) {
	t.Helper()

	nSrc := payloadScalar(t, src, "SELECT count(*) FROM "+payloadWideTable)
	nTgt := payloadScalar(t, tgt, "SELECT count(*) FROM "+payloadWideTable)
	if nSrc != nTgt {
		t.Fatalf("mode %s: row-count divergence: src=%s tgt=%s", mode, nSrc, nTgt)
	}

	columns := []string{
		"id", "seq", "amount", "label", "code", "active",
		"observed_at", "blob", "tags", "doc",
	}
	var mismatches []string
	for _, col := range columns {
		q := fmt.Sprintf(
			"SELECT md5(COALESCE(string_agg(COALESCE(%s::text,'<NULL>'), E'\\n' ORDER BY id), '')) FROM %s",
			payloadQuoteIdent(col), payloadWideTable,
		)
		if s, g := payloadScalar(t, src, q), payloadScalar(t, tgt, q); s != g {
			mismatches = append(mismatches, fmt.Sprintf("column %q: src md5=%s tgt md5=%s", col, s, g))
		}
	}
	if len(mismatches) > 0 {
		t.Fatalf("mode %s: BYTE-IDENTITY FAILURE — target diverged from source on %d "+
			"column-family digest(s) (Bug-74-class silent loss in the changed-set / "+
			"JSONB-round-trip path):\n  - %s", mode, len(mismatches), strings.Join(mismatches, "\n  - "))
	}

	wholeRow := fmt.Sprintf(
		"SELECT md5(COALESCE(string_agg(t::text, E'\\n' ORDER BY t.id), '')) FROM %s t",
		payloadWideTable,
	)
	if s, g := payloadScalar(t, src, wholeRow), payloadScalar(t, tgt, wholeRow); s != g {
		t.Fatalf("mode %s: whole-row digest diverged (src=%s tgt=%s) despite per-column "+
			"digests matching — a column outside the per-column list differs", mode, s, g)
	}
	t.Logf("mode %s: BYTE-IDENTICAL — target matches source across %s rows and %d value-family columns",
		mode, nSrc, len(columns))
}

// payloadExec runs a (possibly multi-statement) DDL/DML block.
func payloadExec(t *testing.T, dsn, stmt string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		t.Fatalf("exec: %v\nstmt: %s", err, payloadFirstLine(stmt))
	}
}

// payloadScalar runs a query expected to return a single string scalar.
func payloadScalar(t *testing.T, dsn, query string) string {
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

func payloadQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func payloadFirstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
