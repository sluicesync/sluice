//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0073 (c) — Vitess online-DDL / `_vt_*` table-lifecycle robustness.
// CORRECTNESS-GATING (silent-loss tier).
//
// On Vitess, online DDL is the DEFAULT schema-change mechanism: an
// `ALTER` builds a shadow table (a `_vt_vrp_*` internal-operation
// table), VReplication-copies into it, atomically cuts over (rename),
// and cleans up. sluice's VStream request uses `Match:"/.*/"` (every
// table), so the Vitess-internal artifacts reach sluice. The Bug-125
// hunt reproduced the COPY filter grabbing a `_vt_vrp_*` shadow during
// an in-flight deploy, which tripped the ADR-0071 scope-name-mismatch
// refusal and aborted the cold-start with zero rows.
//
// These pins assert (ADR-0073 (c)):
//
//	(i)  the logical table's stream SURVIVES an online `ALTER` +
//	     shadow-table DDL mid-stream — the stream is not wedged and
//	     keeps delivering logical-table events with zero loss;
//	(ii) Vitess-internal tables are EXCLUDED — no `_vt_*` table is
//	     copied/applied/counted, and the Bug-125 `_vt_vrp_*`-style
//	     scope-mismatch refusal no longer fires.
//
// vttestserver caveat (Phase A ground truth): the full online-DDL
// scheduler is "not implemented in vtcombo" — vttestserver builds the
// shadow-table DDL (those `_vt_vrp_*` CREATE/ALTER events DO reach the
// stream) but cannot complete the VReplication copy or the atomic
// cutover (the cutover needs tmclient RPCs vtcombo stubs out). So we
// reproduce the internal-table COPY/CDC ROW+FIELD shapes by creating an
// internal-named table DIRECTLY (which produces the exact wire events
// the real shadow would), AND we fire a real `ddl_strategy='vitess'`
// ALTER to prove the shadow-DDL events don't wedge the logical stream.
// Full cutover-with-rows survival is validated against real PlanetScale
// (cdc_vstream_psverify_test.go) — see the ADR.
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestVStream_OnlineDDL' ./internal/engines/mysql/...

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestVStream_OnlineDDL_InternalTablesExcluded_ColdStart pins (ii) on
// the COPY path: a logical table copies byte-clean while an
// internal-named (`_vt_vrp_*`) table — populated with rows that, pre-fix,
// were buffered by bufferCopyRow and could trip the scope-mismatch
// refusal — contributes ZERO rows and triggers NO refusal.
func TestVStream_OnlineDDL_InternalTablesExcluded_ColdStart(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	// A real unified-format vreplication internal-operation name.
	const internalName = "_vt_vrp_6ace8bcef73211ea87e9f875a4d24e90_20200915120410_"
	if !isVitessInternalTable(internalName) {
		t.Fatalf("test bug: %q must be recognized as internal", internalName)
	}

	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(
		"CREATE TABLE `%s` (id BIGINT NOT NULL AUTO_INCREMENT, v VARCHAR(64), PRIMARY KEY(id)) ENGINE=InnoDB",
		internalName,
	))

	// Seed the logical table; seed the internal table with MORE rows so
	// that, pre-fix, the internal table's COPY rows are the ones most
	// likely to interleave and trip enqueueRowLocked's activeTable guard.
	seedNUsers(t, mysqlDSN, "users", 5)
	seedInternalRows(t, mysqlDSN, internalName, 20)
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := eng.OpenSnapshotStream(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Drain the logical table — must get exactly the 5 seeded rows.
	usersTable := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, usersTable)
	if err != nil {
		t.Fatalf("ReadRows(users): %v", err)
	}
	emails := make([]string, 0, 5)
	for r := range rowsCh {
		s, _ := r["email"].(string)
		emails = append(emails, s)
	}
	sort.Strings(emails)
	want := []string{"u1@example.com", "u2@example.com", "u3@example.com", "u4@example.com", "u5@example.com"}
	if !equalSorted(emails, want) {
		t.Fatalf("logical users rows = %v; want %v (online-DDL exclusion must not disturb the logical copy)", emails, want)
	}

	// No scope-mismatch / cap refusal must have fired (Bug-125): the
	// internal table never enters rowBuffer, so the stream's COPY error
	// stays nil.
	if rerr := stream.Rows.Err(); rerr != nil {
		t.Fatalf("snapshot Rows.Err = %v; want nil (an internal `_vt_*` table must not cause a scope-mismatch refusal)", rerr)
	}

	// The internal table must surface ZERO rows even when a consumer
	// explicitly asks for it (post-fix: dropped before buffering).
	internalTbl := &ir.Table{
		Name: internalName,
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "v", Type: ir.Varchar{Length: 64}},
		},
	}
	ich, ierr := stream.Rows.ReadRows(ctx, internalTbl)
	if ierr != nil {
		// A clean "no such table in snapshot" style error is also an
		// acceptable exclusion outcome; just must not surface rows.
		t.Logf("ReadRows(internal) err (acceptable) = %v", ierr)
	} else {
		ic := 0
		for range ich {
			ic++
		}
		if ic != 0 {
			t.Fatalf("internal `_vt_*` table surfaced %d rows via COPY; want 0 (ADR-0073 (c) exclusion)", ic)
		}
	}
}

// TestVStream_OnlineDDL_LogicalStreamSurvivesCutover pins (i): firing a
// real online (`ddl_strategy='vitess'`) ALTER mid-CDC-stream must NOT
// wedge the logical table's stream. vttestserver streams the
// `_vt_vrp_*` shadow-table DDL events during the migration (Phase A
// ground truth); the (c) fix drops those internal-table DDLs so they
// don't clear the logical field cache and wedge the next logical ROW.
// The full cutover can't complete on vtcombo, so we assert
// stream-survival (logical inserts keep flowing across the online ALTER)
// rather than the post-cutover schema — that schema assertion lives in
// the PlanetScale psverify suite.
func TestVStream_OnlineDDL_LogicalStreamSurvivesCutover(t *testing.T) {
	mysqlDSN, grpcEndpoint, _, cleanup := startVTTestServer(t)
	defer cleanup()

	applyVTTestSQL(t, mysqlDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	seedNUsers(t, mysqlDSN, "users", 2)
	time.Sleep(3 * time.Second)

	sluiceDSN := fmt.Sprintf(
		"%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0",
		mysqlDSN, grpcEndpoint,
	)
	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rdr, err := eng.OpenCDCReader(ctx, sluiceDSN)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := rdr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := rdr.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	time.Sleep(2 * time.Second)

	// A pre-ALTER insert: confirm the stream is live.
	applyVTTestSQL(t, mysqlDSN, "INSERT INTO users (email) VALUES ('before-alter@example.com')")

	// Fire the online ALTER. The session var + ALTER share one
	// connection (multiStatements). This streams `_vt_vrp_*` shadow DDL
	// events through sluice's dispatch; the (c) fix must drop them
	// without wedging the logical stream.
	applyVTTestSQL(t, mysqlDSN+"&multiStatements=true", `
		SET @@ddl_strategy='vitess';
		ALTER TABLE users ADD COLUMN nickname VARCHAR(64) NULL;`)

	// Give the migration scheduler a beat to emit the shadow-table DDL
	// events (CREATE/ALTER `_vt_vrp_*`) onto the stream.
	time.Sleep(3 * time.Second)

	// Post-ALTER inserts on the LOGICAL table. If the shadow DDL wedged
	// the stream (cleared the logical field cache → "row without FIELD"
	// loud error, or a spurious internal-table refusal), these never
	// arrive and the reader's Err() is set. We DON'T reference the new
	// `nickname` column — the migration can't cut over on vtcombo, so
	// the serving schema is still the original; the survival property is
	// "logical events keep flowing", independent of the column add.
	for i := 1; i <= 3; i++ {
		applyVTTestSQL(t, mysqlDSN, fmt.Sprintf(
			"INSERT INTO users (email) VALUES ('after-alter-%d@example.com')", i,
		))
	}

	// Drain enough events to cover the before + after inserts. The
	// internal-table DDL/ROW/FIELD events are excluded by the fix, so
	// only logical-table changes count here.
	got := drainVTTestChanges(t, ctx, changes, 4, 90*time.Second)

	// Loud check: the stream must not have errored on the shadow DDL.
	if cdcRdr, ok := rdr.(*vstreamCDCReader); ok {
		if streamErr := cdcRdr.Err(); streamErr != nil {
			t.Fatalf("stream errored across the online ALTER: %v (ADR-0073 (c): shadow DDL must not wedge the logical stream)", streamErr)
		}
	}

	// Count the after-alter inserts that survived the cutover window.
	afterAlter := 0
	for _, c := range got {
		ins, ok := c.(ir.Insert)
		if !ok {
			continue
		}
		email, _ := ins.Row["email"].(string)
		// No internal-table event may ever surface as a change.
		if isVitessInternalTable(ins.Table) {
			t.Fatalf("an internal `_vt_*` table surfaced as a CDC change: table=%q", ins.Table)
		}
		if strings.HasPrefix(email, "after-alter-") {
			afterAlter++
		}
	}
	if afterAlter < 3 {
		t.Fatalf("post-ALTER logical inserts seen = %d; want 3 (the logical stream must survive the online-DDL shadow events)", afterAlter)
	}
}

// seedNUsers inserts n rows uN@example.com into the named table.
func seedNUsers(t *testing.T, dsn, table string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO %s (email) VALUES ('u%d@example.com')", table, i,
		)); err != nil {
			t.Fatalf("seed %s row %d: %v", table, i, err)
		}
	}
}

// seedInternalRows inserts n rows into an internal-named table. The name
// must be backtick-quoted because it contains the `_vt_` lifecycle form.
func seedInternalRows(t *testing.T, dsn, table string, n int) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed-internal open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			"INSERT INTO `%s` (v) VALUES ('iv%d')", table, i,
		)); err != nil {
			t.Fatalf("seed-internal row %d: %v", i, err)
		}
	}
}
