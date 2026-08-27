//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for ADR-0185 (`trigger setup --capture-replicated-writes`):
//
//   - The MECHANISM, inverted: with the opt-in's ENABLE ALWAYS triggers,
//     DML executed under session_replication_role=replica IS captured —
//     first on a pinned session (the applier shape), then through a REAL
//     native logical-replication subscription (two PG16 containers, CREATE
//     PUBLICATION → CREATE SUBSCRIPTION): the exact cell the shipped F1
//     blindness pin (preflight_replica_role_integration_test.go) proves is
//     LOST without the opt-in. Replicated TRUNCATE is ground-truthed
//     against the ALWAYS truncate trigger too — the subscriber applies
//     TRUNCATE via a different executor path than row DML, so it is
//     observed, not assumed.
//   - The ECHO-LOOP refusal (SLUICE-E-CDC-TRIGGER-ECHO-LOOP): relay
//     artifacts (sluice_cdc_state) + the opt-in refuse coded at Setup
//     (dry-run included) AND at both stream-open paths; the same artifacts
//     WITHOUT the opt-in keep the shipped WARN-only behaviour.
//   - The F2 POSTURE doors, both directions on real catalogs: an opt-in
//     install hand-flipped to plain refuses at open, a plain install
//     hand-flipped to ENABLE ALWAYS refuses at open, and a `trigger setup`
//     re-run with the matching flag repairs each (the remedy really runs).

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// readCapturedOps returns the change-log op sequence for one table, in id
// (commit) order.
func readCapturedOps(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"SELECT op FROM public.sluice_change_log WHERE table_name = $1 ORDER BY id", table)
	if err != nil {
		t.Fatalf("read change log: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ops []string
	for rows.Next() {
		var op string
		if err := rows.Scan(&op); err != nil {
			t.Fatalf("scan op: %v", err)
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate change log: %v", err)
	}
	return ops
}

// triggerEnablement returns tgenabled for the named trigger on the table.
func triggerEnablement(t *testing.T, ctx context.Context, db *sql.DB, table, trigger string) string {
	t.Helper()
	var enabled string
	err := db.QueryRowContext(ctx, `
SELECT t.tgenabled::text
  FROM pg_trigger t
  JOIN pg_class c ON c.oid = t.tgrelid
 WHERE c.relname = $1 AND t.tgname = $2 AND NOT t.tgisinternal`, table, trigger).Scan(&enabled)
	if err != nil {
		t.Fatalf("read tgenabled of %s on %s: %v", trigger, table, err)
	}
	return enabled
}

// wantEchoRefusal asserts err carries the SLUICE-E-CDC-TRIGGER-ECHO-LOOP code.
func wantEchoRefusal(t *testing.T, err error, site string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: passed; want the %s refusal", site, sluicecode.CodeCDCTriggerEchoLoop)
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCTriggerEchoLoop {
		t.Fatalf("%s: want %s; got %T: %v", site, sluicecode.CodeCDCTriggerEchoLoop, err, err)
	}
}

func TestCaptureReplicatedWrites_PostureAndEchoDoors(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE crw_t (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	setup := func(t *testing.T, captureReplicated bool) error {
		t.Helper()
		_, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"crw_t"}, CaptureReplicatedWrites: captureReplicated})
		return err
	}
	openWantClean := func(t *testing.T) {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err != nil {
			t.Fatalf("CDC open refused (false refuse): %v", err)
		}
		_ = r.(*CDCReader).Close()
	}
	openWantRefusal := func(t *testing.T, wantAll ...string) error {
		t.Helper()
		r, err := openCDCReader(ctx, dsn, "")
		if err == nil {
			_ = r.(*CDCReader).Close()
			t.Fatalf("CDC open succeeded; want a refusal containing %q", wantAll)
		}
		for _, want := range wantAll {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("refusal missing %q:\n%v", want, err)
			}
		}
		return err
	}

	t.Run("opt-in setup installs ENABLE ALWAYS pairs and opens clean", func(t *testing.T) {
		if err := setup(t, true); err != nil {
			t.Fatalf("Setup(opt-in): %v", err)
		}
		for _, trg := range []string{CaptureTriggerRow, CaptureTriggerTruncate} {
			if got := triggerEnablement(t, ctx, db, "crw_t", trg); got != "A" {
				t.Errorf("tgenabled of %s = %q; want \"A\" (ENABLE ALWAYS)", trg, got)
			}
		}
		openWantClean(t)
	})

	t.Run("replica-role DML and TRUNCATE are captured under the opt-in (the F1 mechanism, inverted)", func(t *testing.T) {
		// SET session_replication_role is session-scoped, so everything
		// must ride the SAME connection — the exact shape of sluice's own
		// privileged applier and of a subscription apply worker.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "SET session_replication_role = replica"); err != nil {
			t.Fatalf("SET replica role: %v", err)
		}
		for _, stmt := range []string{
			"INSERT INTO crw_t VALUES (1, 'replicated')",
			"UPDATE crw_t SET note = 'replicated-2' WHERE id = 1",
			"DELETE FROM crw_t WHERE id = 1",
			"TRUNCATE crw_t",
		} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("replica-role %q: %v", stmt, err)
			}
		}
		got := strings.Join(readCapturedOps(t, ctx, db, "crw_t"), "")
		// Ground truth (real PG 16): ENABLE ALWAYS row AND statement
		// (TRUNCATE) triggers all fire under replica role in an ordinary
		// session. The subscription-applied TRUNCATE path is ground-truthed
		// separately in the rig test below.
		if got != "IUDT" {
			t.Fatalf("captured ops = %q; want \"IUDT\" — a missing member means ENABLE ALWAYS does not cover that verb under replica role, and the ADR-0185 capture-completeness claim must be re-scoped", got)
		}
	})

	t.Run("subscriber shape under the opt-in is supported: no capture-gap WARN", func(t *testing.T) {
		applyPGSQL(t, dsn, `CREATE SUBSCRIPTION crw_sub_catalog CONNECTION 'host=nowhere.invalid dbname=nope' PUBLICATION crw_pub WITH (connect = false)`)
		defer func() {
			applyPGSQL(t, dsn, `ALTER SUBSCRIPTION crw_sub_catalog SET (slot_name = NONE)`)
			applyPGSQL(t, dsn, `DROP SUBSCRIPTION crw_sub_catalog`)
		}()

		setupLogs := captureWarnLogs(t, func() {
			if err := setup(t, true); err != nil {
				t.Fatalf("Setup(opt-in) on a subscriber source: %v", err)
			}
		})
		if strings.Contains(setupLogs, captureGapRiskMarker) {
			t.Errorf("opt-in Setup on a subscriber source WARNed — the supported scenario must not carry the blindness WARN:\n%s", setupLogs)
		}
		openLogs := captureWarnLogs(t, func() { openWantClean(t) })
		if strings.Contains(openLogs, captureGapRiskMarker) {
			t.Errorf("opt-in stream open on a subscriber source WARNed — the supported scenario must not carry the blindness WARN:\n%s", openLogs)
		}
	})

	// The relay artifacts: this source is (or was) the TARGET of another
	// sluice sync — the echo-loop shape under the opt-in.
	applyPGSQL(t, dsn, `
		CREATE TABLE public.sluice_cdc_state (
			stream_id         VARCHAR(255) NOT NULL PRIMARY KEY,
			source_position   TEXT         NOT NULL,
			updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			stop_requested_at TIMESTAMP    NULL
		);
		INSERT INTO public.sluice_cdc_state (stream_id, source_position) VALUES ('upstream-a-to-b', 'pos');
	`)

	t.Run("relay artifacts + opt-in refuse coded at both open paths", func(t *testing.T) {
		err := openWantRefusal(t, "echo loop", "sluice_cdc_state")
		wantEchoRefusal(t, err, "openCDCReader")

		if stream, err := (Engine{}).OpenSnapshotStream(ctx, dsn); err == nil {
			_ = stream.Close()
			t.Fatal("OpenSnapshotStream succeeded on the echo-loop shape; want the coded refusal before any copy")
		} else {
			wantEchoRefusal(t, err, "OpenSnapshotStream")
		}
	})

	t.Run("relay artifacts + opt-in refuse coded at setup, dry-run included", func(t *testing.T) {
		wantEchoRefusal(t, setup(t, true), "Setup")
		_, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"crw_t"}, CaptureReplicatedWrites: true, DryRun: true})
		wantEchoRefusal(t, err, "Setup dry-run")
	})

	t.Run("relay artifacts without the opt-in keep the WARN-only behaviour", func(t *testing.T) {
		setupLogs := captureWarnLogs(t, func() {
			if err := setup(t, false); err != nil {
				t.Fatalf("plain Setup on the relay shape must not refuse: %v", err)
			}
		})
		if !strings.Contains(setupLogs, captureGapRiskMarker) || !strings.Contains(setupLogs, "sluice_cdc_state") {
			t.Errorf("plain Setup on the relay shape should WARN naming the control table:\n%s", setupLogs)
		}
		// The plain re-run converged the opt-in install back to plain
		// triggers + a recorded false posture, so the open is clean (with
		// the WARN), not a posture refusal.
		openLogs := captureWarnLogs(t, func() { openWantClean(t) })
		if !strings.Contains(openLogs, captureGapRiskMarker) {
			t.Errorf("plain stream open on the relay shape should WARN:\n%s", openLogs)
		}
		for _, trg := range []string{CaptureTriggerRow, CaptureTriggerTruncate} {
			if got := triggerEnablement(t, ctx, db, "crw_t", trg); got != "O" {
				t.Errorf("after the plain re-run, tgenabled of %s = %q; want \"O\" (the downgrade must converge)", trg, got)
			}
		}
	})

	applyPGSQL(t, dsn, `DROP TABLE public.sluice_cdc_state`)

	t.Run("posture door: opt-in install hand-flipped to plain refuses at open", func(t *testing.T) {
		if err := setup(t, true); err != nil {
			t.Fatalf("Setup(opt-in): %v", err)
		}
		applyPGSQL(t, dsn, `ALTER TABLE crw_t ENABLE TRIGGER sluice_capture`)
		openWantRefusal(t, CaptureTriggerRow, "--capture-replicated-writes", "NOT being captured")

		// The named remedy really runs: the opt-in re-setup restores 'A'.
		if err := setup(t, true); err != nil {
			t.Fatalf("repair Setup(opt-in): %v", err)
		}
		openWantClean(t)
	})

	t.Run("posture door: plain install hand-flipped to ENABLE ALWAYS refuses at open", func(t *testing.T) {
		if err := setup(t, false); err != nil {
			t.Fatalf("Setup(plain): %v", err)
		}
		applyPGSQL(t, dsn, `ALTER TABLE crw_t ENABLE ALWAYS TRIGGER sluice_capture`)
		openWantRefusal(t, CaptureTriggerRow, "ENABLE ALWAYS", "ORIGIN-ONLY")

		if err := setup(t, false); err != nil {
			t.Fatalf("repair Setup(plain): %v", err)
		}
		openWantClean(t)
	})
}

// startPGOnNetwork boots a postgres:16 container attached to nw under the
// given alias, with the extra server args appended to the postgres command
// (the publisher needs wal_level=logical). Returns the HOST-side DSN.
func startPGOnNetwork(t *testing.T, ctx context.Context, nw *testcontainers.DockerNetwork, alias, dbname string, extraArgs ...string) (dsn string, cleanup func()) {
	t.Helper()
	cmd := append([]string{"postgres"}, extraArgs...)
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16",
		Networks:     []string{nw.Name},
		ExposedPorts: []string{"5432/tcp"},
		NetworkAliases: map[string][]string{
			nw.Name: {alias},
		},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       dbname,
		},
		Cmd: cmd,
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithStartupTimeoutDefault(2 * time.Minute),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start %s container: %v", alias, err)
	}
	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}
	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("%s host: %v", alias, err)
	}
	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		terminate()
		t.Fatalf("%s mapped port: %v", alias, err)
	}
	return fmt.Sprintf("postgres://test:test@%s:%s/%s?sslmode=disable", host, port.Port(), dbname), terminate
}

// waitForCondition polls cond once a second until it returns true or the
// deadline passes.
func waitForCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(1 * time.Second)
	}
}

// TestCaptureReplicatedWrites_NativeSubscriptionRig is the F1 mechanism
// cell through a REAL native subscription: publisher PG16 (wal_level=
// logical) → CREATE PUBLICATION; subscriber PG16 with the opt-in ENABLE
// ALWAYS triggers → CREATE SUBSCRIPTION. Replicated INSERT/UPDATE/DELETE
// land in the subscriber's change log — the exact writes the shipped
// blindness pin proves are LOST without the opt-in — and replicated
// TRUNCATE is ground-truthed against the ALWAYS truncate trigger (the
// subscriber applies TRUNCATE via a separate executor path; observed, not
// assumed).
func TestCaptureReplicatedWrites_NativeSubscriptionRig(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	nw, err := network.New(ctx)
	if err != nil {
		t.Skipf("create docker network (provider likely unavailable): %v", err)
	}
	defer func() {
		rmCtx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = nw.Remove(rmCtx)
	}()

	pubDSN, pubCleanup := startPGOnNetwork(t, ctx, nw, "crwpub", "pubdb", "-c", "wal_level=logical")
	defer pubCleanup()
	subDSN, subCleanup := startPGOnNetwork(t, ctx, nw, "crwsub", "subdb")
	defer subCleanup()

	applyPGSQL(t, pubDSN, `
		CREATE TABLE sub_t (id BIGINT PRIMARY KEY, note TEXT);
		CREATE PUBLICATION crw_pub FOR TABLE sub_t;
	`)
	applyPGSQL(t, subDSN, `CREATE TABLE sub_t (id BIGINT PRIMARY KEY, note TEXT)`)

	if _, err := Setup(ctx, subDSN, SetupOptions{Tables: []string{"sub_t"}, CaptureReplicatedWrites: true}); err != nil {
		t.Fatalf("Setup(opt-in) on the subscriber: %v", err)
	}

	// The subscription dials the publisher through the docker network by
	// alias. copy_data=false: this rig pins the LIVE apply path (the
	// initial tablesync worker also applies under replica role, but a
	// deterministic op sequence is worth more here than one more cell).
	applyPGSQL(t, subDSN, `CREATE SUBSCRIPTION crw_sub CONNECTION 'host=crwpub port=5432 user=test password=test dbname=pubdb' PUBLICATION crw_pub WITH (copy_data = false)`)

	pub, err := sql.Open("pgx", pubDSN)
	if err != nil {
		t.Fatalf("open publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	sub, err := sql.Open("pgx", subDSN)
	if err != nil {
		t.Fatalf("open subscriber: %v", err)
	}
	defer func() { _ = sub.Close() }()

	subCount := func() int64 {
		var n int64
		if err := sub.QueryRowContext(ctx, "SELECT count(*) FROM sub_t").Scan(&n); err != nil {
			t.Fatalf("count sub_t on subscriber: %v", err)
		}
		return n
	}
	subNote := func(id int64) string {
		var note sql.NullString
		err := sub.QueryRowContext(ctx, "SELECT note FROM sub_t WHERE id = $1", id).Scan(&note)
		if err != nil {
			return ""
		}
		return note.String
	}

	// INSERT: replicated row arrives AND is captured.
	if _, err := pub.ExecContext(ctx, "INSERT INTO sub_t VALUES (1, 'from-publisher')"); err != nil {
		t.Fatalf("publisher INSERT: %v", err)
	}
	waitForCondition(t, 90*time.Second, "the replicated INSERT to arrive on the subscriber", func() bool {
		return subCount() == 1
	})

	// UPDATE.
	if _, err := pub.ExecContext(ctx, "UPDATE sub_t SET note = 'updated' WHERE id = 1"); err != nil {
		t.Fatalf("publisher UPDATE: %v", err)
	}
	waitForCondition(t, 60*time.Second, "the replicated UPDATE to arrive", func() bool {
		return subNote(1) == "updated"
	})

	// DELETE.
	if _, err := pub.ExecContext(ctx, "DELETE FROM sub_t WHERE id = 1"); err != nil {
		t.Fatalf("publisher DELETE: %v", err)
	}
	waitForCondition(t, 60*time.Second, "the replicated DELETE to arrive", func() bool {
		return subCount() == 0
	})

	if got := strings.Join(readCapturedOps(t, ctx, sub, "sub_t"), ""); got != "IUD" {
		t.Fatalf("captured ops after replicated I/U/D = %q; want \"IUD\" — the subscription apply worker's writes "+
			"must be captured by the ENABLE ALWAYS triggers (the ADR-0185 mechanism cell)", got)
	}

	// TRUNCATE ground truth: seed rows, replicate a publisher TRUNCATE,
	// and observe whether the subscriber's apply fires the ALWAYS
	// statement trigger (op='T').
	if _, err := pub.ExecContext(ctx, "INSERT INTO sub_t VALUES (2, 'a'), (3, 'b')"); err != nil {
		t.Fatalf("publisher seed INSERT: %v", err)
	}
	waitForCondition(t, 60*time.Second, "the seed rows to arrive", func() bool {
		return subCount() == 2
	})
	if _, err := pub.ExecContext(ctx, "TRUNCATE sub_t"); err != nil {
		t.Fatalf("publisher TRUNCATE: %v", err)
	}
	waitForCondition(t, 60*time.Second, "the replicated TRUNCATE to arrive", func() bool {
		return subCount() == 0
	})
	ops := strings.Join(readCapturedOps(t, ctx, sub, "sub_t"), "")
	// Ground truth (real PG 16, observed by this rig): the subscription
	// apply worker's TRUNCATE executor honours trigger enablement the same
	// way its row workers do — the ENABLE ALWAYS statement trigger fires
	// and the truncate is captured as op='T' after the seed 'I's.
	if ops != "IUDIIT" {
		t.Fatalf("captured ops after the full replicated sequence = %q; want \"IUDIIT\" — if the trailing 'T' is "+
			"missing, the subscriber's TRUNCATE apply path does not fire ALWAYS statement triggers and the ADR-0185 "+
			"TRUNCATE claim (ADR + capture-completeness matrix) must be corrected to a stated residual", ops)
	}

	// The supported shape end-to-end: a REAL enabled subscription on the
	// source plus the opt-in must open with no capture-gap WARN.
	logs := captureWarnLogs(t, func() {
		r, err := openCDCReader(ctx, subDSN, "")
		if err != nil {
			t.Fatalf("CDC open on the subscriber (supported shape): %v", err)
		}
		_ = r.(*CDCReader).Close()
	})
	if strings.Contains(logs, captureGapRiskMarker) {
		t.Errorf("opt-in stream open on a live subscriber WARNed — the supported scenario must not carry the blindness WARN:\n%s", logs)
	}

	// Drop the subscription before the containers go away so its apply
	// worker isn't left dialing a terminated publisher during teardown.
	applyPGSQL(t, subDSN, `ALTER SUBSCRIPTION crw_sub DISABLE`)
	applyPGSQL(t, subDSN, `ALTER SUBSCRIPTION crw_sub SET (slot_name = NONE)`)
	applyPGSQL(t, subDSN, `DROP SUBSCRIPTION crw_sub`)
}

// TestCaptureReplicatedWrites_PreV3MetaOpensWithoutResetup pins the
// tolerant read the VF review (2026-08-27) flagged as a written
// invariant nobody checks: the upgrade-in-place path — a NEW binary
// opening an EXISTING pre-v3 install whose meta table has no
// capture_replicated_writes column, WITHOUT a setup re-run — must open
// clean as origin-only (the to_jsonb projection at
// readCaptureReplicatedWritesPosture). If someone "simplifies" the
// query to a direct column reference, every pre-v3 install would
// false-refuse at open with 42703, and this test is what reddens.
func TestCaptureReplicatedWrites_PreV3MetaOpensWithoutResetup(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE crw_v2 (id BIGINT PRIMARY KEY, note TEXT)`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"crw_v2"}}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	// Reconstruct the exact v2 on-disk shape a pre-ADR-0185 binary left
	// behind: no posture column, schema_version 2.
	applyPGSQL(t, dsn, `ALTER TABLE sluice_change_log_meta DROP COLUMN capture_replicated_writes`)
	applyPGSQL(t, dsn, `UPDATE sluice_change_log_meta SET schema_version = 2`)

	r, err := openCDCReader(ctx, dsn, "")
	if err != nil {
		t.Fatalf("CDC open refused on a genuine v2-shaped meta (the upgrade-in-place path): %v", err)
	}
	_ = r.(*CDCReader).Close()
}
