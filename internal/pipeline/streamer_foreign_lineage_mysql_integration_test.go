//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 A0909-MYSQL-HIGH-1, end to end on one real MySQL: a
// sync whose source becomes a DIFFERENT lineage at the same DSN must
// REFUSE, and the target must keep every row it had — so must the same
// source rolled back BEHIND a position it issued itself (operator
// decision 2026-09-15: the target is ahead of it) — while the same
// source after a plain RESET MASTER (empty executed set) must still take
// the automatic re-snapshot, because there the re-copy is the right
// recovery.
//
// The worker reproduced the loss by replacing the container behind one
// host:port. testcontainers cannot re-bind a port to a new instance, so
// the foreign lineage is produced the way the server itself produces it
// on a restore-from-elsewhere: RESET MASTER, then SET GLOBAL gtid_purged
// to a set under a UUID this server never had. @@gtid_executed is then
// non-empty and shares no UUID with the persisted position — byte for
// byte the state the door grades — and the source's rows are gone, which
// is what makes an automatic re-copy destructive: the target would be
// refilled with NOTHING.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestStreamer_MySQLForeignLineage_RefusesTheAutomaticRecopy(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLGTID(t)
	defer cleanup()

	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('a@example.com'), ('b@example.com'), ('c@example.com');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	filter, err := migcore.NewTableFilter([]string{"users"}, nil)
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	newStream := func() *Streamer {
		return &Streamer{
			Source:    mysqlEng,
			Target:    mysqlEng,
			SourceDSN: srcDSN,
			TargetDSN: tgtDSN,
			StreamID:  "test-foreign-lineage",
			Filter:    filter,
		}
	}

	// Cold start, one live change, clean stop — a persisted GTID position
	// under this server's UUID and four rows on the target.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- newStream().Run(ctx) }()
	if !waitForRowCountMySQL(t, tgtDSN, "users", 3, 60*time.Second) {
		t.Fatal("cold copy did not deliver the seed rows")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users (email) VALUES ('d@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 4, 60*time.Second) {
		t.Fatal("CDC did not deliver the live insert")
	}
	cancel()
	select {
	case <-runErr:
	case <-time.After(30 * time.Second):
		t.Fatal("streamer did not stop after cancel")
	}

	// Become a different lineage at the same DSN: non-empty executed set,
	// no UUID in common, and the rows gone. The replacement's own table
	// is written with sql_log_bin=0 so it adds no GTID under THIS server's
	// UUID — a server keeps its server_uuid across RESET MASTER, and a
	// logged write here would make the executed set SHARE a UUID with the
	// position, which is the "behind" verdict (correctly still
	// auto-recopied), not the foreign one this cell grades. The first cut
	// of this test made exactly that mistake and measured the wrong arm.
	const foreignUUID = "ffffffff-1111-2222-3333-444444444444"
	applyDDLMySQL(t, srcDSN, "DROP TABLE users; RESET MASTER; SET GLOBAL gtid_purged = '"+foreignUUID+":1-5'; "+
		"SET SESSION sql_log_bin = 0; "+
		"CREATE TABLE users (id BIGINT NOT NULL, email VARCHAR(255) NOT NULL, PRIMARY KEY (id)); "+
		"INSERT INTO users VALUES (77, 'REPLACED-HOST'); "+
		"SET SESSION sql_log_bin = 1;")
	if got := strings.TrimSpace(mysqlGlobal(t, srcDSN, "gtid_executed")); got != foreignUUID+":1-5" {
		t.Fatalf("premise gone: gtid_executed = %q; want exactly the foreign set (a GTID under this server's own "+
			"UUID would turn the verdict into 'behind')", got)
	}

	t.Run("foreign lineage: refuses, target untouched", func(t *testing.T) {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer rcancel()
		err := newStream().Run(rctx)
		if err == nil {
			t.Fatal("sync start against a different lineage returned nil")
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || !strings.Contains(err.Error(), foreignLineageMarker) {
			t.Fatalf("want the %s refusal carrying ir.ErrPositionForeignLineage, got: %v", foreignLineageMarker, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("the refusal also reads as an invalid position, which is the auto-recopy route: %v", err)
		}
		// The independent expected value: the target still holds the
		// four rows the OLD source produced, not the replacement's one.
		if got := pollRowCountMySQL(tgtDSN, "users"); got != 4 {
			t.Fatalf("target holds %d rows after the refusal; want the original 4 — the recovery this door "+
				"exists to stop replaces them with the replacement's single row (A0909-MYSQL-HIGH-1)", got)
		}
	})

	t.Run("rebuilt instance with an EMPTY executed set: refuses, target untouched", func(t *testing.T) {
		// Audit 2026-09-15 A0915-MYSQL-HIGH-2. The worker measured this
		// shape by replacing the container behind one host:port with a
		// rebuild that had run RESET MASTER: an EMPTY @@gtid_executed on a
		// server whose @@server_uuid the persisted position never named.
		// The empty arm routed it to the automatic re-copy and the target
		// was reduced to the rebuild's stale rows at exit 0. testcontainers
		// cannot re-bind the port, and a server keeps its uuid across
		// RESET MASTER, so the shape is produced from the other side: the
		// PERSISTED position is re-stamped under a uuid this server never
		// had (byte for byte what a position captured on the replaced
		// instance looks like from here), and the server is RESET. From
		// sluice's evidence — the position and the server's answers — that
		// is a rebuilt node.
		applier, err := mysqlEng.OpenChangeApplier(context.Background(), tgtDSN)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		defer migcore.CloseIf(applier)
		persisted, found, err := applier.ReadPosition(context.Background(), "test-foreign-lineage")
		if err != nil || !found {
			t.Fatalf("ReadPosition: found=%v err=%v", found, err)
		}
		thisUUID := mysqlGlobal(t, srcDSN, "server_uuid")
		if !strings.Contains(persisted.Token, thisUUID) {
			t.Fatalf("premise gone: the persisted position %q does not carry this server's uuid %s", persisted.Token, thisUUID)
		}
		const replacedUUID = "eeeeeeee-5555-6666-7777-888888888888"
		foreignPos := persisted
		foreignPos.Token = strings.ReplaceAll(persisted.Token, thisUUID, replacedUUID)
		writer, ok := applier.(ir.PositionWriter)
		if !ok {
			t.Fatal("the mysql applier no longer implements ir.PositionWriter")
		}
		if err := writer.WritePosition(context.Background(), "test-foreign-lineage", foreignPos); err != nil {
			t.Fatalf("WritePosition: %v", err)
		}
		// Put the original position back for the RESET MASTER cell below,
		// whichever way this cell ends.
		defer func() {
			if err := writer.WritePosition(context.Background(), "test-foreign-lineage", persisted); err != nil {
				t.Errorf("restore the persisted position: %v", err)
			}
		}()
		applyDDLMySQL(t, srcDSN, "RESET MASTER;")
		if got := mysqlGlobal(t, srcDSN, "gtid_executed"); strings.TrimSpace(got) != "" {
			t.Fatalf("premise gone: gtid_executed = %q after RESET MASTER; want empty", got)
		}

		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer rcancel()
		err = newStream().Run(rctx)
		if err == nil {
			t.Fatal("sync start against a rebuilt instance (empty executed set, foreign uuid) returned nil")
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || !strings.Contains(err.Error(), foreignLineageMarker) {
			t.Fatalf("want the %s refusal carrying ir.ErrPositionForeignLineage, got: %v", foreignLineageMarker, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("the refusal also reads as an invalid position, which is the auto-recopy route: %v", err)
		}
		// The independent expected value: the target still holds the
		// four rows; the re-copy would have left it with the one row the
		// source holds now.
		if got := pollRowCountMySQL(tgtDSN, "users"); got != 4 {
			t.Fatalf("target holds %d rows after the refusal; want the original 4 — the automatic re-copy this "+
				"door exists to stop would leave the replacement's single row (A0915-MYSQL-HIGH-2)", got)
		}
	})

	t.Run("same server rolled back BEHIND its own position: refuses, target untouched", func(t *testing.T) {
		// Operator decision 2026-09-15, closing the policy question audit
		// 2026-09-15 A0915-MYSQL-HIGH-2 left open. The server keeps its uuid
		// and presents an executed set BEHIND the persisted position under
		// that uuid — what a primary rolled back past the position, or an
		// in-place restore that seeds gtid_purged from an older backup,
		// looks like. The target is AHEAD of such a source; until the
		// decision the automatic re-copy ran and reduced it to the source's
		// one row. Produced here exactly that way: RESET MASTER, then
		// gtid_purged seeded one transaction short of the persisted set.
		applier, err := mysqlEng.OpenChangeApplier(context.Background(), tgtDSN)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		defer migcore.CloseIf(applier)
		persisted, found, err := applier.ReadPosition(context.Background(), "test-foreign-lineage")
		if err != nil || !found {
			t.Fatalf("ReadPosition: found=%v err=%v", found, err)
		}
		var tok struct {
			Mode    string `json:"mode"`
			GTIDSet string `json:"gtid_set"`
		}
		if err := json.Unmarshal([]byte(persisted.Token), &tok); err != nil || tok.Mode != "gtid" {
			t.Fatalf("premise gone: the persisted position is not a GTID position (token %q, err %v)", persisted.Token, err)
		}
		behind := behindOwnGTIDSet(t, tok.GTIDSet, mysqlGlobal(t, srcDSN, "server_uuid"))
		applyDDLMySQL(t, srcDSN, "RESET MASTER; SET GLOBAL gtid_purged = '"+behind+"';")
		if got := strings.TrimSpace(mysqlGlobal(t, srcDSN, "gtid_executed")); !strings.EqualFold(got, behind) {
			t.Fatalf("premise gone: gtid_executed = %q; want exactly the behind set %q", got, behind)
		}

		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer rcancel()
		err = newStream().Run(rctx)
		if err == nil {
			t.Fatal("sync start against a source rolled back behind its own position returned nil")
		}
		if !errors.Is(err, ir.ErrPositionForeignLineage) || !strings.Contains(err.Error(), foreignLineageMarker) {
			t.Fatalf("want the %s refusal carrying ir.ErrPositionForeignLineage, got: %v", foreignLineageMarker, err)
		}
		if errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("the refusal also reads as an invalid position, which is the auto-recopy route: %v", err)
		}
		if !strings.Contains(err.Error(), "AHEAD") {
			t.Fatalf("the refusal does not carry the rolled-back diagnosis (target AHEAD of the source): %v", err)
		}
		// The independent expected value: the target still holds the four
		// rows; the re-copy would have left the source's single row.
		if got := pollRowCountMySQL(tgtDSN, "users"); got != 4 {
			t.Fatalf("target holds %d rows after the refusal; want the original 4 — the automatic re-copy would "+
				"reduce a target AHEAD of its rolled-back source to the source's single row", got)
		}
	})

	t.Run("same server after RESET MASTER: the automatic re-snapshot still runs", func(t *testing.T) {
		// Empty executed set: no other lineage here, the re-copy is right.
		applyDDLMySQL(t, srcDSN, "RESET MASTER;")
		if got := mysqlGlobal(t, srcDSN, "gtid_executed"); strings.TrimSpace(got) != "" {
			t.Fatalf("premise gone: gtid_executed = %q after RESET MASTER; want empty", got)
		}
		rctx, rcancel := context.WithCancel(context.Background())
		defer rcancel()
		errCh := make(chan error, 1)
		go func() { errCh <- newStream().Run(rctx) }()
		// The re-copy lands the replacement's one row (that is what the
		// source now holds); a refusal here would strand a legitimate
		// reset behind --restart-from-scratch for nothing.
		if !waitForExactRowCountMySQLRows(t, tgtDSN, "users", 1, 2*time.Minute) {
			select {
			case err := <-errCh:
				t.Fatalf("re-snapshot after a plain RESET MASTER did not run: %v", err)
			default:
				t.Fatalf("re-snapshot after RESET MASTER never delivered; target holds %d rows", pollRowCountMySQL(tgtDSN, "users"))
			}
		}
		rcancel()
		select {
		case <-errCh:
		case <-time.After(30 * time.Second):
			t.Fatal("streamer did not stop after cancel")
		}
	})
}

// behindOwnGTIDSet returns set one transaction short — the set this
// server held one transaction earlier. The premise, asserted rather than
// guessed at: a single-interval set under exactly this server's uuid.
func behindOwnGTIDSet(t *testing.T, set, serverUUID string) string {
	t.Helper()
	uuid, interval, ok := strings.Cut(strings.TrimSpace(set), ":")
	if !ok || !strings.EqualFold(uuid, serverUUID) || strings.ContainsAny(interval, ",:") {
		t.Fatalf("premise gone: persisted GTID set %q is not a single interval under this server's uuid %s", set, serverUUID)
	}
	lo, hi, ok := strings.Cut(interval, "-")
	n, err := strconv.Atoi(hi)
	if !ok || err != nil || n < 2 {
		t.Fatalf("premise gone: persisted GTID set %q has no interval a transaction can be taken off", set)
	}
	return uuid + ":" + lo + "-" + strconv.Itoa(n-1)
}

func mysqlGlobal(t *testing.T, dsn, name string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var v string
	if err := db.QueryRow("SELECT @@global." + name).Scan(&v); err != nil {
		t.Fatalf("read @@global.%s: %v", name, err)
	}
	return v
}

func waitForExactRowCountMySQLRows(t *testing.T, dsn, table string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pollRowCountMySQL(dsn, table) == want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
