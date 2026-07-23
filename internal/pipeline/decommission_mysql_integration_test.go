//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL-source variant of the decommission gate: the MySQL family has
// no source-side objects (the binlog IS the stream — no slot, no
// publication), so `sync decommission` degrades to the still-useful
// half: clear the control row, and SAY that nothing durable lives on
// the source. The engine-scope derivation the CLI relies on — mysql
// failing the ir.SlotManagerOpener assertion — is pinned here against
// the real registered engine.

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestDecommission_MySQLSource_ControlRowOnly(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('r1@example.com');
	`)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	// The CLI's engine-scope derivation: a MySQL-family source has no
	// slot manager, so decommission runs with slots == nil. Pin the
	// assertion against the real engine so a future OpenSlotManager on
	// mysql revisits this path deliberately.
	if _, isOpener := mysqlEng.(ir.SlotManagerOpener); isOpener {
		t.Fatal("mysql engine now implements ir.SlotManagerOpener — revisit the decommission control-row-only posture for MySQL sources")
	}

	stream := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  "mysql-wave",
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- stream.Run(runCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "users", 1, 60*time.Second) {
		t.Fatal("cold start never delivered the seed row")
	}
	// Prove the stream reached CDC mode before stopping: a delivered
	// CDC change means the anchor position (the control row) is
	// durably written — cancelling straight after the bulk-copy row
	// races the anchor write and leaves no row to decommission.
	applyDDLMySQL(t, sourceDSN, "INSERT INTO users (email) VALUES ('r2@example.com');")
	if !waitForRowCountMySQL(t, targetDSN, "users", 2, 60*time.Second) {
		t.Fatal("CDC never delivered the post-cold-start insert")
	}
	cancelRun()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("streamer did not return after ctx cancel")
	}

	ctx := context.Background()
	applier, err := mysqlEng.OpenChangeApplier(ctx, targetDSN)
	if err != nil {
		t.Fatalf("open target applier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if _, rowExists, err := readRecordedPublicationState(ctx, applier, "mysql-wave"); err != nil || !rowExists {
		t.Fatalf("precondition: control row missing before decommission (exists=%v, err=%v)", rowExists, err)
	}

	rep, err := DecommissionStream(ctx, applier, nil, "mysql-wave", false)
	if err != nil {
		t.Fatalf("decommission: %v", err)
	}
	if !rep.ControlRowCleared {
		t.Error("control row not cleared")
	}
	if rep.SlotDropped || rep.PublicationDropped {
		t.Errorf("report = %+v; a MySQL source has nothing to drop", rep)
	}
	if rep.SlotSkipped == "" || rep.PublicationSkipped == "" {
		t.Errorf("report = %+v; both skips must say why nothing was removed on the source", rep)
	}

	if _, rowExists, err := readRecordedPublicationState(ctx, applier, "mysql-wave"); err != nil || rowExists {
		t.Fatalf("control row still present after decommission (exists=%v, err=%v)", rowExists, err)
	}
}
