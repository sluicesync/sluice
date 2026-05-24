//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 v0.78.0 task #22 — end-to-end RENAME COLUMN pin (MySQL → MySQL).
//
// Sibling to shard_consolidation_rename_pg_integration_test.go.
// The failure shape pre-fix on MySQL is the same root cause (the
// classifier refused the added=1+dropped=1 same-attribute combo as
// Unrecognized); on the apply side MySQL would crash with Error
// 1054 (`Unknown column 'product_name' in 'field list'`) on the
// post-RENAME INSERT. Mirroring the pin on both engines closes the
// "validate end-to-end before building more" tenet for task #22.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
)

func TestStreamer_RenameColumn_MySQL_LiveCoordination(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyDDLMySQL(t, sourceDSN, `
		CREATE TABLE widgets (
			id BIGINT NOT NULL PRIMARY KEY,
			name VARCHAR(64) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
	`)

	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	const streamID = "test-rename-mysql"
	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		StreamID:  streamID,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 20 * time.Second,
			RetryPeriod:   5 * time.Second,
		},
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
		t.Fatalf("phase A: bulk-copy never landed seed rows")
	}

	applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets RENAME COLUMN name TO product_name;")
	applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, product_name) VALUES (3, 'gamma');")

	if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
		t.Fatalf("phase B: post-RENAME row never landed — RENAME shape may not be classified " +
			"as ShapeKindRenameColumn, or AlterRenameColumn failed to apply")
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgtDB.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var hasNew, hasOld int
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'product_name'`).Scan(&hasNew); err != nil {
		t.Fatalf("check product_name: %v", err)
	}
	if hasNew != 1 {
		t.Error("target widgets.product_name column missing — RENAME apply didn't fire")
	}
	if err := tgtDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'name'`).Scan(&hasOld); err != nil {
		t.Fatalf("check name: %v", err)
	}
	if hasOld != 0 {
		t.Error("target widgets.name column still present — RENAME left both columns")
	}

	// Row data preserved under renamed column.
	var alpha, beta string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 1").Scan(&alpha); err != nil {
		t.Fatalf("read product_name @ id=1: %v", err)
	}
	if alpha != "alpha" {
		t.Errorf("widgets.product_name @ id=1 = %q, want alpha", alpha)
	}
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 2").Scan(&beta); err != nil {
		t.Fatalf("read product_name @ id=2: %v", err)
	}
	if beta != "beta" {
		t.Errorf("widgets.product_name @ id=2 = %q, want beta", beta)
	}

	var gamma string
	if err := tgtDB.QueryRowContext(ctx, "SELECT product_name FROM widgets WHERE id = 3").Scan(&gamma); err != nil {
		t.Fatalf("read product_name @ id=3: %v", err)
	}
	if gamma != "gamma" {
		t.Errorf("widgets.product_name @ id=3 = %q, want gamma", gamma)
	}

	// Lease row reflects applied state.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version
		FROM sluice_shard_consolidation_lease
		WHERE target_table_full_name = ?`
	var (
		applied bool
		version int64
	)
	if err := tgtDB.QueryRowContext(ctx, leaseQ, "source_db.widgets").Scan(&applied, &version); err != nil {
		t.Fatalf("scan lease row: %v (a missing row means the RENAME boundary never routed)", err)
	}
	if !applied {
		t.Error("lease.applied_at should be set after the routed RENAME boundary")
	}
	if version < 1 {
		t.Errorf("lease.applied_schema_version = %d; want >= 1", version)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}

// TestStreamer_RenameColumn_MySQL_LiveCoordination_TypeFamilyMatrix
// mirrors the PG type-family matrix from Bug 86 (v0.78.1). On MySQL
// the v0.78.0 cycle's Focus B passed — MySQL's TableMapEvent decoder
// re-reads information_schema on schema-change boundaries so the CDC
// projection already matches the SchemaReader's, and there is no
// SchemaReader-vs-CDC IR canonicalization asymmetry — but the
// matrix-coverage discipline (Bug 74 lesson) requires that the
// MySQL side ALSO exercise every type family the catalog might add.
// A future change that introduces a MySQL-side IR asymmetry would be
// caught here without needing a separate cycle to discover it.
func TestStreamer_RenameColumn_MySQL_LiveCoordination_TypeFamilyMatrix(t *testing.T) {
	cases := []struct {
		name      string
		extraCol  string
		extraDecl string
	}{
		// Match the PG-side matrix one-for-one (DECIMAL ↔ NUMERIC,
		// TEXT, VARCHAR, INT/INTEGER, TIMESTAMP/DATETIME, BOOLEAN/
		// TINYINT(1)).
		{name: "extra_decimal_nullable", extraCol: "price", extraDecl: "DECIMAL(10,2)"},
		{name: "extra_text_nullable", extraCol: "description", extraDecl: "TEXT"},
		{name: "extra_varchar_nullable", extraCol: "tagline", extraDecl: "VARCHAR(64)"},
		{name: "extra_int_nullable", extraCol: "count", extraDecl: "INT"},
		{name: "extra_datetime_nullable", extraCol: "ts", extraDecl: "DATETIME"},
		{name: "extra_tinyint1_nullable", extraCol: "flag", extraDecl: "TINYINT(1)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
			defer cleanup()

			applyDDLMySQL(t, sourceDSN, `
				CREATE TABLE widgets (
					id BIGINT NOT NULL PRIMARY KEY,
					name VARCHAR(64) NOT NULL,
					`+tc.extraCol+` `+tc.extraDecl+`
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
				INSERT INTO widgets (id, name) VALUES (1, 'alpha'), (2, 'beta');
			`)

			myEng, ok := engines.Get("mysql")
			if !ok {
				t.Fatal("mysql engine not registered")
			}

			streamer := &Streamer{
				Source:    myEng,
				Target:    myEng,
				SourceDSN: sourceDSN,
				TargetDSN: targetDSN,
				StreamID:  "test-rename-mysql-" + tc.name,
				InjectShardColumn: ShardColumnSpec{
					Name:  "source_shard_id",
					Value: "shard_a",
				},
				CoordinateLiveDDL: true,
				ShardCoordinationLease: LeaseConfig{
					LeaseDuration: 30 * time.Second,
					RenewDeadline: 20 * time.Second,
					RetryPeriod:   5 * time.Second,
				},
			}

			streamCtx, streamCancel := context.WithCancel(context.Background())
			defer streamCancel()

			runErr := make(chan error, 1)
			go func() { runErr <- streamer.Run(streamCtx) }()

			if !waitForRowCountMySQL(t, targetDSN, "widgets", 2, 30*time.Second) {
				t.Fatalf("phase A: bulk-copy never landed seed rows")
			}

			applyDDLMySQL(t, sourceDSN, "ALTER TABLE widgets RENAME COLUMN name TO product_name;")
			applyDDLMySQL(t, sourceDSN, "INSERT INTO widgets (id, product_name) VALUES (3, 'gamma');")

			if !waitForRowCountMySQL(t, targetDSN, "widgets", 3, 60*time.Second) {
				t.Fatalf("phase B (%s): post-RENAME row never landed", tc.name)
			}

			tgtDB, err := sql.Open("mysql", targetDSN)
			if err != nil {
				t.Fatalf("open target: %v", err)
			}
			defer func() { _ = tgtDB.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			var hasNew, hasOld int
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'product_name'`).Scan(&hasNew); err != nil {
				t.Fatalf("check product_name: %v", err)
			}
			if hasNew != 1 {
				t.Errorf("target widgets.product_name column missing — RENAME apply didn't fire")
			}
			if err := tgtDB.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM information_schema.columns
				WHERE table_schema = DATABASE() AND table_name = 'widgets' AND column_name = 'name'`).Scan(&hasOld); err != nil {
				t.Fatalf("check name: %v", err)
			}
			if hasOld != 0 {
				t.Errorf("target widgets.name column still present — RENAME left both columns")
			}

			streamCancel()
			select {
			case <-runErr:
			case <-time.After(15 * time.Second):
				t.Fatal("Streamer.Run did not return after ctx cancel")
			}
		})
	}
}
