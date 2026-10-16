//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestMigrate_MySQLToMySQL_ViewReadsTheTargetDatabase is Bug 314 on a real
// server. MySQL stores a view body with the database name written into every
// table reference, so a body copied verbatim into a differently named target
// database still names the source's. Here the source and target databases
// share one server, which is the silent shape: the target view resolves and
// serves the source's rows at exit 0. A row written to the source after the
// migrate is the discriminator: it must not appear through the target view.
// The control is a view reading a database outside the migration, which must
// stay qualified.
func TestMigrate_MySQLToMySQL_ViewReadsTheTargetDatabase(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQL(t)
	defer cleanup()
	applyMySQLDDL(t, srcDSN, `
		CREATE DATABASE IF NOT EXISTS outside_db;
		CREATE TABLE outside_db.ref (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
		INSERT INTO outside_db.ref VALUES (7);
		CREATE TABLE vq (id INT NOT NULL PRIMARY KEY, s VARCHAR(20)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO vq VALUES (1, 'a');
		CREATE VIEW v_own AS SELECT id, s FROM vq;
		CREATE VIEW v_join AS SELECT a.id, b.s FROM vq a JOIN vq b ON a.id = b.id;
		CREATE VIEW v_lit AS SELECT id, '`+"`source_db`.`vq`"+`' AS t FROM vq;
		CREATE VIEW v_outside AS SELECT id FROM outside_db.ref;
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	src, _ := engines.Get("mysql")
	mig := &Migrator{Source: src, Target: src, SourceDSN: srcDSN, TargetDSN: tgtDSN}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	applyMySQLDDL(t, srcDSN, `INSERT INTO vq VALUES (2, 'b');`)

	tgt, err := sql.Open("mysql", tgtDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = tgt.Close() }()
	for _, v := range []string{"v_own", "v_join"} {
		var n int
		if err := tgt.QueryRowContext(ctx, "SELECT count(*) FROM "+v).Scan(&n); err != nil {
			t.Fatalf("read %s on the target: %v", v, err)
		}
		if n != 1 {
			t.Errorf("target view %s returns %d rows; the target table holds 1, so the view is reading the source database", v, n)
		}
	}
	var lit string
	if err := tgt.QueryRowContext(ctx, "SELECT t FROM v_lit WHERE id = 1").Scan(&lit); err != nil {
		t.Fatalf("read v_lit on the target: %v", err)
	}
	if want := "`source_db`.`vq`"; lit != want {
		t.Errorf("v_lit's string literal changed: got %q, want %q", lit, want)
	}
	var def string
	if err := tgt.QueryRowContext(ctx,
		"SELECT view_definition FROM information_schema.views WHERE table_schema = DATABASE() AND table_name = 'v_outside'").Scan(&def); err != nil {
		t.Fatalf("read v_outside's definition: %v", err)
	}
	var outside int
	if err := tgt.QueryRowContext(ctx, "SELECT id FROM v_outside").Scan(&outside); err != nil || outside != 7 {
		t.Errorf("v_outside should still read outside_db.ref (id 7): got %d, %v (definition %s)", outside, err, def)
	}
}
