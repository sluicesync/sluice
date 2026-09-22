//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-6 follow-up (value-fidelity review FOLLOW-UP 3): MySQL VIRTUAL
// generated columns landing on a PostgreSQL 18 target, across the shapes
// PG 18 refuses on a VIRTUAL column and accepts on STORED. Before the
// follow-up the emitter promoted only for target<18 or a plain index on
// the column, so an expression-indexed VIRTUAL column and an ENUM-typed
// one both failed on 18+ (CREATE INDEX: "indexes on virtual generated
// columns are not supported"; CREATE TABLE: "cannot have a user-defined
// type") where STORED used to work.
//
// Three columns on one MySQL table:
//   - plain_v  VIRTUAL, nothing on it       → lands VIRTUAL ('v')
//   - idx_v    VIRTUAL, functional index     → promoted statically ('s')
//   - enum_v   VIRTUAL, MySQL ENUM type      → promoted statically ('s');
//                                              Bug 25 renders it TEXT + CHECK
//
// The extension-function cell the review asked for (an ADR-0016
// translation landing on pgcrypto's digest(), which PG 18 refuses on
// VIRTUAL as a user-defined function) cannot be built from a MySQL
// source: `--enable-pg-extension` is refused for non-PG sources (ADR-0032,
// "only supported on PG sources"), so SHA1/SHA2 never rewrite to digest()
// on this path and would fail at CREATE TABLE as an unknown function
// whatever the storage class — a pre-existing limitation, not a VIRTUAL
// one. That class is bound writer-level on the same pinned server by
// TestGeneratedColumns_VirtualPromotionRetry_PG18 (engines/postgres),
// which drives the CREATE TABLE retry with a real user-defined function
// and a domain in the expression.
//
// The independent expected value is the target's pg_attribute.attgenerated
// and the migrated values themselves.

package pipeline

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
)

func TestMigrate_MySQLToPostgres18_VirtualGeneratedShapes(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgresImage(t, "postgres:18")
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, `
		CREATE TABLE docs (
			id      BIGINT NOT NULL AUTO_INCREMENT,
			qty     INT    NOT NULL,
			mood    ENUM('calm','busy') NOT NULL,
			plain_v INT    GENERATED ALWAYS AS (qty * 3) VIRTUAL,
			idx_v   INT    GENERATED ALWAYS AS (qty * 5) VIRTUAL,
			enum_v  ENUM('calm','busy') GENERATED ALWAYS AS (mood) VIRTUAL,
			PRIMARY KEY (id),
			INDEX docs_idx_v_expr ((idx_v + 1))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO docs (qty, mood) VALUES (2, 'calm'), (3, 'busy');
	`)

	mysqlEng, _ := engines.Get("mysql")
	pgEng, _ := engines.Get("postgres")

	buf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	mig := &Migrator{Source: mysqlEng, Target: pgEng, SourceDSN: mysqlSource, TargetDSN: pgTarget}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
	logs := buf.String()

	tgt, err := sql.Open("pgx", pgTarget)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = tgt.Close() }()

	gen := map[string]string{}
	rows, err := tgt.QueryContext(ctx, `
		SELECT a.attname, a.attgenerated::text
		FROM   pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
		WHERE  c.relname = 'docs' AND a.attnum > 0 AND NOT a.attisdropped`)
	if err != nil {
		t.Fatalf("read attgenerated: %v", err)
	}
	for rows.Next() {
		var name, g string
		if err := rows.Scan(&name, &g); err != nil {
			t.Fatalf("scan: %v", err)
		}
		gen[name] = g
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if gen["plain_v"] != "v" {
		t.Errorf("plain_v attgenerated = %q; want v (nothing blocks VIRTUAL on PG 18)", gen["plain_v"])
	}
	if gen["idx_v"] != "s" {
		t.Errorf("idx_v attgenerated = %q; want s (expression-indexed: promoted statically)", gen["idx_v"])
	}
	if gen["enum_v"] != "s" {
		t.Errorf("enum_v attgenerated = %q; want s (user-defined column type: promoted statically)", gen["enum_v"])
	}
	if n := strings.Count(logs, "GENERATED-VIRTUAL-PROMOTED-TO-STORED"); n < 2 {
		t.Errorf("expected a promotion WARN for idx_v and for enum_v; got %d:\n%s", n, logs)
	}

	// The values compute on the target for every column.
	var plain, idx int
	var enumV string
	if err := tgt.QueryRowContext(ctx, `SELECT plain_v, idx_v, enum_v FROM docs WHERE id = 1`).Scan(&plain, &idx, &enumV); err != nil {
		t.Fatalf("read migrated row: %v", err)
	}
	if plain != 6 || idx != 10 || enumV != "calm" {
		t.Errorf("migrated row id=1: plain_v=%d idx_v=%d enum_v=%q; want 6, 10, calm", plain, idx, enumV)
	}
	// The index over the promoted column built (the whole reason for the
	// static promotion).
	var idxCount int
	if err := tgt.QueryRowContext(ctx, `SELECT count(*) FROM pg_indexes WHERE tablename = 'docs' AND indexdef LIKE '%idx_v%'`).Scan(&idxCount); err != nil {
		t.Fatalf("count indexes: %v", err)
	}
	if idxCount != 1 {
		t.Errorf("expression index over idx_v: %d on the target; want 1", idxCount)
	}
}
