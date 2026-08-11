//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The expression-escaping matrix, ground-truthed end to end against a
// real MySQL 8 (the 2026-08-11 escaping chunk; filed in the audit
// backlog as the CHECK read-back escaping defect, live since v0.97.0).
//
// The read half: information_schema renders a stored expression as its
// SHOW CREATE spelling plus ONE extra escaping layer (' → \', \ → \\),
// and the predecessor's blind `\'` → `'` replace mangled every literal
// whose VALUE carries an apostrophe — and silently mis-valued every
// backslash-carrying literal on a PG target. The write half: the IR now
// carries the SQL-standard spelling (apostrophes doubled, backslashes
// bare), and the MySQL emit boundary re-escapes backslashes per the
// writer's resolved sql_mode.
//
// The independent expected values here are the server's own: the raw
// information_schema bytes (premise leg), the round-tripped table's
// CHECK enforcement on INSERT (a value the source pattern accepts must
// land; one it refuses must refuse), and the generated column's computed
// value — never sluice's own rendering compared against itself.
package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaReader_ExpressionEscapingMatrix(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	// The fixture is a Go RAW string, so its bytes reach the server
	// verbatim: '^a\\d$' below is the standard-mode SQL spelling of the
	// pattern value `^a\d$`. (Shell-mediated fixtures are unusable for
	// this work — the measurement sessions lost backslashes to quoting
	// layers twice before this was written down.)
	applyDDL(t, dsn, `
		CREATE TABLE esc (
			c VARCHAR(40),
			g VARCHAR(60) AS (CONCAT(c, '''s')) STORED,
			d VARCHAR(60) DEFAULT (CONCAT('x''y', c)),
			CONSTRAINT esc_apos CHECK (c REGEXP '^o''brien$')
		);
		CREATE TABLE escb (
			c VARCHAR(40),
			CONSTRAINT escb_bs CHECK (c REGEXP '^a\\d$')
		);
		CREATE INDEX esc_fidx ON esc ((CONCAT(c, '''s')));
	`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// ---- Premise leg: the information_schema layer is what the decoder
	// ---- was built against. If MySQL ever changes this rendering, the
	// ---- decoder's premise is gone and this fails before anything
	// ---- downstream mis-decodes quietly.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var rawClause string
	if err := db.QueryRowContext(
		ctx, `
		SELECT cc.CHECK_CLAUSE
		FROM information_schema.CHECK_CONSTRAINTS cc
		WHERE cc.CONSTRAINT_SCHEMA = DATABASE() AND cc.CONSTRAINT_NAME = 'esc_apos'`,
	).Scan(&rawClause); err != nil {
		t.Fatalf("read raw CHECK_CLAUSE: %v", err)
	}
	if !strings.Contains(rawClause, `\\\'`) {
		t.Fatalf("raw CHECK_CLAUSE = %q; expected the measured I_S escape layer (interior apostrophe as \\\\\\') — "+
			"the decoder's premise does not hold on this server", rawClause)
	}
	t.Run("premise_is_rendering_is_mode_invariant", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'`); err != nil {
			t.Fatalf("set NBE: %v", err)
		}
		var nbeClause string
		if err := conn.QueryRowContext(
			ctx, `
			SELECT cc.CHECK_CLAUSE
			FROM information_schema.CHECK_CONSTRAINTS cc
			WHERE cc.CONSTRAINT_SCHEMA = DATABASE() AND cc.CONSTRAINT_NAME = 'esc_apos'`,
		).Scan(&nbeClause); err != nil {
			t.Fatalf("read under NBE: %v", err)
		}
		if nbeClause != rawClause {
			t.Fatalf("CHECK_CLAUSE differs by reading session's sql_mode:\n  default: %q\n  NBE:     %q\n"+
				"the decoder assumes the rendering is mode-invariant", rawClause, nbeClause)
		}
	})
	t.Run("premise_nbe_refuses_apostrophe_check_at_ddl", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("conn: %v", err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'`); err != nil {
			t.Fatalf("set NBE: %v", err)
		}
		_, err = conn.ExecContext(ctx,
			`CREATE TABLE esc_nbe (c VARCHAR(10), CONSTRAINT nbe_apos CHECK (c REGEXP '^o''brien$'))`)
		if err == nil {
			t.Fatal("MySQL accepted an apostrophe-carrying CHECK under NO_BACKSLASH_ESCAPES — the measured " +
				"DDL-time refusal (the reason that matrix axis collapses) no longer holds; re-derive the matrix")
		}
	})

	// ---- The read half: the IR must hold the portable spelling.
	r, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := r.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema, err := r.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tables := map[string]*ir.Table{}
	for _, tt := range schema.Tables {
		tables[tt.Name] = tt
	}
	esc, escb := tables["esc"], tables["escb"]
	if esc == nil || escb == nil {
		t.Fatalf("fixture tables missing from read schema: esc=%v escb=%v", esc != nil, escb != nil)
	}

	wantChecks := map[string]string{
		"esc_apos": `regexp_like(c,'^o''brien$')`,
		"escb_bs":  `regexp_like(c,'^a\d$')`,
	}
	for _, tbl := range []*ir.Table{esc, escb} {
		for _, chk := range tbl.CheckConstraints {
			want, tracked := wantChecks[chk.Name]
			if !tracked {
				continue
			}
			if chk.Expr != want {
				t.Errorf("CHECK %s Expr = %q; want %q (portable spelling: '' doubling, bare backslash)",
					chk.Name, chk.Expr, want)
			}
			delete(wantChecks, chk.Name)
		}
	}
	for name := range wantChecks {
		t.Errorf("CHECK %s missing from the read schema", name)
	}
	var gcol *ir.Column
	for _, c := range esc.Columns {
		if c.Name == "g" {
			gcol = c
		}
	}
	if gcol == nil || gcol.GeneratedExpr != `concat(c,'''s')` {
		t.Errorf("generated g = %+v; want GeneratedExpr %q", gcol, `concat(c,'''s')`)
	}

	// ---- The write half + the server-enforcement leg: round-trip the
	// ---- schema through the MySQL SchemaWriter onto a second database
	// ---- and let the TARGET SERVER prove the predicates survived.
	rtDSN, rtCleanup := newSharedDB(t, "sluice_esc_rt")
	defer rtCleanup()
	w, err := Engine{}.OpenSchemaWriter(ctx, rtDSN)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := w.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	rtSchema := &ir.Schema{Tables: []*ir.Table{esc, escb}}
	if err := w.CreateTablesWithoutConstraints(ctx, rtSchema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints round trip: %v", err)
	}
	if err := w.CreateIndexes(ctx, rtSchema); err != nil {
		t.Fatalf("CreateIndexes round trip: %v", err)
	}
	if err := w.CreateConstraints(ctx, rtSchema); err != nil {
		t.Fatalf("CreateConstraints round trip: %v", err)
	}
	rtDB, err := sql.Open("mysql", rtDSN)
	if err != nil {
		t.Fatalf("open rt: %v", err)
	}
	defer rtDB.Close()

	if _, err := rtDB.ExecContext(ctx, `INSERT INTO esc (c) VALUES ('o''brien')`); err != nil {
		t.Errorf("the round-tripped CHECK refused the value the source pattern accepts: %v", err)
	}
	if _, err := rtDB.ExecContext(ctx, `INSERT INTO esc (c) VALUES ('nope')`); err == nil {
		t.Error("the round-tripped CHECK accepted a value the source pattern refuses — the predicate was weakened in transit")
	}
	var gval string
	if err := rtDB.QueryRowContext(ctx, `SELECT g FROM esc WHERE c = 'o''brien'`).Scan(&gval); err != nil {
		t.Fatalf("read generated value: %v", err)
	}
	if gval != "o'brien's" {
		t.Errorf("generated column computed %q; want %q", gval, "o'brien's")
	}
	if _, err := rtDB.ExecContext(ctx, `INSERT INTO escb (c) VALUES ('a7')`); err != nil {
		t.Errorf("backslash-pattern CHECK refused 'a7' (pattern ^a\\d$ should accept it): %v", err)
	}
	if _, err := rtDB.ExecContext(ctx, `INSERT INTO escb (c) VALUES ('ax')`); err == nil {
		t.Error(`backslash-pattern CHECK accepted 'ax' — the backslash was eaten on emit and '^a\d$' became '^ad$'`)
	}

	// ---- Idempotent loop: reading the round-tripped schema must yield
	// ---- the identical portable spellings (no escaping drift per hop).
	r2, err := Engine{}.OpenSchemaReader(ctx, rtDSN)
	if err != nil {
		t.Fatalf("OpenSchemaReader rt: %v", err)
	}
	defer func() {
		if c, ok := r2.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	schema2, err := r2.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema rt: %v", err)
	}
	for _, tt := range schema2.Tables {
		for _, chk := range tt.CheckConstraints {
			switch chk.Name {
			case "esc_apos":
				if chk.Expr != `regexp_like(c,'^o''brien$')` {
					t.Errorf("second-hop CHECK esc_apos = %q; escaping drifts per hop", chk.Expr)
				}
			case "escb_bs":
				if chk.Expr != `regexp_like(c,'^a\d$')` {
					t.Errorf("second-hop CHECK escb_bs = %q; escaping drifts per hop", chk.Expr)
				}
			}
		}
	}
}
