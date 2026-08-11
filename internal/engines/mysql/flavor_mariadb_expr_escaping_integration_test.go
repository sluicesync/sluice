//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MariaDB arm of the expression-escaping matrix (pre-v0.120.0
// value-fidelity review, GAP 1).
//
// normalizeExprForFlavor is a family dispatch in the Bug-74 sense: the
// MySQL-family input carries information_schema's extra escaping layer
// and the MariaDB family does not, and the 2026-08-11 chunk pinned only
// the MySQL family against a live server — MariaDB's side rested on a
// one-time measurement recorded in unit fixtures. If any MariaDB line
// changed its I_S rendering (grew the MySQL-style layer, or stored
// as-typed text), the SHOW CREATE-variant decode would silently alter
// literal values — the same silent class the chunk cured. This test is
// the premise pin: the raw I_S bytes themselves are asserted (no extra
// layer), the rendering is asserted mode-invariant, and the reader's IR
// spellings are held across every supported LTS line, so a per-line
// rendering change fails a named test instead of silently mis-decoding.
package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestMariaDB_ExpressionEscaping_PremiseAndDecode(t *testing.T) {
	for _, image := range mariadbLTSImages() {
		image := image
		t.Run(image, func(t *testing.T) {
			dsn := newMariaDB(t, image, "mdb_esc")
			execSQLScript(t, dsn, `
				CREATE TABLE esc (
					c VARCHAR(40),
					g VARCHAR(60) AS (CONCAT(c, '''s')) STORED,
					d VARCHAR(60) DEFAULT (CONCAT('x''y', c)),
					CONSTRAINT esc_apos CHECK (c REGEXP '^o''brien$'),
					CONSTRAINT esc_bs   CHECK (c REGEXP '^a\\d$')
				);
			`)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			// ---- Premise: MariaDB's I_S renders the SHOW CREATE spelling
			// ---- with NO extra escaping layer. An interior apostrophe is
			// ---- \' (one backslash), never MySQL's \\\' — if this fails,
			// ---- normalizeExprForFlavor's MariaDB arm is decoding the
			// ---- wrong input shape and values are being altered.
			readClause := func(conn interface {
				QueryRowContext(context.Context, string, ...any) *sql.Row
			}, name string,
			) string {
				var clause string
				if err := conn.QueryRowContext(
					ctx, `
					SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS
					WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ?`, name,
				).Scan(&clause); err != nil {
					t.Fatalf("read %s: %v", name, err)
				}
				return clause
			}
			apos := readClause(db, "esc_apos")
			if !strings.Contains(apos, `\'`) || strings.Contains(apos, `\\\'`) {
				t.Fatalf("esc_apos CHECK_CLAUSE = %q; want the bare SHOW CREATE spelling (interior apostrophe "+
					"as \\', never the MySQL I_S layer's \\\\\\') — the MariaDB decode premise does not hold on %s",
					apos, image)
			}
			bs := readClause(db, "esc_bs")
			if !strings.Contains(bs, `\\d`) || strings.Contains(bs, `\\\\d`) {
				t.Fatalf("esc_bs CHECK_CLAUSE = %q; want the SHOW CREATE spelling's \\\\d, never the MySQL "+
					"layer's quadrupled form — premise does not hold on %s", bs, image)
			}

			// ---- Premise: the rendering is invariant to the READING
			// ---- session's sql_mode (the decoder never consults it).
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("conn: %v", err)
			}
			defer conn.Close()
			if _, err := conn.ExecContext(ctx, `SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'`); err != nil {
				t.Fatalf("set NBE: %v", err)
			}
			if nbe := readClause(conn, "esc_apos"); nbe != apos {
				t.Fatalf("CHECK_CLAUSE differs by reading session's sql_mode on %s:\n  default: %q\n  NBE:     %q",
					image, apos, nbe)
			}

			// ---- The decode: the reader's IR spellings, portable form.
			eng := Engine{Flavor: FlavorMariaDB}
			r, err := eng.OpenSchemaReader(ctx, dsn)
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
			var esc *ir.Table
			for _, tt := range schema.Tables {
				if tt.Name == "esc" {
					esc = tt
				}
			}
			if esc == nil {
				t.Fatal("table esc missing from read schema")
			}
			wantChecks := map[string]string{
				"esc_apos": `c regexp '^o''brien$'`,
				"esc_bs":   `c regexp '^a\d$'`,
			}
			for _, chk := range esc.CheckConstraints {
				want, tracked := wantChecks[chk.Name]
				if !tracked {
					continue
				}
				if chk.Expr != want {
					t.Errorf("%s: CHECK %s Expr = %q; want %q (portable spelling)", image, chk.Name, chk.Expr, want)
				}
				delete(wantChecks, chk.Name)
			}
			for name := range wantChecks {
				t.Errorf("%s: CHECK %s missing from the read schema", image, name)
			}
			for _, c := range esc.Columns {
				switch c.Name {
				case "g":
					if c.GeneratedExpr != `concat(c,'''s')` {
						t.Errorf("%s: generated g = %q; want %q", image, c.GeneratedExpr, `concat(c,'''s')`)
					}
				case "d":
					if de, ok := c.Default.(ir.DefaultExpression); !ok || de.Expr != `concat('x''y',c)` {
						t.Errorf("%s: default d = %#v; want DefaultExpression %q", image, c.Default, `concat('x''y',c)`)
					}
				}
			}
		})
	}
}
