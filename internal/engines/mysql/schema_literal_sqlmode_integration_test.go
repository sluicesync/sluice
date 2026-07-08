//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Live pin for audit finding N-5: MySQL's schema-metadata literal PRINTING is
// sql_mode-independent, so the reader-side decoders (scanMySQLQuotedString and
// everything above it) are correctly UNCONDITIONAL — unlike the emit side,
// which must thread backslashIsMySQLEscape because the server PARSES literals
// mode-dependently.
//
// Ground truth (2026-07-08, MySQL 8.0.46 + 8.4.10, {default, NBE} read session
// × {default, NBE} creation session — all four cells byte-identical):
//
//   - SHOW CREATE TABLE (binary defaults, column/table comments) and
//     information_schema COLUMN_TYPE (ENUM/SET labels) always render with the
//     fixed escape set `\0 \n \r \\` + doubled `''`, NO_BACKSLASH_ESCAPES or
//     not. A hypothetical NBE-aware "raw" decode would CORRUPT: `'C:\\temp'`
//     read raw is two backslashes.
//   - information_schema COLUMN_DEFAULT / COLUMN_COMMENT / TABLE_COMMENT
//     arrive already decoded (no escapes) in both modes; TABLE_COMMENT (and
//     binary COLUMN_DEFAULT) NUL-truncate, COLUMN_COMMENT does not.
//
// This test pins that invariant on a real server for every literal-bearing
// decode family — binary defaults (quoted + hex forms), ENUM + SET labels,
// table comment (incl. the NUL-recovery path), column comment, string
// (VARCHAR) default — under BOTH reader session modes, with the NBE session
// reached through the REAL --mysql-sql-mode plumbing layer
// (Engine.WithSQLMode → openDB, the exact path cli.go's applyEngineOptions
// drives — the Bug-180 "pin through the CLI layer" lesson) and PROVEN on the
// reader's own connection via @@SESSION.sql_mode. If a future MySQL version
// starts rendering these literals mode-dependently, this fails loudly instead
// of the reader silently recovering wrong schema-metadata bytes.

package mysql

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestSchemaLiteralDecode_SQLModeMatrix_ByteExact(t *testing.T) {
	srcDSN, cleanup := newSharedDB(t, "sluice_nbe_literal_decode")
	defer cleanup()

	// Backslash shapes per family: literal `\t` (backslash + t), `C:\temp`
	// (path), trailing backslash, doubled backslash, a REAL 0x09 (the
	// escape-sequence-written contrast), and a NUL. The DDL is written under
	// the default (backslash-escaping) session applyDDL opens, so every
	// backslash below is doubled once for Go and once for MySQL's lexer.
	//
	// Stored ground truth (verified live via SELECT HEX(...) during the N-5
	// investigation):
	//   b = 00 5C 74 21   v = 5C 74 00 7A   h = FF 5C 74
	//   e labels = "a\tb"(real tab), `a\tb`, `C:\temp`, `end\`, `a\\b`, nul+0x00+x
	//   f labels = `r\w`, "plain"
	//   s = `C:\temp`     c comment = `C:\temp`     table comment = `end\`
	applyDDL(t, srcDSN, "CREATE TABLE meta (\n"+
		"  id INT NOT NULL,\n"+
		"  b  BINARY(4)    DEFAULT '\\0\\\\t!',\n"+
		"  v  VARBINARY(8) DEFAULT '\\\\t\\0z',\n"+
		"  h  BINARY(3)    DEFAULT 0xFF5C74,\n"+
		"  e  ENUM('a\tb','a\\\\tb','C:\\\\temp','end\\\\','a\\\\\\\\b','nul\\0x') NOT NULL,\n"+
		"  f  SET('r\\\\w','plain') NOT NULL,\n"+
		"  s  VARCHAR(20) DEFAULT 'C:\\\\temp',\n"+
		"  c  INT COMMENT 'C:\\\\temp',\n"+
		"  PRIMARY KEY (id)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='end\\\\';")
	applyDDL(t, srcDSN, "CREATE TABLE nul_cmt (id INT NOT NULL, PRIMARY KEY (id)) "+
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='x\\0y';")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The RAW bytes the IR must hold — identical under both reader modes.
	wantEnum := []string{"a\tb", `a\tb`, `C:\temp`, `end\`, `a\\b`, "nul\x00x"}
	wantSet := []string{`r\w`, "plain"}
	wantDefaults := map[string]ir.DefaultValue{
		"b": ir.DefaultExpression{Expr: "0x005C7421", Dialect: hexLiteralDialect},
		"v": ir.DefaultExpression{Expr: "0x5C74007A", Dialect: hexLiteralDialect},
		"h": ir.DefaultExpression{Expr: "0xFF5C74", Dialect: hexLiteralDialect},
		"s": ir.DefaultLiteral{Value: `C:\temp`},
	}

	modes := []struct {
		name    string
		eng     ir.Engine
		wantNBE bool
	}{
		// Override-free engine: the strict default (no NBE) — the control leg.
		{"default_strict", Engine{}, false},
		// The operator's --mysql-sql-mode with NO_BACKSLASH_ESCAPES, applied
		// through the same WithSQLMode builder cli.go's applyEngineOptions uses.
		{"no_backslash_escapes", Engine{}.WithSQLMode(defaultStrictSQLMode + ",NO_BACKSLASH_ESCAPES"), true},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			sr, err := m.eng.OpenSchemaReader(ctx, srcDSN)
			if err != nil {
				t.Fatalf("open reader: %v", err)
			}
			r := sr.(*SchemaReader)
			defer func() { _ = r.Close() }()

			// Plumbing pin: the reader's OWN connection must actually run
			// under the claimed mode — a green decode on the wrong session
			// would pin nothing (the Bug-180 shape).
			var sessionMode string
			if err := r.db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode").Scan(&sessionMode); err != nil {
				t.Fatalf("read reader session sql_mode: %v", err)
			}
			if got := strings.Contains(sessionMode, "NO_BACKSLASH_ESCAPES"); got != m.wantNBE {
				t.Fatalf("reader session sql_mode = %q; NO_BACKSLASH_ESCAPES presence = %v, want %v", sessionMode, got, m.wantNBE)
			}

			schema, err := r.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("read schema: %v", err)
			}
			meta := requireTable(t, schema, "meta")

			if got := enumValues(t, meta, "e"); !reflect.DeepEqual(got, wantEnum) {
				t.Errorf("ENUM labels = %q; want %q", got, wantEnum)
			}
			if got := enumValues(t, meta, "f"); !reflect.DeepEqual(got, wantSet) {
				t.Errorf("SET labels = %q; want %q", got, wantSet)
			}
			for col, want := range wantDefaults {
				if got := columnByName(t, meta, col).Default; !reflect.DeepEqual(got, want) {
					t.Errorf("column %q default = %#v; want %#v", col, got, want)
				}
			}
			if got := columnByName(t, meta, "c").Comment; got != `C:\temp` {
				t.Errorf("column comment = %q; want %q", got, `C:\temp`)
			}
			if got := meta.Comment; got != `end\` {
				t.Errorf("table comment = %q; want %q", got, `end\`)
			}
			// The SHOW CREATE recovery leg: TABLE_COMMENT NUL-truncates in
			// information_schema, so this value only survives via the
			// decodeMySQLQuotedString path over SHOW CREATE output.
			if got := requireTable(t, schema, "nul_cmt").Comment; got != "x\x00y" {
				t.Errorf("NUL-bearing table comment = %q; want %q", got, "x\x00y")
			}
		})
	}
}

// columnByName returns tbl's column named col, failing the test if absent.
func columnByName(t *testing.T, tbl *ir.Table, col string) *ir.Column {
	t.Helper()
	for _, c := range tbl.Columns {
		if c.Name == col {
			return c
		}
	}
	t.Fatalf("table %q missing column %q", tbl.Name, col)
	return nil
}
