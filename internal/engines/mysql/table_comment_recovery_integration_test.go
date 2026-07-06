//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration round-trip pin for the TABLE_COMMENT NUL-truncation fix.
// information_schema.tables.TABLE_COMMENT C-string-truncates a table comment at
// its first NUL byte (exactly like COLUMN_DEFAULT); SHOW CREATE TABLE is
// faithful. The reader now recovers a non-empty comment from SHOW CREATE and
// decodes its `COMMENT='…'` clause. This test asserts a NUL-bearing comment
// round-trips byte-exact MySQL→MySQL, and that a plain-comment / no-comment
// table is unaffected (the amortization gate — a no-comment table pays no extra
// SHOW CREATE — is pinned purely by TestTablesNeedingShowCreate).

package mysql

import (
	"context"
	"testing"
	"time"
)

func TestTableComment_NULRecovery_RoundTrip_MySQLToMySQL(t *testing.T) {
	srcDSN, srcCleanup := newSharedDB(t, "sluice_tblcmt_src")
	defer srcCleanup()
	tgtDSN, tgtCleanup := newSharedDB(t, "sluice_tblcmt_tgt")
	defer tgtCleanup()

	// Raw Go string so `\0` reaches MySQL as the literal escape it decodes to a
	// real NUL. info_schema TABLE_COMMENT truncates `a\0b` to `a`; SHOW CREATE
	// keeps the full value.
	applyDDL(t, srcDSN, "CREATE TABLE nul_cmt (id INT NOT NULL, PRIMARY KEY (id)) "+
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='before\\0after';")
	applyDDL(t, srcDSN, "CREATE TABLE plain_cmt (id INT NOT NULL, PRIMARY KEY (id)) "+
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='a plain comment';")
	applyDDL(t, srcDSN, "CREATE TABLE no_cmt (id INT NOT NULL, PRIMARY KEY (id)) "+
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	wantComments := map[string]string{
		"nul_cmt":   "before\x00after", // recovered faithfully from SHOW CREATE
		"plain_cmt": "a plain comment", // unaffected
		"no_cmt":    "",                // no comment, no extra recovery
	}

	srcSchema := readMySQLSchema(ctx, t, srcDSN)
	for name, want := range wantComments {
		if got := requireTable(t, srcSchema, name).Comment; got != want {
			t.Errorf("source table %q comment = %q; want %q", name, got, want)
		}
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, tgtDSN)
	if err != nil {
		t.Fatalf("open target writer: %v", err)
	}
	defer closeIf(sw)
	if err := sw.CreateTablesWithoutConstraints(ctx, srcSchema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints on target: %v", err)
	}

	tgtSchema := readMySQLSchema(ctx, t, tgtDSN)
	for name, want := range wantComments {
		if got := requireTable(t, tgtSchema, name).Comment; got != want {
			t.Errorf("target table %q comment = %q; want %q (byte-exact round-trip)", name, got, want)
		}
	}
}
