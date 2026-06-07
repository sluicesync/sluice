// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestWithDatabase verifies the per-database DSN clone (ADR-0074):
// only the database component changes; host/credentials/params survive.
func TestWithDatabase(t *testing.T) {
	const base = "root:pw@tcp(127.0.0.1:3306)/source_db?parseTime=true"
	got, err := Engine{}.WithDatabase(base, "billing")
	if err != nil {
		t.Fatalf("WithDatabase: %v", err)
	}
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatalf("re-parse clone DSN %q: %v", got, err)
	}
	if cfg.DBName != "billing" {
		t.Errorf("DBName = %q; want billing", cfg.DBName)
	}
	if cfg.User != "root" || cfg.Passwd != "pw" {
		t.Errorf("credentials not preserved: user=%q pass=%q", cfg.User, cfg.Passwd)
	}
	if cfg.Addr != "127.0.0.1:3306" {
		t.Errorf("addr = %q; want 127.0.0.1:3306", cfg.Addr)
	}
}

// TestWithDatabaseFromServerDSN verifies a database-LESS server DSN can
// be specialized to a concrete database (the multi-database entry case
// where the operator gave no database in the source DSN).
func TestWithDatabaseFromServerDSN(t *testing.T) {
	const server = "root:pw@tcp(127.0.0.1:3306)/"
	got, err := Engine{}.WithDatabase(server, "app_a")
	if err != nil {
		t.Fatalf("WithDatabase: %v", err)
	}
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if cfg.DBName != "app_a" {
		t.Errorf("DBName = %q; want app_a", cfg.DBName)
	}
}

// TestParseServerDSNAllowsEmptyDatabase confirms the database-optional
// parse used by the lister/deriver accepts a DSN with no database, while
// the strict parseDSN still refuses one.
func TestParseServerDSNAllowsEmptyDatabase(t *testing.T) {
	const server = "root:pw@tcp(127.0.0.1:3306)/"
	if _, err := parseServerDSN(server); err != nil {
		t.Errorf("parseServerDSN should accept a database-less DSN; got %v", err)
	}
	if _, err := parseDSN(server); err == nil {
		t.Error("parseDSN should still refuse a database-less DSN")
	} else if !strings.Contains(err.Error(), "database name") {
		t.Errorf("parseDSN err = %v; want a 'database name' message", err)
	}
}

// TestSystemDatabasesExcluded pins the always-excluded set (ADR-0074):
// the four server-internal databases must never be user-migratable.
func TestSystemDatabasesExcluded(t *testing.T) {
	for _, sys := range []string{"information_schema", "performance_schema", "mysql", "sys"} {
		if _, ok := systemDatabases[sys]; !ok {
			t.Errorf("%q missing from systemDatabases", sys)
		}
	}
	if _, ok := systemDatabases["app_a"]; ok {
		t.Error("user database app_a wrongly in systemDatabases")
	}
}

// TestEnsureDatabaseEmptyNameRefuses ensures EnsureDatabase refuses an
// empty database name before any connection attempt.
func TestEnsureDatabaseEmptyNameRefuses(t *testing.T) {
	err := Engine{}.EnsureDatabase(t.Context(), "root:pw@tcp(127.0.0.1:3306)/", "")
	if err == nil || !strings.Contains(err.Error(), "database name is empty") {
		t.Fatalf("err = %v; want empty-name refusal", err)
	}
}
