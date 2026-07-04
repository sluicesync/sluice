// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"sluicesync.dev/sluice/internal/engines"
)

// TestApplyEngineOptions_MySQL pins that applyEngineOptions applies the
// --mysql-sql-mode and --zero-date flags onto a MySQL engine (via the concrete
// engine's With* builders, task 2.5) and leaves the --sqlite-date-encoding arm
// inert (MySQL doesn't implement WithDateEncoding). It returns a NEW engine value
// (the registry's is untouched), mirroring labelEngine.
func TestApplyEngineOptions_MySQL(t *testing.T) {
	e, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	g := &Globals{
		MySQLSQLMode: "NO_BACKSLASH_ESCAPES",
		ZeroDate:     "null",
		// SQLiteDateEncoding is inert on MySQL; a value here must not error.
		SQLiteDateEncoding: "unixepoch",
	}
	got, err := applyEngineOptions(e, g)
	if err != nil {
		t.Fatalf("applyEngineOptions: %v", err)
	}
	if got.Name() != "mysql" {
		t.Errorf("engine Name() = %q; want mysql", got.Name())
	}
	// A bad --zero-date is refused loudly through the same path.
	if _, err := applyEngineOptions(e, &Globals{ZeroDate: "bogus"}); err == nil {
		t.Error("applyEngineOptions with --zero-date=bogus: err = nil; want a loud refusal")
	}
}

// TestApplyEngineOptions_SQLite pins the --sqlite-date-encoding arm on a SQLite
// engine, and that an invalid value refuses loudly.
func TestApplyEngineOptions_SQLite(t *testing.T) {
	e, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}
	got, err := applyEngineOptions(e, &Globals{SQLiteDateEncoding: "julian"})
	if err != nil {
		t.Fatalf("applyEngineOptions(sqlite): %v", err)
	}
	if got.Name() != "sqlite" {
		t.Errorf("engine Name() = %q; want sqlite", got.Name())
	}
	if _, err := applyEngineOptions(e, &Globals{SQLiteDateEncoding: "bogus"}); err == nil {
		t.Error("applyEngineOptions with --sqlite-date-encoding=bogus: err = nil; want a loud refusal")
	}
}

// TestApplyEngineOptions_Postgres pins the passthrough: Postgres implements none
// of the value-fidelity option interfaces, so it flows through unchanged even
// when every flag is set.
func TestApplyEngineOptions_Postgres(t *testing.T) {
	e, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	got, err := applyEngineOptions(e, &Globals{MySQLSQLMode: "X", ZeroDate: "null", SQLiteDateEncoding: "julian"})
	if err != nil {
		t.Fatalf("applyEngineOptions(postgres): %v", err)
	}
	if got != e {
		t.Errorf("applyEngineOptions should pass a non-option engine through unchanged; got %T", got)
	}
}
