//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The per-server `lower_case_table_names` memo, against a real server: the
// ENVIRONMENTAL PREMISE that licenses it, and the reuse it exists for.
//
// The memo's safety argument is "one answer per server, and it cannot change
// under a running process". That is a fact about MySQL, not about sluice, so
// CLAUDE.md's premise-naming step says it owes a check in the same change
// rather than a sentence in a comment. This file is that check.

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestLowerCaseTableNamesIsReadOnlyAtRuntime is the premise check. If a server
// ever let this be set at runtime, memoising it per process would be a silent
// correctness bug — two databases created either side of the change would fold
// differently while sluice used one cached answer — so the memo's own comment
// points here.
//
// It asserts on the SERVER's refusal (error 1238, "read only variable"), never
// on sluice's behaviour, so nothing about it can be satisfied by the code under
// test.
func TestLowerCaseTableNamesIsReadOnlyAtRuntime(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var current int
	if err := db.QueryRowContext(ctx, "SELECT @@global.lower_case_table_names").Scan(&current); err != nil {
		t.Fatalf("read lower_case_table_names: %v", err)
	}
	// Ask for the OTHER value, so a server that happened to accept the current
	// one as a no-op cannot pass this vacuously.
	other := 1
	if current != 0 {
		other = 0
	}

	_, err = db.ExecContext(ctx, "SET GLOBAL lower_case_table_names = ?", other)
	if err == nil {
		t.Fatalf("the server ACCEPTED `SET GLOBAL lower_case_table_names = %d` (it was %d). The per-server "+
			"memo in lower_case_table_names_memo.go caches this for the life of the process on the premise "+
			"that it cannot change under a running server — that premise is now false and the memo must go, "+
			"or be scoped to something shorter than a process", other, current)
	}
	if msg := strings.ToLower(err.Error()); !strings.Contains(msg, "read only") {
		t.Fatalf("SET GLOBAL lower_case_table_names failed with %q; the memo's premise is specifically that "+
			"it is REFUSED AS READ-ONLY, not that it happened to fail", err)
	}
}

// TestLowerCaseTableNames_MemoisedPerServer pins the reuse itself, and does it
// by an observation that only holds if no connection was made: after one real
// read warms the memo, a DSN with DELIBERATELY WRONG CREDENTIALS to the same
// server still answers. A memo that had regressed to a per-call read would
// surface the authentication failure instead.
//
// The cost this represents is the point: both callers of lowerCaseTableNames
// are per-namespace and one is a fan-out, so before the memo a 200-database run
// made ~400 connect/read/close cycles that could not disagree.
func TestLowerCaseTableNames_MemoisedPerServer(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	resetLCTMemo(t)

	e := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	want, err := e.lowerCaseTableNames(ctx, dsn)
	if err != nil {
		t.Fatalf("first read: %v", err)
	}

	// Same server, credentials that cannot authenticate. Reaching the server
	// is the failure this asserts against.
	broken := breakDSNCredentials(t, dsn)
	if _, err := sql.Open("mysql", broken); err != nil {
		t.Fatalf("the broken DSN must still PARSE, or this test proves nothing: %v", err)
	}
	got, err := e.lowerCaseTableNames(ctx, broken)
	if err != nil {
		t.Fatalf("the second read connected instead of using the memo (%v). Both callers are per-namespace "+
			"and one is a fan-out, so this is ~2 connections per database", err)
	}
	if got != want {
		t.Errorf("memoised lower_case_table_names = %d; want %d", got, want)
	}
}

// breakDSNCredentials rewrites a DSN's password to one that cannot
// authenticate, leaving net/address — the memo key — untouched.
func breakDSNCredentials(t *testing.T, dsn string) string {
	t.Helper()
	at := strings.LastIndex(dsn, "@")
	if at < 0 {
		t.Fatalf("DSN %q has no credential section to break", dsn)
	}
	colon := strings.Index(dsn[:at], ":")
	if colon < 0 {
		t.Fatalf("DSN %q has no password to break", dsn)
	}
	return dsn[:colon+1] + "definitely-not-the-password" + dsn[at:]
}
