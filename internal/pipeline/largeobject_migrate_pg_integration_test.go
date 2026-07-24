//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 205 end-to-end pin: the large-object census's NAMED-SUSPECT branch
// must be reachable on a real run. An in-scope oid/lo column is itself an
// unsupported column type that the schema read refuses loudly — pre-fix the
// census ran AFTER the schema read, so the refusal always preceded it and
// the named WARN was dead code end-to-end (only the unit seam, fed
// hand-built schemas the pipeline can never produce, exercised it). The
// census now runs BEFORE ReadSchema, so the operator sees the WARN naming
// every suspect table.column (the large-object context: why the oid column
// exists, what the blobs mean, the recovery recipes) and THEN the unchanged
// loud type refusal. The quieter no-suspect branch keeps its
// WARN-and-proceed contract.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestMigrate_PG_LargeObjectSuspectWarnPrecedesOidRefusal(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, `
		CREATE TABLE docs (
			id      BIGINT PRIMARY KEY,
			content OID
		);
		CREATE TABLE plain (
			id  BIGINT PRIMARY KEY,
			qty INTEGER
		);
		INSERT INTO docs (id, content) VALUES (1, lo_from_bytea(0, '\xdeadbeef'));
		INSERT INTO plain (id, qty) VALUES (1, 42);
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Full scope: the oid column is in scope, so the run must still REFUSE
	// at the schema read (oid is genuinely unsupported — the refusal is the
	// pre-existing loud floor) — but the named-suspect WARN must land FIRST
	// so the refusal arrives with its large-object context.
	logs := captureLogs(t)
	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	err := mig.Run(ctx)
	if err == nil {
		t.Fatal("Migrator.Run = nil; want the oid unsupported-type refusal (an in-scope oid column must stay loud)")
	}
	if !strings.Contains(err.Error(), `unsupported data_type "oid"`) {
		t.Fatalf("Migrator.Run error = %v; want the postgres unsupported data_type oid refusal", err)
	}
	out := logs.String()
	for _, want := range []string{
		"1 large object(s)",
		"docs.content", // the named suspect — dead code pre-fix (Bug 205)
		"docs/type-mapping.md",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("named-suspect WARN missing %q (Bug 205: the census must run before the schema-read refusal):\n%s", want, out)
		}
	}

	// Quieter-branch regression: excluding the suspect table keeps the
	// advisory's WARN-and-proceed contract — the los still exist, so the
	// quieter WARN fires, nothing is named, and the run succeeds.
	logs = captureLogs(t)
	filter, err := migcore.NewTableFilter(nil, []string{"docs"})
	if err != nil {
		t.Fatalf("NewTableFilter: %v", err)
	}
	mig = &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
		Filter:    filter,
	}
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run with --exclude-table docs = %v; want nil (quieter branch proceeds)", err)
	}
	out = logs.String()
	if !strings.Contains(out, "no in-scope column is typed oid/lo") {
		t.Errorf("quieter WARN missing:\n%s", out)
	}
	if strings.Contains(out, "docs.content") {
		t.Errorf("excluded suspect must not be named:\n%s", out)
	}
}
