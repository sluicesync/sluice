//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 A0909-PG-MEDIUM-1 on a real PostgreSQL: `ALTER TABLE …
// SET SCHEMA` on a captured table writes its DDL marker under the NEW
// schema (pg_event_trigger_ddl_commands().schema_name reports the
// destination), the capture trigger moves with the table, and the reader's
// schema check discarded the marker before the observed-DDL refusal could
// fire — the stream ran on at exit 0 with the target frozen, after one
// CAPTURE-OUT-OF-SCOPE WARN. At the base commit the marker halted the
// stream loudly. The relation's OID survives the move; the reader now
// grades a foreign-schema marker by it.

package pgtrigger

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_DDLRefusal_SetSchema(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	applyPGSQL(t, dsn, `CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT); CREATE SCHEMA other;`)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"t"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	e := Engine{}
	reader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	out, err := reader.StreamChanges(ctx, ir.Position{})
	if err != nil {
		t.Fatalf("StreamChanges: %v", err)
	}
	cdc, _ := reader.(*CDCReader)
	if cdc == nil || len(cdc.capturedRelIDs) == 0 {
		t.Fatalf("the reader did not load its captured relations' OIDs at open; the cell below would grade the wrong regime")
	}

	applyPGSQL(t, dsn, `ALTER TABLE public.t SET SCHEMA other;`)
	// A write through the moved table: the trigger moved with it and the
	// row lands under the new schema — the shape the base commit ran on
	// silently after discarding the marker.
	applyPGSQL(t, dsn, `INSERT INTO other.t (id, label) VALUES (1, 'after-move');`)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, ok := <-out:
			if !ok {
				err := cdc.Err()
				if err == nil {
					t.Fatal("channel closed but Err = nil; want the observed-DDL refusal")
				}
				if !contains(err.Error(), "DDL") {
					t.Errorf("Err = %v; want the observed-DDL refusal", err)
				}
				return
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("the stream ran on for 10s after ALTER TABLE … SET SCHEMA on a captured table: the DDL marker was " +
		"discarded by the schema check and the halt became a silent stall (A0909-PG-MEDIUM-1)")
}
