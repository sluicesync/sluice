//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 A0909-PG-MEDIUM-1 on a real PostgreSQL: a DDL marker
// for a table this stream captures must reach the observed-DDL refusal
// even when it arrives under a FOREIGN schema.
//
// `ALTER TABLE … SET SCHEMA` writes the marker under the destination
// schema (pg_event_trigger_ddl_commands().schema_name reports where the
// table landed) and the capture trigger moves with the table, so the
// reader's schema check — which exists so a decoy relation in a schema
// another role controls cannot halt this stream — discarded it. The
// refusal never fired, the trigger kept writing rows the reader dropped
// after one CAPTURE-OUT-OF-SCOPE WARN, and the stream ran on at exit 0
// with the target frozen. At v0.144.0 the marker halted loudly.
//
// The discriminator is the OID of the CAPTURED relation the command
// concerns, which the capture function records on the marker and which
// survives the move. The cells below are the four shapes that identity
// has to get right, and three of them were found by the pre-tag
// value-fidelity review of the first cut — which recorded the command's
// OWN object id instead, so an index (objid = the index) and an ALTER on
// a partitioned parent (objid = the parent, triggers on the partitions)
// both recorded an OID the reader could never match, and a reader opened
// AFTER the move loaded a captured-OID set scoped to its own schema and
// so no longer contained the moved table at all.

package pgtrigger

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// expectDDLHalt opens a reader at resumeFrom, runs apply, and requires the
// stream to close with the observed-DDL refusal. A stream that keeps
// running is the defect: the marker was discarded and the halt became a
// silent stall.
func expectDDLHalt(t *testing.T, ctx context.Context, dsn string, resumeFrom ir.Position, cell string, apply func()) {
	t.Helper()
	reader, err := (Engine{}).OpenCDCReader(ctx, dsn)
	if err != nil {
		t.Fatalf("%s: OpenCDCReader: %v", cell, err)
	}
	cdc, ok := reader.(*CDCReader)
	if !ok {
		t.Fatalf("%s: OpenCDCReader returned %T; want *CDCReader", cell, reader)
	}
	defer func() { _ = cdc.Close() }()

	streamCtx, streamCancel := context.WithTimeout(ctx, 30*time.Second)
	defer streamCancel()
	out, err := cdc.StreamChanges(streamCtx, resumeFrom)
	if err != nil {
		t.Fatalf("%s: StreamChanges: %v", cell, err)
	}
	if len(cdc.capturedRelIDs) == 0 {
		t.Fatalf("%s: the reader loaded no captured relation OIDs at open; the cell below would grade the wrong regime", cell)
	}
	apply()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case _, open := <-out:
			if !open {
				err := cdc.Err()
				if err == nil {
					t.Fatalf("%s: the stream closed with Err = nil; want the observed-DDL refusal", cell)
				}
				if !contains(err.Error(), "DDL") {
					t.Errorf("%s: Err = %v; want the observed-DDL refusal", cell, err)
				}
				t.Logf("%s: halted as documented: %v", cell, err)
				return
			}
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatalf("%s: the stream ran on for 15s after DDL on a captured table whose marker arrives under a foreign "+
		"schema — the marker was discarded and the loud halt became a silent stall (A0909-PG-MEDIUM-1)", cell)
}

func TestCDCReader_DDLRefusal_ForeignSchemaMarkers(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	mustExec := func(stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}
	// A position anchored at the current tip, so a cell that moves a table
	// BEFORE opening its reader still has the marker pending.
	tipPos := func() ir.Position {
		t.Helper()
		var lastID int64
		if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(id),0) FROM public.sluice_change_log").Scan(&lastID); err != nil {
			t.Fatalf("read last id: %v", err)
		}
		p, err := encodePos(pgTriggerPos{LastID: lastID})
		if err != nil {
			t.Fatalf("encodePos: %v", err)
		}
		return p
	}

	mustExec(`CREATE SCHEMA other`)
	mustExec(`CREATE TABLE public.mv (id int PRIMARY KEY, v int)`)
	mustExec(`CREATE TABLE public.mr (id int PRIMARY KEY, v int)`)
	mustExec(`CREATE TABLE public.mi (id int PRIMARY KEY, v int)`)
	mustExec(`CREATE TABLE public.pp (id int, v int, PRIMARY KEY (id)) PARTITION BY RANGE (id)`)
	mustExec(`CREATE TABLE public.pp_p1 PARTITION OF public.pp FOR VALUES FROM (1) TO (100)`)
	// The supported route for a partitioned source: sync start and migrate
	// refuse a declaratively partitioned parent at preflight, so the
	// operator installs on the PARTITIONS.
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"mv", "mr", "mi", "pp_p1"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	t.Run("live stream: SET SCHEMA on a captured table", func(t *testing.T) {
		expectDDLHalt(t, ctx, dsn, tipPos(), "live/set-schema", func() {
			mustExec(`ALTER TABLE public.mv SET SCHEMA other`)
			mustExec(`INSERT INTO other.mv (id, v) VALUES (1, 1)`)
		})
	})

	t.Run("resume: the reader opens AFTER the move, with the marker pending", func(t *testing.T) {
		// The shape an operator actually meets: the stream halts on the
		// move, they restart it, and the resumed reader re-reads the same
		// marker. A captured-OID set scoped to the reader's own schema no
		// longer contains the moved table, so the second read let it
		// through — the halt would fire exactly once and then go quiet.
		from := tipPos()
		mustExec(`ALTER TABLE public.mr SET SCHEMA other`)
		mustExec(`INSERT INTO other.mr (id, v) VALUES (1, 1)`)
		expectDDLHalt(t, ctx, dsn, from, "resume/set-schema", func() {})
	})

	t.Run("CREATE INDEX on an already-moved captured table", func(t *testing.T) {
		// object_type='index', so the command's own object is the INDEX;
		// only the relation the index is ON carries a capture trigger.
		mustExec(`ALTER TABLE public.mi SET SCHEMA other`)
		from := tipPos()
		expectDDLHalt(t, ctx, dsn, from, "moved/create-index", func() {
			mustExec(`CREATE INDEX mi_v_idx ON other.mi (v)`)
		})
	})

	t.Run("ALTER on a partitioned parent that lives in another schema", func(t *testing.T) {
		// SET SCHEMA on a partitioned parent moves the PARENT only; the
		// captured partition stays in public. The command's own object is
		// then the parent, in a schema this stream does not read, while the
		// relation that carries the capture trigger is the partition.
		mustExec(`ALTER TABLE public.pp SET SCHEMA other`)
		var partSchema string
		if err := db.QueryRowContext(
			ctx,
			`SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 'pp_p1'`,
		).Scan(&partSchema); err != nil {
			t.Fatalf("read partition schema: %v", err)
		}
		if partSchema != "public" {
			t.Skipf("this server moved the partition with its parent (partition schema %q); the cell's premise does not hold here", partSchema)
		}
		from := tipPos()
		expectDDLHalt(t, ctx, dsn, from, "moved-parent/alter", func() {
			mustExec(`ALTER TABLE other.pp ALTER COLUMN v TYPE bigint`)
		})
	})
}
