//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0056 diagnose surfaces on the
// Postgres engine: ListSchemaHistory enumerating live cdc-schema-
// history rows, and DiagnoseBundle producing a structured snapshot
// against a real PG server.

package postgres

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/diagnose"
	"sluicesync.dev/sluice/internal/ir"
)

// TestDiagnose_ListSchemaHistory_OrdersByCreatedAtDesc pins that the
// diagnose ListSchemaHistory surface returns rows ordered most-recent
// first and respects the cap. Cross-references ADR-0049 (schema-
// history storage) + ADR-0056 (bundle assembly).
func TestDiagnose_ListSchemaHistory_OrdersByCreatedAtDesc(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Insert three boundaries against the same stream. The diagnose
	// reader should pull the most-recent two when cap=2.
	for i, lsn := range []string{"0/1000000", "0/2000000", "0/3000000"} {
		anchor := ir.Position{Engine: "postgres", Token: lsn}
		tbl := &ir.Table{
			Schema: schema, Name: "widgets",
			Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
		}
		if err := writeSchemaVersion(ctx, db, schema, "stream-1", "src_app", "widgets", anchor, tbl); err != nil {
			t.Fatalf("writeSchemaVersion[%d]: %v", i, err)
		}
		// Sleep to ensure created_at ordering is deterministic.
		time.Sleep(20 * time.Millisecond)
	}

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	reader, ok := applier.(ir.SchemaHistoryReader)
	if !ok {
		t.Fatalf("PG applier does not implement ir.SchemaHistoryReader")
	}
	rows, err := reader.ListSchemaHistory(ctx, "stream-1", 2)
	if err != nil {
		t.Fatalf("ListSchemaHistory: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2 (cap respected)", len(rows))
	}
	// Most-recent first: 0/3000000 then 0/2000000.
	if rows[0].AnchorPosition != "0/3000000" {
		t.Errorf("row[0].AnchorPosition = %q, want 0/3000000 (most recent first)", rows[0].AnchorPosition)
	}
	if rows[1].AnchorPosition != "0/2000000" {
		t.Errorf("row[1].AnchorPosition = %q, want 0/2000000", rows[1].AnchorPosition)
	}
}

// TestDiagnose_ListSchemaHistory_AbsentTable returns an empty slice
// when sluice_cdc_schema_history does not exist yet. Pins the
// graceful-degrade contract — diagnose against a target that pre-
// dates ADR-0049 must still produce a useful bundle.
func TestDiagnose_ListSchemaHistory_AbsentTable(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Intentionally do NOT call ensureSchemaHistoryTable. The reader
	// should return an empty slice rather than an error.
	reader := applier.(ir.SchemaHistoryReader)
	rows, err := reader.ListSchemaHistory(ctx, "stream-1", 10)
	if err != nil {
		t.Fatalf("ListSchemaHistory on absent table: %v (want nil error)", err)
	}
	if len(rows) != 0 {
		t.Errorf("ListSchemaHistory returned %d rows; want 0 on absent table", len(rows))
	}
}

// TestDiagnose_DiagnoseBundle_EmbedsServerVersionAndState pins the
// engine-side probe shape: version() result lands in EngineVersion,
// pg_replication_slots query lands in EngineState (even an empty
// list — the probe ran, no slots exist yet).
func TestDiagnose_DiagnoseBundle_EmbedsServerVersionAndState(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
	sr, err := eng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	prober, ok := sr.(ir.DiagnoseProber)
	if !ok {
		t.Fatalf("PG SchemaReader does not implement ir.DiagnoseProber")
	}
	snap, err := prober.DiagnoseBundle(ctx, "stream-x")
	if err != nil {
		t.Fatalf("DiagnoseBundle: %v", err)
	}
	if snap.EngineName != "postgres" {
		t.Errorf("EngineName = %q, want postgres", snap.EngineName)
	}
	if !strings.Contains(snap.EngineVersion, "PostgreSQL") {
		t.Errorf("EngineVersion = %q; want it to contain 'PostgreSQL'", snap.EngineVersion)
	}
	if len(snap.EngineState) == 0 {
		t.Errorf("EngineState is empty; want JSON payload")
	}
	var state map[string]any
	if err := json.Unmarshal(snap.EngineState, &state); err != nil {
		t.Fatalf("unmarshal EngineState: %v", err)
	}
	if state["stream_id_scope"] != "stream-x" {
		t.Errorf("EngineState.stream_id_scope = %v, want stream-x", state["stream_id_scope"])
	}
}

// TestDiagnose_BundleEndToEnd_AssemblesAgainstLivePG is the end-to-end
// integration pin: cold-start a PG target with a sluice_cdc_state row,
// run the diagnose assembler against it, verify the resulting ZIP
// contains a manifest, a state dump, and a redacted target DSN.
//
// This is the "Validate end-to-end before building more" tenet for
// the diagnose feature.
func TestDiagnose_BundleEndToEnd_AssemblesAgainstLivePG(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}

	// Ensure the cdc_state control table with the full sluice schema
	// shape (slot_name, source_dsn_fingerprint, target_schema columns
	// the ListStreams query expects), then seed a row.
	{
		applier, err := eng.OpenChangeApplier(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenChangeApplier: %v", err)
		}
		if err := applier.EnsureControlTable(ctx); err != nil {
			t.Fatalf("EnsureControlTable: %v", err)
		}
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	applyPGApplier(t, dsn, `
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position)
		VALUES ('e2e-stream', '0/ABCDEF01');
	`)

	var buf bytes.Buffer
	err := diagnose.Write(ctx, &buf, diagnose.Request{
		StreamID:        "e2e-stream",
		PrivacyLevel:    diagnose.PrivacyStandard,
		TargetEngine:    eng,
		TargetDSN:       dsn,
		SluiceVersion:   "v0.74.2-test",
		SluiceCommit:    "deadbeef",
		SluiceBuildDate: "2026-05-22",
		CLIArgs:         []string{"diagnose", "--target", dsn},
		Now:             time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("diagnose.Write: %v", err)
	}

	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}
	files := map[string][]byte{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("zip Open(%s): %v", f.Name, err)
		}
		var b bytes.Buffer
		if _, err := b.ReadFrom(rc); err != nil {
			t.Fatalf("read %s: %v", f.Name, err)
		}
		_ = rc.Close()
		files[f.Name] = b.Bytes()
	}

	// Manifest present + carries the redacted DSN.
	mf := files["bundle.json"]
	if mf == nil {
		t.Fatalf("bundle.json missing from end-to-end bundle; files = %v", keys(files))
	}
	var m diagnose.Manifest
	if err := json.Unmarshal(mf, &m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if m.TargetDSNRedacted == "" {
		t.Errorf("manifest TargetDSNRedacted empty")
	}
	if strings.Contains(m.TargetDSNRedacted, "@") {
		t.Errorf("manifest TargetDSNRedacted leaks userinfo: %q", m.TargetDSNRedacted)
	}

	// State dump present and contains the seeded stream-id.
	state := files["state/cdc_state.json"]
	if state == nil {
		t.Fatalf("state/cdc_state.json missing from end-to-end bundle")
	}
	if !strings.Contains(string(state), "e2e-stream") {
		t.Errorf("state dump did not include the seeded stream-id; got %s", string(state))
	}
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
