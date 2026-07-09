//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end protocol pins for the ADR-0153 flavor default, through the
// engine REGISTRY (the layer the CLI actually resolves — the Bug-180
// lesson) against a real MySQL server. Two observables:
//
//   - the server's own Com_stmt_prepare counter: a bulk write through the
//     planetscale-flavor writer on a param-free DSN must issue ZERO
//     COM_STMT_PREPAREs (interpolation — 1 COM_QUERY round trip per
//     statement), while the same write under an explicit
//     interpolateParams=false must prepare at least once (binary
//     protocol). This observes the WIRE, not a config field.
//   - the change applier's retained pipelineCfg (the cfg the ADR-0104
//     concurrent lane pool re-opens from): the resolved protocol must
//     reach it so lane connections inherit the same protocol as the
//     serial pool.

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// comStmtPrepareCount reads the server-global Com_stmt_prepare counter on a
// throwaway probe connection. Global (not session) because the writer under
// test owns its own pool; in-package integration tests run sequentially
// against the shared container, so the delta is attributable.
func comStmtPrepareCount(t *testing.T, dsn string) int64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("probe open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var name string
	var val int64
	if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Com_stmt_prepare'").Scan(&name, &val); err != nil {
		t.Fatalf("Com_stmt_prepare probe: %v", err)
	}
	return val
}

// TestInterpolationFlavorDefault_ProtocolPin_EndToEnd drives the registry's
// planetscale engine at a real server and pins which statement protocol the
// bulk write actually used, per DSN shape.
func TestInterpolationFlavorDefault_ProtocolPin_EndToEnd(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_proto")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}

	rows := make([]ir.Row, 200)
	for i := range rows {
		rows[i] = ir.Row{"id": int64(i + 1), "v": fmt.Sprintf("row-%d", i)}
	}

	run := func(tableName, dsnParam string) int64 {
		t.Helper()
		applyDDL(t, dsn, fmt.Sprintf("CREATE TABLE %s (id BIGINT NOT NULL, v VARCHAR(32) NULL, PRIMARY KEY (id)) ENGINE=InnoDB;", tableName))
		tbl := readTableIR(t, ctx, dsn, tableName)
		rw, err := eng.OpenRowWriter(ctx, dsn+dsnParam)
		if err != nil {
			t.Fatalf("OpenRowWriter(%s): %v", dsnParam, err)
		}
		defer closeIf(rw)

		before := comStmtPrepareCount(t, dsn)
		in := make(chan ir.Row, len(rows))
		for _, r := range rows {
			in <- r
		}
		close(in)
		if err := rw.WriteRows(ctx, tbl, in); err != nil {
			t.Fatalf("WriteRows(%s): %v", dsnParam, err)
		}
		return comStmtPrepareCount(t, dsn) - before
	}

	if delta := run("proto_default", ""); delta != 0 {
		t.Errorf("planetscale flavor, param-free DSN: %d COM_STMT_PREPAREs on the wire; want 0 (the ADR-0153 interpolation default)", delta)
	}
	if delta := run("proto_explicit", "&interpolateParams=false"); delta == 0 {
		t.Error("planetscale flavor, explicit interpolateParams=false: 0 COM_STMT_PREPAREs; the explicit binary-protocol opt-out did not reach the wire")
	}
}

// TestInterpolationFlavorDefault_ApplierLanePoolCfg pins that the resolved
// protocol reaches the change applier's retained pipelineCfg — the config
// the ADR-0104 concurrent key-hash lane pool re-opens from — for every
// flavor × DSN-shape cell. A flip that reached the serial pool but not the
// lane pool would silently split the CDC apply path across protocols.
func TestInterpolationFlavorDefault_ApplierLanePoolCfg(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_lane")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cases := []struct {
		engine string
		param  string
		want   bool
	}{
		{"planetscale", "", true},
		{"planetscale", "&interpolateParams=false", false},
		{"vitess", "", true},
		{"mysql", "", false},
		// The vanilla DSN opt-in (docs/throughput-tuning.md): the resolved
		// state — not the flavor — is what reaches every derived pool.
		{"mysql", "&interpolateParams=true", true},
	}
	for _, c := range cases {
		eng, ok := engines.Get(c.engine)
		if !ok {
			t.Fatalf("engine %q not registered", c.engine)
		}
		a, err := eng.OpenChangeApplier(ctx, dsn+c.param)
		if err != nil {
			t.Fatalf("OpenChangeApplier(%s%s): %v", c.engine, c.param, err)
		}
		got := a.(*ChangeApplier).pipelineCfg.InterpolateParams
		closeApplier(a)
		if got != c.want {
			t.Errorf("%s with param %q: pipelineCfg.InterpolateParams = %v; want %v", c.engine, c.param, got, c.want)
		}
	}
}

// TestInterpolationFlavorDefault_RowReaderKeepsBinaryProtocol pins the
// ADR-0153 read-fidelity exemption end-to-end: a planetscale-flavor
// RowReader on a param-free DSN must keep the prepared/binary protocol for
// its arg-bearing PK-paged reads — because MySQL's FLOAT→text conversion
// does not round-trip float32 (stored 8388608 renders "8388610"), a
// text-protocol chunk read would silently display-round every FLOAT
// column. The pin seeds the poster-child value and requires the EXACT
// stored float32 back, plus wire evidence (COM_STMT_PREPAREs > 0) that the
// read really ran prepared.
func TestInterpolationFlavorDefault_RowReaderKeepsBinaryProtocol(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "interp_readbin")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applyDDL(t, dsn, "CREATE TABLE fl_read (id BIGINT NOT NULL, fl FLOAT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;")
	seed, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	defer func() { _ = seed.Close() }()
	// Bind as a binary-protocol param so the stored float32 is exactly 2^23.
	if _, err := seed.ExecContext(ctx, "INSERT INTO fl_read (id, fl) VALUES (?, ?)", int64(1), float64(1<<23)); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	eng, ok := engines.Get("planetscale")
	if !ok {
		t.Fatal("planetscale engine not registered")
	}
	rr, err := eng.OpenRowReader(ctx, dsn) // param-free: the exemption must hold
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer closeIf(rr)
	reader := rr.(*RowReader)
	tbl := readTableIR(t, ctx, dsn, "fl_read")

	before := comStmtPrepareCount(t, dsn)
	// after-cursor bound arg → the statement shape the protocol governs.
	ch, err := reader.ReadRowsBatch(ctx, tbl, []any{int64(0)}, 10)
	if err != nil {
		t.Fatalf("ReadRowsBatch: %v", err)
	}
	var got []ir.Row
	for row := range ch {
		got = append(got, row)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("ReadRowsBatch stream: %v", err)
	}
	if delta := comStmtPrepareCount(t, dsn) - before; delta == 0 {
		t.Error("planetscale-flavor chunk read issued 0 COM_STMT_PREPAREs; the read-fidelity exemption did not hold (text-protocol FLOAT display-rounding would follow)")
	}
	if len(got) != 1 {
		t.Fatalf("read %d rows; want 1", len(got))
	}
	fl, ok := got[0]["fl"].(float64)
	if !ok || fl != float64(1<<23) {
		t.Errorf("fl read back as %#v (%T); want exactly %v — the FLOAT display-rounding class leaked into the chunked read", got[0]["fl"], got[0]["fl"], float64(1<<23))
	}
}
