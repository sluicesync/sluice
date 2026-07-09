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
