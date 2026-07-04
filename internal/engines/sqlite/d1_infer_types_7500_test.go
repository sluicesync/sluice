// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Differential pin for the D1 code-7500 class (the v0.99.167 escape, ADR-0145):
// live D1 rejects the --infer-types char-class GLOB validation patterns with
// HTTP 400 code 7500 ("LIKE or GLOB pattern too complex") while local modernc
// accepts the SAME SQL — a divergence that was only ever discoverable on a real
// D1 until this pin. The mock D1 here rejects char-class GLOBs exactly like the
// live service, and the tests pin BOTH legs: the direct (--no-stage-local) path
// hits the rejection LOUDLY, and the --stage-local path — auto-engaged for a D1
// source when --infer-types is set (cmd/sluice/cli.go, autoStage) — never sends
// a char-class GLOB to D1 at all, so the identical validation runs locally to
// completion.

package sqlite

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// glob7500Handler wraps an inner mock-D1 handler, rejecting any statement that
// carries a char-class GLOB pattern the way live D1 does: HTTP 400 with the
// standard envelope carrying code 7500. (D1's SQLite build caps LIKE/GLOB
// pattern length/complexity; the char-class conformance GLOBs — isoDateGlob,
// isoDateTimeGlob, uuidGlob — all exceed it, size-independently. modernc has no
// such cap, which is exactly the divergence under pin.) rejected counts the
// rejections so a test can assert a path NEVER trips the class.
func glob7500Handler(inner d1Handler, rejected *atomic.Int64) d1Handler {
	return func(sqlStr string, params []string) (int, []byte) {
		if strings.Contains(sqlStr, "GLOB") && strings.Contains(sqlStr, "[") {
			if rejected != nil {
				rejected.Add(1)
			}
			return http.StatusBadRequest, d1Err(7500, "LIKE or GLOB pattern too complex")
		}
		return inner(sqlStr, params)
	}
}

// seedInferSource seeds the modernc db backing the mock D1: name-hint candidate
// columns whose data conforms, so ONLY the transport decides the outcome.
func seedInferSource(t *testing.T) *sql.DB {
	t.Helper()
	path := seedDB(
		t,
		`CREATE TABLE ev (id INTEGER PRIMARY KEY, created_at TEXT, ref_uuid TEXT, flag INTEGER)`,
		`INSERT INTO ev VALUES
			(1, '2024-01-15 10:30:00', '550e8400-e29b-41d4-a716-446655440000', 1),
			(2, '2024-02-20 08:00:00', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 0)`,
	)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestD1InferTypes_Code7500_DirectPathRefusesLoudly pins the pre-v0.99.167
// shape at the engine seam: against a D1 that rejects char-class GLOBs, the
// direct-transport --infer-types validation (what --no-stage-local runs) fails
// LOUDLY for the GLOB-using families (temporal, uuid) — naming the HTTP status
// and the D1 code-7500 message — while the non-GLOB families (boolean, JSON)
// still validate over the same transport. Pristine conforming data on a tiny
// table: the rejection is pattern-complexity, not data or size.
func TestD1InferTypes_Code7500_DirectPathRefusesLoudly(t *testing.T) {
	src := seedInferSource(t)
	client := startMockD1(t, glob7500Handler(execD1Handler(src), nil))
	r := &D1SchemaReader{client: client}
	ctx := context.Background()

	for _, tc := range []struct {
		col    string
		target ir.Type
	}{
		{"created_at", ir.Timestamp{}},
		{"ref_uuid", ir.UUID{}},
	} {
		t.Run(tc.col, func(t *testing.T) {
			_, _, _, err := r.ValidateInferredType(ctx, "ev", tc.col, tc.target)
			if err == nil {
				t.Fatalf("%s: want a loud code-7500 refusal from the direct D1 path; got nil (a silent pass here means the mock no longer mirrors live D1)", tc.col)
			}
			if !strings.Contains(err.Error(), "HTTP 400") || !strings.Contains(err.Error(), "7500") ||
				!strings.Contains(err.Error(), "pattern too complex") {
				t.Errorf("%s: err = %q; must surface the HTTP status and the D1 code-7500 message", tc.col, err)
			}
		})
	}

	// Differential control: the boolean check (NOT IN, no GLOB) runs to
	// completion over the SAME rejecting transport — the failure class is the
	// char-class GLOB, nothing else.
	conforms, resolved, validated, err := r.ValidateInferredType(ctx, "ev", "flag", ir.Boolean{})
	if err != nil {
		t.Fatalf("boolean over the rejecting transport: %v (only GLOB statements should be rejected)", err)
	}
	if !conforms || validated != 2 || resolved != (ir.Boolean{}) {
		t.Errorf("boolean: conforms=%v validated=%d resolved=%v; want true/2/Boolean", conforms, validated, resolved)
	}
}

// TestD1InferTypes_Code7500_StageLocalAvoids pins the shipped default: staging
// the same D1 locally (what --infer-types auto-engages for a D1 source) issues
// ZERO char-class GLOBs against D1 — the staging read is CAST/typeof pages only
// — and the identical GLOB validation then runs to completion on the staged
// file, promoting exactly what a limit-free D1 would have.
func TestD1InferTypes_Code7500_StageLocalAvoids(t *testing.T) {
	src := seedInferSource(t)
	var rejected atomic.Int64
	client := startMockD1(t, glob7500Handler(execD1Handler(src), &rejected))
	ctx := context.Background()

	dest := filepath.Join(t.TempDir(), "staged.db")
	if err := stageD1ClientToLocalFile(ctx, client, dest, nil); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if n := rejected.Load(); n != 0 {
		t.Fatalf("staging sent %d char-class GLOB statement(s) to D1 — the stage-local path must never trip the code-7500 class", n)
	}

	sr, err := Engine{}.OpenSchemaReader(ctx, dest)
	if err != nil {
		t.Fatalf("open staged reader: %v", err)
	}
	r := sr.(*SchemaReader)
	t.Cleanup(func() { _ = r.Close() })

	for _, tc := range []struct {
		col          string
		target       ir.Type
		wantResolved ir.Type
	}{
		{"created_at", ir.Timestamp{}, ir.Timestamp{Precision: 6, WithTimeZone: false}},
		{"ref_uuid", ir.UUID{}, ir.UUID{}},
	} {
		t.Run(tc.col, func(t *testing.T) {
			conforms, resolved, validated, err := r.ValidateInferredType(ctx, "ev", tc.col, tc.target)
			if err != nil {
				t.Fatalf("%s on the staged file: %v", tc.col, err)
			}
			if !conforms || validated != 2 || resolved != tc.wantResolved {
				t.Errorf("%s: conforms=%v validated=%d resolved=%v; want true/2/%v",
					tc.col, conforms, validated, resolved, tc.wantResolved)
			}
		})
	}
}
