// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug 157 Q2 pins. emitCrossEngineTranslationNotices is the shared
// advisory-notice helper called by both the migrate path
// (phaseTranslateAndGateSchema) and the `sync` cold-start path
// (streamer_coldstart.go / streamer_multidb.go). These pins exercise the
// engine-pair matrix directly on the helper; the cross-engine sync
// integration test is the end-to-end coverage that the helper is actually
// wired into the cold-start dispatch before the copy.
//
// Pin the class, not the representative: the three scanners dispatch on
// OPPOSITE engine-pair directions — unsigned-bigint is MySQL→PG, while
// unconstrained-numeric and wide-varchar are PG→MySQL — so a single engine
// pair can never trigger all three. The matrix below pins each notice in
// its own applicable direction AND the same-engine no-op (the load-bearing
// "no false WARN on the lossless path" assertion, the Bug-157 ground
// truth).

// mysqlToPGNarrowingSchema models a MySQL→PG schema whose only
// cross-engine narrowing is the unsigned-bigint range loss (Bug 11): an
// AUTO_INCREMENT unsigned PK plus an unsigned-bigint FK. A 32-bit unsigned
// column must NOT be flagged.
func mysqlToPGNarrowingSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
			{Name: "customer_id", Type: ir.Integer{Width: 64, Unsigned: true}},
			{Name: "qty", Type: ir.Integer{Width: 32, Unsigned: true}}, // not 64-bit → not flagged
		},
	}}}
}

// pgToMySQLNarrowingSchema models a PG→MySQL schema that triggers BOTH the
// unconstrained-numeric widening (Bug 69) and the wide-varchar down-map
// (Bug 72) — the two PG→MySQL-direction notices the helper must emit
// together when both are present.
func pgToMySQLNarrowingSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "ledger",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "amount", Type: ir.Decimal{Unconstrained: true}}, // Bug 69
			{Name: "note", Type: ir.Varchar{Length: 70000}},         // Bug 72
		},
	}}}
}

// benignSchema has no cross-engine-narrowing columns in any direction — a
// plain signed-bigint PK and a narrow varchar.
func benignSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
	}}}
}

// captureWarnLogs swaps in a slog handler that records WARN-level records,
// restoring the previous default on cleanup. Returns a func that yields the
// captured buffer's current contents.
func captureWarnLogs(t *testing.T) func() string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf.String
}

func TestEmitCrossEngineTranslationNotices_UnsignedBigintMySQLToPG(t *testing.T) {
	logs := captureWarnLogs(t)
	emitCrossEngineTranslationNotices(context.Background(), mysqlToPGNarrowingSchema(), "mysql", "postgres", "sync cold-start")
	out := logs()
	if !strings.Contains(out, "bigint unsigned") {
		t.Errorf("MySQL→PG: missing unsigned-bigint WARN; logs:\n%s", out)
	}
	if !strings.Contains(out, "sync cold-start") {
		t.Errorf("MySQL→PG: WARN does not carry the mode label; logs:\n%s", out)
	}
	// The two PG→MySQL-direction notices must NOT fire in the MySQL→PG
	// direction.
	if strings.Contains(out, "DECIMAL") || strings.Contains(out, "down-mapped") {
		t.Errorf("MySQL→PG: spurious PG→MySQL-direction WARN; logs:\n%s", out)
	}
}

func TestEmitCrossEngineTranslationNotices_NumericAndVarcharPGToMySQL(t *testing.T) {
	logs := captureWarnLogs(t)
	emitCrossEngineTranslationNotices(context.Background(), pgToMySQLNarrowingSchema(), "postgres", "mysql", "migrate")
	out := logs()
	// Both PG→MySQL-direction notices must fire when both shapes are present.
	if !strings.Contains(out, "DECIMAL") {
		t.Errorf("PG→MySQL: missing unconstrained-numeric WARN; logs:\n%s", out)
	}
	if !strings.Contains(out, "down-mapped") {
		t.Errorf("PG→MySQL: missing wide-varchar WARN; logs:\n%s", out)
	}
	// The MySQL→PG-direction notice must NOT fire.
	if strings.Contains(out, "bigint unsigned") {
		t.Errorf("PG→MySQL: spurious MySQL→PG-direction WARN; logs:\n%s", out)
	}
}

// TestEmitCrossEngineTranslationNotices_SameEngineNoOp is the load-bearing
// Bug-157 ground truth: a same-engine sync (MySQL→MySQL or PG→PG) must emit
// ZERO notices even when the schema carries unsigned-bigint /
// unconstrained-numeric / wide-varchar columns — the bigint-unsigned→MySQL
// and varchar→PG paths are lossless, so a false WARN there would be the
// exact misdiagnosis Bug 157 was about.
func TestEmitCrossEngineTranslationNotices_SameEngineNoOp(t *testing.T) {
	cases := []struct {
		name           string
		schema         *ir.Schema
		source, target string
	}{
		{"mysql→mysql with unsigned bigint", mysqlToPGNarrowingSchema(), "mysql", "mysql"},
		{"planetscale→mysql with unsigned bigint", mysqlToPGNarrowingSchema(), "planetscale", "mysql"},
		{"postgres→postgres with unconstrained numeric + wide varchar", pgToMySQLNarrowingSchema(), "postgres", "postgres"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureWarnLogs(t)
			emitCrossEngineTranslationNotices(context.Background(), tc.schema, tc.source, tc.target, "sync cold-start")
			if out := logs(); strings.TrimSpace(out) != "" {
				t.Errorf("same-engine %s→%s emitted notices (want zero); logs:\n%s", tc.source, tc.target, out)
			}
		})
	}
}

// TestEmitCrossEngineTranslationNotices_NoneWhenBenign pins that a
// cross-engine pair with NO triggering columns emits nothing — the
// common case (values that fit) flows silently.
func TestEmitCrossEngineTranslationNotices_NoneWhenBenign(t *testing.T) {
	for _, pair := range [][2]string{{"mysql", "postgres"}, {"postgres", "mysql"}} {
		logs := captureWarnLogs(t)
		emitCrossEngineTranslationNotices(context.Background(), benignSchema(), pair[0], pair[1], "migrate")
		if out := logs(); strings.TrimSpace(out) != "" {
			t.Errorf("benign %s→%s emitted notices (want zero); logs:\n%s", pair[0], pair[1], out)
		}
	}
}
