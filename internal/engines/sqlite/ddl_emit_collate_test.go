// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

// Pin for the cross-engine collation-drop WARN on the SQLite emitter:
// SQLite's collation namespace (BINARY/NOCASE/RTRIM) shares no names
// with MySQL or PG and the SQLite reader never populates ir Collation,
// so EVERY carried collation is foreign here — dropped with one WARN
// per table (docs/type-mapping.md "Charsets and collations").

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestEmitTableDef_CrossEngineCollationWarn_SQLite(t *testing.T) {
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	tbl := &ir.Table{
		Name: "customers",
		Columns: []*ir.Column{
			{Name: "region_code", Type: ir.Text{Size: ir.TextLong, Collation: "C"}, Nullable: true},
			{Name: "title", Type: ir.Varchar{Length: 100, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, Nullable: true},
			{Name: "plain", Type: ir.Text{Size: ir.TextLong}, Nullable: true},
		},
	}
	def, err := emitTableDef(tbl)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if strings.Contains(def, "COLLATE") {
		t.Errorf("table def = %q; no source collation may reach SQLite DDL", def)
	}
	out := buf.String()
	if got := strings.Count(out, "source collations have no"); got != 1 {
		t.Fatalf("want exactly 1 per-table WARN; got %d:\n%s", got, out)
	}
	for _, want := range []string{"customers", "region_code (C)", "title (utf8mb4_0900_ai_ci)"} {
		if !strings.Contains(out, want) {
			t.Errorf("WARN should name %q; got %q", want, out)
		}
	}
	if strings.Contains(out, "plain") {
		t.Errorf("collation-free column must not be listed; got %q", out)
	}

	// A collation-free table stays WARN-free.
	buf.Reset()
	if _, err := emitTableDef(&ir.Table{
		Name:    "quiet",
		Columns: []*ir.Column{{Name: "body", Type: ir.Text{Size: ir.TextLong}, Nullable: true}},
	}); err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("collation-free table must not WARN; got %q", buf.String())
	}
}
