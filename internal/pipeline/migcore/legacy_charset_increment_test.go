// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestCapturedBeforeCharsetDecode pins the version boundary: v0.156.2 and
// earlier captured non-UTF-8 change-stream text as stored bytes, v0.156.3
// is the first release that converts it.
func TestCapturedBeforeCharsetDecode(t *testing.T) {
	for v, want := range map[string]bool{
		"0.156.2": true, "v0.156.2": true, "0.156.0": true, "0.155.9": true, "0.99.292": true,
		"0.156.3": false, "0.156.10": false, "0.157.0": false, "1.0.0": false,
		"dev": false, "": false, "0.156": false,
	} {
		if got := capturedBeforeCharsetDecode(v); got != want {
			t.Errorf("capturedBeforeCharsetDecode(%q) = %v; want %v", v, got, want)
		}
	}
}

// TestWarnLegacyCharsetIncrement names exactly the non-UTF-8 string
// columns of a pre-fix MySQL-family incremental, and stays quiet for a
// fixed version, a Postgres source, and a UTF-8-only schema.
func TestWarnLegacyCharsetIncrement(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "t", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "l1", Type: ir.Varchar{Length: 8, Charset: "latin1"}},
		{Name: "u8", Type: ir.Text{Charset: "utf8mb4"}},
		{Name: "sj", Type: ir.Char{Length: 4, Charset: "sjis"}},
	}}}}
	run := func(m *irbackup.Manifest) string {
		prev := slog.Default()
		defer slog.SetDefault(prev)
		var buf logcapture.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		WarnLegacyCharsetIncrement(context.Background(), "chain restore", m)
		return buf.String()
	}

	out := run(&irbackup.Manifest{Kind: irbackup.BackupKindIncremental, SourceEngine: "mysql", SluiceVersion: "0.156.2", Schema: schema})
	if !strings.Contains(out, LegacyCharsetIncrementMarker) || !strings.Contains(out, "l1 (latin1)") || !strings.Contains(out, "sj (sjis)") {
		t.Fatalf("pre-fix mysql incremental: log = %q; want the marker naming l1 and sj", out)
	}
	if !strings.Contains(out, "U+FFFD") || !strings.Contains(out, "different character") {
		t.Errorf("the WARN must name both recorded forms (U+FFFD and a different character): %q", out)
	}
	if strings.Contains(out, "u8") || strings.Contains(out, "id") {
		t.Errorf("the WARN named a column the pre-fix stream carried faithfully: %q", out)
	}
	for name, m := range map[string]*irbackup.Manifest{
		"fixed version":              {Kind: irbackup.BackupKindIncremental, SourceEngine: "mysql", SluiceVersion: "0.156.3", Schema: schema},
		"postgres source":            {Kind: irbackup.BackupKindIncremental, SourceEngine: "postgres", SluiceVersion: "0.156.2", Schema: schema},
		"no schema":                  {Kind: irbackup.BackupKindIncremental, SourceEngine: "mysql", SluiceVersion: "0.156.2"},
		"a full":                     {Kind: irbackup.BackupKindFull, SourceEngine: "mysql", SluiceVersion: "0.156.2", Schema: schema},
		"legacy empty kind (a full)": {SourceEngine: "mysql", SluiceVersion: "0.156.2", Schema: schema},
	} {
		if out := run(m); out != "" {
			t.Errorf("%s: logged %q; want nothing", name, out)
		}
	}
}
