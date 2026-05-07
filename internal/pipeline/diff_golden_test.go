// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestDiffer_Golden_Text snapshots the human-readable diff output
// against testdata/diff/users_drift.txt. Same -update mechanism as
// TestPreviewer_Golden_Text — operators editing the diff formatter
// regenerate via -update, review the diff, and commit.
func TestDiffer_Golden_Text(t *testing.T) {
	src := &previewStubEngine{
		name: "postgres",
		schema: &ir.Schema{Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: false},
					{Name: "created_at", Type: ir.Timestamp{Precision: 6}, Nullable: false, Default: ir.DefaultExpression{Expr: "now()"}},
				},
				Indexes: []*ir.Index{
					{Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}, Unique: true},
				},
			},
		}},
	}
	tgt := &previewStubEngine{
		name: "postgres",
		schema: &ir.Schema{Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
					{Name: "email", Type: ir.Varchar{Length: 100}, Nullable: false}, // narrowed
					// created_at missing
				},
				Indexes: []*ir.Index{
					{Name: "users_email_idx", Columns: []ir.IndexColumn{{Column: "email"}}, Unique: true},
				},
			},
			{
				Name:    "deprecated_log",
				Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
			},
		}},
		// A PG-flavour column-def renderer for the missing-column
		// suggestion path. Doesn't exercise every IR type — the
		// engine packages have their own ddl_emit tests for that —
		// but is enough to confirm the diff renderer threads the
		// emitter's output into the ALTER suggestion.
		emitColDef: func(_ *ir.Table, c *ir.Column) (string, error) {
			frag := `"` + c.Name + `" ` + c.Type.String()
			if !c.Nullable {
				frag += " NOT NULL"
			}
			if e, ok := c.Default.(ir.DefaultExpression); ok {
				frag += " DEFAULT " + e.Expr
			}
			return frag, nil
		},
	}

	var buf bytes.Buffer
	d := &Differ{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
	}
	if _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	goldenPath := filepath.Join("testdata", "diff", "users_drift.txt")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(goldenPath, buf.Bytes(), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden %q: %v\nrun with -update to create", goldenPath, err)
	}
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("golden mismatch in %s\n--- got ---\n%s\n--- want ---\n%s",
			goldenPath, buf.String(), string(want))
	}
}
