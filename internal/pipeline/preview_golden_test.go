package pipeline

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// updateGolden controls whether the golden-file snapshot tests refresh
// their on-disk fixture from the live output. Run as:
//
//	go test -run TestPreviewer_Golden_Text -update ./internal/pipeline/
//
// The default (false) compares against the on-disk fixture and fails
// on mismatch. Operators editing the formatter regenerate via -update,
// review the diff, and commit.
var updateGolden = flag.Bool("update", false, "update golden files in testdata/")

// TestPreviewer_Golden_Text snapshots the human-readable preview
// output against testdata/preview/users_pg_to_mysql.txt. Format
// regressions (header changes, table separators, hint phrasing) fail
// loudly here rather than silently changing operator-facing output.
func TestPreviewer_Golden_Text(t *testing.T) {
	src := &previewStubEngine{
		name: "postgres",
		schema: &ir.Schema{Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.UUID{}},
					{Name: "email", Type: ir.Varchar{Length: 255}},
					{Name: "bio", Type: ir.Text{Size: ir.TextLong}},
				},
			},
		}},
	}
	tgt := &previewStubEngine{
		name: "mysql",
		schema: &ir.Schema{Tables: []*ir.Table{
			{
				Name: "users",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.UUID{}},
					{Name: "email", Type: ir.Varchar{Length: 255}},
					{Name: "bio", Type: ir.Text{Size: ir.TextLong}},
				},
			},
		}},
		stmts: []ir.DDLStatement{
			{
				Table: "users",
				Kind:  "CREATE TABLE",
				SQL:   "CREATE TABLE `users` (\n  `id` CHAR(36) NOT NULL,\n  `email` VARCHAR(255) NOT NULL,\n  `bio` LONGTEXT NOT NULL\n) ENGINE=InnoDB",
			},
		},
	}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	goldenPath := filepath.Join("testdata", "preview", "users_pg_to_mysql.txt")
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
	// Normalize CRLF to LF on the read-side so the test is resilient
	// to a Windows checkout where git's autocrlf converted the golden
	// file. The .gitattributes rule should keep the file LF-only on
	// disk; the normalize is belt-and-suspenders.
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("golden mismatch in %s\n--- got ---\n%s\n--- want ---\n%s",
			goldenPath, buf.String(), string(want))
	}
}
