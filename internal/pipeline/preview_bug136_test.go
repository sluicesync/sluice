// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Preview-surface pins for Bug 136: `schema preview` against a MySQL
// target must SURFACE an index key part on a TEXT/BLOB/JSON-landing
// column — the shape `migrate` refuses — instead of silently rendering
// the invalid index DDL. Preview stays exit-0 (the operator needs to
// see the full DDL shape); the dedicated section announces that
// `migrate` will refuse and names the --type-override escape hatch.

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// previewBug136Schema mirrors the canonical repro: a UNIQUE index on
// an unbounded PG text column.
func previewBug136Schema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Text{Size: ir.TextLong}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{{
			Name:    "users_email_key",
			Unique:  true,
			Kind:    ir.IndexKindBTree,
			Columns: []ir.IndexColumn{{Column: "email"}},
		}},
	}}}
}

func previewBug136Stmts() []ir.DDLStatement {
	return []ir.DDLStatement{
		{
			Table: "users",
			Kind:  "CREATE TABLE",
			SQL:   "CREATE TABLE `users` (\n  `id` BIGINT NOT NULL,\n  `email` LONGTEXT NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
		},
		{
			Table: "users",
			Kind:  "CREATE INDEX",
			SQL:   "ALTER TABLE `users` ADD UNIQUE INDEX `users_email_key` (`email`) USING BTREE",
		},
	}
}

// TestPreviewer_Run_Bug136TextIndexAdvisory pins the text-format
// advisory: preview exits 0, renders the DDL, and the dedicated
// section names the offending key part, says `migrate` will refuse,
// and spells out the workaround — so the operator sees the Error 1170
// shape before running anything.
func TestPreviewer_Run_Bug136TextIndexAdvisory(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewBug136Schema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewBug136Schema(), stmts: previewBug136Stmts()}

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
		t.Fatalf("Run: %v (preview must stay advisory — exit 0)", err)
	}

	out := buf.String()
	for _, want := range []string{
		"un-indexable TEXT/BLOB key parts: 1",    // header count line
		"migrate WILL REFUSE",                    // the refusal announced
		"Un-indexable TEXT/BLOB index key parts", // the dedicated section
		"users.email (text -> LONGTEXT)",         // the offending key part named
		`UNIQUE index "users_email_key"`,         // ... with its index
		"Error 1170",                             // the MySQL failure named
		"--type-override TABLE.COL=varchar(N)",   // the escape hatch
		"ADD UNIQUE INDEX `users_email_key`",     // DDL still rendered for inspection
	} {
		if !strings.Contains(out, want) {
			t.Errorf("preview output missing %q; got:\n%s", want, out)
		}
	}
}

// TestPreviewer_Run_Bug136JSONOutput pins the stable JSON shape:
// tooling gates on a non-empty text_index_refusals list.
func TestPreviewer_Run_Bug136JSONOutput(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewBug136Schema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewBug136Schema(), stmts: previewBug136Stmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "json",
		Out:       &buf,
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var out PreviewJSON
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal preview JSON: %v", err)
	}
	if len(out.TextIndexRefusals) != 1 {
		t.Fatalf("TextIndexRefusals = %+v; want exactly 1", out.TextIndexRefusals)
	}
	got := out.TextIndexRefusals[0]
	want := PreviewJSONTextIndex{
		Table:      "users",
		Index:      "users_email_key",
		Column:     "email",
		SourceType: "text",
		TargetType: "LONGTEXT",
		Unique:     true,
	}
	if got != want {
		t.Errorf("TextIndexRefusals[0] = %+v; want %+v", got, want)
	}
}

// TestPreviewer_Run_Bug136OverrideSuppressesAdvisory pins the escape
// hatch on the preview surface: the scan runs on the post-override
// schema, so `--type-override users.email=varchar(255)` suppresses the
// section entirely.
func TestPreviewer_Run_Bug136OverrideSuppressesAdvisory(t *testing.T) {
	src := &previewStubEngine{name: "postgres", schema: previewBug136Schema()}
	tgt := &previewStubEngine{name: "mysql", schema: previewBug136Schema(), stmts: previewBug136Stmts()}

	var buf bytes.Buffer
	prev := &Previewer{
		Source:    src,
		Target:    tgt,
		SourceDSN: "src",
		TargetDSN: "tgt",
		Format:    "text",
		Out:       &buf,
		Mappings: []config.Mapping{{
			Table:             "users",
			Column:            "email",
			TargetType:        "varchar",
			TargetTypeOptions: map[string]any{"length": 255},
		}},
	}
	if err := prev.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out := buf.String(); strings.Contains(out, "Un-indexable TEXT/BLOB") {
		t.Errorf("override applied but advisory still rendered:\n%s", out)
	}
}
