// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestStampKeyCollationHistory_Unestablished pins the SECOND tier the
// history stamp raises, [irbackup.FormatVersionKeyCollationUnestablished]
// (KEY-COLLATION-PG-LOOSENED-REPLAY): a history entry recording a key column
// whose collation is not established for its rows — named, no character
// set, determinism Unknown, the Postgres reader's shape when the column's
// pg_attribute row is newer than the decoded rows — raises the segment to
// 14, whether the column is the entry's own primary key or a UNIQUE key only
// the parent's schema records (a change stream's snapshot carries no
// secondary indexes), in every character family (Varchar, Char, Text) and
// through a domain, and for a table the window created (no parent table).
// An entry that does not decode stamps 14 as well. Controls stay below 14: a
// non-key column so stamped, a key whose collation the stream established
// (nondeterministic or deterministic), a key with no collation recorded
// (VStream), a MySQL-shaped key (a character set beside the name, which no
// reader treats as not established), a database-default key the stream
// could not classify (no name), and a FULL manifest. The 13 tier is graded
// beside it so the two stay distinct: a key whose collation NAME changed is
// stamped exactly 13. The independent expected value is the rule stated
// here, graded per cell.
func TestStampKeyCollationHistory_Unestablished(t *testing.T) {
	unest := func(fam string) ir.Type {
		switch fam {
		case "varchar":
			return ir.Varchar{Length: 20, Collation: "ci", Determinism: ir.CollationDeterminismUnknown}
		case "char":
			return ir.Char{Length: 20, Collation: "ci", Determinism: ir.CollationDeterminismUnknown}
		case "domain":
			return ir.Domain{Name: "ci_text", BaseType: ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationDeterminismUnknown}}
		}
		return ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationDeterminismUnknown}
	}
	nondet := ir.Text{Size: ir.TextLong, Collation: "ci", Determinism: ir.CollationNonDeterministic}
	established := ir.Text{Size: ir.TextLong, Collation: "C", Determinism: ir.CollationDeterministic}
	// keyed "pk": k is the table's (and the snapshot's) primary key; "unique":
	// the primary key is id and k is a UNIQUE key, which only the parent
	// schema records (withIndexes=false strips it, as a change stream's
	// snapshot does).
	table := func(k, note ir.Type, keyed string, withIndexes bool) *ir.Table {
		tbl := &ir.Table{Schema: "s", Name: "kd", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "k", Type: k}, {Name: "note", Type: note},
		}}
		if keyed == "pk" {
			tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}}
			return tbl
		}
		tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}
		if withIndexes {
			tbl.Indexes = []*ir.Index{{Name: "uk", Unique: true, Columns: []ir.IndexColumn{{Column: "k"}}}}
		}
		return tbl
	}
	parent := func(keyed string) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{table(nondet, nondet, keyed, true)}}
	}
	snap := func(k, note ir.Type, keyed string) []*ir.Table { return []*ir.Table{table(k, note, keyed, false)} }
	type cell struct {
		name    string
		kind    string
		before  *ir.Schema
		history []*ir.Table
		raw     []byte // a TableJSON that does not decode, appended after history
		want    int    // the version the segment must carry; start when unstamped
	}
	const start = irbackup.FormatVersionInjectiveChunkAAD
	const fv13, fv14 = irbackup.FormatVersionKeyCollationDelta, irbackup.FormatVersionKeyCollationUnestablished
	incr := irbackup.BackupKindIncremental
	cases := make([]cell, 0, 24)
	for _, fam := range []string{"varchar", "char", "text", "domain"} {
		cases = append(
			cases,
			cell{"pk/" + fam, incr, parent("pk"), snap(unest(fam), nondet, "pk"), nil, fv14},
			cell{"unique-from-parent/" + fam, incr, parent("unique"), snap(unest(fam), nondet, "unique"), nil, fv14},
		)
	}
	cases = append(
		cases,
		cell{
			"pk/after an established version of the same key", incr, parent("pk"),
			append(snap(nondet, nondet, "pk"), snap(unest("text"), nondet, "pk")...), nil, fv14,
		},
		cell{"pk/table the window created (no parent table)", incr, &ir.Schema{}, snap(unest("text"), nondet, "pk"), nil, fv14},
		cell{"pk/no parent schema at all", incr, nil, snap(unest("text"), nondet, "pk"), nil, fv14},
		cell{
			"pk/name changed AND not established (both tiers, 14 wins)", incr, parent("pk"),
			append(snap(established, nondet, "pk"), snap(unest("text"), nondet, "pk")...), nil, fv14,
		},
		cell{"undecodable entry", incr, parent("pk"), nil, []byte("{not json"), fv14},

		cell{"13 only: key collation name changed, established", incr, parent("pk"), snap(established, nondet, "pk"), nil, fv13},

		cell{"control: non-key column not established", incr, parent("unique"), snap(nondet, unest("text"), "unique"), nil, start},
		cell{"control: key nondeterministic, established", incr, parent("pk"), snap(nondet, nondet, "pk"), nil, start},
		cell{
			"control: key deterministic database default", incr,
			&ir.Schema{Tables: []*ir.Table{table(ir.Text{Size: ir.TextLong, Determinism: ir.CollationDeterministic}, nondet, "pk", true)}},
			snap(ir.Text{Size: ir.TextLong, Determinism: ir.CollationDeterministic}, nondet, "pk"), nil, start,
		},
		cell{"control: key with no collation recorded (VStream)", incr, parent("pk"), snap(ir.Varchar{Length: 20}, nondet, "pk"), nil, start},
		cell{
			"control: key database default the stream could not classify (no name, Unknown)", incr,
			&ir.Schema{Tables: []*ir.Table{table(ir.Text{Size: ir.TextLong}, nondet, "pk", true)}},
			snap(ir.Text{Size: ir.TextLong}, nondet, "pk"), nil, start,
		},
		cell{
			"control: MySQL-shaped key (charset beside the name, Unknown)", incr,
			&ir.Schema{Tables: []*ir.Table{table(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, nondet, "pk", true)}},
			snap(ir.Varchar{Length: 20, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, nondet, "pk"), nil, start,
		},
		cell{"control: full manifest", irbackup.BackupKindFull, parent("pk"), snap(unest("text"), nondet, "pk"), nil, start},
	)
	stamped := 0
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &irbackup.Manifest{Kind: c.kind, FormatVersion: start}
			for _, h := range c.history {
				js, err := ir.MarshalTable(h)
				if err != nil {
					t.Fatal(err)
				}
				m.SchemaHistory = append(m.SchemaHistory, &irbackup.SchemaHistoryEntry{Schema: "s", Table: "kd", TableJSON: js})
			}
			if c.raw != nil {
				m.SchemaHistory = append(m.SchemaHistory, &irbackup.SchemaHistoryEntry{Schema: "s", Table: "kd", TableJSON: c.raw})
			}
			stampKeyCollationHistory(m, c.before)
			if m.FormatVersion != c.want {
				t.Errorf("FormatVersion = %d; want %d", m.FormatVersion, c.want)
			}
		})
		if c.want == fv14 {
			stamped++
		}
	}
	if stamped < 8 {
		t.Fatalf("%d cells expect the 14 stamp; want at least the 8 family × key cells — the matrix lost its stamped half", stamped)
	}
}
