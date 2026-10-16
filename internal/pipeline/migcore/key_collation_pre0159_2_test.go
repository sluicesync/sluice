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

// TestSluiceVersionBefore pins the shared boundary helper at both cut-overs
// it serves (v0.156.3 for the legacy charset, v0.159.2 for key collations).
func TestSluiceVersionBefore(t *testing.T) {
	for _, tc := range []struct {
		v            string
		minor, patch int
		want         bool
	}{
		{"0.159.1", 159, 2, true},
		{"v0.159.1", 159, 2, true},
		{"0.159.0", 159, 2, true},
		{"0.158.9", 159, 2, true},
		{"0.99.292", 159, 2, true},
		{"0.159.2", 159, 2, false},
		{"0.159.3", 159, 2, false},
		{"0.159.10", 159, 2, false},
		{"0.160.0", 159, 2, false},
		{"1.0.0", 159, 2, false},
		{"0.159.1-rc1", 159, 2, true},
		{"dev", 159, 2, false},
		{"", 159, 2, false},
		{"0.159", 159, 2, false},
	} {
		if got := sluiceVersionBefore(tc.v, tc.minor, tc.patch); got != tc.want {
			t.Errorf("sluiceVersionBefore(%q, %d, %d) = %v; want %v", tc.v, tc.minor, tc.patch, got, tc.want)
		}
	}
}

// TestWarnUnverifiableKeyCollationIncrement pins the WARN's scope on every
// axis it keys on: version (below v0.159.2 only), source engine (each
// MySQL-family name, never Postgres or SQLite), manifest kind (incrementals
// only), and key type (a character column in a primary or unique key —
// every character family, through a domain too — and nothing else).
func TestWarnUnverifiableKeyCollationIncrement(t *testing.T) {
	run := func(m *irbackup.Manifest) string {
		prev := slog.Default()
		defer slog.SetDefault(prev)
		var buf logcapture.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		WarnUnverifiableKeyCollationIncrement(context.Background(), "chain restore", m)
		return buf.String()
	}
	vc := ir.Varchar{Length: 16, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}
	table := func(name string, col ir.Type, key func(*ir.Table)) *ir.Table {
		tbl := &ir.Table{Name: name, Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "k", Type: col},
		}}
		tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true}
		key(tbl)
		return tbl
	}
	pkOnK := func(tbl *ir.Table) {
		tbl.PrimaryKey = &ir.Index{Columns: []ir.IndexColumn{{Column: "k"}}, Unique: true}
	}
	uniqueOnK := func(tbl *ir.Table) {
		tbl.Indexes = []*ir.Index{{Name: "uk", Columns: []ir.IndexColumn{{Column: "k"}}, Unique: true}}
	}
	mk := func(engine, version string, tables ...*ir.Table) *irbackup.Manifest {
		return &irbackup.Manifest{
			Kind: irbackup.BackupKindIncremental, SourceEngine: engine, SluiceVersion: version,
			Schema: &ir.Schema{Tables: tables},
		}
	}

	t.Run("warns", func(t *testing.T) {
		for name, m := range map[string]*irbackup.Manifest{
			"mysql/pk/varchar":        mk("mysql", "0.159.1", table("t", vc, pkOnK)),
			"mariadb/unique/varchar":  mk("mariadb", "0.159.0", table("t", vc, uniqueOnK)),
			"planetscale/unique/char": mk("planetscale", "v0.158.1", table("t", ir.Char{Length: 4, Collation: "utf8mb4_bin"}, uniqueOnK)),
			"vitess/pk/text":          mk("vitess", "0.157.0", table("t", ir.Text{Collation: "utf8mb4_bin"}, pkOnK)),
			"mydumper/unique/domain":  mk("mydumper", "0.120.0", table("t", ir.Domain{Name: "d", BaseType: vc}, uniqueOnK)),
			"mysql/composite/varchar2": mk("mysql", "0.159.1", table("t", vc, func(tbl *ir.Table) {
				tbl.PrimaryKey.Columns = append(tbl.PrimaryKey.Columns, ir.IndexColumn{Column: "k"})
			})),
			// KEY-COLLATION-FUNCTIONAL-KEY-PART: a functional key decides
			// which rows are one through its column, as a plain key does.
			// Until the fix this case was pinned QUIET.
			"mysql/functional unique key": mk("mysql", "0.159.1", table("t", vc, func(tbl *ir.Table) {
				tbl.Indexes = []*ir.Index{{Name: "fx", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(k)"}}}}
			})),
		} {
			out := run(m)
			if !strings.Contains(out, KeyCollationPre01592Marker) || !strings.Contains(out, "columns=k") || !strings.Contains(out, "table=t") {
				t.Errorf("%s: log = %q; want the marker naming t.k", name, out)
			}
			if !strings.Contains(out, "`sluice verify`") || !strings.Contains(out, "v0.159.2") {
				t.Errorf("%s: log = %q; want the check and the remedy", name, out)
			}
			if strings.Contains(out, "columns=id") || strings.Contains(out, "columns=k, id") || strings.Contains(out, "id, k") {
				t.Errorf("%s: log = %q; named the integer key column", name, out)
			}
		}
	})

	t.Run("quiet", func(t *testing.T) {
		for name, m := range map[string]*irbackup.Manifest{
			"fixed version":         mk("mysql", "0.159.2", table("t", vc, pkOnK)),
			"later version":         mk("mysql", "0.160.1", table("t", vc, pkOnK)),
			"unparseable version":   mk("mysql", "dev", table("t", vc, pkOnK)),
			"postgres source":       mk("postgres", "0.159.1", table("t", vc, pkOnK)),
			"sqlite source":         mk("sqlite", "0.159.1", table("t", vc, pkOnK)),
			"integer key only":      mk("mysql", "0.159.1", table("t", vc, func(*ir.Table) {})),
			"non-unique index on k": mk("mysql", "0.159.1", table("t", vc, func(tbl *ir.Table) { tbl.Indexes = []*ir.Index{{Name: "ix", Columns: []ir.IndexColumn{{Column: "k"}}}} })),
			"functional non-unique index": mk("mysql", "0.159.1", table("t", vc, func(tbl *ir.Table) {
				tbl.Indexes = []*ir.Index{{Name: "fx", Columns: []ir.IndexColumn{{Expression: "lower(k)"}}}}
			})),
			"functional unique key over another column": mk("mysql", "0.159.1", table("t", vc, func(tbl *ir.Table) {
				tbl.Indexes = []*ir.Index{{Name: "fx", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(other)"}}}}
			})),
			"non-character key (int)": mk("mysql", "0.159.1", table("t", ir.Integer{Width: 32}, pkOnK)),
			"a full": {
				Kind: irbackup.BackupKindFull, SourceEngine: "mysql", SluiceVersion: "0.159.1",
				Schema: &ir.Schema{Tables: []*ir.Table{table("t", vc, pkOnK)}},
			},
			"no schema": {Kind: irbackup.BackupKindIncremental, SourceEngine: "mysql", SluiceVersion: "0.159.1"},
		} {
			if out := run(m); out != "" {
				t.Errorf("%s: logged %q; want nothing", name, out)
			}
		}
	})
}
