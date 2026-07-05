// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"path"
	"reflect"
	"strings"
	"testing"
)

func TestSplitSQLStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "plain statements",
			in:   "CREATE TABLE a (id int);\nCREATE TABLE b (id int);",
			want: []string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"},
		},
		{
			name: "semicolon inside single-quoted literal",
			in:   "COMMENT ON TABLE t IS 'has; a semicolon';",
			want: []string{"COMMENT ON TABLE t IS 'has; a semicolon'"},
		},
		{
			name: "doubled single quote then semicolon",
			in:   "INSERT INTO t VALUES ('it''s; fine');",
			want: []string{"INSERT INTO t VALUES ('it''s; fine')"},
		},
		{
			name: "semicolon inside double-quoted identifier",
			in:   `CREATE TABLE "odd;name" (id int);`,
			want: []string{`CREATE TABLE "odd;name" (id int)`},
		},
		{
			name: "semicolon inside dollar-quoted body",
			in:   "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql;\nSELECT 2;",
			want: []string{"CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql", "SELECT 2"},
		},
		{
			name: "tagged dollar quote",
			in:   "CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $notit$; $body$ LANGUAGE sql;",
			want: []string{"CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; $notit$; $body$ LANGUAGE sql"},
		},
		{
			name: "line comment with semicolon elided",
			in:   "-- preamble; not a statement\nCREATE TABLE a (id int); -- trailing; comment\n",
			want: []string{"CREATE TABLE a (id int)"},
		},
		{
			name: "nested block comment with semicolon elided",
			in:   "/* outer; /* inner; */ still outer; */ CREATE TABLE a (id int);",
			want: []string{"CREATE TABLE a (id int)"},
		},
		{
			name: "trailing statement without terminator",
			in:   "CREATE TABLE a (id int);\nCREATE TABLE b (id int)",
			want: []string{"CREATE TABLE a (id int)", "CREATE TABLE b (id int)"},
		},
		{
			name: "dollar sign that is not a dollar quote",
			in:   "CREATE POLICY p ON t USING (owner = current_setting('app.u')); SELECT $1;",
			want: []string{"CREATE POLICY p ON t USING (owner = current_setting('app.u'))", "SELECT $1"},
		},
		{
			name: "empty and whitespace-only fragments dropped",
			in:   " ;\n\n;CREATE TABLE a (id int);;",
			want: []string{"CREATE TABLE a (id int)"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitDumpStatements(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("splitDumpStatements(%q) = %#v; want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeDumpStatement(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"whitespace collapse", "CREATE TABLE a (\n    id   int\n)", "CREATE TABLE a ( id int )"},
		{"SET skipped", "SET statement_timeout = 0", ""},
		{"set_config preamble skipped", "SELECT pg_catalog.set_config('search_path', '', false)", ""},
		{"psql metacommand skipped", `\connect somedb`, ""},
		{"empty", "   \n\t ", ""},
		{"ordinary SELECT kept", "SELECT pg_catalog.setval('public.s', 42, true)", "SELECT pg_catalog.setval('public.s', 42, true)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeDumpStatement(tc.in); got != tc.want {
				t.Errorf("normalizeDumpStatement(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDumpStatementKey(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			"create table",
			"CREATE TABLE public.orders ( id bigint NOT NULL )",
			"CREATE TABLE public.orders",
		},
		{
			"create unique index folds to index",
			"CREATE UNIQUE INDEX customers_email_unique ON public.customers USING btree (email)",
			"CREATE INDEX customers_email_unique",
		},
		{
			"create index",
			"CREATE INDEX orders_active_idx ON public.orders USING btree (customer_id) WHERE (status <> 'cancelled'::public.order_status)",
			"CREATE INDEX orders_active_idx",
		},
		{
			"create type",
			"CREATE TYPE public.order_status AS ENUM ( 'pending', 'paid' )",
			"CREATE TYPE public.order_status",
		},
		{
			"create domain",
			"CREATE DOMAIN public.email_address AS text CONSTRAINT email_address_check CHECK ((VALUE ~ '^[^@]+@[^@]+$'::text))",
			"CREATE DOMAIN public.email_address",
		},
		{
			"create sequence",
			"CREATE SEQUENCE public.order_number_seq START WITH 1000 INCREMENT BY 5",
			"CREATE SEQUENCE public.order_number_seq",
		},
		{
			"create extension if not exists",
			"CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA public",
			"CREATE EXTENSION hstore",
		},
		{
			"create materialized view",
			"CREATE MATERIALIZED VIEW public.mv AS SELECT 1",
			"CREATE VIEW public.mv",
		},
		{
			"alter table only add constraint",
			"ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) REFERENCES public.customers(id) ON DELETE CASCADE",
			"ALTER TABLE public.orders ADD CONSTRAINT orders_customer_fk",
		},
		{
			"alter table alter column identity",
			"ALTER TABLE public.customers ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY ( SEQUENCE NAME public.customers_id_seq START WITH 1 )",
			"ALTER TABLE public.customers ALTER COLUMN id",
		},
		{
			"alter table attach partition",
			"ALTER TABLE ONLY public.events ATTACH PARTITION public.events_2026h1 FOR VALUES FROM ('2026-01-01') TO ('2026-07-01')",
			"ALTER TABLE public.events ATTACH PARTITION public.events_2026h1",
		},
		{
			"alter index attach partition",
			"ALTER INDEX public.events_pkey ATTACH PARTITION public.events_2026h1_pkey",
			"ALTER INDEX public.events_pkey ATTACH PARTITION public.events_2026h1_pkey",
		},
		{
			"alter sequence owned by",
			"ALTER SEQUENCE public.order_number_seq OWNED BY public.orders.order_number",
			"ALTER SEQUENCE public.order_number_seq OWNED",
		},
		{
			"comment on table",
			"COMMENT ON TABLE public.customers IS 'Registered customers'",
			"COMMENT ON TABLE public.customers",
		},
		{
			"comment on column",
			"COMMENT ON COLUMN public.customers.region_code IS 'ISO region'",
			"COMMENT ON COLUMN public.customers.region_code",
		},
		{
			"comment on materialized view",
			"COMMENT ON MATERIALIZED VIEW public.mv IS 'x'",
			"COMMENT ON MATERIALIZED VIEW public.mv",
		},
		{
			"fallback first three tokens",
			"GRANT SELECT ON public.t TO someone",
			"GRANT SELECT ON",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dumpStatementKey(tc.in); got != tc.want {
				t.Errorf("dumpStatementKey(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseSchemaDump(t *testing.T) {
	dump := `
--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SELECT pg_catalog.set_config('search_path', '', false);

CREATE TABLE public.a (
    id bigint NOT NULL
);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_pkey PRIMARY KEY (id);
`
	got := ParseSchemaDump(dump)
	want := []dumpStatement{
		{Key: "CREATE TABLE public.a", Body: "CREATE TABLE public.a ( id bigint NOT NULL )"},
		{Key: "ALTER TABLE public.a ADD CONSTRAINT a_pkey", Body: "ALTER TABLE ONLY public.a ADD CONSTRAINT a_pkey PRIMARY KEY (id)"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseSchemaDump = %#v; want %#v", got, want)
	}
	if n := CountCreateStatements(got); n != 1 {
		t.Errorf("CountCreateStatements = %d; want 1", n)
	}
}

// TestParseSchemaDump_VacuousGuardCountsCreates pins the vacuous-pass
// guard's counting primitive: a normalizer that eats every statement
// yields zero CREATEs, which the harness compares against the seed's
// declared floor and fails loudly instead of reading empty-diff as
// parity.
func TestParseSchemaDump_VacuousGuardCountsCreates(t *testing.T) {
	// A dump reduced entirely to preamble parses to zero statements.
	empty := ParseSchemaDump("SET x = 1;\nSET y = 2;\nSELECT pg_catalog.set_config('search_path', '', false);")
	if len(empty) != 0 || CountCreateStatements(empty) != 0 {
		t.Fatalf("preamble-only dump: got %d statements (%d CREATEs); want 0/0", len(empty), CountCreateStatements(empty))
	}
	// The guard fires when the count undershoots the declared floor.
	const declaredFloor = 3
	if CountCreateStatements(empty) >= declaredFloor {
		t.Fatal("vacuous guard would NOT have fired; the undercount check is broken")
	}
}

func TestDiffDumpStatements(t *testing.T) {
	sluice := []dumpStatement{
		{Key: "CREATE TABLE public.a", Body: "CREATE TABLE public.a ( id bigint )"},
		{Key: "CREATE TABLE public.b", Body: "CREATE TABLE public.b ( id bigint )"},
		{Key: "CREATE INDEX only_in_sluice", Body: "CREATE INDEX only_in_sluice ON public.a USING btree (id)"},
	}
	oracle := []dumpStatement{
		{Key: "CREATE TABLE public.a", Body: "CREATE TABLE public.a ( id bigint )"},
		{Key: "CREATE TABLE public.b", Body: "CREATE TABLE public.b ( id bigint, extra text )"},
		{Key: "CREATE SEQUENCE public.only_in_oracle", Body: "CREATE SEQUENCE public.only_in_oracle START WITH 1"},
	}
	d := DiffDumpStatements(sluice, oracle)

	if d.Empty() {
		t.Fatal("diff reported parity; want three divergences")
	}
	if len(d.OnlyInSluice) != 1 || d.OnlyInSluice[0].Key != "CREATE INDEX only_in_sluice" {
		t.Errorf("OnlyInSluice = %#v; want the sluice-only index", d.OnlyInSluice)
	}
	if len(d.OnlyInOracle) != 1 || d.OnlyInOracle[0].Key != "CREATE SEQUENCE public.only_in_oracle" {
		t.Errorf("OnlyInOracle = %#v; want the oracle-only sequence", d.OnlyInOracle)
	}
	if len(d.Mismatched) != 1 || d.Mismatched[0].Key != "CREATE TABLE public.b" {
		t.Fatalf("Mismatched = %#v; want one mismatch on public.b", d.Mismatched)
	}
	if d.Mismatched[0].Sluice == d.Mismatched[0].Oracle {
		t.Error("mismatch bodies are identical; expected the differing bodies to be carried")
	}
}

func TestDiffDumpStatements_ParityAndMultiset(t *testing.T) {
	same := []dumpStatement{
		{Key: "CREATE TABLE public.a", Body: "CREATE TABLE public.a ( id bigint )"},
	}
	if d := DiffDumpStatements(same, same); !d.Empty() {
		t.Errorf("identical inputs: diff = %#v; want empty", d)
	}

	// Duplicate keys (two statements sharing a key on one side) must
	// not silently cancel a genuine surplus.
	dupA := []dumpStatement{
		{Key: "K", Body: "x"},
		{Key: "K", Body: "x"},
	}
	dupB := []dumpStatement{
		{Key: "K", Body: "x"},
	}
	d := DiffDumpStatements(dupA, dupB)
	if len(d.OnlyInSluice) != 1 || len(d.OnlyInOracle) != 0 || len(d.Mismatched) != 0 {
		t.Errorf("multiset diff = %#v; want exactly one sluice-side surplus", d)
	}
}

func TestMatchDumpParityAllowlist(t *testing.T) {
	allow := []dumpParityAllowlistEntry{
		{Pattern: "CREATE TABLE public.orders", Reason: "specific", Citation: "docs/x.md"},
		{Pattern: "*sluice_migrate_*", Reason: "state tables", Citation: "internal/pipeline/resume.go"},
		{Pattern: "CREATE SEQUENCE public.gap_seq", Reason: "latent gap", Citation: DumpParityTriageCitation},
	}

	if e := MatchDumpParityAllowlist("CREATE TABLE public.orders", allow); e == nil || e.Reason != "specific" {
		t.Errorf("exact match = %+v; want the specific entry", e)
	}
	if e := MatchDumpParityAllowlist("CREATE TABLE public.sluice_migrate_state", allow); e == nil || e.Reason != "state tables" {
		t.Errorf("glob match = %+v; want the state-tables entry", e)
	}
	if e := MatchDumpParityAllowlist("ALTER TABLE public.sluice_migrate_state ADD CONSTRAINT sluice_migrate_state_pkey", allow); e == nil {
		t.Error("glob should match the state-table constraint key")
	}
	if e := MatchDumpParityAllowlist("CREATE TABLE public.customers", allow); e != nil {
		t.Errorf("unlisted key matched %+v; want nil", e)
	}

	// The TRIAGE marker must be distinguishable from a documented
	// citation — the harness banners TRIAGE entries separately.
	e := MatchDumpParityAllowlist("CREATE SEQUENCE public.gap_seq", allow)
	if e == nil || e.Citation != DumpParityTriageCitation {
		t.Fatalf("TRIAGE entry = %+v; want citation %q", e, DumpParityTriageCitation)
	}
}

// TestDumpParityAllowlist_Hygiene pins the reviewability contract of
// the checked-in allowlist: every entry carries a reason and a
// citation, every pattern is valid under path.Match, and TRIAGE
// entries are recognizable by the marker alone.
func TestDumpParityAllowlist_Hygiene(t *testing.T) {
	if len(DumpParityAllowlist) == 0 {
		t.Fatal("DumpParityAllowlist is empty; the harness depends on at least the state-table entry")
	}
	for _, e := range DumpParityAllowlist {
		if strings.TrimSpace(e.Pattern) == "" {
			t.Errorf("entry with empty pattern: %+v", e)
		}
		if _, err := path.Match(e.Pattern, ""); err != nil {
			t.Errorf("invalid pattern %q: %v", e.Pattern, err)
		}
		if strings.TrimSpace(e.Reason) == "" {
			t.Errorf("entry %q has no reason", e.Pattern)
		}
		if strings.TrimSpace(e.Citation) == "" {
			t.Errorf("entry %q has no citation; cite the doc/ADR/source or mark it %q", e.Pattern, DumpParityTriageCitation)
		}
	}
}
