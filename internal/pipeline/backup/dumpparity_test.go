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

// idx builds a CREATE INDEX dumpStatement the way ParseSchemaDump would:
// the key is object-identity (verb+kind+NAME), the body is the collapsed
// statement text. unique renders a CREATE UNIQUE INDEX.
func idx(name, table, cols string, unique bool) dumpStatement {
	verb := "CREATE INDEX "
	if unique {
		verb = "CREATE UNIQUE INDEX "
	}
	body := verb + name + " ON " + table + " USING btree (" + cols + ")"
	return dumpStatement{Key: "CREATE INDEX " + name, Body: body}
}

// TestDiffDumpStatements_IndexRenamePairing pins the Phase-2
// body-equivalence pairing pass (Finding B). sluice deliberately renames
// Postgres indexes (pgIndexName qualification, GitHub #26), so a
// renamed-but-identical index must NOT read as a divergence — but the
// pairing must never mask a genuine drop/add/body-change. The load-bearing
// pin is (b): a dropped index still surfaces even amid renames.
func TestDiffDumpStatements_IndexRenamePairing(t *testing.T) {
	t.Run("(a) renamed-identical pairs to zero divergence", func(t *testing.T) {
		// sluice qualifies wl_user -> watchlist_wl_user; same body.
		sluice := []dumpStatement{idx("watchlist_wl_user", "public.watchlist", "wl_user", false)}
		oracle := []dumpStatement{idx("wl_user", "public.watchlist", "wl_user", false)}
		d := DiffDumpStatements(sluice, oracle)
		if !d.Empty() {
			t.Fatalf("renamed-identical index reported a divergence: %#v", d)
		}
	})

	t.Run("(b) dropped index still surfaces (not masked by a concurrent rename)", func(t *testing.T) {
		// One genuine rename AND one genuine drop in the same diff: the
		// rename must not consume the dropped index's slot.
		sluice := []dumpStatement{
			idx("actor_actor_user", "public.actor", "actor_user", true), // renamed
		}
		oracle := []dumpStatement{
			idx("actor_user", "public.actor", "actor_user", true),       // rename partner
			idx("dropped_idx", "public.actor", "some_other_col", false), // DROPPED by sluice
		}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 0 {
			t.Errorf("OnlyInSluice = %#v; want empty (the rename paired)", d.OnlyInSluice)
		}
		if len(d.OnlyInOracle) != 1 || d.OnlyInOracle[0].Key != "CREATE INDEX dropped_idx" {
			t.Fatalf("OnlyInOracle = %#v; want exactly the dropped index — a rename MUST NOT mask a drop", d.OnlyInOracle)
		}
	})

	t.Run("(c) added index still surfaces", func(t *testing.T) {
		sluice := []dumpStatement{idx("added_idx", "public.t", "col_a", false)}
		var oracle []dumpStatement
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 1 || d.OnlyInSluice[0].Key != "CREATE INDEX added_idx" {
			t.Fatalf("OnlyInSluice = %#v; want the added index (no body match to pair)", d.OnlyInSluice)
		}
	})

	t.Run("(d) same-name body-change is a Phase-1 mismatch", func(t *testing.T) {
		sluice := []dumpStatement{idx("t_idx", "public.t", "col_a", false)}
		oracle := []dumpStatement{idx("t_idx", "public.t", "col_a, col_b", false)}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.Mismatched) != 1 || d.Mismatched[0].Key != "CREATE INDEX t_idx" {
			t.Fatalf("Mismatched = %#v; want one body mismatch on t_idx", d.Mismatched)
		}
		if len(d.OnlyInSluice) != 0 || len(d.OnlyInOracle) != 0 {
			t.Errorf("body-change should not appear as drop/add: %#v", d)
		}
	})

	t.Run("(d') renamed-AND-body-changed surfaces as drop+add", func(t *testing.T) {
		// Different name AND different columns: no signature match, so it
		// is NOT paired — it must surface, not vanish.
		sluice := []dumpStatement{idx("t_new_idx", "public.t", "col_a", false)}
		oracle := []dumpStatement{idx("t_old_idx", "public.t", "col_a, col_b", false)}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 1 || len(d.OnlyInOracle) != 1 {
			t.Fatalf("renamed+body-changed = %#v; want a drop and an add (both surfaced)", d)
		}
	})

	t.Run("(e) two distinct indexes on same table do not mis-pair", func(t *testing.T) {
		// Both renamed, distinct column sets: each must pair with its own
		// same-body partner, never cross. If cross-pairing happened the
		// diff would still be empty, so we instead prove no-mispair with
		// an ASYMMETRIC case: a single sluice index on col_a must not pair
		// with an oracle index on col_b.
		sluice := []dumpStatement{idx("s_a", "public.t", "col_a", false)}
		oracle := []dumpStatement{idx("o_b", "public.t", "col_b", false)}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 1 || len(d.OnlyInOracle) != 1 {
			t.Fatalf("distinct-column indexes mis-paired: %#v", d)
		}
	})

	t.Run("(e2) two renames with distinct bodies pair correctly", func(t *testing.T) {
		sluice := []dumpStatement{
			idx("t_a", "public.t", "col_a", false),
			idx("t_b", "public.t", "col_b", false),
		}
		oracle := []dumpStatement{
			idx("a", "public.t", "col_a", false),
			idx("b", "public.t", "col_b", false),
		}
		if d := DiffDumpStatements(sluice, oracle); !d.Empty() {
			t.Fatalf("two distinct renames should pair to parity: %#v", d)
		}
	})

	t.Run("(f) ambiguous identical-body signatures are NOT silently paired", func(t *testing.T) {
		// Two indexes per side sharing one signature (degenerate redundant
		// indexes): the pairing must refuse to guess and surface all four
		// rather than arbitrarily cancel. Surface > mask.
		sluice := []dumpStatement{
			idx("s_one", "public.t", "col_a", false),
			idx("s_two", "public.t", "col_a", false),
		}
		oracle := []dumpStatement{
			idx("o_one", "public.t", "col_a", false),
			idx("o_two", "public.t", "col_a", false),
		}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 2 || len(d.OnlyInOracle) != 2 {
			t.Fatalf("ambiguous signatures = %#v; want all four surfaced, none paired", d)
		}
	})

	t.Run("(f2) ambiguous unequal counts leave all as divergences", func(t *testing.T) {
		// 1 sluice, 2 oracle sharing a signature: refuse to pair the one
		// sluice index with either oracle candidate.
		sluice := []dumpStatement{idx("s_one", "public.t", "col_a", false)}
		oracle := []dumpStatement{
			idx("o_one", "public.t", "col_a", false),
			idx("o_two", "public.t", "col_a", false),
		}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 1 || len(d.OnlyInOracle) != 2 {
			t.Fatalf("unequal ambiguous counts = %#v; want all three surfaced, none paired", d)
		}
	})

	t.Run("uniqueness difference is NOT a rename", func(t *testing.T) {
		// Same table+cols but one UNIQUE, one not: distinct signatures, so
		// they must not pair — a uniqueness change is a real divergence.
		sluice := []dumpStatement{idx("s_u", "public.t", "col_a", true)}
		oracle := []dumpStatement{idx("o_p", "public.t", "col_a", false)}
		d := DiffDumpStatements(sluice, oracle)
		if len(d.OnlyInSluice) != 1 || len(d.OnlyInOracle) != 1 {
			t.Fatalf("uniqueness change mis-paired as a rename: %#v", d)
		}
	})
}

// TestIndexBodySignature pins the signature extraction directly: only
// CREATE INDEX statements yield a signature, the name is excluded, and
// every defining attribute (table, cols, method, uniqueness, WHERE) is
// included so a change in any of them breaks the match.
func TestIndexBodySignature(t *testing.T) {
	sig := func(body string) string {
		s, ok := indexBodySignature(body)
		if !ok {
			t.Fatalf("indexBodySignature(%q) = not-an-index; want a signature", body)
		}
		return s
	}

	// Rename: identical everything except the name -> identical signature.
	a := sig("CREATE INDEX wl_user ON public.watchlist USING btree (wl_user)")
	b := sig("CREATE INDEX watchlist_wl_user ON public.watchlist USING btree (wl_user)")
	if a != b {
		t.Errorf("rename signatures differ:\n a=%q\n b=%q", a, b)
	}

	// UNIQUE participates in the signature.
	if u := sig("CREATE UNIQUE INDEX x ON public.t USING btree (c)"); u == sig("CREATE INDEX y ON public.t USING btree (c)") {
		t.Error("UNIQUE and non-UNIQUE indexes share a signature; uniqueness must distinguish them")
	}

	// Partial-index predicate participates.
	p1 := sig("CREATE INDEX a ON public.o USING btree (s) WHERE (s <> 'x'::text)")
	p2 := sig("CREATE INDEX b ON public.o USING btree (s) WHERE (s <> 'y'::text)")
	if p1 == p2 {
		t.Error("different WHERE predicates share a signature; the partial clause must participate")
	}

	// Non-index statements yield no signature.
	for _, notIdx := range []string{
		"CREATE TABLE public.t ( id bigint )",
		"CREATE SEQUENCE public.s START WITH 1",
		"ALTER TABLE ONLY public.t ADD CONSTRAINT t_pkey PRIMARY KEY (id)",
	} {
		if s, ok := indexBodySignature(notIdx); ok {
			t.Errorf("indexBodySignature(%q) = %q, true; want not-an-index", notIdx, s)
		}
	}
}

// TestCanonicalizeIntLiteralDefaults pins the typed-literal integer
// default normalization: sluice's `'0'::smallint` and pg_dump's bare `0`
// must collapse to the same catalog text (a documented cosmetic), while a
// non-integer typed default, a non-integer cast target, and a genuine
// value change are LEFT UNDER COMPARISON so real divergences still surface.
func TestCanonicalizeIntLiteralDefaults(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		// The cosmetic class: stripped to bare integer.
		{"smallint default", "x smallint DEFAULT '0'::smallint NOT NULL", "x smallint DEFAULT 0 NOT NULL"},
		{"bigint default", "x bigint DEFAULT '0'::bigint NOT NULL", "x bigint DEFAULT 0 NOT NULL"},
		{"integer default", "x integer DEFAULT '1'::integer", "x integer DEFAULT 1"},
		{"negative literal", "x integer DEFAULT '-5'::integer", "x integer DEFAULT -5"},
		{"trailing comma preserved", "CREATE TABLE t ( a int DEFAULT '7'::integer, b int )", "CREATE TABLE t ( a int DEFAULT 7, b int )"},
		{"trailing paren preserved", "( a int DEFAULT '3'::integer)", "( a int DEFAULT 3)"},
		{"already bare unchanged", "x smallint DEFAULT 0 NOT NULL", "x smallint DEFAULT 0 NOT NULL"},
		// The signal that must NOT be collapsed.
		{"text literal untouched", "x text DEFAULT ''::text NOT NULL", "x text DEFAULT ''::text NOT NULL"},
		{"non-numeric text untouched", "x text DEFAULT 'page'::text NOT NULL", "x text DEFAULT 'page'::text NOT NULL"},
		{"timestamptz literal untouched", "x timestamp with time zone DEFAULT '1970-01-01 00:00:00+00'::timestamp with time zone", "x timestamp with time zone DEFAULT '1970-01-01 00:00:00+00'::timestamp with time zone"},
		{"non-int cast target untouched", "x numeric DEFAULT '0'::numeric", "x numeric DEFAULT '0'::numeric"},
		{"nextval regclass untouched", "SET DEFAULT nextval('public.t_id_seq'::regclass)", "SET DEFAULT nextval('public.t_id_seq'::regclass)"},
		{"no cast at all untouched", "CREATE TABLE t ( id bigint NOT NULL )", "CREATE TABLE t ( id bigint NOT NULL )"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalizeIntLiteralDefaults(tc.in); got != tc.want {
				t.Errorf("canonicalizeIntLiteralDefaults(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}

	// A value change survives the strip: '0'::smallint vs '1'::smallint
	// canonicalize to different bare integers, so the diff still fires.
	if canonicalizeIntLiteralDefaults("DEFAULT '0'::smallint") == canonicalizeIntLiteralDefaults("DEFAULT '1'::smallint") {
		t.Error("a genuine default-value change was masked by canonicalization")
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
