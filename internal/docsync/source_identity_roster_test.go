// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"sort"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Blank-imported for their init() self-registration, the same set
	// cmd/sluice/main.go links — these tests iterate the REAL registry, so a
	// new engine package added there must be added here too (the non-vacuity
	// floor below fails rather than silently under-reporting).
	_ "sluicesync.dev/sluice/internal/engines/d1-trigger"
	_ "sluicesync.dev/sluice/internal/engines/flatfile"
	_ "sluicesync.dev/sluice/internal/engines/mydumper"
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/pgtrigger"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
	_ "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
)

// sourceIdentityExempt is the fail-by-default exemption map for
// [TestEverySourceEngineDescribesItsIdentity]. It is EMPTY on purpose
// (audit 2026-09-15 F-1): every engine can be a `migrate --resume`
// SOURCE against a target that stores migration state, so every one of
// them must be able to say which source a DSN names. An entry here needs
// a reason, and the reason would have to explain why a resume pointed at
// a DIFFERENT source of that engine may adopt the first one's completed
// copy and exit 0 having copied nothing — the class this gate keeps
// closed.
var sourceIdentityExempt = map[string]string{}

// identityDSNs are the four DSNs one engine is graded on. The ROSTER is
// derived from engines.Names(), so a new engine fails this gate for want
// of an entry rather than joining it ungraded; the DSNs themselves are
// necessarily hand-written, because only a human knows what "the same
// source, spelled differently" means for a given engine.
type identityDSNs struct {
	// canonical is a well-formed DSN for this engine.
	canonical string

	// dataset differs from canonical ONLY in which database / file /
	// dataset it names. Its identity MUST differ: this is the whole
	// finding — before F-1 the orchestrator parsed three DSN shapes and
	// rendered "" for everything else, so two SQLite files, two D1
	// databases, two dumps, two flat files and two MySQL DSNs spelled
	// `user:pw@/db` all compared EQUAL, and the auto-derived migration id
	// collapsed with them because it hashes the same unparsed host.
	dataset string

	// reachability differs from canonical ONLY in how to REACH the same
	// source — a renamed host, a different port, a different D1 account.
	// Its identity MUST be identical: ADR-0015 makes --migration-id the
	// operator's assertion of a stable identity across DNS shifts and
	// host renames, so a DNS move, a failover or a replica must still
	// resume. "" for an engine that has no reachability axis (a local
	// file is reached exactly one way).
	reachability string

	// namespace differs from canonical ONLY in the schema within the same
	// database. Its identity MUST differ. "" for a flat-namespace engine,
	// where the database IS the namespace.
	namespace string
}

// The values the identity must never carry: hosts, ports, accounts and
// credentials. Chosen so they cannot occur inside a legitimate dataset
// name below — the anti-vacuity check under the loop proves each one is
// actually present in some DSN, so this cannot pass by naming tokens no
// DSN contains.
var identityForbiddenTokens = []string{
	"db-a", "db-b", "5432", "6432", "3306", "3307", "hunter2", "acct-a", "acct-b",
}

var sourceIdentityDSNs = map[string]identityDSNs{
	// The MySQL family: one type, four registrations. A MySQL database IS
	// its namespace (SchemaScopeFlat), so there is no namespace axis.
	"mysql":       {canonical: "root:hunter2@tcp(db-a:3306)/app_one", dataset: "root:hunter2@tcp(db-a:3306)/app_two", reachability: "root:hunter2@tcp(db-b:3307)/app_one"},
	"mariadb":     {canonical: "root:hunter2@tcp(db-a:3306)/app_one", dataset: "root:hunter2@tcp(db-a:3306)/app_two", reachability: "root:hunter2@tcp(db-b:3307)/app_one"},
	"planetscale": {canonical: "root:hunter2@tcp(db-a:3306)/app_one", dataset: "root:hunter2@tcp(db-a:3306)/app_two", reachability: "root:hunter2@tcp(db-b:3307)/app_one"},
	"vitess":      {canonical: "root:hunter2@tcp(db-a:3306)/app_one", dataset: "root:hunter2@tcp(db-a:3306)/app_two", reachability: "root:hunter2@tcp(db-b:3307)/app_one"},

	// Postgres and its trigger-CDC flavor: the two engines with a real
	// namespace axis, both reading the sluice-custom `schema` parameter.
	"postgres": {
		canonical:    "postgres://u:hunter2@db-a:5432/app_one",
		dataset:      "postgres://u:hunter2@db-a:5432/app_two",
		reachability: "postgres://u:hunter2@db-b:6432/app_one",
		namespace:    "postgres://u:hunter2@db-a:5432/app_one?schema=tenant_b",
	},
	"postgres-trigger": {
		canonical:    "postgres://u:hunter2@db-a:5432/app_one",
		dataset:      "postgres://u:hunter2@db-a:5432/app_two",
		reachability: "postgres://u:hunter2@db-b:6432/app_one",
		namespace:    "postgres://u:hunter2@db-a:5432/app_one?schema=tenant_b",
	},

	// The file engines. No reachability axis: a path IS how you reach it,
	// and it is also which dataset it is — which is exactly why excluding
	// the "host" cannot be read as excluding the path.
	"sqlite":         {canonical: "./app_one.db", dataset: "./app_two.db"},
	"sqlite-trigger": {canonical: "./app_one.db", dataset: "./app_two.db"},
	"mydumper":       {canonical: "/dumps/one", dataset: "/dumps/two"},
	"csv":            {canonical: "./one.csv", dataset: "./two.csv"},
	"tsv":            {canonical: "./one.tsv", dataset: "./two.tsv"},
	"ndjson":         {canonical: "./one.ndjson", dataset: "./two.ndjson"},

	// D1: the database id is the dataset; the ACCOUNT is reachability, for
	// the same reason a host is — `d1://<account>/<db>` and `d1://<db>`
	// (account from the environment) are two spellings of one database.
	"d1":         {canonical: "d1://acct-a/db-one", dataset: "d1://acct-a/db-two", reachability: "d1://acct-b/db-one"},
	"d1-trigger": {canonical: "d1://acct-a/db-one", dataset: "d1://acct-a/db-two", reachability: "d1://acct-b/db-one"},
}

// TestEverySourceEngineDescribesItsIdentity is the registry-derived
// roster for audit 2026-09-15 F-1: `migrate --resume` refuses a foreign
// source, and that refusal is only as good as the identity it compares.
//
// SCOPE, so the name cannot be read as broader than the truth. It grades
// three properties of [ir.SourceIdentityDescriber], per registered
// engine, off real engine values from the real registry:
//
//   - the engine IMPLEMENTS the surface at all (an engine that does not
//     contributes only its name, and two different sources under it
//     compare equal);
//   - a DSN differing ONLY in the dataset yields a DIFFERENT identity —
//     the silent-adoption direction;
//   - a DSN differing ONLY in how to reach the same source yields the
//     SAME identity (ADR-0015), and the identity contains no host, port,
//     account or credential — the false-refusal and the leak directions.
//
// What it does NOT grade: that the engine's parse agrees with the
// engine's own CONNECTION path on a pathological DSN. That is graded by
// [TestSourceIdentityIsInjectiveThroughRealDSNs] for the shapes with
// measured defects, and by each engine's own DSN tests.
func TestEverySourceEngineDescribesItsIdentity(t *testing.T) {
	names := engines.Names()
	if len(names) < 12 {
		t.Fatalf("registry holds %d engines (%v); the blank-import list has drifted from cmd/sluice and this "+
			"gate is checking a subset of the fleet", len(names), names)
	}

	var graded, exempted, nonRelational []string
	usedTokens := map[string]bool{}
	reachabilityCells, namespaceCells := 0, 0

	for _, name := range names {
		e, ok := engines.Get(name)
		if !ok {
			t.Fatalf("engines.Names() reported %q but engines.Get did not return it", name)
		}
		if reason, isExempt := sourceIdentityExempt[name]; isExempt {
			if reason == "" {
				t.Errorf("%q is exempt with an EMPTY reason; an exemption without a reason is indistinguishable "+
					"from an oversight", name)
			}
			exempted = append(exempted, name)
			continue
		}

		dsns, listed := sourceIdentityDSNs[name]
		if !listed {
			t.Errorf("engine %q has no entry in sourceIdentityDSNs.\n\nAdd one — a canonical DSN, a sibling "+
				"differing ONLY in the database/file/dataset, and (where the engine has one) a sibling "+
				"differing only in how to reach the same source. Without it the engine joins the fleet "+
				"UNGRADED, which is how `migrate --resume` came to adopt a foreign source's completed copy "+
				"for every engine the orchestrator's own DSN parser did not recognise (audit F-1).", name)
			continue
		}
		for _, tok := range identityForbiddenTokens {
			if strings.Contains(dsns.canonical+dsns.dataset+dsns.reachability+dsns.namespace, tok) {
				usedTokens[tok] = true
			}
		}

		d, ok := e.(ir.SourceIdentityDescriber)
		if !ok {
			t.Errorf("engine %q does not implement ir.SourceIdentityDescriber.\n\nIts identity is then the "+
				"ENGINE NAME alone, so a `migrate --resume` pointed at a DIFFERENT database, file or dataset "+
				"of this engine compares EQUAL, adopts the recorded copy and exits 0 having copied nothing. "+
				"Implement SourceIdentity(dsn) (no context, no connection, no host), or add the engine to "+
				"sourceIdentityExempt with the reason that is acceptable.", name)
			continue
		}
		graded = append(graded, name)
		if !strings.HasPrefix(name, "postgres") && !isMySQLFamilyName(name) {
			nonRelational = append(nonRelational, name)
		}

		canonical := d.SourceIdentity(dsns.canonical)
		if canonical == (ir.SourceIdentity{}) {
			t.Errorf("engine %q renders NO identity for a well-formed DSN (%q); every source of this engine "+
				"would then compare equal", name, dsns.canonical)
			continue
		}
		if sibling := d.SourceIdentity(dsns.dataset); sibling == canonical {
			t.Errorf("engine %q renders the SAME identity (%+v) for two DSNs naming DIFFERENT datasets:\n"+
				"  %s\n  %s\nA --resume against the second would adopt the first's completed copy and exit 0 "+
				"having copied nothing.", name, canonical, dsns.canonical, dsns.dataset)
		}
		if dsns.reachability != "" {
			reachabilityCells++
			if moved := d.SourceIdentity(dsns.reachability); moved != canonical {
				t.Errorf("engine %q renders a DIFFERENT identity for the same source reached another way:\n"+
					"  %s -> %+v\n  %s -> %+v\nADR-0015 makes --migration-id the operator's assertion of a "+
					"stable identity across DNS shifts and host renames, so this refuses a legitimate resume "+
					"after a failover or a rename.", name, dsns.canonical, canonical, dsns.reachability, moved)
			}
		}
		if dsns.namespace != "" {
			namespaceCells++
			if other := d.SourceIdentity(dsns.namespace); other == canonical {
				t.Errorf("engine %q renders the SAME identity for two NAMESPACES of one database:\n  %s\n  %s",
					name, dsns.canonical, dsns.namespace)
			}
		}
		// Nothing about WHERE the source lives, or how to authenticate to
		// it, may reach a control row.
		for _, tok := range identityForbiddenTokens {
			if strings.Contains(canonical.Database, tok) || strings.Contains(canonical.Schema, tok) {
				t.Errorf("engine %q's identity %+v carries %q — a host, port, account or credential. The "+
					"identity is persisted to a control table on the TARGET and printed in refusals; it must "+
					"carry neither where the source lives nor how to reach it.", name, canonical, tok)
			}
		}
	}

	// Anti-vacuity, four ways.
	if len(graded) < 12 {
		t.Fatalf("graded only %d engines (%v); want every registered engine", len(graded), graded)
	}
	if len(nonRelational) < 2 {
		t.Fatalf("graded only %d engines outside the Postgres/MySQL families (%v). Those two families are the "+
			"ONLY shapes the pre-F-1 orchestrator could parse, so a roster that does not reach the file, D1 "+
			"and dump engines is grading exactly the cases that already worked", len(nonRelational), nonRelational)
	}
	if reachabilityCells < 2 || namespaceCells < 1 {
		t.Fatalf("exercised %d reachability cells and %d namespace cells; both directions must be live, or "+
			"an identity that simply returned the whole DSN would pass", reachabilityCells, namespaceCells)
	}
	for _, tok := range identityForbiddenTokens {
		if !usedTokens[tok] {
			t.Errorf("forbidden token %q appears in no DSN in this roster, so asserting its absence from every "+
				"identity proves nothing; drop it or put it in a DSN", tok)
		}
	}
	if len(exempted) > 0 {
		t.Errorf("sourceIdentityExempt is expected to be EMPTY (every engine describes its source identity "+
			"since audit 2026-09-15 F-1); it exempts %v", exempted)
	}
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
	}
	for name := range sourceIdentityDSNs {
		if !registered[name] {
			t.Errorf("sourceIdentityDSNs names %q, which is not a registered engine — the entry is stale; drop it", name)
		}
	}
	sort.Strings(graded)
	t.Logf("source-identity roster: %s", strings.Join(graded, ", "))
}

// isMySQLFamilyName reports whether name is one of the flavors the mysql
// engine type registers. Used only by the anti-vacuity floor above, to
// count how much of the roster lies OUTSIDE the two DSN families the
// pre-F-1 orchestrator could parse.
func isMySQLFamilyName(name string) bool {
	switch name {
	case "mysql", "mariadb", "planetscale", "vitess":
		return true
	}
	return false
}

// TestSourceIdentityIsInjectiveThroughRealDSNs is the other half of the
// door's injectivity, and the half audit 2026-09-15 F-3 found broken.
//
// The pipeline's own test grades the FRAMING — fields in, one string out
// — and says so. It cannot grade the step before it, because the
// orchestrator no longer has a DSN parser to grade: distinct DSNs must
// yield distinct FIELDS, and that is the engines' half. An injective
// framing over a colliding extractor is precisely the vacuous door F-1
// found, so both halves are pinned.
//
// The first block is the family sweep: hostile dataset and namespace
// names fed THROUGH a real DSN of each accepted form. The second block
// is the measured F-3 cells, each of which admitted a foreign resume.
func TestSourceIdentityIsInjectiveThroughRealDSNs(t *testing.T) {
	describer := func(t *testing.T, engine string) ir.SourceIdentityDescriber {
		t.Helper()
		e, ok := engines.Get(engine)
		if !ok {
			t.Fatalf("engine %q is not registered", engine)
		}
		d, ok := e.(ir.SourceIdentityDescriber)
		if !ok {
			t.Fatalf("engine %q does not describe its identity", engine)
		}
		return d
	}

	// Hostile names, each fed through a real DSN of a given form. The
	// forms are the ones an operator actually types, and the names are the
	// ones that alias under a naive extractor: the field separator, an
	// `=`, a quote, a space, and the key syntax itself.
	families := []struct {
		name   string
		engine string
		// dsn renders one hostile dataset name into a DSN of this form.
		dsn func(dataset string) string
		// datasets that must all yield DISTINCT identities.
		datasets []string
	}{
		{
			name: "postgres URI", engine: "postgres",
			dsn:      func(d string) string { return "postgres://u:hunter2@h:5432/" + strings.ReplaceAll(d, "/", "%2F") },
			datasets: []string{"app", "app_two", "a=b", "a;database=b", "a%20b", "a.b"},
		},
		{
			name: "postgres libpq key/value", engine: "postgres",
			// Single-quoted, with the libpq escape for an embedded quote —
			// which is the whole point: a whitespace split reads the first
			// word of a quoted value and calls it the database.
			dsn: func(d string) string {
				return "host=h port=5432 user=u password=hunter2 dbname='" +
					strings.ReplaceAll(strings.ReplaceAll(d, `\`, `\\`), `'`, `\'`) + "'"
			},
			datasets: []string{"app", "app two", "app three", "a=b", `a'b`, `a\b`, "a;dbname=b"},
		},
		{
			name: "mysql tcp", engine: "mysql",
			dsn:      func(d string) string { return "root:hunter2@tcp(h:3306)/" + d },
			datasets: []string{"app", "app_two", "a-b", "a.b"},
		},
		{
			name: "mysql with no protocol section", engine: "mysql",
			// The form the pre-F-1 extractor did not recognise AT ALL, so
			// every such DSN rendered one identity.
			dsn:      func(d string) string { return "root:hunter2@/" + d },
			datasets: []string{"app", "app_two", "a-b", "a.b"},
		},
		{
			name: "sqlite file path", engine: "sqlite",
			dsn:      func(d string) string { return "./" + d + ".db" },
			datasets: []string{"one", "two", "a b", "a=b", "a;database=b"},
		},
		{
			name: "d1 database id", engine: "d1",
			dsn:      func(d string) string { return "d1://acct/" + d },
			datasets: []string{"db-one", "db-two", "a=b", "a;database=b"},
		},
		{
			name: "mydumper dump directory", engine: "mydumper",
			dsn:      func(d string) string { return "/dumps/" + d },
			datasets: []string{"one", "two", "a b", "a=b"},
		},
		{
			name: "flat file", engine: "csv",
			dsn:      func(d string) string { return "./" + d + ".csv" },
			datasets: []string{"one", "two", "a b", "a=b"},
		},
	}

	for _, f := range families {
		t.Run(f.name, func(t *testing.T) {
			d := describer(t, f.engine)
			seen := map[ir.SourceIdentity]string{}
			for _, dataset := range f.datasets {
				dsn := f.dsn(dataset)
				id := d.SourceIdentity(dsn)
				if id == (ir.SourceIdentity{}) {
					t.Errorf("dataset %q -> DSN %q renders NO identity; every source of this form would "+
						"compare equal", dataset, dsn)
					continue
				}
				if prev, dup := seen[id]; dup {
					t.Errorf("datasets %q and %q render the SAME identity %+v through %q — a --resume against "+
						"one adopts the other's completed copy and exits 0 having copied nothing",
						prev, dataset, id, dsn)
				}
				seen[id] = dataset
			}
			if len(seen) != len(f.datasets) {
				t.Errorf("collapsed %d datasets into %d identities", len(f.datasets), len(seen))
			}
		})
	}

	// The measured F-3 cells: each pair was shown to render ONE identity
	// by the pre-tag value-fidelity review, and each therefore admitted a
	// resume against a different database.
	t.Run("libpq key/value cells measured by the F-3 review", func(t *testing.T) {
		d := describer(t, "postgres")
		mustDiffer := []struct{ why, a, b string }{
			{
				"a quoted value carrying a space was cut at the space",
				`host=h dbname='a b'`, `host=h dbname='a c'`,
			},
			{
				"a repeated keyword took the FIRST value; libpq and pgx take the LAST",
				`dbname=a dbname=b`, `dbname=a dbname=c`,
			},
			{
				"a quoted PASSWORD containing `dbname=` was read as the database",
				`password='s dbname=x' dbname=real1`, `password='s dbname=x' dbname=real2`,
			},
		}
		for _, c := range mustDiffer {
			if a, b := d.SourceIdentity(c.a), d.SourceIdentity(c.b); a == b {
				t.Errorf("%s:\n  %s\n  %s\nboth render %+v, so a --resume against one adopts the other's copy",
					c.why, c.a, c.b, a)
			}
		}
		// LAST-WINS is not just "they differ" — the value must be the one
		// the driver will actually connect to.
		if got, want := d.SourceIdentity(`dbname=a dbname=b`).Database, "b"; got != want {
			t.Errorf("dbname=a dbname=b -> %q; libpq and pgx connect to %q, so the identity names a database "+
				"this run never reads", got, want)
		}
		if got, want := d.SourceIdentity(`password='s dbname=x' dbname=real1`).Database, "real1"; got != want {
			t.Errorf("a quoted password swallowed the database: got %q, want %q", got, want)
		}
		if got, want := d.SourceIdentity(`host=h dbname='a b'`).Database, "a b"; got != want {
			t.Errorf("quoted value: got %q, want %q", got, want)
		}
		// Two spellings of ONE source must still resume across each other.
		if a, b := d.SourceIdentity(`host=h dbname=app schema=public`), d.SourceIdentity(`host=h dbname=app`); a != b {
			t.Errorf("an explicit schema=public and an absent one render %+v and %+v; a legitimate resume "+
				"across the two spellings would be refused", a, b)
		}
	})

	// The MySQL re-spelling the F-3 note asked about: before F-1 the two
	// forms rendered two identities, so re-spelling a DSN between runs
	// refused a legitimate resume. They are one source and now compare
	// equal — because the DRIVER'S parser answers, and the host it
	// resolves is excluded either way.
	t.Run("two spellings of one MySQL source", func(t *testing.T) {
		d := describer(t, "mysql")
		bare := d.SourceIdentity("root:hunter2@/db1")
		tcp := d.SourceIdentity("root:hunter2@tcp(127.0.0.1:3306)/db1")
		if bare != tcp {
			t.Errorf("root:hunter2@/db1 -> %+v but root:hunter2@tcp(127.0.0.1:3306)/db1 -> %+v; re-spelling a "+
				"DSN between runs would refuse a legitimate resume", bare, tcp)
		}
		if bare.Database != "db1" {
			t.Errorf("the protocol-less MySQL form rendered database %q, want %q — this is the spelling the "+
				"pre-F-1 extractor did not recognise at all", bare.Database, "db1")
		}
		// A password containing `?` used to truncate the database, because
		// the extractor cut the DSN at the first question mark.
		if got := d.SourceIdentity("root:hun?ter2@tcp(h:3306)/db1").Database; got != "db1" {
			t.Errorf("a password containing '?' rendered database %q, want %q", got, "db1")
		}
	})
}
