// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package engines

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The pg_catalog qualification roster (audit 2026-09-01, SEC-CRIT-1).
//
// # The class
//
// PostgreSQL resolves an unqualified function call by collecting every
// candidate of that name from every schema on the session's search_path
// and then picking the BEST type match — schema order only breaks ties
// between candidates with IDENTICAL signatures. So for any built-in whose
// declared parameter types are not an exact match for the call's argument
// types — polymorphic built-ins (`to_jsonb(anyelement)`,
// `array_to_json(anyarray)`, `format(text, VARIADIC "any")`) and every
// call that needs an implicit cast (`quote_ident(text)` over a `name`
// column) — a role that can CREATE in any schema on the path plants an
// exact-typed overload and it wins, and the body runs with the CALLER's
// privileges. sluice's client connections are typically a superuser (the
// pgtrigger flavor requires one for CREATE EVENT TRIGGER) with the default
// `"$user", public` path. SEC-1 (v0.134.1) closed this inside the SECURITY
// DEFINER capture functions; this roster closes the identical mechanism in
// the SQL sluice itself sends — reproduced to `ALTER ROLE … SUPERUSER` on
// real PG 16 through `readInstallMeta`'s `to_jsonb(m)` before the fix.
//
// # How the universe is derived (not hand-listed)
//
// The set of names that CAN be hijacked is the set of functions in
// pg_catalog: `testdata/pg16_catalog_procs.txt` is
//
//	SELECT DISTINCT proname FROM pg_proc
//	 WHERE pronamespace = 'pg_catalog'::regnamespace ORDER BY 1
//
// on PostgreSQL 16.15 (2,694 names). Every string literal in every
// non-test .go file under the two Postgres engine packages is scanned for
// `<name>(` where <name> is in that set; each such call must be spelled
// `pg_catalog.<name>(`. A qualified reference restricts resolution to that
// one schema, which no unprivileged role can create in.
//
// WHAT THIS GATE REACHES, stated so the name cannot be read as broader than
// the truth: SQL that `internal/engines/postgres` and
// `internal/engines/pgtrigger` SPELL in Go string literals. Honest
// residuals: (1) SQL assembled at runtime from source-catalog text
// (`pg_get_expr` output re-emitted as target DDL) — that text is the
// operator's own objects and resolves under the operator's path on the
// target, by design; (2) operator-supplied predicates (`--where`), which
// are the operator's to qualify; (3) zero-argument built-ins, which an
// overload can only shadow from a schema EARLIER than pg_catalog on the
// path — impossible unless the operator lists pg_catalog after an
// attacker-writable schema — are still graded here because qualifying them
// costs nothing and keeps the rule one sentence; (4) the MySQL/SQLite
// engines, which have no schema-search resolution of this shape. The
// connect-level `search_path = pg_catalog, pg_temp` pin that would make this
// roster redundant is the scheduled follow-up — it needs every EXTENSION
// type and function sluice emits (PostGIS, hstore, citext, uuid-ossp,
// pgcrypto) namespace-qualified first, or it refuses working configurations.
//
// # Exemptions
//
// Fail-by-default: an unqualified call passes only with an entry in
// [pgCatalogQualificationExempt], keyed `<package>/<file>:<GoFunc>:<name>`,
// each carrying the reason. The only legitimate reasons are (a) the name is
// a SQL-standard syntactic form with no function-call spelling in that
// position, (b) the literal is prose (a hint or error message that NAMES a
// function for the operator), which never reaches a server — none today,
// because qualified prose is still true prose and the fix qualified it
// rather than exempting it — or (c) the literal is a PATTERN compared
// against text the SOURCE produced, where qualifying it would silently
// stop it matching.
//
// Mutation-verified in both directions (2026-09-01): removing the
// `pg_catalog.` from readInstallMeta's `to_jsonb(m)` fails this gate naming
// the site; the in-test scanner self-check fails if the scanner stops
// flagging a canned unqualified literal or starts flagging a qualified one;
// the floor below fails if the walker stops seeing the qualified calls the
// fix left behind.
func TestPGClientSQLQualifiesEveryCatalogFunction(t *testing.T) {
	t.Parallel()
	procs := loadPGCatalogProcs(t)

	// Scanner self-check — the in-test mutation floor. A scanner that does
	// not flag the canned hijackable literal, or that flags the qualified
	// one, grades nothing while reporting green.
	if got := unqualifiedCatalogCalls("SELECT to_jsonb(m) ->> 'x' FROM t m", procs); len(got) != 1 || got[0].name != "to_jsonb" {
		t.Fatalf("scanner self-check: expected exactly [to_jsonb] from the canned unqualified literal, got %v", got)
	}
	if got := unqualifiedCatalogCalls("SELECT pg_catalog.to_jsonb(m), pg_catalog.count(*) FROM t m", procs); len(got) != 0 {
		t.Fatalf("scanner self-check: the canned qualified literal must not flag, got %v", got)
	}

	var violations []string
	exemptUsed := map[string]bool{}
	qualifiedSeen := 0
	for _, pkg := range []string{"postgres", "pgtrigger"} {
		err := filepath.WalkDir(pkg, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return fs.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					lit, ok := n.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					qualifiedSeen += strings.Count(lit.Value, "pg_catalog.")
					for _, hit := range unqualifiedCatalogCalls(lit.Value, procs) {
						key := filepath.ToSlash(path) + ":" + fn.Name.Name + ":" + hit.name
						if _, ok := pgCatalogQualificationExempt[key]; ok {
							exemptUsed[key] = true
							continue
						}
						violations = append(violations, fmt.Sprintf("%s (%s) — `%s(` must be spelled `pg_catalog.%s(`; context: %q",
							key, fset.Position(lit.Pos()), hit.name, hit.name, hit.context))
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", pkg, err)
		}
	}
	sort.Strings(violations)
	for _, v := range violations {
		t.Error(v)
	}
	for key := range pgCatalogQualificationExempt {
		if !exemptUsed[key] {
			t.Errorf("stale exemption %q: no unqualified call matches it any more — delete the entry", key)
		}
	}
	// Anti-vacuity floor: the qualification the SEC-CRIT-1 fix wrote is
	// ~60 sites; a walker that sees fewer than 30 `pg_catalog.` spellings
	// has stopped reading the packages it exists to grade.
	if qualifiedSeen < 30 {
		t.Fatalf("saw only %d `pg_catalog.` spellings across both packages (floor 30) — the walker stopped seeing the SQL this gate grades", qualifiedSeen)
	}
}

// pgCatalogQualificationExempt is the fail-by-default exemption map; see
// the gate doc for the two admissible reasons. Keys are
// `<package>/<file>:<GoFunc>:<name>`.
var pgCatalogQualificationExempt = map[string]string{
	// (a) SQL-standard syntactic forms: EXTRACT(field FROM source) is
	// grammar, not a function call, and `pg_catalog.extract(epoch from x)`
	// is a syntax error (the function-call spelling only exists from PG 14).
	"postgres/ddl_default_sqlite.go:translateSQLiteStrftimeDefault:extract": "EXTRACT(field FROM source) is a syntactic form; qualifying it is a syntax error",
	// Multi-argument unnest(a, b) is grammar too: the parser expands the
	// BARE spelling into ROWS FROM(unnest(a), unnest(b)); the qualified
	// spelling is looked up as a two-argument function and does not exist
	// (`pg_catalog.unnest(smallint[], smallint[]) does not exist`, caught by
	// TestClientSQL_DecoyOverloadsInPublicNeverFire on a real PG 16 — the
	// source-text gate alone was green). Single-argument unnest stays
	// qualified everywhere else.
	"postgres/schema_reader.go:populateForeignKeys:unnest": "multi-argument unnest(a, b) is a syntactic ROWS FROM expansion; the qualified two-argument function does not exist",
	// (c) source-side PATTERNS, never sent to a server: these literals are
	// compared against text the SOURCE produced (a SQLite default
	// expression; a MySQL CAST type spec; pg_get_expr output, which renders
	// pg_catalog functions unqualified because pg_catalog is always
	// visible). Qualifying a pattern would silently stop it matching —
	// the rewriter that qualified the tree did exactly that to the
	// auto-increment detector before review caught it.
	"postgres/ddl_default_sqlite.go:translateSQLiteDefaultExpr:date": "SQLite source default-expression pattern",
	"postgres/ddl_default_sqlite.go:translateSQLiteDefaultExpr:time": "SQLite source default-expression pattern",
	"postgres/expr_translate.go:rewriteCASTCharCharset:char":         "MySQL source CAST type-spec pattern",
	"postgres/schema_reader.go:isAutoIncrement:nextval":              "pattern over pg_get_expr output, which spells pg_catalog functions unqualified",
	"postgres/sequence_reader.go:parseNextvalSequence:nextval":       "pattern over pg_get_expr output, which spells pg_catalog functions unqualified",
}

type catalogCall struct {
	name    string
	context string
}

// catalogCallRE matches an identifier IMMEDIATELY followed by `(` — no
// whitespace, which is how every call in this repo is spelled and what
// separates a call from prose such as "empty position (forces …)". The
// optional qualifier group decides the verdict: `pg_catalog.` passes; any
// other qualifier (an explicit `public.`) or none at all flags. The group
// deliberately admits only identifier characters so a literal's opening
// quote cannot be swallowed into it.
var catalogCallRE = regexp.MustCompile(`([A-Za-z0-9_]+\.)?\b([a-z_][a-z0-9_]*)\(`)

// unqualifiedCatalogCalls returns every call in the RAW literal text whose
// name is a pg_catalog function and which is not spelled `pg_catalog.name(`.
// It scans the raw (quoted, escaped) literal so that byte offsets stay
// meaningful to a rewriter; escape sequences cannot spell an identifier
// followed by `(`, so scanning raw text loses nothing.
func unqualifiedCatalogCalls(raw string, procs map[string]bool) []catalogCall {
	var out []catalogCall
	for _, m := range catalogCallRE.FindAllStringSubmatchIndex(raw, -1) {
		name := raw[m[4]:m[5]]
		if !procs[name] {
			continue
		}
		if m[2] >= 0 {
			qual := raw[m[2]:m[3]]
			if qual == "pg_catalog." {
				continue
			}
		}
		// A `%`-verb or `$`-param immediately before the name is Go
		// formatting or a bind parameter, never a SQL call site.
		if start := m[4]; start > 0 {
			switch raw[start-1] {
			case '%', '$':
				continue
			}
		}
		lo, hi := m[0]-20, m[1]+20
		if lo < 0 {
			lo = 0
		}
		if hi > len(raw) {
			hi = len(raw)
		}
		out = append(out, catalogCall{name: name, context: raw[lo:hi]})
	}
	return out
}

func loadPGCatalogProcs(t *testing.T) map[string]bool {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", "pg16_catalog_procs.txt"))
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer func() { _ = f.Close() }()
	procs := map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" && !strings.HasPrefix(line, "#") {
			procs[line] = true
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	// Fixture floor: PG 16 ships ~2,700 pg_catalog functions; a truncated
	// fixture would silently shrink the universe. The three named entries
	// are the ones the SEC-CRIT-1 reproduction hijacked.
	if len(procs) < 2000 {
		t.Fatalf("fixture carries only %d names (floor 2000) — regenerate it from a real PG 16", len(procs))
	}
	for _, must := range []string{"to_jsonb", "array_to_json", "quote_ident"} {
		if !procs[must] {
			t.Fatalf("fixture is missing %q — it is not the pg_catalog proname dump this gate expects", must)
		}
	}
	return procs
}
