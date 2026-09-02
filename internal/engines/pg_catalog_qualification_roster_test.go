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
// pg_catalog: `testdata/pg_catalog_procs.txt` is the UNION of
//
//	SELECT DISTINCT proname FROM pg_proc
//	 WHERE pronamespace = 'pg_catalog'::regnamespace ORDER BY 1
//
// across the majors sluice supports — PostgreSQL 16.15, 17.11, 18.6 and
// 19beta3 (2,847 names; 16 alone is 2,694, 17 adds 37, 18 adds 106 more,
// 19beta3 another 47). A union, because a name that exists on ANY
// supported major can be hijacked on that major and must be graded
// everywhere; when a new major ships, regenerate the fixture from a real
// container of it and re-union. Every string literal in every
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
// roster redundant — the remedy pg_dump adopted for CVE-2018-1058, the same
// search_path function-resolution class — is the scheduled follow-up: it
// needs every EXTENSION type and function sluice emits (PostGIS, hstore,
// citext, uuid-ossp, pgcrypto) namespace-qualified first, or it refuses
// working configurations.
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
	if got := unqualifiedCatalogCalls("SELECT LOWER(c.data_type) FROM information_schema.columns c", procs); len(got) != 1 || got[0].name != "lower" {
		t.Fatalf("scanner self-check: an UPPERCASE spelling must flag exactly like lowercase, got %v", got)
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
			// Every top-level declaration, not only function bodies: SQL
			// held in package-level const/var blocks is sent to a server
			// exactly like SQL spelled inline (the pre-tag VF review of
			// v0.137.4 found 17 such sites the first cut never graded).
			for _, decl := range f.Decls {
				owner := topLevelDeclName(decl)
				ast.Inspect(decl, func(n ast.Node) bool {
					lit, ok := n.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					qualifiedSeen += strings.Count(strings.ToLower(lit.Value), "pg_catalog.")
					for _, hit := range unqualifiedCatalogCalls(lit.Value, procs) {
						key := filepath.ToSlash(path) + ":" + owner + ":" + hit.name
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
	// (a) SQL-standard syntactic forms. EXTRACT is handled by name in the
	// scanner (every spelling here is the FROM form); type modifiers by the
	// typmod rule. Multi-argument unnest(a, b) is grammar too: the parser expands the
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
	"postgres/expr_translate.go:rewriteCASTCharCharset:varchar":      "PG target type spec assembled from a bare varchar( fragment plus a length; a type, not a call",
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
//
// Case-insensitive on purpose: SQL folds unquoted identifiers, so
// `LOWER(c.data_type)` resolves exactly like `lower(...)` — and the first
// cut's lowercase-only pattern left ~100 UPPERCASE spellings ungraded, one
// of them the schema reader's column-type read, hijacked live through a
// planted `public.lower(character varying)` (pre-tag VF review, v0.137.4).
var catalogCallRE = regexp.MustCompile(`([A-Za-z0-9_]+\.)?\b([A-Za-z_][A-Za-z0-9_]*)\(`)

// typmodArgsRE matches the text after a `(` when it is a type modifier:
// one or two integer literals or `%d` verbs, then the closing paren.
var typmodArgsRE = regexp.MustCompile(`^\s*(\d+|%d)\s*(,\s*(\d+|%d)\s*)?\)`)

// topLevelDeclName names the declaration a literal lives in, for the
// exemption key: the function for a FuncDecl, the first declared name for a
// const/var/type block.
func topLevelDeclName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		return d.Name.Name
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.ValueSpec:
				if len(s.Names) > 0 {
					return s.Names[0].Name
				}
			case *ast.TypeSpec:
				return s.Name.Name
			}
		}
	}
	return "<decl>"
}

// unqualifiedCatalogCalls returns every call in the RAW literal text whose
// name is a pg_catalog function and which is not spelled `pg_catalog.name(`.
// It scans the raw (quoted, escaped) literal so that byte offsets stay
// meaningful to a rewriter; escape sequences cannot spell an identifier
// followed by `(`, so scanning raw text loses nothing.
func unqualifiedCatalogCalls(raw string, procs map[string]bool) []catalogCall {
	var out []catalogCall
	for _, m := range catalogCallRE.FindAllStringSubmatchIndex(raw, -1) {
		name := strings.ToLower(raw[m[4]:m[5]])
		if !procs[name] {
			continue
		}
		// A type modifier is not a call: `VARCHAR(255)`, `NUMERIC(%d,%d)`,
		// `CHAR(1)`, `BIT(8)` name a TYPE in DDL, and a type name resolves
		// through pg_type, where an unprivileged role's function cannot
		// shadow anything. Derived from the argument text — digits, commas,
		// spaces and `%d` verbs only — not from a list of type names, so a
		// real call to one of the coercion functions of the same name
		// (`varchar(x, 10, true)`) still grades.
		if rest := raw[m[1]:]; typmodArgsRE.MatchString(rest) {
			continue
		}
		// EXTRACT(field FROM source) is grammar, not a call, in every
		// spelling this repo uses; `pg_catalog.extract(epoch from x)` is a
		// syntax error. The function-call form only exists from PG 14 and
		// is never spelled here.
		if name == "extract" {
			continue
		}
		if m[2] >= 0 {
			qual := raw[m[2]:m[3]]
			if strings.EqualFold(qual, "pg_catalog.") {
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
	f, err := os.Open(filepath.Join("testdata", "pg_catalog_procs.txt"))
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
	// Fixture floor: the 16–19 union is 2,847 names and no single major is
	// below 2,690; a fixture under 2,800 has lost a major (or was
	// regenerated from one), which silently shrinks the universe. The
	// three named entries are the ones the SEC-CRIT-1 reproduction
	// hijacked; the three version sentinels — `icu_unicode_version`
	// (first in 17), `crc32` (first in 18), `error_on_null` (first in
	// 19beta3), each measured absent from every earlier major — prove the
	// union was actually taken rather than one major re-dumped.
	if len(procs) < 2800 {
		t.Fatalf("fixture carries only %d names (floor 2800: the PG 16–19 union) — regenerate from real containers of every supported major and re-union", len(procs))
	}
	for _, must := range []string{"icu_unicode_version", "crc32", "error_on_null"} {
		if !procs[must] {
			t.Fatalf("fixture is missing %q (a 17+/18+/19+ sentinel) — it is not the cross-major union this gate expects", must)
		}
	}
	for _, must := range []string{"to_jsonb", "array_to_json", "quote_ident"} {
		if !procs[must] {
			t.Fatalf("fixture is missing %q — it is not the pg_catalog proname dump this gate expects", must)
		}
	}
	return procs
}
