// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// utcGateExemptFiles are the non-test files in the scanned packages whose
// string literals legitimately carry CURRENT_TIMESTAMP: they translate or
// describe a USER schema's expressions (whose semantics are the user's),
// never a sluice-owned table's clock. Each entry says why; a new file
// holding session-clock SQL must earn an entry here or use [utcNowSQL].
var utcGateExemptFiles = map[string]string{
	"postgres/expr_translate.go":  "translates a MySQL source expression (NOW()/CURRENT_TIMESTAMP(N)) into the user's own generated/DEFAULT/CHECK expression",
	"postgres/reserved_idents.go": "a reserved-keyword list for identifier quoting, not SQL",
}

// utcGateExemptLiterals exempts single literals, not files, where the file
// also carries SQL the gate must keep policing: key "<pkg>/<file>", value
// a substring identifying the one literal. Each must still match a literal
// (a stale entry fails).
var utcGateExemptLiterals = map[string]string{
	// WARN prose naming a MySQL source column's ON UPDATE CURRENT_TIMESTAMP;
	// the rest of ddl_emit.go (every DEFAULT it renders) stays in scope.
	"postgres/ddl_emit.go": "source column re-stamps itself on UPDATE (ON UPDATE CURRENT_TIMESTAMP)",
}

var (
	// sessionClockSQL is the value that stores the SESSION zone's digits
	// when written into a naive TIMESTAMP: CURRENT_TIMESTAMP (a timestamptz
	// cast through TimeZone) and LOCALTIMESTAMP (the session's local time).
	sessionClockSQL = regexp.MustCompile(`(?i)\b(CURRENT_TIMESTAMP|LOCALTIMESTAMP)\b`)
	// naiveNowDefault is a naive TIMESTAMP column defaulting to a bare
	// now(): the same cast, spelled differently. TIMESTAMPTZ does not match
	// (no word boundary after TIMESTAMP), nor does TIMESTAMP WITH TIME ZONE.
	naiveNowDefault = regexp.MustCompile(`(?i)\bTIMESTAMP\b(\s*\(\s*\d+\s*\))?\s+(NOT\s+NULL\s+|NULL\s+)?DEFAULT\s+\(?\s*(pg_catalog\.)?now\(\)`)
	// createsTable marks a literal that creates a sluice-owned table —
	// the anti-vacuity floor counts them.
	createsTable = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS`)
)

// TestControlTableSQL_WritesUTCIntoNaiveTimestamps is the roster gate for
// GC-39 item 2: no SQL string literal in the Postgres or pgtrigger engine
// packages writes the SESSION clock into a sluice-owned table. The control
// tables' timestamp columns are naive TIMESTAMP, read back as UTC and aged
// against the process clock, so CURRENT_TIMESTAMP / LOCALTIMESTAMP / a
// naive `DEFAULT now()` stores the session zone's digits — measured seven
// hours stale on an America/Los_Angeles database and nine hours in the
// future (a stall alarm failing open) on Asia/Tokyo. Every such write must
// use [utcNowSQL].
//
// Reach, stated so the name is not read as broader than it is: it scans
// the SQL string LITERALS of every non-test .go file in
// internal/engines/postgres and internal/engines/pgtrigger (derived from
// the directory, not a hand list), minus [utcGateExemptFiles]. It flags
// CURRENT_TIMESTAMP / LOCALTIMESTAMP anywhere and a naive TIMESTAMP column
// DEFAULTing to a bare now(). It does NOT see a DML `SET col = now()` into
// a naive column (the column's type is not in the literal) — the
// integration pin TestControlTables_TimestampsAreUTC_UnderANonUTCDatabaseZone
// grades those behaviourally. MySQL control tables are out of scope: they
// use MySQL TIMESTAMP under sluice's pinned `+00:00` session. It also
// polices ddl_default_sqlite.go, the SQLite current-instant DEFAULT
// translator (GC-39 item 3), which renders on [utcNowSQL] and so needs no
// exemption.
func TestControlTableSQL_WritesUTCIntoNaiveTimestamps(t *testing.T) {
	var (
		filesScanned, tableDDLs, utcRefs int
		violations                       []string
		literalExemptUsed                = map[string]bool{}
	)
	for _, dir := range []string{".", filepath.Join("..", "pgtrigger")} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		pkg := filepath.Base(mustAbs(t, dir))
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel := pkg + "/" + name
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse %s: %v", rel, err)
			}
			filesScanned++
			ast.Inspect(f, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "utcNowSQL" {
					utcRefs++
				}
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				s, err := strconv.Unquote(lit.Value)
				if err != nil {
					return true
				}
				if createsTable.MatchString(s) {
					tableDDLs++
				}
				if _, exempt := utcGateExemptFiles[rel]; exempt {
					return true
				}
				if lit, ok := utcGateExemptLiterals[rel]; ok && strings.Contains(s, lit) {
					literalExemptUsed[rel] = true
					return true
				}
				if m := sessionClockSQL.FindString(s); m != "" {
					violations = append(violations, fset.Position(lit.Pos()).String()+": "+m)
				}
				if m := naiveNowDefault.FindString(s); m != "" {
					violations = append(violations, fset.Position(lit.Pos()).String()+": "+m)
				}
				return true
			})
		}
	}
	// Anti-vacuity floor: the walk reached both packages' files, the
	// sluice-owned table DDLs, and the sites the fix routed through
	// utcNowSQL. A walker that silently scans nothing fails here.
	if filesScanned < 40 || tableDDLs < 6 || utcRefs < 10 {
		t.Fatalf("gate reached too little to mean anything: %d files, %d CREATE TABLE IF NOT EXISTS literals, %d utcNowSQL references (want >=40, >=6, >=10)",
			filesScanned, tableDDLs, utcRefs)
	}
	for _, v := range violations {
		t.Errorf("%s — writes the SESSION clock; a sluice-owned naive TIMESTAMP must be written with utcNowSQL (GC-39 item 2), or the file must earn a utcGateExemptFiles entry", v)
	}
	for rel, lit := range utcGateExemptLiterals {
		if !literalExemptUsed[rel] {
			t.Errorf("utcGateExemptLiterals[%s] = %q matches no literal any more — remove the stale exemption", rel, lit)
		}
	}
	for rel := range utcGateExemptFiles {
		parts := strings.SplitN(rel, "/", 2)
		dir := "."
		if parts[0] == "pgtrigger" {
			dir = filepath.Join("..", "pgtrigger")
		}
		if _, err := os.Stat(filepath.Join(dir, parts[1])); err != nil {
			t.Errorf("utcGateExemptFiles names %s, which no longer exists — remove the stale exemption", rel)
		}
	}
}

func mustAbs(t *testing.T, p string) string {
	t.Helper()
	a, err := filepath.Abs(p)
	if err != nil {
		t.Fatalf("abs %s: %v", p, err)
	}
	return a
}
