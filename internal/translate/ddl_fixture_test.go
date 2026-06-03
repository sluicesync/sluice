//go:build integration && ddlfixture

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// DDL-fixture harness: exercise sluice's MySQL schema reader against
// the Dolt sqllogictest createtable1 corpus (~13,725 lines / ~4,575
// `statement ok` CREATE TABLE entries — all DDL, no INSERT/SELECT
// noise). Phase 1 only: apply each DDL to a real MySQL 8.0
// testcontainer, read the resulting schema back through sluice's IR,
// and assert structural invariants. Phase 2 (writer round-trip) and
// Phase 3 (cross-engine emit) are explicitly out of scope.
//
// Opt-in via the `ddlfixture` build tag (on top of the `integration`
// tag for the testcontainer dependency) so default CI doesn't pay
// the testcontainer + corpus cost. To run locally on Windows:
//
//	export TESTCONTAINERS_RYUK_DISABLED=true
//	export PATH="/c/Program Files/Rancher Desktop/resources/resources/win32/bin:$PATH"
//	go test -tags="integration ddlfixture" -count=1 \
//	    -run TestDDLFixture -timeout=40m ./internal/translate/...
//
// Wall-clock budget: at ~3-4 stmts/sec the full corpus wants
// ~25 min; the in-test context is 30 min and the `-timeout` flag
// should be at least 5 min larger to give the deferred summary
// breathing room. For quick iteration, set `SLUICE_DDL_FIXTURE_MAX`
// to cap the corpus (e.g. `SLUICE_DDL_FIXTURE_MAX=500` for a
// ~2-min smoke run).
//
// What the harness asserts (per surviving CREATE TABLE):
//
//  1. Table is non-nil after read.
//  2. Column count matches the DDL's column count (extracted via a
//     simple top-level comma split).
//  3. ReadSchema returns no error for the just-created table (the IR
//     never carries a sentinel "unknown" type; an unrecognised
//     source type causes ReadSchema to fail loudly with
//     `mysql: unsupported data_type ...`, and we surface that as
//     a gap).
//  4. If the DDL declares a PRIMARY KEY, the IR's `PrimaryKey != nil`.
//
// MySQL refusals (parse/syntax/unsupported-feature) are recorded as
// `mysql-refused` and not counted as sluice bugs — they reflect
// fixture-side dialect drift, not translator gaps.
//
// The corpus + provenance lives in `testdata/sqllogictest/`; see
// `NOTICE` there for upstream source, SHA, license context, and
// refresh instructions.

package translate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"

	"github.com/testcontainers/testcontainers-go"
	mysqltc "github.com/testcontainers/testcontainers-go/modules/mysql"
)

//go:embed testdata/sqllogictest/createtable1.test
var ddlFixtureFS embed.FS

const ddlFixturePath = "testdata/sqllogictest/createtable1.test"

// ddlStatement is one parsed `statement ok` ... `CREATE TABLE` block
// from the fixture. Only blocks whose first SQL token is `CREATE TABLE`
// survive parsing; non-DDL `statement ok` blocks (INSERT/SELECT/etc.)
// and `statement error` blocks are dropped at parse time.
type ddlStatement struct {
	// line is the 1-based line in the fixture where the `statement ok`
	// marker sits; useful for grepping the corpus when a gap surfaces.
	line int

	// table is the unquoted table name parsed from the DDL. Empty when
	// the parser couldn't extract one (very rare; treated as a parse
	// gap and skipped).
	table string

	// sql is the verbatim CREATE TABLE statement, terminating semicolon
	// stripped. Applied to MySQL exactly as-is.
	sql string
}

// TestDDLFixture is the entry point for the createtable1 harness.
// Runs against a single fresh MySQL 8.0 testcontainer; each CREATE
// TABLE is applied in isolation (table dropped between iterations to
// keep information_schema clean).
//
// Failure model:
//
//   - MySQL refusals: counted under `mysql-refused`, first ~10 logged
//     for visibility, rest only counted. Not a test failure.
//   - sluice-side gaps (column-count mismatch, PK lost when declared,
//     read error): each `t.Errorf` with the offending DDL substring so
//     the corpus is greppable. Failures expected on first run; that's
//     the point — they're real translator-side bugs to file.
func TestDDLFixture(t *testing.T) {
	// Skip cleanly when Docker isn't available (dev machine without
	// it, Linux rootless on Windows, etc.). CI Linux runners have
	// Docker and would run the test for real; the harness is opt-in
	// via the `ddlfixture` tag so non-opt-in CI never sees it.
	testcontainers.SkipIfProviderIsNotHealthy(t)

	statements, err := parseDDLFixture()
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	if len(statements) == 0 {
		t.Fatal("parsed zero CREATE TABLE statements; fixture is empty or parser is broken")
	}
	if maxN := envInt("SLUICE_DDL_FIXTURE_MAX"); maxN > 0 && maxN < len(statements) {
		t.Logf("SLUICE_DDL_FIXTURE_MAX=%d; truncating corpus from %d to %d statements",
			maxN, len(statements), maxN)
		statements = statements[:maxN]
	}

	// The full corpus is ~4,575 CREATE TABLE statements; at the
	// observed ~3-4 stmts/sec on Rancher Desktop the loop wants
	// ~25 min wall-clock. The outer go-test timeout is what the
	// operator sets via `-timeout`; the in-test context is sized to
	// fit comfortably inside a 35-min budget so the deferred
	// summary always runs.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	container, err := mysqltc.Run(
		ctx,
		"mysql:8.0",
		mysqltc.WithDatabase("sluice_ddlfixture"),
		mysqltc.WithUsername("test"),
		mysqltc.WithPassword("test"),
	)
	if err != nil {
		t.Fatalf("start mysql container: %v", err)
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}()

	dsn, err := container.ConnectionString(ctx, "parseTime=true", "multiStatements=true")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	reader, err := mysqlEng.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	tally := newRunTally()

	for _, stmt := range statements {
		// Bail when the per-run budget is exhausted. The deferred
		// summary at the bottom still runs and reports what was
		// achieved so the operator gets a partial baseline instead
		// of "test killed, zero output."
		if ctx.Err() != nil {
			t.Logf("context cancelled mid-loop after %d/%d statements; emitting partial summary",
				tally.corpus, len(statements))
			tally.budgetExceeded = true
			break
		}
		tally.corpus++

		applyCtx, applyCancel := context.WithTimeout(ctx, 30*time.Second)
		if _, applyErr := db.ExecContext(applyCtx, stmt.sql); applyErr != nil {
			applyCancel()
			tally.recordMySQLRefusal(stmt, applyErr)
			continue
		}
		applyCancel()
		tally.mysqlApplied++

		// Read back the just-created table's IR shape and assert
		// invariants. Even though we read the entire schema each
		// pass, the `DROP TABLE` between iterations keeps the
		// catalog small (one table per round-trip).
		readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
		schema, readErr := reader.ReadSchema(readCtx)
		readCancel()
		if readErr != nil {
			// `mysql: unsupported data_type ...` lands here, as do
			// any other reader errors. Treat as a sluice gap and
			// surface loudly so the failing DDL is greppable.
			tally.recordReadError(stmt, readErr)
			t.Errorf("ReadSchema failed for %q (line %d): %v\n    DDL: %s",
				stmt.table, stmt.line, readErr, snippet(stmt.sql))
			dropTable(ctx, db, stmt.table)
			continue
		}

		table := findTableInSchema(schema, stmt.table)
		if table == nil {
			tally.recordMissingTable(stmt)
			t.Errorf("ReadSchema did not return table %q (line %d); have %v\n    DDL: %s",
				stmt.table, stmt.line, tableNamesInSchema(schema), snippet(stmt.sql))
			dropTable(ctx, db, stmt.table)
			continue
		}

		clean := true

		wantCols := countDDLColumns(stmt.sql)
		gotCols := len(table.Columns)
		if wantCols > 0 && gotCols != wantCols {
			tally.recordColumnCountMismatch(stmt, wantCols, gotCols)
			t.Errorf("column count mismatch for %q (line %d): IR has %d, DDL declared %d\n    DDL: %s",
				stmt.table, stmt.line, gotCols, wantCols, snippet(stmt.sql))
			clean = false
		}

		if ddlDeclaresPrimaryKey(stmt.sql) && table.PrimaryKey == nil {
			tally.recordPKLost(stmt)
			t.Errorf("PRIMARY KEY lost for %q (line %d): DDL declares one, IR PrimaryKey == nil\n    DDL: %s",
				stmt.table, stmt.line, snippet(stmt.sql))
			clean = false
		}

		// All invariants passed — count as IR-clean. The column-count
		// check is skipped when extractColumnList can't locate a body
		// (extremely rare; CREATE TABLE ... LIKE / SELECT shapes);
		// such statements still count as IR-clean since the read
		// succeeded and the PK check (if applicable) didn't fire.
		if clean {
			tally.irClean++
		}

		dropTable(ctx, db, stmt.table)
	}

	tally.summarize(t)
}

// dropTable best-effort drops a table between iterations to keep the
// catalog small. Failures are silent: the next CREATE TABLE will
// either fail (caught as `mysql-refused`) or succeed with a fresh
// definition.
func dropTable(ctx context.Context, db *sql.DB, table string) {
	if table == "" {
		return
	}
	dropCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Identifier already came from the fixture's backtick-quoted name;
	// re-quote defensively for the DROP. MySQL accepts both quoted and
	// unquoted identifiers here.
	_, _ = db.ExecContext(dropCtx, "DROP TABLE IF EXISTS `"+strings.ReplaceAll(table, "`", "")+"`")
}

// findTableInSchema mirrors the helper in engines/mysql tests but
// lives here so this package isn't coupled to engine-internal test
// helpers.
func findTableInSchema(s *ir.Schema, name string) *ir.Table {
	if s == nil {
		return nil
	}
	for _, t := range s.Tables {
		if strings.EqualFold(t.Name, name) {
			return t
		}
	}
	return nil
}

func tableNamesInSchema(s *ir.Schema) []string {
	if s == nil {
		return nil
	}
	out := make([]string, 0, len(s.Tables))
	for _, t := range s.Tables {
		out = append(out, t.Name)
	}
	sort.Strings(out)
	return out
}

// snippet trims a DDL statement for log output: collapses whitespace
// runs, caps at 160 chars with an ellipsis. The full statement is
// always greppable in the corpus file via the `line` number printed
// alongside.
func snippet(stmt string) string {
	collapsed := strings.Join(strings.Fields(stmt), " ")
	const maxLen = 160
	if len(collapsed) <= maxLen {
		return collapsed
	}
	return collapsed[:maxLen] + "..."
}

// parseDDLFixture walks the embedded createtable1.test file, scoops
// each `statement ok` block, keeps only those whose payload starts
// with `CREATE TABLE` (after stripping comments / whitespace), and
// returns them in fixture order.
//
// Fixture format (sqllogictest):
//
//	statement ok|error
//	<sql, possibly multi-line, terminated by a blank line or EOF>
//
// The parser is intentionally permissive: blocks it can't make sense
// of are dropped without erroring, since the fixture is treated as
// upstream-truth and parser fragility shouldn't fail the suite.
func parseDDLFixture() ([]ddlStatement, error) {
	raw, err := ddlFixtureFS.ReadFile(ddlFixturePath)
	if err != nil {
		return nil, fmt.Errorf("read embedded fixture %q: %w", ddlFixturePath, err)
	}

	lines := strings.Split(string(raw), "\n")
	var out []ddlStatement
	used := make(map[string]int) // dedupe table-name collisions within the corpus

	i := 0
	for i < len(lines) {
		line := strings.TrimRight(lines[i], "\r")
		// Look for a `statement ok` header. The fixture also contains
		// `statement error` lines (followed by an expected-error
		// regex on the next line, then the SQL) which we skip
		// wholesale; only `statement ok` reaches us.
		trim := strings.TrimSpace(line)
		switch {
		case trim == "statement ok":
			startLine := i + 1 // 1-based
			i++
			sqlLines, end := collectStatementBody(lines, i)
			i = end
			sqlText := strings.Join(sqlLines, "\n")
			sqlText = strings.TrimSpace(sqlText)
			sqlText = strings.TrimSuffix(sqlText, ";")
			if !looksLikeCreateTable(sqlText) {
				continue
			}
			tableName := extractTableName(sqlText)
			if tableName == "" {
				// Couldn't parse out a name — drop quietly rather
				// than fight the corpus parser.
				continue
			}
			// Deduplicate identical table names by suffixing — the
			// upstream fixture has many `t<digits><suffix>` shapes
			// but they're already distinct in our scan; collisions
			// only happen on quirky parser misreads. Suffixing keeps
			// the harness deterministic.
			if n := used[tableName]; n > 0 {
				tableName = fmt.Sprintf("%s__d%d", tableName, n)
			}
			used[tableName]++
			out = append(out, ddlStatement{
				line:  startLine,
				table: tableName,
				sql:   sqlText,
			})
		case strings.HasPrefix(trim, "statement error"):
			// Skip the error-regex line, then the SQL body, then the
			// blank separator.
			i++
			_, end := collectStatementBody(lines, i)
			i = end
		default:
			i++
		}
	}

	return out, nil
}

// collectStatementBody walks from `start` until a blank line (or EOF),
// returning the body lines and the index *past* the blank separator.
// The fixture uses blank lines as record separators; we follow the
// same convention.
func collectStatementBody(lines []string, start int) (body []string, next int) {
	i := start
	for i < len(lines) {
		ln := strings.TrimRight(lines[i], "\r")
		if strings.TrimSpace(ln) == "" {
			return body, i + 1
		}
		body = append(body, ln)
		i++
	}
	return body, i
}

func looksLikeCreateTable(stmt string) bool {
	// Strip a leading `--` line if present.
	s := strings.TrimSpace(stmt)
	for strings.HasPrefix(s, "--") {
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			return false
		}
		s = strings.TrimSpace(s[nl+1:])
	}
	upper := strings.ToUpper(s)
	if strings.HasPrefix(upper, "CREATE TABLE") {
		return true
	}
	if strings.HasPrefix(upper, "CREATE TEMPORARY TABLE") {
		return true
	}
	return false
}

// createTableHead matches `CREATE [TEMPORARY] TABLE [IF NOT EXISTS]
// <name>` and extracts <name>. Identifier forms supported:
//
//   - Backtick-quoted: `t1710a`
//   - Double-quoted: "t1710a"
//   - Bare: t1710a
var createTableHead = regexp.MustCompile(
	`(?is)^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:` +
		"`" + `([^` + "`" + `]+)` + "`" +
		`|"([^"]+)"|([A-Za-z_][A-Za-z0-9_$.]*))`,
)

func extractTableName(stmt string) string {
	m := createTableHead.FindStringSubmatch(stmt)
	if m == nil {
		return ""
	}
	for i := 1; i <= 3; i++ {
		if m[i] != "" {
			// Strip schema-qualified prefix (`db`.`t` → `t`). MySQL
			// applies DDL into the current schema by default; we
			// only care about the bare table name for DROP and IR
			// lookup.
			name := m[i]
			if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
				name = name[dot+1:]
			}
			return strings.Trim(name, "`\"")
		}
	}
	return ""
}

// countDDLColumns counts the column definitions in a CREATE TABLE
// statement by splitting the parenthesised column list at top-level
// commas (depth 0), then filtering out constraint clauses
// (PRIMARY KEY, KEY, UNIQUE, CHECK, CONSTRAINT, FOREIGN KEY, INDEX,
// FULLTEXT, SPATIAL).
//
// Returns 0 when the column list can't be located (e.g. CREATE TABLE
// ... LIKE / SELECT shapes). Callers treat 0 as "no expectation;
// skip the column-count assertion."
func countDDLColumns(stmt string) int {
	body, ok := extractColumnList(stmt)
	if !ok {
		return 0
	}
	parts := splitTopLevelCommas(body)
	count := 0
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if isConstraintClause(p) {
			continue
		}
		count++
	}
	return count
}

// extractColumnList returns the substring between the first
// top-level `(` and its matching `)` in the CREATE TABLE. Returns
// (body, true) on success, ("", false) otherwise. Handles backtick-
// and double-quoted identifiers (which can legitimately contain
// parens, though the corpus doesn't lean on that) by skipping inside
// quotes. Single-quoted strings (COMMENT 'text...') also escape
// paren tracking.
func extractColumnList(stmt string) (string, bool) {
	// Find the first `(` outside of any quote.
	open := -1
	var inSingle, inDouble, inBack bool
	for i := 0; i < len(stmt); i++ {
		c := stmt[i]
		switch {
		case inSingle:
			if c == '\\' && i+1 < len(stmt) {
				i++
				continue
			}
			if c == '\'' {
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case inBack:
			if c == '`' {
				inBack = false
			}
		default:
			switch c {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '`':
				inBack = true
			case '(':
				open = i
			}
		}
		if open >= 0 {
			break
		}
	}
	if open < 0 {
		return "", false
	}

	depth := 0
	inSingle = false
	inDouble = false
	inBack = false
	for i := open; i < len(stmt); i++ {
		c := stmt[i]
		switch {
		case inSingle:
			if c == '\\' && i+1 < len(stmt) {
				i++
				continue
			}
			if c == '\'' {
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case inBack:
			if c == '`' {
				inBack = false
			}
		default:
			switch c {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '`':
				inBack = true
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					return stmt[open+1 : i], true
				}
			}
		}
	}
	return "", false
}

// splitTopLevelCommas splits `body` on commas at parenthesis depth 0,
// skipping inside quotes. Mirrors extractColumnList's quote/paren
// state machine.
func splitTopLevelCommas(body string) []string {
	var out []string
	depth := 0
	var inSingle, inDouble, inBack bool
	last := 0
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case inSingle:
			if c == '\\' && i+1 < len(body) {
				i++
				continue
			}
			if c == '\'' {
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case inBack:
			if c == '`' {
				inBack = false
			}
		default:
			switch c {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '`':
				inBack = true
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth == 0 {
					out = append(out, body[last:i])
					last = i + 1
				}
			}
		}
	}
	out = append(out, body[last:])
	return out
}

// constraintLeading matches a column-list entry whose first token
// (case-insensitive) is a known constraint/index keyword. These
// don't count toward the column-count assertion.
var constraintLeading = regexp.MustCompile(
	`(?i)^\s*(CONSTRAINT\b|PRIMARY\s+KEY\b|FOREIGN\s+KEY\b|UNIQUE(\s+(KEY|INDEX))?\b|KEY\b|INDEX\b|FULLTEXT(\s+(KEY|INDEX))?\b|SPATIAL(\s+(KEY|INDEX))?\b|CHECK\s*\()`,
)

func isConstraintClause(part string) bool {
	return constraintLeading.MatchString(part)
}

// ddlDeclaresPrimaryKey reports whether the CREATE TABLE statement
// declares a PRIMARY KEY clause anywhere in its column list — both
// the inline column-attribute form (`id BIGINT PRIMARY KEY ...`) and
// the table-level form (`PRIMARY KEY (id)`). False positives in
// strings/comments are avoided by lightly scrubbing quoted spans
// first.
//
// The check is best-effort: if the regex misses an exotic shape, the
// caller falls through without the PK assertion firing, which is
// the safe direction (we'd rather under-assert than fire false PK-
// lost errors against a fixture we don't fully grasp).
func ddlDeclaresPrimaryKey(stmt string) bool {
	stripped := stripQuotedSpans(stmt)
	return primaryKeyMarker.MatchString(stripped)
}

var primaryKeyMarker = regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\b`)

// stripQuotedSpans replaces backtick / double-quote / single-quote
// runs with spaces so regex matches over the result can't trip over
// `COMMENT 'PRIMARY KEY in a comment'` shapes. Best-effort; doesn't
// handle every escape pathology but adequate for the corpus.
func stripQuotedSpans(stmt string) string {
	out := make([]byte, len(stmt))
	copy(out, stmt)
	var inSingle, inDouble, inBack bool
	for i := 0; i < len(out); i++ {
		c := out[i]
		switch {
		case inSingle:
			if c == '\\' && i+1 < len(out) {
				out[i] = ' '
				out[i+1] = ' '
				i++
				continue
			}
			if c == '\'' {
				inSingle = false
			} else {
				out[i] = ' '
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			} else {
				out[i] = ' '
			}
		case inBack:
			if c == '`' {
				inBack = false
			} else {
				out[i] = ' '
			}
		default:
			switch c {
			case '\'':
				inSingle = true
			case '"':
				inDouble = true
			case '`':
				inBack = true
			}
		}
	}
	return string(out)
}

// runTally aggregates per-statement outcomes for the end-of-run
// summary. Counts are flat ints; refusal samples are capped at
// `maxRefusalSamples` to keep the log readable.
type runTally struct {
	corpus       int
	mysqlApplied int
	irClean      int

	mysqlRefusals    int
	refusalSamples   []refusalSample
	readErrors       int
	missingTables    int
	colCountFailures int
	pkLost           int

	budgetExceeded bool

	gapDetails []string
}

const maxRefusalSamples = 10

type refusalSample struct {
	line int
	err  string
	sql  string
}

func newRunTally() *runTally {
	return &runTally{}
}

func (r *runTally) recordMySQLRefusal(stmt ddlStatement, err error) {
	r.mysqlRefusals++
	if len(r.refusalSamples) < maxRefusalSamples {
		r.refusalSamples = append(r.refusalSamples, refusalSample{
			line: stmt.line,
			err:  err.Error(),
			sql:  snippet(stmt.sql),
		})
	}
}

func (r *runTally) recordReadError(stmt ddlStatement, err error) {
	r.readErrors++
	r.gapDetails = append(r.gapDetails,
		fmt.Sprintf("read-error  line=%d table=%q err=%v ddl=%s",
			stmt.line, stmt.table, err, snippet(stmt.sql)))
}

func (r *runTally) recordMissingTable(stmt ddlStatement) {
	r.missingTables++
	r.gapDetails = append(r.gapDetails,
		fmt.Sprintf("missing     line=%d table=%q ddl=%s",
			stmt.line, stmt.table, snippet(stmt.sql)))
}

func (r *runTally) recordColumnCountMismatch(stmt ddlStatement, want, got int) {
	r.colCountFailures++
	r.gapDetails = append(r.gapDetails,
		fmt.Sprintf("colcount    line=%d table=%q want=%d got=%d ddl=%s",
			stmt.line, stmt.table, want, got, snippet(stmt.sql)))
}

func (r *runTally) recordPKLost(stmt ddlStatement) {
	r.pkLost++
	r.gapDetails = append(r.gapDetails,
		fmt.Sprintf("pk-lost     line=%d table=%q ddl=%s",
			stmt.line, stmt.table, snippet(stmt.sql)))
}

func (r *runTally) summarize(t *testing.T) {
	t.Helper()

	totalGaps := r.readErrors + r.missingTables + r.colCountFailures + r.pkLost

	prefix := "DDL-fixture summary"
	if r.budgetExceeded {
		prefix = "DDL-fixture summary (PARTIAL — budget exceeded)"
	}
	t.Logf("%s: %d corpus / %d MySQL-applied / %d IR-clean / %d gaps "+
		"(read-errors=%d missing=%d col-count=%d pk-lost=%d) / %d mysql-refused",
		prefix, r.corpus, r.mysqlApplied, r.irClean, totalGaps,
		r.readErrors, r.missingTables, r.colCountFailures, r.pkLost,
		r.mysqlRefusals)

	if r.mysqlRefusals > 0 {
		t.Logf("mysql-refused samples (first %d of %d):", len(r.refusalSamples), r.mysqlRefusals)
		for _, s := range r.refusalSamples {
			t.Logf("  line=%d  err=%s\n    DDL: %s", s.line, s.err, s.sql)
		}
	}

	// Sluice-side gaps are surfaced as t.Errorf at the call site, so
	// the test status is already set. Here we just echo the catalogue
	// so a reader doesn't have to scroll through interleaved Errorf
	// lines.
	if len(r.gapDetails) > 0 {
		t.Logf("sluice-side gap catalogue (%d entries; each was already reported via t.Errorf):", len(r.gapDetails))
		for _, g := range r.gapDetails {
			t.Logf("  %s", g)
		}
	}
}

// envInt returns the integer value of an env var, or 0 if unset /
// non-numeric / non-positive. Used by SLUICE_DDL_FIXTURE_MAX to let
// operators short-cap the corpus during quick iteration.
func envInt(name string) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
