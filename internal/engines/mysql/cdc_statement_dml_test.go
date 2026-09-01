// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestStatementDMLVerb pins the tripwire's lexer in both directions:
// every row-DML verb (with the comment/whitespace shapes the server
// actually logs) trips it, and every legitimate QueryEvent the arm sees
// under ROW logging does not — the false-positive analysis from the
// file comment, as a table.
func TestStatementDMLVerb(t *testing.T) {
	t.Parallel()

	trips := map[string]string{
		"insert":           "INSERT INTO t VALUES (1)",
		"insert_lowercase": "insert into t values (1)",
		"update":           "UPDATE t SET x = 1 WHERE id = 2",
		"delete":           "DELETE FROM t WHERE id = 2",
		"replace":          "REPLACE INTO t VALUES (1)",
		"leading_ws":       "  \t\nINSERT INTO t VALUES (1)",
		"block_comment":    "/* app comment */ UPDATE t SET x = 1",
		"line_comment":     "-- generated\nDELETE FROM t",
		"hash_comment":     "# generated\nREPLACE INTO t VALUES (1)",
		"delete_from_h":    "DELETE FROM history WHERE id = 1", // HISTORY exemption must key on the token, not a prefix of the table name
		// CTE-DML (STATEMENT-DML-WITH): a WITH-prefixed QueryEvent under
		// ROW logging can only be statement-format CTE UPDATE/DELETE —
		// WITH…SELECT is never binlogged, so first-token WITH trips.
		"cte_update":           "WITH x AS (SELECT id FROM t WHERE flag) UPDATE t SET v = 1 WHERE id IN (SELECT id FROM x)",
		"cte_delete_lowercase": "with x as (select id from t) delete from t where id in (select id from x)",
		"cte_recursive":        "WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT n+1 FROM x) DELETE FROM t WHERE id IN (SELECT n FROM x)",
		"cte_comment":          "/* app */ WITH x AS (SELECT id FROM t) UPDATE t SET v = 2",
	}
	for name, q := range trips {
		verb, ok := statementDMLVerb(q)
		if !ok {
			t.Errorf("%s: statementDMLVerb(%q) did not trip; want a DML verb", name, q)
			continue
		}
		if !statementDMLVerbs[verb] {
			t.Errorf("%s: verb = %q; want one of the DML verbs", name, verb)
		}
	}

	passes := map[string]string{
		"begin":                  "BEGIN",
		"create":                 "CREATE TABLE t (id INT)",
		"ctas":                   "CREATE TABLE t2 AS SELECT * FROM t", // CTAS arrives tagged CREATE; its rows follow as row events
		"alter":                  "ALTER TABLE t ADD COLUMN y INT",
		"drop":                   "DROP TABLE t",
		"truncate":               "TRUNCATE TABLE t",
		"rename":                 "RENAME TABLE t TO u",
		"savepoint":              "SAVEPOINT sp1",
		"rollback_to":            "ROLLBACK TO SAVEPOINT sp1",
		"xa_start":               "XA START X'1234',X'5678',1",
		"grant":                  "GRANT SELECT ON app.* TO 'x'@'%'",
		"analyze":                "ANALYZE TABLE t",
		"set":                    "SET @@SESSION.gtid_next = 'AUTOMATIC'",
		"mariadb_delete_history": "DELETE HISTORY FROM t BEFORE SYSTEM_TIME '2026-01-01'",
		"comment_only":           "-- INSERT swallowed by a comment",
		"hash_only":              "# UPDATE swallowed",
		"unterminated":           "/* unterminated INSERT",
		"versioned_residue":      "/*!40000 INSERT INTO t VALUES (1) */", // documented residue: versioned comments lex as comments
		"empty":                  "",
	}
	for name, q := range passes {
		if verb, ok := statementDMLVerb(q); ok {
			t.Errorf("%s: statementDMLVerb(%q) tripped on %q; want pass", name, q, verb)
		}
	}
}

// TestStatementDMLError pins the refusal's shape: the code, the verb,
// the scope note, the payload-free correlation coordinate that replaced
// the statement digest (audit 2026-08-31 SEC-5), and — audit 2026-08-27 A4 —
// that the statement's PAYLOAD never reaches the error: the binlog
// text carries row values (PII) that would bypass --redact by riding
// the refusal into logs and reports.
func TestStatementDMLError(t *testing.T) {
	t.Parallel()
	long := "INSERT INTO t VALUES ('alice@example.com','555-0100')" + strings.Repeat(",(1)", 200)
	err := statementDMLError("INSERT", "app", "ends at binlog mysql-bin.000004:9182", long)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
	}
	msg := err.Error()
	for _, phrase := range []string{
		"INSERT", `"app"`, "…", "binlog_format", "silently dropping",
		"INSERT INTO t VALUES",                 // the sanitized lead: verb + table stay diagnostic
		"ends at binlog mysql-bin.000004:9182", // the payload-free correlation coordinate
	} {
		if !strings.Contains(msg, phrase) {
			t.Errorf("message missing %q; got: %v", phrase, msg)
		}
	}
	// The payload must be withheld: no string-literal content, no
	// VALUES tuple, in either the message or the hint. (A bare `'`
	// probe would false-positive on the hint's own SQL, so the pins are
	// the quoted fragments themselves.)
	for _, leaked := range []string{"alice@example.com", "555-0100", "'alice", "(1)"} {
		if strings.Contains(msg, leaked) || strings.Contains(ce.Hint, leaked) {
			t.Errorf("refusal leaked payload fragment %q: %v (hint %q)", leaked, msg, ce.Hint)
		}
	}
	if len(msg) > 1200 {
		t.Errorf("message did not bound the echoed lead: %d bytes", len(msg))
	}
	if ce.Hint == "" || !strings.Contains(ce.Hint, "variables_by_thread") {
		t.Errorf("hint = %q; want the session-hunt remedy", ce.Hint)
	}
	if !strings.Contains(ce.Hint, "performance_schema=ON") || !strings.Contains(ce.Hint, "SHOW PROCESSLIST") {
		t.Errorf("hint = %q; want the performance_schema precondition + the MariaDB fallback (audit 2026-08-27 A6)", ce.Hint)
	}

	// The digest is GONE (audit 2026-08-31 SEC-5): a recomputable
	// sha256 prefix over the statement text is an oracle for a
	// low-entropy withheld value, and its only user already holds the
	// plaintext. Pinned as an absence so a "restore the correlation
	// aid" edit has to read the rationale first.
	for _, gone := range []string{"sha256", "digest"} {
		if strings.Contains(msg, gone) || strings.Contains(ce.Hint, gone) {
			t.Errorf("refusal still carries %q: the statement-text digest is a brute-force oracle for the "+
				"value it stands in for; the binlog coordinate replaced it (see statementDMLError)", gone)
		}
	}

	// The WITH verb names its class: "a WITH statement" alone would leave
	// the operator guessing what a read-only-looking keyword is doing in
	// a refusal about DML.
	withMsg := statementDMLError("WITH", "app", "gtid 6a3175a8-…:9", "WITH x AS (SELECT id FROM t) UPDATE t SET v = 1").Error()
	if !strings.Contains(withMsg, "CTE-DML") {
		t.Errorf("WITH refusal does not name CTE-DML; got: %v", withMsg)
	}
}

// TestStatementDMLLocator pins the payload-free correlation coordinate
// in its three reachable shapes — file/pos with the ROTATE seen, GTID
// mode before any ROTATE, and a nil header — and, load-bearingly, that
// none of them is a function of the statement's bytes.
func TestStatementDMLLocator(t *testing.T) {
	t.Parallel()

	r := &CDCReader{currentFile: "mysql-bin.000007"}
	got := r.statementDMLLocator(&replication.EventHeader{LogPos: 4210, Timestamp: 1767225600})
	for _, want := range []string{"ends at binlog mysql-bin.000007:4210", "committed 2026-01-01T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Errorf("locator = %q; want it to contain %q", got, want)
		}
	}

	// GTID mode: no ROTATE seen yet, so no file name — the position and
	// the staged GTID still locate the event.
	g := &CDCReader{pendingGTID: "6a3175a8-0000-0000-0000-000000000000:7"}
	got = g.statementDMLLocator(&replication.EventHeader{LogPos: 812})
	for _, want := range []string{"ends at binlog position 812", "gtid 6a3175a8-0000-0000-0000-000000000000:7"} {
		if !strings.Contains(got, want) {
			t.Errorf("gtid-mode locator = %q; want it to contain %q", got, want)
		}
	}
	if strings.Contains(got, "committed") {
		t.Errorf("gtid-mode locator = %q; a zero header timestamp must not render as an epoch commit time", got)
	}

	if none := (&CDCReader{}).statementDMLLocator(nil); none != "no binlog coordinate available" {
		t.Errorf("empty locator = %q; want the explicit unavailable string, never a misleading zero", none)
	}
}

// TestStatementDMLLead pins the sanitizer's cut rule in both
// directions: cut at the first token that is not identifier material,
// keep the verb + table + leading column name, cap the length.
func TestStatementDMLLead(t *testing.T) {
	t.Parallel()
	cases := map[string]struct{ in, want string }{
		"paren_first":            {"INSERT INTO t VALUES (1,'x')", "INSERT INTO t VALUES…"},
		"single_quote_first":     {"UPDATE t SET v = 'secret' WHERE id = 9", "UPDATE t SET v…"},
		"double_quote_first":     {`DELETE FROM t WHERE v = "secret"`, "DELETE FROM t WHERE v…"},
		"numeric_literal_eq_cut": {"UPDATE t SET ssn=078051120", "UPDATE t SET ssn…"},
		// The four shapes the blocklist cut missed (audit 2026-08-31
		// SEC-5): none contains a quote, paren, or `=`.
		"gt_unquoted_numeric":  {"DELETE FROM patients WHERE ssn > 078051120", "DELETE FROM patients WHERE ssn…"},
		"between_range":        {"DELETE FROM patients WHERE mrn BETWEEN 4820113 AND 4820119", "DELETE FROM patients WHERE mrn BETWEEN…"},
		"not_equal_angle":      {"DELETE FROM users WHERE ssn <> 078051120", "DELETE FROM users WHERE ssn…"},
		"bare_keyword_literal": {"DELETE FROM t WHERE id IS NULL", "DELETE FROM t WHERE id IS…"},
		// Names survive: a qualified, backticked table is the diagnostic
		// half and is a NAME by MySQL's grammar, never a value.
		"qualified_backticked": {"UPDATE `app`.`patients` SET ssn = 1", "UPDATE `app`.`patients` SET ssn…"},
		"no_literals_at_all":   {"DELETE FROM t", "DELETE FROM t"},
		"cap": {
			"UPDATE " + strings.Repeat("very_long_table_name_", 10) + " SET a = b",
			"UPDATE " + strings.Repeat("very_long_table_name_", 10)[:statementDMLEchoCap-len("UPDATE ")] + "…",
		},
	}
	for name, tc := range cases {
		if got := statementDMLLead(tc.in); got != tc.want {
			t.Errorf("%s: statementDMLLead(%q) = %q; want %q", name, tc.in, got, tc.want)
		}
	}
}

// statementDMLPredicateOperators is the operator half of the redaction
// family matrix. It is deliberately the whole comparison/range family
// rather than a representative (the Bug 74 lesson applied to a
// redactor): the shipped blocklist cut kept its promise for `=` — the
// one member anyone pinned — and broke it for every other member, which
// is exactly what a per-representative pin cannot see.
//
// The word operators are here for the same reason as the punctuation
// ones: they do not cut themselves, so a cell only passes if the
// OPERAND after them cuts.
var statementDMLPredicateOperators = map[string]string{
	"eq":      "ssn = %s",
	"gt":      "ssn > %s",
	"lt":      "ssn < %s",
	"gte":     "ssn >= %s",
	"lte":     "ssn <= %s",
	"ne":      "ssn <> %s",
	"bang_ne": "ssn != %s",
	"between": "ssn BETWEEN %s AND %s",
	"in":      "ssn IN (%s)",
	"like":    "ssn LIKE %s",
	"is":      "ssn IS %s",
}

// statementDMLLiteralKinds is the literal half: every lexical form a
// MySQL value can take, each paired with the token that must NOT
// survive into the refusal. `secret` is a substring of the rendered
// literal, so a cell is only meaningful if the raw statement contains
// it — asserted below as the anti-vacuity floor.
// Widened 2026-09-01 after the pre-publish value-fidelity review pointed out
// that six kinds do not exercise a grammar the comment above calls complete.
// The allowlist cut is claimed to exclude EVERY MySQL literal form by
// construction, so the matrix has to carry every form the grammar admits —
// otherwise the claim rests on the six that happened to be listed.
var statementDMLLiteralKinds = map[string]struct{ literal, secret string }{
	"quoted_string":       {"'078051120'", "078051120"},
	"dquoted_string":      {`"078051120"`, "078051120"},
	"unquoted_numeric":    {"078051120", "078051120"},
	"signed_numeric":      {"-078051120", "078051120"},
	"leading_dot_numeric": {".078051120", "078051120"},
	"scientific_numeric":  {"7805112e0", "7805112"},
	"hex":                 {"0x4D7953514C", "4D7953514C"},
	"hex_xquote":          {"X'4D7953514C'", "4D7953514C"},
	"hex_xquote_lower":    {"x'4D7953514C'", "4D7953514C"},
	"bit":                 {"b'01001101'", "01001101"},
	"bit_upper":           {"B'01001101'", "01001101"},
	"bit_0b":              {"0b01001101", "01001101"},
	"introducer_utf8":     {"_utf8'078051120'", "078051120"},
	"introducer_binary":   {"_binary'078051120'", "078051120"},
	"national_string":     {"N'078051120'", "078051120"},
	"temporal_date":       {"DATE '2026-09-01'", "2026-09-01"},
	"temporal_timestamp":  {"TIMESTAMP '2026-09-01 12:00:00'", "2026-09-01"},
	"temporal_odbc":       {"{d '2026-09-01'}", "2026-09-01"},
	"interval":            {"INTERVAL 078051120 DAY", "078051120"},
	"placeholder":         {"?", "?"},
	// A variable REFERENCE carries no value in the statement text, but it
	// must still be cut: it is not identifier material, and in a longer
	// predicate whatever follows it would be a value. The secret is
	// deliberately unlike any column name the matrix uses — `@ssn` against
	// a column named `ssn` reports the legitimately-KEPT column as a leak,
	// which is a fixture bug that reads exactly like a real finding.
	"user_variable":   {"@v078051120", "078051120"},
	"system_variable": {"@@session.v078051120", "078051120"},
	"null":            {"NULL", "NULL"},
	"null_lower":      {"null", "null"},
	"boolean":         {"TRUE", "TRUE"},
	"boolean_false":   {"FALSE", "FALSE"},
	"unknown":         {"UNKNOWN", "UNKNOWN"},
}

// TestStatementDMLLead_CommentPrefixedKeepsItsDiagnostic pins the fix for a
// diagnostic loss the pre-publish review of v0.137.0 found: the cut did not
// skip leading comments, and a comment's first byte is outside the identifier
// allowlist, so the whole lead collapsed to "…".
//
// That is the SAFE direction — it over-cuts and cannot leak — but it erased
// the verb, table and columns for an entire traffic class, since
// comment-prefixed DML is the normal shape from Vitess and PlanetScale
// (`/*vt+ …*/`), ProxySQL and tracing-annotated clients. Both halves are
// asserted here: the diagnostic survives, and the value still does not.
func TestStatementDMLLead_CommentPrefixedKeepsItsDiagnostic(t *testing.T) {
	t.Parallel()
	// The LONG cells are the ones that matter and the ones v0.137.0's first
	// cut missed (Bug 258): the cut offset advanced past the comment, but
	// the lead was still SLICED from 0, so the comment text was kept and a
	// comment longer than the echo cap consumed all of it. Short comments
	// passed, which is exactly why short-comment-only pins did not catch it.
	// The sqlcommenter/`traceparent` prefix below is ~110 bytes and is the
	// real-world shape, not a contrived one.
	for name, q := range map[string]string{
		"block":      "/* trace-id=abc */ UPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"vitess":     "/*vt+ QUERY_TIMEOUT_MS=30000 */ UPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"line":       "-- trace\nUPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"hash":       "# trace\nUPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"two_blocks": "/* a */ /* b */ UPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"sqlcommenter": "/*traceparent='00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'," +
			"controller='checkout',action='pay'*/ UPDATE patients SET note = 'x' WHERE ssn = '078051120'",
		"long_line_comment": "-- " + strings.Repeat("annotation ", 20) + "\nUPDATE patients SET note = 'x' WHERE ssn = '078051120'",
	} {
		lead := statementDMLLead(q)
		if !strings.Contains(lead, "UPDATE") || !strings.Contains(lead, "patients") {
			t.Errorf("%s: statementDMLLead(%q) = %q — the verb and table are the diagnostic half and "+
				"must survive a leading comment", name, q, lead)
		}
		if strings.Contains(lead, "078051120") || strings.Contains(lead, "'x'") {
			t.Errorf("%s: statementDMLLead(%q) = %q — a literal survived; skipping the comment must "+
				"move the START of the cut, never widen what it keeps", name, q, lead)
		}
		// start <= cut is a PANIC-FREEDOM precondition, not a nicety:
		// statementDMLLead slices query[start:cut], so an inversion is a
		// crash on a refusal path. It holds by construction only because
		// statementDMLCut re-derives its start from the same pure
		// statementDMLCommentSkip and never moves backwards — a coupling
		// nothing else states. (Mutation-verified in the v0.137.1 review:
		// forcing statementDMLCut's own start to 0 panics with
		// "slice bounds out of range" on the first comment-prefixed input.)
		if s, c := statementDMLCommentSkip(q), statementDMLCut(q); s > c {
			t.Errorf("%s: statementDMLCommentSkip=%d > statementDMLCut=%d for %q — statementDMLLead "+
				"slices query[start:cut] and would panic on a refusal path", name, s, c, q)
		}
		// The comment text itself must not be echoed. Asserting the verb
		// survives is NOT sufficient on its own: with the comment kept and
		// a short comment, both hold at once, which is how Bug 258 passed.
		if strings.Contains(lead, "trace") || strings.Contains(lead, "annotation") || strings.Contains(lead, "vt+") {
			t.Errorf("%s: statementDMLLead(%q) = %q — the comment TEXT was echoed. It is the caller's "+
				"annotation, not sluice's statement, and echoing it spends the cap on non-diagnostic bytes",
				name, q, lead)
		}
	}

	// An unterminated block comment has no provable end, so it cuts at 0.
	// Without this the skip could be written to run past the whole
	// statement on malformed input.
	if lead := statementDMLLead("/* unterminated UPDATE patients SET ssn = '078051120'"); strings.Contains(lead, "078051120") {
		t.Errorf("an unterminated comment leaked: %q", lead)
	}
}

// TestStatementDMLLead_VersionedCommentWithAQuoteIsNotTrusted pins the
// v0.137.1 guard. MySQL lexes a `/*! … */` EXECUTABLE comment's contents as
// SQL, so a `*/` inside a string literal does NOT terminate it — while the
// skip is quote-blind, which is right for a plain comment and wrong for a
// versioned one. A versioned comment carrying `*/` inside a quoted value
// therefore stopped the skip MID-VALUE, and the remainder of the value was
// echoed into a refusal that promises values are withheld.
//
// The statement below is not contrived: it is what real MySQL 8.0.46 wrote to
// the binlog for a genuine statement-format UPDATE, observed by the v0.137.1
// pre-tag review via SHOW BINLOG EVENTS.
func TestStatementDMLLead_VersionedCommentWithAQuoteIsNotTrusted(t *testing.T) {
	t.Parallel()

	const secret = "078051120"
	q := `/*!40000 UPDATE patients SET note='a */ UPDATE ssn` + secret + `' WHERE id=1 */`
	if !strings.Contains(q, secret) {
		t.Fatalf("the fixture lost its secret: %q", q)
	}
	if lead := statementDMLLead(q); strings.Contains(lead, secret) {
		t.Errorf("statementDMLLead(%q) = %q — a row-value fragment reached the refusal. A `/*!` comment "+
			"whose skipped span carries a quote must not be trusted as a comment; the safe fallback is "+
			"to cut at 0 and lose the diagnostic", q, lead)
	}

	// The guard must be NARROW. A versioned comment without a quote is the
	// common real shape (mysqldump, Vitess) and must still be skipped, or
	// the guard would quietly disable the Bug 258 fix for those.
	plain := "/*!40000 */ DELETE FROM patients WHERE ssn = '" + secret + "'"
	lead := statementDMLLead(plain)
	if !strings.HasPrefix(lead, "DELETE FROM patients WHERE ssn") {
		t.Errorf("statementDMLLead(%q) = %q — a quote-free versioned comment must still be skipped", plain, lead)
	}
	if strings.Contains(lead, secret) {
		t.Errorf("statementDMLLead(%q) = %q — literal survived", plain, lead)
	}
}

// TestStatementDMLLeadFamilyMatrix is the class pin for the "values
// withheld" promise: EVERY comparison/range operator family × EVERY
// literal kind, asserting no literal token survives into the echoed
// lead while the diagnostic prefix (verb, table, column) does.
//
// Some cells are not valid SQL (`ssn IS 0x4D…`). They are included
// deliberately: the redactor is a lexer over whatever the server
// logged, and a promise that holds only for grammatical input is not a
// promise. The cut must be decided by the token shape, not by whether
// the statement would parse.
func TestStatementDMLLeadFamilyMatrix(t *testing.T) {
	t.Parallel()

	const wantPrefix = "DELETE FROM patients WHERE ssn"
	cells := 0
	for opName, opFmt := range statementDMLPredicateOperators {
		for litName, lit := range statementDMLLiteralKinds {
			name := opName + "/" + litName
			args := []any{lit.literal}
			if strings.Count(opFmt, "%s") == 2 { // BETWEEN takes two operands
				args = append(args, lit.literal)
			}
			stmt := "DELETE FROM patients WHERE " + fmt.Sprintf(opFmt, args...)

			// THIRD AXIS, added 2026-09-01. Bug 258 moved where the cut
			// STARTS, and this matrix ran entirely at start == 0 while the
			// comment test ran many comment forms against one literal
			// shape — so the product of the two was untested precisely on
			// the axis that had just changed. Every comment prefix here
			// carries no quote, so none trips the `/*!` guard.
			for _, prefix := range []string{
				"",
				"/* app */ ",
				"/*vt+ QUERY_TIMEOUT_MS=30000 */ ",
				"/*!40000 */ ",
				"-- trace\n",
				"# trace\n",
				"  \t\n",
			} {
				query := prefix + stmt

				// Anti-vacuity, per cell: the secret must actually be in
				// the input, or "the lead does not contain it" proves
				// nothing.
				if !strings.Contains(query, lit.secret) {
					t.Fatalf("%s: the cell's own statement %q does not contain the secret %q — the assertion "+
						"below would pass vacuously", name, query, lit.secret)
				}

				lead := statementDMLLead(query)
				if strings.Contains(lead, lit.secret) {
					t.Errorf("%s [prefix %q]: statementDMLLead(%q) = %q — the %s literal survived into the "+
						"refusal, which the refusal's own text promises it will not (\"values withheld\"). "+
						"Binlog statement text carries row values; a value that rides the refusal into logs "+
						"and reports bypasses --redact entirely.", name, prefix, query, lead, litName)
				}
				// The other direction: a redactor that returned "" would
				// pass every check above. The diagnostic prefix must
				// survive — including past a leading comment.
				if !strings.HasPrefix(lead, wantPrefix) {
					t.Errorf("%s [prefix %q]: statementDMLLead(%q) = %q; want it to keep the diagnostic "+
						"prefix %q — verb, table and column are what make the refusal actionable",
						name, prefix, query, lead, wantPrefix)
				}
				cells++
			}
		}
	}

	// Anti-vacuity floor. A cell count compared against the product of
	// the two tables is SELF-REFERENTIAL — both sides shrink together —
	// so the floor names the families instead: dropping `>` from the
	// operator table must fail here by name, because "the pin covered
	// the one operator someone thought of" is the exact defect this
	// matrix exists for. (Caught by the mutation run: an earlier
	// `cells != len(ops)*len(lits) || cells < 60` form passed happily
	// with `>` deleted.)
	for _, op := range []string{"eq", "gt", "lt", "gte", "lte", "ne", "bang_ne", "between", "in", "like", "is"} {
		if _, ok := statementDMLPredicateOperators[op]; !ok {
			t.Errorf("the operator family %q is missing from statementDMLPredicateOperators — the matrix has "+
				"narrowed back toward a representative", op)
		}
	}
	for _, lit := range []string{"quoted_string", "unquoted_numeric", "hex", "bit", "null", "boolean"} {
		if _, ok := statementDMLLiteralKinds[lit]; !ok {
			t.Errorf("the literal kind %q is missing from statementDMLLiteralKinds", lit)
		}
	}
	const commentPrefixes = 7 // the third axis; keep in step with the loop above
	if want := len(statementDMLPredicateOperators) * len(statementDMLLiteralKinds) * commentPrefixes; cells != want {
		t.Fatalf("exercised %d cells; want %d (%d operator families x %d literal kinds x %d comment prefixes)",
			cells, want, len(statementDMLPredicateOperators), len(statementDMLLiteralKinds), commentPrefixes)
	}
	// Absolute floor, separate from the product above: the product shrinks
	// on BOTH sides when a table is trimmed, so it cannot notice a matrix
	// that narrowed back toward a representative. Set at today's size.
	if cells < 11*27*7 {
		t.Fatalf("exercised %d cells; the matrix has shrunk below the 11 x 27 x 7 it covered when this "+
			"floor was set — a literal kind, operator family or comment prefix was removed", cells)
	}
}

// TestDispatch_StatementDMLTripwire pins the belt's WIRING in the
// QueryEvent arm (the lexer tests above cannot fail if the dispatcher
// stops calling it): an in-scope statement-DML QueryEvent stops the
// stream with the coded refusal, an OUT-of-scope one falls through to
// the generic arm (Bug 246 — a statement-format writer on an unrelated
// database must not kill the sync), and a DDL QueryEvent keeps flowing.
func TestDispatch_StatementDMLTripwire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	queryEv := func(schema, stmt string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: hdr(replication.QUERY_EVENT),
			Event:  &replication.QueryEvent{Schema: []byte(schema), Query: []byte(stmt)},
		}
	}

	t.Run("in_scope_dml_refuses", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		err := r.dispatch(ctx, queryEv("app", "INSERT INTO t VALUES (1)"), out)
		if err == nil {
			t.Fatal("dispatch of in-scope statement DML = nil; want the coded refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
	})

	t.Run("no_default_db_dml_refuses", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		err := r.dispatch(ctx, queryEv("", "UPDATE app.t SET x = 1"), out)
		if err == nil {
			t.Fatal("dispatch of no-default-db statement DML = nil; want the coded refusal (empty schema is in scope, matching the generic arm)")
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
	})

	t.Run("in_scope_cte_dml_refuses", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		err := r.dispatch(ctx, queryEv("app", "WITH x AS (SELECT id FROM t) UPDATE t SET v = 1 WHERE id IN (SELECT id FROM x)"), out)
		if err == nil {
			t.Fatal("dispatch of in-scope statement CTE-DML = nil; want the coded refusal (STATEMENT-DML-WITH)")
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
	})

	t.Run("out_of_scope_dml_survives", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		if err := r.dispatch(ctx, queryEv("other", "INSERT INTO t VALUES (1)"), out); err != nil {
			t.Fatalf("dispatch of OUT-of-scope statement DML = %v; want nil (the stream must survive an unrelated database's writer)", err)
		}
	})

	t.Run("ddl_keeps_flowing", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		if err := r.dispatch(ctx, queryEv("app", "ALTER TABLE t ADD COLUMN y INT"), out); err != nil {
			t.Fatalf("dispatch of in-scope DDL = %v; want nil", err)
		}
	})
}
