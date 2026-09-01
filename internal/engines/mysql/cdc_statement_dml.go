// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// M2 capture-completeness sweep, critic P2 — the STATEMENT-DML
// dispatch belt.
//
// preflightBinlogFormat gates the GLOBAL binlog_format at CDC open, and
// its own doc records the residue: a SUPER session's SET SESSION
// binlog_format=STATEMENT slips the gate, and its DML then arrives as
// QueryEvent TEXT that used to fall into the generic-DDL arm — schema
// cache cleared, nothing applied, no error. This belt closes that
// residue at dispatch: under ROW logging a row-DML statement NEVER
// legitimately arrives as query text, so its arrival is proof of a
// statement-logged write sluice cannot apply, and the stream stops
// loudly instead of silently dropping it. (The same belt catches a
// resume replaying a binlog segment recorded before the global was
// flipped to ROW.)
//
// False-positive analysis for the first-token test, against the
// QueryEvent traffic the arm actually sees under ROW logging (stated
// here per the sweep's commit discipline):
//
//   - BEGIN / COMMIT are consumed by the arms above the belt; XA verbs
//     by dispatchXAStatement; SAVEPOINT / ROLLBACK TO SAVEPOINT start
//     with neither verb.
//   - CREATE TABLE … AS SELECT arrives tagged CREATE (its rows follow
//     as row events); INSERT…SELECT, REPLACE, LOAD DATA and
//     trigger/procedure body writes are all row-logged under ROW —
//     none produce a DML-verb QueryEvent.
//   - MariaDB's ANNOTATE_ROWS event carries the original DML text of
//     row events, but as its own event type, never a QueryEvent (and
//     sluice's syncer does not request annotation).
//   - MariaDB `DELETE HISTORY FROM …` (system-versioned-table
//     maintenance) is the one statement-shaped DELETE a ROW-format
//     server may legitimately write as a statement; the second-token
//     exemption below keeps it on the generic-DDL path.
//
//   - WITH (CTE-DML, MySQL 8.0 / MariaDB 10.2+) IS enumerated
//     (STATEMENT-DML-WITH, the VF-review 2026-08-26 follow-up): WITH
//     can only prefix SELECT / UPDATE / DELETE, a WITH…SELECT performs
//     no writes and is never binlogged under ANY format, and under ROW
//     logging CTE UPDATE/DELETE ride row events — so a WITH-prefixed
//     QueryEvent is proof of statement-format CTE-DML. WITH RECURSIVE
//     lexes the same first token and needs no second-token handling.
//
// Known residue, stated not implied: statement-format LOAD DATA
// arrives as Begin/Execute_load_query events (a different event type
// the dispatcher's default arm ignores), and a DML wrapped entirely in
// a /*!vvvvv … */ versioned comment lexes as a comment here. Both
// remain the documented session-override residue, now narrowed to
// those shapes (CTE-DML moved from residue to the verb set above).
// The belt is scope-gated like the generic arm's
// cache clear (Bug 246: a statement-format writer on an UNRELATED
// database must not kill the sync); the trade is that a statement
// whose session default database is out of scope but which writes into
// a synced database cross-database slips it — the narrower remainder
// of the same residue.

// statementDMLVerbs are the row-DML leading keywords that cannot arrive
// as query text under ROW logging. WITH is the CTE-DML entry (see the
// file comment's false-positive analysis: a WITH-prefixed QueryEvent can
// only be statement-format CTE UPDATE/DELETE — WITH…SELECT is never
// binlogged, and DDL never begins with WITH).
var statementDMLVerbs = map[string]bool{
	"INSERT":  true,
	"UPDATE":  true,
	"DELETE":  true,
	"REPLACE": true,
	"WITH":    true,
}

// statementDMLVerb returns the leading SQL keyword of query (after
// whitespace and comments) when it is a row-DML verb. MariaDB's
// `DELETE HISTORY` is exempted (see the file comment).
func statementDMLVerb(query string) (string, bool) {
	first, rest := leadingSQLKeyword(query)
	if !statementDMLVerbs[first] {
		return "", false
	}
	if first == "DELETE" {
		if second, _ := leadingSQLKeyword(rest); second == "HISTORY" {
			return "", false
		}
	}
	return first, true
}

// leadingSQLKeyword lexes the first bare keyword of s — skipping
// whitespace, `/* … */` block comments (versioned included), `-- `
// line comments, and `#` line comments — returning it uppercased along
// with the remainder of s after the token. Empty when s holds no
// keyword.
func leadingSQLKeyword(s string) (keyword, rest string) {
	i := 0
scan:
	for i < len(s) {
		switch c := s[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return "", ""
			}
			i += end + 4
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			// MySQL's `--` comment requires trailing whitespace; a bare
			// `--x` is not a comment (and not a keyword either way).
			if i+2 < len(s) && (s[i+2] == ' ' || s[i+2] == '\t') {
				i = skipToEOL(s, i)
				continue
			}
			return "", ""
		case c == '#':
			i = skipToEOL(s, i)
		default:
			break scan
		}
	}
	start := i
	for i < len(s) && (isKeywordByte(s[i]) || (s[i] >= '0' && s[i] <= '9' && i > start)) {
		i++
	}
	if i == start {
		return "", ""
	}
	return strings.ToUpper(s[start:i]), s[i:]
}

func skipToEOL(s string, i int) int {
	if nl := strings.IndexByte(s[i:], '\n'); nl >= 0 {
		return i + nl + 1
	}
	return len(s)
}

func isKeywordByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// dispatchStatementGuards is the QueryEvent arm's statement-shaped
// guard pair, run after the BEGIN/COMMIT arms and before the fold +
// generic-DDL handling: the CDCPOS-1 XA verb dispatch (handled=true
// consumes the event) and the M2 STATEMENT-DML tripwire (a coded,
// stream-fatal refusal when query is a row-DML statement whose session
// default database — schema, "" = none selected — is in the stream's
// scope). (false, nil) falls through to the generic-DDL arm, whose own
// cache clear is gated on the same scope condition.
func (r *CDCReader) dispatchStatementGuards(q, schema string, hdr *replication.EventHeader) (handled bool, err error) {
	if handled, err := r.dispatchXAStatement(q); handled || err != nil {
		return handled, err
	}
	verb, isDML := statementDMLVerb(q)
	if !isDML {
		return false, nil
	}
	if schema != "" && !r.databaseInScope(schema) {
		return false, nil
	}
	return false, statementDMLError(verb, schema, r.statementDMLLocator(hdr), q)
}

// statementDMLLocator renders the payload-free coordinate the refusal
// carries in place of the old sha256 prefix (see [statementDMLError]
// for why): where the offending event sits in the binlog, when it was
// committed, and — in GTID mode — the transaction it belongs to. Every
// component is stream metadata, so none of it is a function of the
// statement's bytes.
//
// Each component is best-effort and named only when known: currentFile
// is empty until the dump's opening ROTATE arrives, and pendingGTID is
// empty in file/pos mode and between transactions.
func (r *CDCReader) statementDMLLocator(hdr *replication.EventHeader) string {
	parts := make([]string, 0, 3)
	if hdr != nil {
		if r.currentFile != "" {
			parts = append(parts, fmt.Sprintf("ends at binlog %s:%d", r.currentFile, hdr.LogPos))
		} else {
			parts = append(parts, fmt.Sprintf("ends at binlog position %d", hdr.LogPos))
		}
		if ts := binlogEventCommitTime(hdr); !ts.IsZero() {
			parts = append(parts, "committed "+ts.UTC().Format(time.RFC3339))
		}
	}
	if r.pendingGTID != "" {
		parts = append(parts, "gtid "+r.pendingGTID)
	}
	if len(parts) == 0 {
		return "no binlog coordinate available"
	}
	return strings.Join(parts, ", ")
}

// statementDMLEchoCap bounds the sanitized leading text carried on the
// refusal.
const statementDMLEchoCap = 80

// bareLiteralKeywords are the only MySQL literals that lex as plain
// words, so they are the only ones [statementDMLCut]'s identifier
// allowlist would otherwise carry through. Enumerated rather than
// derived because MySQL's grammar has exactly these four.
var bareLiteralKeywords = map[string]bool{
	"NULL":    true,
	"TRUE":    true,
	"FALSE":   true,
	"UNKNOWN": true,
}

// statementDMLLead returns the diagnostic prefix of a row-DML
// statement, cut at the first token that is not identifier material
// (see [statementDMLCut]) and capped at statementDMLEchoCap bytes. The
// verb, table name, and leading column name are diagnostic; the VALUES
// are not, and must never reach the error (audit 2026-08-27 A4: the
// binlog text carries row values — PII that would bypass --redact by
// riding the refusal into logs and reports).
func statementDMLLead(query string) string {
	cut := statementDMLCut(query)
	lead := strings.TrimRight(query[:cut], " \t\r\n")
	if len(lead) > statementDMLEchoCap {
		lead = lead[:statementDMLEchoCap]
	}
	if len(lead) < len(query) {
		lead += "…"
	}
	return lead
}

// statementDMLCut returns the byte offset at which the echoed lead must
// stop: the start of the first token that is not an identifier, a
// keyword, or a name separator.
//
// This is an ALLOWLIST over MySQL's lexical grammar, and that is the
// load-bearing property (audit 2026-08-31 SEC-5 / A-4). The shipped
// v0.132.2 form was a blocklist — cut at the first `'`, `"`, `(` or `=`
// — which is a set of delimiters that HAPPEN to precede a value in the
// shapes anyone thought to test. It kept its promise for `SET ssn=…`
// and broke it for every other comparison: `WHERE ssn > 078051120`,
// `WHERE mrn BETWEEN 4820113 AND 4820119`, `WHERE ssn <> …` contain
// none of the four characters, so the row value rode the refusal into
// operator logs intact. Enumerating `> < >= <= <> !=` would have closed
// those and left the next unlisted operator open; inverting the test
// closes the class.
//
// Allowed, and nothing else:
//
//   - whitespace, `.` and `,` — name qualification and list separators;
//     no MySQL value is spelled with them alone;
//   - a backtick-quoted identifier (with the “ “ “ escape) — MySQL's
//     only identifier quote, so its contents are a NAME by the grammar,
//     never a value;
//   - a bare word `[A-Za-z_$][A-Za-z0-9_$]*` that is not one of
//     [bareLiteralKeywords].
//
// Why that is complete for MySQL's grammar: every literal form either
// begins with a character outside the allowed set — `'…'` / `"…"`
// (string), a digit or `.`-digit (decimal, float, `0x…` hex, `0b…`
// bit), the quote inside an introduced literal (`X'…'`, `b'…'`,
// `_utf8'…'`, `N'…'`, `DATE '…'`, `{d '…'}`), the digit in an
// `INTERVAL 3 DAY`, `?` for a placeholder, `@` for a user variable — or
// it is one of the four bare keyword literals, which are enumerated.
// And every OPERATOR built from punctuation (`= <=> > >= < <= <> != :=
// + - * / % ^ | & ~ << >> ! && || -> ->>`) falls outside the allowed
// set by construction, so the comparison family is spanned rather than
// listed. The WORD operators (`AND OR NOT XOR LIKE RLIKE REGEXP IN
// BETWEEN IS DIV MOD SOUNDS MEMBER OF`) deliberately do NOT cut: they
// carry no value themselves, and the operand that follows one is what
// cuts — `BETWEEN 4820113` cuts at the digit, `IN (…)` at the paren,
// `LIKE 'a%'` at the quote, `IS NULL` at the keyword literal.
//
// The cost is small and stated: `WHERE id IS NULL` now renders as
// `… WHERE id IS…`. A value is a value even when it is `NULL`, and the
// column name — the diagnostic half — is upstream of the cut.
func statementDMLCut(query string) int {
	// Leading comments are skipped the same way [leadingSQLKeyword] skips
	// them, and for a reason worth stating: a comment's first byte is `/`
	// or `-` or `#`, none of which the allowlist below admits, so without
	// this the cut landed at offset 0 and the whole diagnostic rendered as
	// `…`. That is the SAFE direction — it over-cuts, it cannot leak — but
	// it erased the verb, table and columns for an entire traffic class,
	// since comment-prefixed DML is the normal shape from Vitess and
	// PlanetScale (`/*vt+ …*/`), ProxySQL, and tracing-annotated clients.
	// The comment text itself is NOT kept: only the offset advances.
	i := statementDMLCommentSkip(query)
	for i < len(query) {
		switch c := query[i]; {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '.' || c == ',':
			i++
		case c == '`':
			end := backtickIdentEnd(query, i)
			if end < 0 {
				return i // unterminated: nothing past it is provably a name
			}
			i = end
		case isKeywordByte(c) || c == '$':
			start := i
			for i < len(query) && (isKeywordByte(query[i]) || query[i] == '$' || (query[i] >= '0' && query[i] <= '9')) {
				i++
			}
			if bareLiteralKeywords[strings.ToUpper(query[start:i])] {
				return start
			}
		default:
			// A digit-initial token (every unquoted numeric, hex and bit
			// literal), a quote, an operator character, a paren — cut.
			return i
		}
	}
	return len(query)
}

// statementDMLCommentSkip returns the offset of the first byte of query
// that is not leading whitespace or a leading comment, using the same
// comment forms [leadingSQLKeyword] recognises. On anything malformed
// (an unterminated block comment) it returns 0, so the caller falls back
// to cutting immediately — the safe direction.
func statementDMLCommentSkip(query string) int {
	i := 0
	for i < len(query) {
		switch c := query[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return 0
			}
			i += end + 4
		case c == '-' && i+1 < len(query) && query[i+1] == '-':
			if i+2 < len(query) && (query[i+2] == ' ' || query[i+2] == '\t') {
				i = skipToEOL(query, i)
				continue
			}
			return i
		case c == '#':
			i = skipToEOL(query, i)
		default:
			return i
		}
	}
	return i
}

// backtickIdentEnd returns the offset just past the backtick-quoted
// identifier starting at query[i], or -1 when it is unterminated. A
// doubled backtick is an escaped backtick within the identifier.
func backtickIdentEnd(query string, i int) int {
	j := i + 1
	for {
		k := strings.IndexByte(query[j:], '`')
		if k < 0 {
			return -1
		}
		j += k + 1
		if j < len(query) && query[j] == '`' {
			j++ // escaped backtick; keep scanning
			continue
		}
		return j
	}
}

// statementDMLError builds the coded, stream-fatal refusal for a
// row-DML statement arriving as query text. schema is the QueryEvent's
// session default database ("" when none was selected); locator is the
// payload-free binlog coordinate from
// [CDCReader.statementDMLLocator]. The statement is identified by verb
// + byte length + that coordinate plus the sanitized lead from
// [statementDMLLead] — never the payload.
//
// The correlation aid is deliberately the COORDINATE and not a digest
// of the statement text (audit 2026-08-31 SEC-5). The refusal carried
// `sha256(query)[:4]` for the operator to recompute against their own
// query log — but that recomputation IS a brute-force oracle: against a
// known statement template with one low-entropy unknown (a 9-digit
// national identifier is ~10^9 candidates over 2^32 buckets) the prefix
// usually determines the withheld value uniquely for anyone holding the
// log. Lengthening it makes that strictly worse; truncating it further
// weakens the oracle and the correlation together; salting it
// per-process removes the oracle by removing the use case, since the
// operator can no longer recompute it and the refusal is stream-fatal
// so there is exactly one per process to correlate internally. The
// asymmetry that settles it: anyone who CAN recompute the digest
// already holds the query log containing the plaintext, so it gave the
// legitimate user nothing they did not have and gave a log-only reader
// the value. The binlog file/position, event timestamp and GTID
// identify the statement exactly, are directly usable against
// mysqlbinlog, and are functions of the stream rather than of the
// bytes — so they cannot be inverted at all.
func statementDMLError(verb, schema, locator, query string) error {
	if verb == "WITH" {
		// Name the class, not just the token: the WITH entry exists for
		// statement-format CTE-DML (file comment).
		verb = "WITH-prefixed (CTE-DML)"
	}
	where := "with no default database selected"
	if schema != "" {
		where = fmt.Sprintf("(session default database %q)", schema)
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCStatementDML,
		"find and clear the writing session's binlog_format override (performance_schema."+
			"variables_by_thread WHERE VARIABLE_NAME='binlog_format'; MySQL-only: requires performance_schema=ON, and "+
			"MariaDB lacks the variables_by_thread table entirely, so on MariaDB find the writer via "+
			"SHOW PROCESSLIST and interrogate candidate sessions), ensure @@GLOBAL.binlog_format=ROW, then start the sync fresh "+
			"(--restart-from-scratch) — the statement-logged writes are only recoverable by re-snapshot",
		fmt.Errorf(
			"mysql: cdc: a %s statement arrived as binlog QUERY-event text %s (%d bytes, %s, "+
				"leading text %q; values withheld — locate the statement by that binlog coordinate, e.g. "+
				"mysqlbinlog against your own server) — "+
				"under ROW logging DML is written as row events, so statement text here is proof a writing "+
				"session overrode binlog_format to STATEMENT/MIXED (or this resume is replaying a segment "+
				"recorded before the global was set to ROW). sluice deliberately never executes replayed SQL "+
				"text against the target, so this write CANNOT be applied — stopping loudly instead of "+
				"silently dropping it. Clear the session override (or fix the old segment by starting past "+
				"it), ensure @@GLOBAL.binlog_format=ROW, then start the sync fresh: a resume would "+
				"deterministically re-refuse at this same event, and only a fresh snapshot recopies the "+
				"statement-logged writes",
			verb, where, len(query), locator, statementDMLLead(query),
		),
	)
}
