// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

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
// Known residue, stated not implied: statement-format LOAD DATA
// arrives as Begin/Execute_load_query events (a different event type
// the dispatcher's default arm ignores); a DML wrapped entirely in
// a /*!vvvvv … */ versioned comment lexes as a comment here; and
// CTE-DML (`WITH x AS (…) UPDATE/DELETE …`, MySQL 8.0 / MariaDB
// 10.2+) lexes first-token WITH, which this belt does not enumerate —
// a WITH-prefixed statement in a ROW-mode binlog is almost certainly
// statement-format CTE-DML (SELECTs are never binlogged), so adding
// WITH to the verb set is a filed follow-up (VF review 2026-08-26)
// rather than an unreviewed pre-tag behavior change. All three remain
// the documented session-override residue, now narrowed to those
// shapes. The belt is scope-gated like the generic arm's
// cache clear (Bug 246: a statement-format writer on an UNRELATED
// database must not kill the sync); the trade is that a statement
// whose session default database is out of scope but which writes into
// a synced database cross-database slips it — the narrower remainder
// of the same residue.

// statementDMLVerbs are the row-DML leading keywords that cannot arrive
// as query text under ROW logging.
var statementDMLVerbs = map[string]bool{
	"INSERT":  true,
	"UPDATE":  true,
	"DELETE":  true,
	"REPLACE": true,
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
func (r *CDCReader) dispatchStatementGuards(q, schema string) (handled bool, err error) {
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
	return false, statementDMLError(verb, schema, q)
}

// statementDMLEchoCap bounds the sanitized leading text carried on the
// refusal.
const statementDMLEchoCap = 80

// statementDMLLead returns the diagnostic prefix of a row-DML
// statement, cut BEFORE the first string-literal quote or opening
// paren — whichever comes first — and capped at statementDMLEchoCap
// bytes. The verb and table name (which precede the first paren or
// literal in every DML shape) are diagnostic; the VALUES are not, and
// must never reach the error (audit 2026-08-27 A4: the binlog text
// carries row values — PII that would bypass --redact by riding the
// refusal into logs and reports). Numeric literals after SET/WHERE can
// survive the cut only within the byte cap; the cap is why it is 80,
// not the old 160.
func statementDMLLead(query string) string {
	cut := len(query)
	for i := 0; i < len(query); i++ {
		if c := query[i]; c == '\'' || c == '"' || c == '(' {
			cut = i
			break
		}
	}
	lead := strings.TrimRight(query[:cut], " \t\r\n")
	if len(lead) > statementDMLEchoCap {
		lead = lead[:statementDMLEchoCap]
	}
	if len(lead) < len(query) {
		lead += "…"
	}
	return lead
}

// statementDMLError builds the coded, stream-fatal refusal for a
// row-DML statement arriving as query text. schema is the QueryEvent's
// session default database ("" when none was selected). The statement
// itself is identified by verb + byte length + a sha256 prefix of the
// full text (enough for the operator to correlate with their own query
// log) plus the sanitized lead from [statementDMLLead] — never the
// payload.
func statementDMLError(verb, schema, query string) error {
	digest := sha256.Sum256([]byte(query))
	where := "with no default database selected"
	if schema != "" {
		where = fmt.Sprintf("(session default database %q)", schema)
	}
	return sluicecode.Wrap(
		sluicecode.CodeCDCStatementDML,
		"find and clear the writing session's binlog_format override (performance_schema."+
			"variables_by_thread WHERE VARIABLE_NAME='binlog_format'; requires performance_schema=ON — "+
			"MariaDB defaults it OFF, so there find the writer via SHOW PROCESSLIST and interrogate "+
			"candidate sessions), ensure @@GLOBAL.binlog_format=ROW, then start the sync fresh "+
			"(--restart-from-scratch) — the statement-logged writes are only recoverable by re-snapshot",
		fmt.Errorf(
			"mysql: cdc: a %s statement arrived as binlog QUERY-event text %s (%d bytes, sha256 %s, "+
				"leading text %q; values withheld — correlate via the digest against your own query log) — "+
				"under ROW logging DML is written as row events, so statement text here is proof a writing "+
				"session overrode binlog_format to STATEMENT/MIXED (or this resume is replaying a segment "+
				"recorded before the global was set to ROW). sluice deliberately never executes replayed SQL "+
				"text against the target, so this write CANNOT be applied — stopping loudly instead of "+
				"silently dropping it. Clear the session override (or fix the old segment by starting past "+
				"it), ensure @@GLOBAL.binlog_format=ROW, then start the sync fresh: a resume would "+
				"deterministically re-refuse at this same event, and only a fresh snapshot recopies the "+
				"statement-logged writes",
			verb, where, len(query), hex.EncodeToString(digest[:4]), statementDMLLead(query),
		),
	)
}
