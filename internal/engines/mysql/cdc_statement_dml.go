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
// The four shapes that USED to be residue, closed by audit 2026-09-01
// SLM-3 (every one observed on real MySQL 8.0.46 as rows missing from
// the target with the stream alive, then confirmed live by a plain
// INSERT that DID refuse in the same stream):
//
//  1. Statement-format LOAD DATA arrives as Begin_load_query +
//     Execute_load_query events, not a QueryEvent. The dispatcher now
//     has an arm for the Execute (cdc_statement_load_data.go) that
//     decodes the event's own schema + statement text and raises this
//     same refusal with verb LOAD DATA, scope-gated identically.
//  2. A DML wrapped ENTIRELY in a /*!vvvvv … */ versioned comment used
//     to lex as a comment. MySQL EXECUTES a versioned comment's
//     contents, so [skipLeadingSQLComments] now skips only the
//     `/*!vvvvv` marker and keeps lexing inside it — the verb is found
//     where the server found it.
//  3. `--` followed by a NEWLINE. The old lexer treated `--` as a
//     comment only before a space or tab ("MySQL's `--` comment
//     requires trailing whitespace"). That premise is false: MySQL's
//     rule is whitespace OR a control character, so `--\nINSERT …` —
//     which a raw driver sends verbatim (the `mysql` CLI strips the
//     bare line client-side, which is why hand testing missed it) —
//     executes, is binlogged as-is, and lexed here as no keyword.
//  4. Cross-database DML from an OUT-of-scope session database
//     (`USE mysql; INSERT INTO src.t …`). The scope gate keyed only on
//     the QueryEvent's session default database, so the write into the
//     synced database slipped as "unrelated". The gate now also asks
//     the statement itself ([statementNamesInScopeDatabase]): a
//     `<db>.` qualifier naming a synced database is in scope.
//
// And the class closure behind all four: a QueryEvent that carries
// NON-comment text which lexes to NO keyword is, under ROW logging,
// something this lexer does not understand — and the only thing it has
// ever turned out to be is a statement-logged write. It now REFUSES
// (verb "unrecognised") instead of falling to the generic-DDL arm. A
// comment-ONLY event (whitespace + comments, nothing to execute) still
// falls through: MySQL never binlogs an empty statement, and MariaDB
// sends exactly that shape on purpose — the `# Dummy event replacing
// event type 160 …` QueryEvent that pads out an ANNOTATE_ROWS event a
// non-annotating replica did not ask for — so refusing it would kill
// every MariaDB stream with binlog_annotate_row_events=ON.
//
// The v0.137.1 "third residue" — a `/*!` comment whose contents carry
// `*/` inside a string literal, which made the quote-blind span scan
// stop mid-value — is DISSOLVED rather than guarded by (2): neither
// scanner searches for the end of a versioned comment any more, because
// its contents are SQL and the lex stops at the verb (detection) or the
// first literal (redaction), both of which come before any `*/` a value
// could hide. The posture that made the old asymmetry deliberate is
// unchanged and worth restating: a false refusal outranks a silent
// miss, so DETECTION is never made more conservative than the server.
// One residue of that posture is stated here: a `/*!NNNNN` whose
// version exceeds the SOURCE's is a comment to the server and SQL to
// this lexer, so `/*!99999 INSERT … */ ALTER …` would refuse on a
// working configuration — loud, wrong, recoverable, and not a shape any
// known client emits (mysqldump versions are never above the dumping
// server's).
//
// The belt is scope-gated like the generic arm's cache clear (Bug 246:
// a statement-format writer on an UNRELATED database must not kill the
// sync). Scope is now the union of the session default database and any
// database the statement qualifies a name with; a statement that names
// an in-scope database anywhere outside a string literal — even as a
// SELECT source (`INSERT INTO other.t SELECT … FROM src.t`) — refuses,
// which is the loud direction for the only population that can reach it
// (an already-misconfigured session writing across databases).
//
// The VStream lane is the sibling here and it is REACHED, not exempt:
// the vendored vstreamer forwards statement-format INSERT/UPDATE/DELETE/
// REPLACE as VEvents of those TYPES (with the SQL in `Dml`), and both
// hand-mirrored dispatchers dropped them at their default arm.
// [vstreamStatementDMLError] is their refusal; statement LOAD DATA on a
// tablet never reaches sluice at all (vstreamer's `IsQuery()` is
// QUERY_EVENT-only, so Execute_load_query is dropped upstream) — that
// one is exempt, and stated.

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

// leadingSQLKeyword lexes the first bare keyword of s — past whatever
// [skipLeadingSQLComments] recognises as whitespace or comment —
// returning it uppercased along with the remainder of s after the
// token. Empty when s holds no keyword: comment-only text, an
// unterminated block comment, or non-comment text that does not begin
// with a word ([statementIsCommentOnly] tells those apart).
func leadingSQLKeyword(s string) (keyword, rest string) {
	i := skipLeadingSQLComments(s)
	if i < 0 {
		return "", ""
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

// skipLeadingSQLComments returns the offset of the first byte of s that
// is neither whitespace nor part of a leading comment, following the
// server's own comment grammar (audit 2026-09-01 AQP-2: this is the ONE
// copy both the detection lexer and the redaction cut use, so the next
// grammar correction cannot land in one scanner and not the other).
// Returns -1 when a plain block comment is unterminated — the text has
// no provable end, so no offset into it is trustworthy.
//
// The grammar, as MySQL lexes it:
//
//   - whitespace: space, tab, newline, carriage return, form feed and
//     vertical tab;
//   - `/* … */` — a plain block comment, quote-blind (a `*/` inside a
//     quoted value terminates it on the server too);
//   - `/*!vvvvv … */` and MariaDB's `/*M!vvvvvv … */` — EXECUTABLE
//     comments: the server lexes their contents as SQL, so only the
//     marker and its version digits are skipped and lexing continues
//     INSIDE; the matching `*/` is consumed as a closer when it is met
//     at comment depth (the `/*!40000 */ DELETE …` shape mysqldump and
//     Vitess emit), and never searched for;
//   - `/*+ … */` — an optimizer hint is a plain block comment to this
//     scan (the server only interprets one directly after a verb);
//   - `-- ` — a line comment when `--` is followed by whitespace OR a
//     control character (0x00–0x20, 0x7F) or by the end of the text.
//     That is the server's rule, ground-truthed on 8.0.46: `--\nINSERT`
//     executes the INSERT, `--x` is a syntax error. The old scan
//     accepted only space and tab (SLM-3 shape 3);
//   - `#` — a line comment to end of line.
//
// A bare `--x` returns its own offset: it is not a comment, so nothing
// past it is provably one either.
func skipLeadingSQLComments(s string) int {
	i, depth := 0, 0
	for i < len(s) {
		switch c := s[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			if marker := versionedCommentMarkerLen(s[i:]); marker > 0 {
				i += marker
				depth++
				continue
			}
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return -1
			}
			i += end + 4
		case c == '*' && depth > 0 && i+1 < len(s) && s[i+1] == '/':
			depth--
			i += 2
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			if i+2 >= len(s) || s[i+2] <= ' ' || s[i+2] == 0x7f {
				i = skipToEOL(s, i)
				continue
			}
			return i
		case c == '#':
			i = skipToEOL(s, i)
		default:
			return i
		}
	}
	return i
}

// versionedCommentMarkerLen returns the byte length of the executable-
// comment opener at the start of s — `/*!` or `/*M!` plus its optional
// version digits — or 0 when s does not start with one.
func versionedCommentMarkerLen(s string) int {
	n := 0
	switch {
	case strings.HasPrefix(s, "/*!"):
		n = 3
	case strings.HasPrefix(s, "/*M!"):
		n = 4
	default:
		return 0
	}
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	return n
}

// statementIsCommentOnly reports whether s is nothing but whitespace and
// comments — text the server would not execute and (on MySQL) would not
// binlog at all. It is the one no-keyword shape the belt lets through:
// MariaDB pads a suppressed ANNOTATE_ROWS event with a `# Dummy event …`
// QueryEvent of exactly this shape (see the file comment).
func statementIsCommentOnly(s string) bool {
	return skipLeadingSQLComments(s) == len(s)
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

// statementDMLUnrecognisedVerb is the verb the class-closure refusal
// carries: the event held non-comment text this lexer found no keyword
// in (SLM-3). Named so the refusal reads as what it is — "sluice could
// not classify this statement" — rather than as a DML verb it never saw.
const statementDMLUnrecognisedVerb = "unrecognised (no leading keyword)"

// dispatchStatementGuards is the QueryEvent arm's statement-shaped
// guard pair, run after the BEGIN/COMMIT arms and before the fold +
// generic-DDL handling: the CDCPOS-1 XA verb dispatch (handled=true
// consumes the event) and the M2 STATEMENT-DML tripwire (a coded,
// stream-fatal refusal when query is a row-DML statement in the
// stream's scope). (false, nil) falls through to the generic-DDL arm,
// whose own cache clear is gated on the session-database scope.
//
// Two statements trip it: one whose leading keyword is a row-DML verb,
// and one whose text lexes to NO keyword at all despite carrying
// non-comment content (SLM-3's class closure — see the file comment for
// why comment-only text is let through). Scope is
// [CDCReader.statementInScope]: the session default database — schema,
// "" = none selected — OR a `<db>.` qualifier in the statement naming a
// synced database (SLM-3 shape 4).
func (r *CDCReader) dispatchStatementGuards(q, schema string, hdr *replication.EventHeader) (handled bool, err error) {
	if handled, err := r.dispatchXAStatement(q); handled || err != nil {
		return handled, err
	}
	verb, isDML := statementDMLVerb(q)
	if !isDML {
		if first, _ := leadingSQLKeyword(q); first != "" || statementIsCommentOnly(q) {
			return false, nil
		}
		verb = statementDMLUnrecognisedVerb
	}
	if !r.statementInScope(schema, q) {
		return false, nil
	}
	return false, statementDMLError(verb, schema, r.statementDMLLocator(hdr), q)
}

// statementInScope is the belt's scope predicate: the statement's
// session default database is in scope (or none was selected), or the
// statement text itself qualifies a name with an in-scope database.
func (r *CDCReader) statementInScope(schema, q string) bool {
	if schema == "" || r.databaseInScope(schema) {
		return true
	}
	return statementNamesInScopeDatabase(q, r.databaseInScope)
}

// statementNamesInScopeDatabase reports whether q carries a
// `<name>.` qualifier — a bare or backtick-quoted identifier
// immediately followed by a dot — for which inScope(name) holds,
// anywhere outside a string literal or comment. It is the SLM-3 shape-4
// door: a statement-format writer whose session database is unrelated
// to the sync can still write INTO a synced database by qualifying the
// table, and the session-database gate alone called that "unrelated".
//
// The scan is deliberately a whole-statement token walk rather than the
// audit's "one token past the verb": the qualifier's position varies by
// verb and modifier (`INSERT IGNORE INTO src.t`, `UPDATE LOW_PRIORITY
// src.t`, `DELETE t FROM src.t`, `WITH c AS (…) UPDATE src.t`), and the
// loud direction is the right one for the only population that reaches
// it. The cost is stated in the file comment: a synced database named
// only as a SELECT source refuses too. String literals are skipped with
// MySQL's quoting rules (`\` escapes, doubled quotes) so a value that
// happens to contain `src.` is never mistaken for a qualifier.
func statementNamesInScopeDatabase(q string, inScope func(string) bool) bool {
	i := 0
	for i < len(q) {
		switch c := q[i]; {
		case c == '\'' || c == '"':
			i = skipQuotedLiteral(q, i)
		case c == '`':
			end := backtickIdentEnd(q, i)
			if end < 0 {
				return false
			}
			if end < len(q) && q[end] == '.' && inScope(strings.ReplaceAll(q[i+1:end-1], "``", "`")) {
				return true
			}
			i = end
		case isKeywordByte(c) || c == '$':
			start := i
			for i < len(q) && (isKeywordByte(q[i]) || q[i] == '$' || (q[i] >= '0' && q[i] <= '9')) {
				i++
			}
			if i < len(q) && q[i] == '.' && inScope(q[start:i]) {
				return true
			}
		case c == '/' || c == '-' || c == '#':
			// A comment mid-statement: skip it whole so a `src.` inside
			// an annotation neither trips nor hides anything. A bare
			// `/` or `-` operator advances one byte.
			if n := skipLeadingSQLComments(q[i:]); n > 0 {
				i += n
			} else {
				i++
			}
		default:
			i++
		}
	}
	return false
}

// skipQuotedLiteral returns the offset just past the string literal
// opening at q[i] (a `'` or `"`), honouring MySQL's backslash escapes
// and doubled-quote escapes. An unterminated literal consumes the rest
// of q — nothing after it can be a qualifier the server saw.
func skipQuotedLiteral(q string, i int) int {
	quote := q[i]
	for j := i + 1; j < len(q); j++ {
		switch q[j] {
		case '\\':
			j++ // the escaped byte, whatever it is
		case quote:
			if j+1 < len(q) && q[j+1] == quote {
				j++ // doubled quote: an escaped quote, keep scanning
				continue
			}
			return j + 1
		}
	}
	return len(q)
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
	// Start where [statementDMLCut] starts — past any leading comment.
	// Slicing from 0 keeps the comment TEXT, which is what shipped in
	// v0.137.0 and is Bug 258: the cut offset advanced correctly, so
	// short comments looked fixed, while a long one (an sqlcommenter /
	// `traceparent` prefix runs ~110 bytes) spent the entire echo cap on
	// comment text and left no verb and no table — the same diagnostic
	// loss the comment skip was added to fix, one layer along. The
	// comment is not diagnostic: it is the caller's annotation, not
	// sluice's statement.
	start := statementDMLCommentSkip(query)
	cut := statementDMLCut(query)
	lead := strings.TrimRight(query[start:cut], " \t\r\n")
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
// that is not leading whitespace or a leading comment — the SAME
// grammar the detection lexer uses, via [skipLeadingSQLComments]
// (audit 2026-09-01 AQP-2: the two scanners were hand-copied and had
// already diverged in effect). On an unterminated block comment it
// returns 0, so the caller falls back to cutting immediately — the safe
// direction.
//
// The v0.137.1 quote guard that used to live here (refuse to trust a
// `/*!` span carrying a quote) is gone with the span search it guarded:
// a versioned comment's contents are lexed as SQL, so the skip stops at
// the marker and the allowlist cut handles whatever follows — a `*/`
// hidden inside a quoted value is never reached, because the quote cuts
// first. TestStatementDMLLead_VersionedCommentContentsAreSQL keeps the
// original observed fixture pinned in both directions.
func statementDMLCommentSkip(query string) int {
	if i := skipLeadingSQLComments(query); i > 0 {
		return i
	}
	return 0
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
	carrier := "binlog QUERY-event text"
	switch verb {
	case "WITH":
		// Name the class, not just the token: the WITH entry exists for
		// statement-format CTE-DML (file comment).
		verb = "WITH-prefixed (CTE-DML)"
	case statementDMLLoadDataVerb:
		// Statement-format LOAD DATA rides its own event class (SLM-3
		// shape 1); name it so the coordinate resolves to the right
		// event in mysqlbinlog.
		carrier = "a binlog EXECUTE_LOAD_QUERY event's statement text"
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
			"mysql: cdc: a %s statement arrived as %s %s (%d bytes, %s, "+
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
			verb, carrier, where, len(query), locator, statementDMLLead(query),
		),
	)
}
