// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// This file is the MySQL-dialect statement layer shared by the schema and
// data paths: a boundary splitter that finds top-level `;` without being
// fooled by string/identifier/comment syntax, a bounded streaming reader
// over it (an incremental lexer whose state survives read-block
// boundaries, so a statement, string, or comment spanning blocks splits
// exactly as it would in one whole-file scan — each byte is examined
// once), and the comment/versioned-comment stripper the statement
// classifiers use.
//
// The sqlite engine's splitDumpChunk is deliberately NOT reused: SQLite
// text has no backslash escapes, no backtick identifiers, and no `#`
// comments, so its states are wrong for MySQL text (a `\'` inside a MySQL
// string would desynchronise it). Same shape, MySQL states.

// splitMySQLChunk breaks MySQL SQL text into complete top-level statements
// plus a trailing CARRY — the fragment after the last top-level ';', which
// may be an unterminated statement/string/comment the next block completes.
// It respects single-quoted strings and double-quoted strings (both with
// backslash escapes and doubled-quote escapes), backtick identifiers (with
// doubled-backtick escapes, no backslash), `-- ` and `#` line comments, and
// `/* */` block comments (including `/*!` versioned blocks — for SPLITTING
// they are opaque; the classifier unwraps them). A two-char delimiter whose
// first byte ends the chunk is simply left in the carry; the re-scan
// resolves it. Bytewise iteration is safe: every delimiter is ASCII and
// UTF-8 continuation bytes never collide with one.
func splitMySQLChunk(script string) (stmts []string, carry string) {
	start, n := 0, len(script)
	for i := 0; i < n; i++ {
		switch script[i] {
		case '\'', '"':
			q := script[i]
			i++
			for i < n {
				switch script[i] {
				case '\\':
					i += 2 // escaped byte (a trailing backslash lands in the carry)
					continue
				case q:
					if i+1 < n && script[i+1] == q {
						i += 2 // doubled quote is an escape, not a terminator
						continue
					}
				default:
					i++
					continue
				}
				break
			}
		case '`':
			i++
			for i < n {
				if script[i] == '`' {
					if i+1 < n && script[i+1] == '`' {
						i += 2 // `` is an escaped backtick
						continue
					}
					break
				}
				i++
			}
		case '#':
			for i < n && script[i] != '\n' {
				i++
			}
		case '-':
			// MySQL requires whitespace (or end of input) after `--` for a
			// line comment; `a--b` is arithmetic. At a chunk boundary the
			// ambiguous prefix stays in the carry via the enclosing loop.
			if i+1 < n && script[i+1] == '-' && (i+2 >= n || script[i+2] == ' ' || script[i+2] == '\t' ||
				script[i+2] == '\n' || script[i+2] == '\r') {
				for i < n && script[i] != '\n' {
					i++
				}
			}
		case '/':
			if i+1 < n && script[i+1] == '*' {
				i += 2
				for i < n {
					if script[i] == '*' && i+1 < n && script[i+1] == '/' {
						break
					}
					i++
				}
				i++ // skip the '*'; the loop's i++ skips the '/'
			}
		case ';':
			if s := strings.TrimSpace(script[start:i]); s != "" {
				stmts = append(stmts, s)
			}
			start = i + 1
		}
	}
	return stmts, script[start:]
}

const (
	// statementReadBlockBytes is how much of a dump file is read per block.
	statementReadBlockBytes = 1 << 20 // 1 MiB

	// maxStatementBytes bounds a single statement (the carry). mydumper and
	// the pscale dumper both target ~1 MiB statements; --statement-size can
	// raise that, so the cap is generous — but a carry past it means the
	// file is not statement-structured (or a quote never closed) and the
	// read refuses loudly instead of buffering the whole file (named wart:
	// the statement-size ceiling, ADR-0161 §6).
	maxStatementBytes = 256 << 20
)

// errStatementTooLarge is wrapped with file context by the reader.
var errStatementTooLarge = fmt.Errorf(
	"statement exceeds the %d MiB ceiling (not a statement-structured dump, or an unterminated string)",
	maxStatementBytes>>20,
)

// scanState is the incremental splitter's lexer state, carried across
// appended blocks. States past scanTop either sit INSIDE a multi-byte
// token (string/identifier/comment) or hold a PENDING one-byte lookahead
// (a possible two-char delimiter or escape whose deciding byte has not
// arrived yet — the stateful equivalent of splitMySQLChunk's leave-it-
// in-the-carry-and-re-scan resolution). Because the machine consumes one
// byte at a time, its trajectory — and therefore every statement
// boundary — is independent of where the read blocks happen to fall.
type scanState uint8

const (
	scanTop          scanState = iota
	scanString                 // inside '…' or "…" (the quote byte is stmtScanner.quote)
	scanStringEscape           // consumed a backslash inside a string; next byte is literal
	scanStringQuote            // saw the quote byte; doubled-quote-escape lookahead pending
	scanBacktick               // inside a `…` identifier
	scanBacktickTick           // saw a backtick; doubled-backtick-escape lookahead pending
	scanDash1                  // saw '-' at top level; second '-' lookahead pending
	scanDash2                  // saw '--' at top level; the comment-deciding whitespace lookahead pending
	scanSlash                  // saw '/' at top level; '*' lookahead pending
	scanLineComment            // inside a `-- ` / `#` comment, consuming to newline
	scanBlockComment           // inside /* … */ (versioned blocks included — opaque for splitting)
	scanBlockStar              // saw '*' inside a block comment; '/' lookahead pending
)

// stmtScanner incrementally lexes MySQL text into top-level statements.
// It holds the unemitted carry plus the lexer state at the carry's end,
// so appending the next block resumes scanning where the last one left
// off — each byte is examined exactly once. Its statement boundaries are
// byte-identical to a whole-input [splitMySQLChunk] scan (pinned by the
// differential test); the predecessor rebuilt and re-lexed carry+block
// per 1 MiB block, which made statement SIZE quadratic in cost — a
// 64 MiB --statement-size dump read ~15× slower per byte (audit
// 2026-07-15 MED-P1).
type stmtScanner struct {
	buf   []byte // carry: bytes after the last emitted statement boundary
	pos   int    // buf[:pos] is scanned — proven boundary-free given state
	state scanState
	quote byte // the active quote byte while in the scanString* states
}

// append adds the next read block and returns the complete statements it
// finishes, in order. The remainder stays buffered as the carry.
func (s *stmtScanner) append(block []byte) []string {
	s.buf = append(s.buf, block...)
	var stmts []string
	buf := s.buf
	start := 0 // carry-relative index where the current statement begins
	i := s.pos
	for i < len(buf) {
		c := buf[i]
		switch s.state {
		case scanTop:
			switch c {
			case '\'', '"':
				s.quote = c
				s.state = scanString
			case '`':
				s.state = scanBacktick
			case '#':
				s.state = scanLineComment
			case '-':
				s.state = scanDash1
			case '/':
				s.state = scanSlash
			case ';':
				if stmt := strings.TrimSpace(string(buf[start:i])); stmt != "" {
					stmts = append(stmts, stmt)
				}
				start = i + 1
			}
		case scanString:
			switch c {
			case '\\':
				s.state = scanStringEscape
			case s.quote:
				s.state = scanStringQuote
			}
		case scanStringEscape:
			s.state = scanString
		case scanStringQuote:
			if c != s.quote {
				s.state = scanTop
				continue // the string ended before c; re-dispatch it at top level
			}
			s.state = scanString // doubled quote is an escape, not a terminator
		case scanBacktick:
			if c == '`' {
				s.state = scanBacktickTick
			}
		case scanBacktickTick:
			if c != '`' {
				s.state = scanTop
				continue // the identifier ended before c; re-dispatch
			}
			s.state = scanBacktick // `` is an escaped backtick
		case scanDash1:
			if c != '-' {
				s.state = scanTop
				continue // lone '-' was arithmetic; re-dispatch c
			}
			s.state = scanDash2
		case scanDash2:
			// MySQL requires whitespace (or end of input) after `--` for a
			// line comment; `a--b` is arithmetic. A further '-' shifts the
			// pending pair right (`--- ` comments from the SECOND dash).
			switch c {
			case ' ', '\t', '\r':
				s.state = scanLineComment
			case '\n':
				s.state = scanTop // an empty `--` comment; the newline ends it
			case '-':
				// stay in scanDash2
			default:
				s.state = scanTop
				continue // both dashes were arithmetic; re-dispatch c
			}
		case scanSlash:
			if c != '*' {
				s.state = scanTop
				continue // lone '/' was arithmetic; re-dispatch c
			}
			s.state = scanBlockComment
		case scanLineComment:
			if c == '\n' {
				s.state = scanTop
			}
		case scanBlockComment:
			if c == '*' {
				s.state = scanBlockStar
			}
		case scanBlockStar:
			switch c {
			case '/':
				s.state = scanTop
			case '*':
				// stay: this '*' may pair with the NEXT byte
			default:
				s.state = scanBlockComment
			}
		}
		i++
	}
	// Compact the emitted prefix out of the carry; the resume offset now
	// covers the whole (scanned) remainder.
	if start > 0 {
		n := copy(s.buf, s.buf[start:])
		s.buf = s.buf[:n]
	}
	s.pos = len(s.buf)
	return stmts
}

// carryLen reports the buffered partial-statement size (the
// maxStatementBytes ceiling's input).
func (s *stmtScanner) carryLen() int { return len(s.buf) }

// finish returns the trimmed final fragment (a trailing statement with
// no ';', or a dangling comment/string) and releases the buffer.
func (s *stmtScanner) finish() string {
	rest := strings.TrimSpace(string(s.buf))
	s.buf = nil
	return rest
}

// statementStream yields complete top-level statements from r, one at a
// time, never holding more than one read block plus one partial statement
// in memory. Next returns io.EOF after the final statement.
type statementStream struct {
	r       io.Reader
	block   []byte
	scan    stmtScanner
	pending []string
	eof     bool
}

// newStatementStream wraps r. blockSize <= 0 uses the default; tests pass
// tiny blocks to pin boundary-straddling tokens.
func newStatementStream(r io.Reader, blockSize int) *statementStream {
	if blockSize <= 0 {
		blockSize = statementReadBlockBytes
	}
	return &statementStream{r: r, block: make([]byte, blockSize)}
}

// Next returns the next statement, or io.EOF when the input is exhausted.
// A final unterminated fragment (a trailing statement with no ';') is
// yielded as a statement — the classifiers decide whether it is legal.
func (s *statementStream) Next() (string, error) {
	for {
		if len(s.pending) > 0 {
			stmt := s.pending[0]
			s.pending = s.pending[1:]
			return stmt, nil
		}
		if s.eof {
			if rest := s.scan.finish(); rest != "" {
				return rest, nil
			}
			return "", io.EOF
		}
		nr, rerr := s.r.Read(s.block)
		if nr > 0 {
			s.pending = s.scan.append(s.block[:nr])
			if s.scan.carryLen() > maxStatementBytes {
				return "", errStatementTooLarge
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return "", rerr
			}
			s.eof = true
		}
	}
}

// stripLeadingCommentsAndSpace removes leading whitespace, `-- ` and `#`
// line comments, and plain `/* */` block comments from stmt. A leading
// VERSIONED comment (`/*!40101 SET NAMES binary*/`) is NOT a comment —
// MySQL executes its body — so it is UNWRAPPED: the `/*!NNNNN` opener and
// its matching `*/` are removed and the inner text spliced in. The result
// starts at the statement's first meaningful token (or is empty for a
// comment-only fragment).
func stripLeadingCommentsAndSpace(stmt string) string {
	for {
		stmt = strings.TrimLeft(stmt, " \t\r\n")
		switch {
		case strings.HasPrefix(stmt, "#"):
			stmt = dropLine(stmt)
		case strings.HasPrefix(stmt, "--") &&
			(len(stmt) == 2 || stmt[2] == ' ' || stmt[2] == '\t' || stmt[2] == '\n' || stmt[2] == '\r'):
			stmt = dropLine(stmt)
		case strings.HasPrefix(stmt, "/*!"):
			// Versioned executable comment: strip `/*!` + the version digits,
			// splice the body back together with whatever follows the `*/`.
			end := strings.Index(stmt, "*/")
			if end < 0 {
				return stmt // unterminated; the caller's parser will refuse loudly
			}
			body := stmt[3:end]
			body = strings.TrimLeft(body, "0123456789")
			stmt = body + stmt[end+2:]
		case strings.HasPrefix(stmt, "/*"):
			end := strings.Index(stmt, "*/")
			if end < 0 {
				return "" // comment swallows the rest
			}
			stmt = stmt[end+2:]
		default:
			return stmt
		}
	}
}

// dropLine removes everything up to and including the next newline.
func dropLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return ""
}

// utf8BOM is the UTF-8 byte-order mark. mydumper itself never writes one,
// but a dump re-copied through a BOM-happy editor or shell can gain it; it
// is stripped (losslessly — a BOM is not data) at file start with a WARN,
// the flatfile engines' posture. A BOM anywhere ELSE in a file falls to
// [errNonSQLFragment] — left alone it would glue itself to the following
// statement, whose keyword would lex empty.
const utf8BOM = "\xef\xbb\xbf"

// fragmentHeadBytes bounds the quoted excerpt a non-SQL-fragment refusal
// carries: enough to recognise the offending bytes, not a log flood.
const fragmentHeadBytes = 40

// errNonSQLFragment classifies a statement whose keyword lexed empty. A
// pure comment/whitespace fragment is legitimate — nil, skip it. Anything
// else is content with no leading SQL keyword — a severed INSERT tail
// (torn dump), re-encoded bytes, an unterminated comment — and MUST refuse
// naming the bytes: the bare `case "":` skip this replaces dropped such
// statements silently, and the verify count path rode the same skip, so
// `verify --depth count` CONFIRMED the loss (audit 2026-07-15 CRITICAL-1).
func errNonSQLFragment(stmt string) error {
	frag := stripLeadingCommentsAndSpace(stmt)
	if frag == "" {
		return nil
	}
	head := frag
	if len(head) > fragmentHeadBytes {
		head = head[:fragmentHeadBytes] + "…"
	}
	return fmt.Errorf("statement fragment %q does not begin with a SQL keyword — severed INSERT "+
		"fragment or non-SQL content (torn or re-encoded dump); re-copy the dump", head)
}

// statementKeyword returns the statement's first word, uppercased —
// "CREATE", "SET", "INSERT", … — after comment stripping. Empty for a
// comment-only fragment.
func statementKeyword(stmt string) string {
	stmt = stripLeadingCommentsAndSpace(stmt)
	end := 0
	for end < len(stmt) {
		c := stmt[end]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			end++
			continue
		}
		break
	}
	return strings.ToUpper(stmt[:end])
}

// allowedSetNames is the charset allowlist for a data chunk's `SET NAMES`
// header. These are the spellings under which the dump's string bytes are
// UTF-8-compatible (binary = the table's own charset bytes, gated
// separately at schema parse; utf8/utf8mb3 are UTF-8 subsets). Any other
// declared charset would require transcoding, which this reader refuses
// rather than performing silently (ADR-0161 §5).
var allowedSetNames = map[string]bool{
	"binary":  true,
	"utf8":    true,
	"utf8mb3": true,
	"utf8mb4": true,
}

// checkSetStatement validates a SET statement found in a data chunk or
// schema file, and reports whether it carried a (UTC) TIME_ZONE
// assignment — the signal [RowReader] uses to WARN on dumps that never
// declare one. MySQL's SET is a COMMA-SEPARATED assignment list, so EVERY
// assignment is inspected (respecting quoted commas — sql_mode values
// contain them): a `SET SESSION sql_mode=…, SESSION time_zone='+05:30'`
// must hit the same gate as the single-assignment spelling
// (audit-2026-07-15 MED-D0-1 — the first cut checked only the first
// assignment, and the second one shifted every instant silently). `SET
// NAMES <charset>` outside the UTF-8-compatible allowlist and a TIME_ZONE
// assignment other than '+00:00'/UTC are LOUD refusals (mis-decoded text /
// shifted instants are the silent alternative); every other session
// variable a dump sets (sql_mode, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS,
// character_set_client, …) is irrelevant to a file read and is skipped.
//
// The TIME_ZONE match covers every spelling MySQL accepts — bare
// `TIME_ZONE`, scope-qualified `SESSION/GLOBAL/LOCAL TIME_ZONE`, and the
// system-variable forms `@@time_zone` / `@@session.time_zone` — in any
// position of the assignment list, so a non-UTC header cannot slip past
// the gate under an alternate spelling or behind another assignment.
func checkSetStatement(stmt string) (sawTimeZone bool, err error) {
	body := stripLeadingCommentsAndSpace(stmt)
	fields := strings.Fields(body)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") {
		return false, nil
	}
	for _, assign := range splitTopLevelCommas(strings.TrimSpace(body[len(fields[0]):])) {
		assign = strings.TrimSpace(assign)
		if assign == "" {
			continue
		}
		afields := strings.Fields(assign)
		if strings.EqualFold(afields[0], "NAMES") {
			if len(afields) < 2 {
				return sawTimeZone, fmt.Errorf("malformed SET NAMES statement %q", body)
			}
			cs := strings.ToLower(strings.Trim(afields[1], "'\"`;"))
			if !allowedSetNames[cs] {
				return sawTimeZone, fmt.Errorf("dump declares SET NAMES %s — only UTF-8-compatible dump charsets "+
					"(binary, utf8, utf8mb3, utf8mb4) are supported; re-dump with a UTF-8 connection "+
					"charset rather than have sluice transcode silently", cs)
			}
			continue
		}
		// SET TIME_ZONE='+00:00' (mydumper emits it unconditionally;
		// mysqldump under --tz-utc): only UTC is accepted — temporal
		// literals in the dump are interpreted as UTC, so a non-UTC session
		// offset would silently shift every instant.
		key, val, ok := strings.Cut(assign, "=")
		if !ok || !isTimeZoneVariable(key) {
			continue
		}
		tz := strings.Trim(strings.TrimSpace(val), "'\" ;")
		if tz != "+00:00" && !strings.EqualFold(tz, "UTC") {
			return sawTimeZone, fmt.Errorf("dump declares SET TIME_ZONE=%q — only '+00:00'/UTC is supported "+
				"(temporal values are interpreted as UTC; a non-UTC dump would shift instants silently)", tz)
		}
		sawTimeZone = true
	}
	return sawTimeZone, nil
}

// splitTopLevelCommas splits a SET statement's assignment list on commas
// that sit OUTSIDE string/identifier quoting and block comments — a
// sql_mode value like 'NO_ZERO_DATE,ANSI' must stay one assignment, and
// bytes inside quotes must never be mistaken for a further assignment.
// The quote states mirror [splitMySQLChunk] (backslash + doubled-quote
// escapes in strings, doubled-backtick in identifiers); line comments
// cannot appear here because the statement splitter already consumed
// statement boundaries and [stripLeadingCommentsAndSpace] the prefix.
func splitTopLevelCommas(s string) []string {
	var parts []string
	start, n := 0, len(s)
	for i := 0; i < n; i++ {
		switch s[i] {
		case '\'', '"':
			q := s[i]
			i++
			for i < n {
				switch s[i] {
				case '\\':
					i += 2
					continue
				case q:
					if i+1 < n && s[i+1] == q {
						i += 2
						continue
					}
				default:
					i++
					continue
				}
				break
			}
		case '`':
			i++
			for i < n {
				if s[i] == '`' {
					if i+1 < n && s[i+1] == '`' {
						i += 2
						continue
					}
					break
				}
				i++
			}
		case '/':
			if i+1 < n && s[i+1] == '*' {
				i += 2
				for i < n {
					if s[i] == '*' && i+1 < n && s[i+1] == '/' {
						break
					}
					i++
				}
				i++ // skip the '*'; the loop's i++ skips the '/'
			}
		case ',':
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}

// isTimeZoneVariable reports whether the left-hand side of a SET
// assignment names the session/global time_zone variable, in any of
// MySQL's accepted spellings: `TIME_ZONE`, `SESSION TIME_ZONE`,
// `GLOBAL TIME_ZONE`, `LOCAL TIME_ZONE`, `@@time_zone`,
// `@@session.time_zone`, `@@global.time_zone`, `@@local.time_zone`.
func isTimeZoneVariable(key string) bool {
	// The variable name is the LAST whitespace-separated token (scope
	// keywords precede it in the `SET SESSION time_zone` form).
	name := key
	if fs := strings.Fields(key); len(fs) > 0 {
		name = fs[len(fs)-1]
	}
	name = strings.TrimPrefix(name, "@@")
	lower := strings.ToLower(name)
	for _, scope := range []string{"session.", "global.", "local."} {
		if strings.HasPrefix(lower, scope) {
			lower = lower[len(scope):]
			break
		}
	}
	return lower == "time_zone"
}
