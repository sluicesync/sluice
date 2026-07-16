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
// over it (the sqlite dump.go carry pattern — a statement, string, or
// comment may span read-block boundaries and is resolved by re-scanning
// the carried fragment with the next block), and the comment/versioned-
// comment stripper the statement classifiers use.
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

// statementStream yields complete top-level statements from r, one at a
// time, never holding more than one read block plus one partial statement
// in memory. Next returns io.EOF after the final statement.
type statementStream struct {
	r       io.Reader
	block   []byte
	carry   string
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
			if rest := strings.TrimSpace(s.carry); rest != "" {
				s.carry = ""
				return rest, nil
			}
			return "", io.EOF
		}
		nr, rerr := s.r.Read(s.block)
		if nr > 0 {
			stmts, rest := splitMySQLChunk(s.carry + string(s.block[:nr]))
			s.carry = rest
			s.pending = stmts
			if len(s.carry) > maxStatementBytes {
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
// schema file, and reports whether it was a (UTC) TIME_ZONE header — the
// signal [RowReader] uses to WARN on dumps that never declare one. `SET
// NAMES <charset>` outside the UTF-8-compatible allowlist and a TIME_ZONE
// assignment other than '+00:00'/UTC are LOUD refusals (mis-decoded text /
// shifted instants are the silent alternative); every other session
// variable a dump sets (sql_mode, FOREIGN_KEY_CHECKS, UNIQUE_CHECKS,
// character_set_client, …) is irrelevant to a file read and is skipped.
//
// The TIME_ZONE match covers every spelling MySQL accepts — bare
// `TIME_ZONE`, scope-qualified `SESSION/GLOBAL/LOCAL TIME_ZONE`, and the
// system-variable forms `@@time_zone` / `@@session.time_zone` — so a
// non-UTC header cannot slip past the gate under an alternate spelling.
func checkSetStatement(stmt string) (sawTimeZone bool, err error) {
	body := stripLeadingCommentsAndSpace(stmt)
	fields := strings.Fields(body)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SET") {
		return false, nil
	}
	if strings.EqualFold(fields[1], "NAMES") {
		if len(fields) < 3 {
			return false, fmt.Errorf("malformed SET NAMES statement %q", body)
		}
		cs := strings.ToLower(strings.Trim(fields[2], "'\"`;"))
		if !allowedSetNames[cs] {
			return false, fmt.Errorf("dump declares SET NAMES %s — only UTF-8-compatible dump charsets "+
				"(binary, utf8, utf8mb3, utf8mb4) are supported; re-dump with a UTF-8 connection "+
				"charset rather than have sluice transcode silently", cs)
		}
		return false, nil
	}
	// SET TIME_ZONE='+00:00' (mydumper emits it unconditionally; mysqldump
	// under --tz-utc): only UTC is accepted — temporal literals in the dump
	// are interpreted as UTC, so a non-UTC session offset would silently
	// shift every instant.
	rest := strings.TrimSpace(body[len(fields[0]):])
	key, val, ok := strings.Cut(rest, "=")
	if !ok || !isTimeZoneVariable(key) {
		return false, nil
	}
	tz := strings.Trim(strings.TrimSpace(val), "'\" ;")
	if tz != "+00:00" && !strings.EqualFold(tz, "UTC") {
		return false, fmt.Errorf("dump declares SET TIME_ZONE=%q — only '+00:00'/UTC is supported "+
			"(temporal values are interpreted as UTC; a non-UTC dump would shift instants silently)", tz)
	}
	return true, nil
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
