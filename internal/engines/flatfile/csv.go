// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// utf8BOM is the UTF-8 byte-order mark. Stripped (losslessly — a BOM is
// not data) at file start with a WARN; a UTF-16/32 BOM was already refused
// by the signature sniff.
const utf8BOM = "\xef\xbb\xbf"

// maxFieldBytes bounds a single CSV field (and, in ndjson.go, a single
// line). Named wart: a malformed quoted field (an unbalanced quote) would
// otherwise buffer the rest of the file into memory; real fields are
// nowhere near this, so a carry past the cap means the file is not
// delimiter-structured and refuses rather than buffering unbounded (the
// ADR-0161 §6 maxStatementBytes posture).
const maxFieldBytes = 64 << 20 // 64 MiB

// csvField is one lexed field: its decoded text and whether it was quoted.
// The quoted flag is LOAD-BEARING for the NULL contract — a quoted field is
// always data; only unquoted text can mean NULL — which is exactly why this
// package carries its own RFC 4180 lexer instead of encoding/csv (which
// collapses `a,,b` and `a,"",b` to the same record).
type csvField struct {
	text   string
	quoted bool
}

// csvLexer streams RFC 4180 records off a reader: quoted fields (with `""`
// escapes) may embed delimiters, quotes, and CR/LF; records end at LF or
// CRLF (both accepted); the final record needs no trailing newline. The
// lexer is STRICT — a bare quote inside an unquoted field, text after a
// closing quote, a lone CR outside quotes, or an unterminated quoted field
// refuses loudly naming the record and line — never a guessed parse.
type csvLexer struct {
	r     *bufio.Reader
	delim byte
	path  string

	line   int // 1-based physical line of the record being lexed
	record int // 1-based record ordinal (incl. a header record)

	// width is the established record width (set by the caller after the
	// first record; 0 = not yet known). It is LOAD-BEARING for the blank-line
	// rule (ADR-0163 F1): in a ONE-column file, a lone empty unquoted line is
	// a legitimate record — skipping it as "blank" would silently drop a NULL
	// row under --csv-null='' and silently bypass the ambiguity refusal
	// otherwise. Empty lines are treated as blank (skipped) only when the
	// width is >1 or not yet established.
	width int
}

func newCSVLexer(r *bufio.Reader, delim byte, path string) (*csvLexer, error) {
	// Strip a UTF-8 BOM at file start (lossless; Excel and PowerShell love
	// to prepend one). WARN so a byte-count-conscious operator isn't surprised.
	head, err := r.Peek(len(utf8BOM))
	if err == nil && string(head) == utf8BOM {
		_, _ = r.Discard(len(utf8BOM))
		slog.Warn("csv: stripped a UTF-8 byte-order mark at file start", slog.String("file", path))
	}
	return &csvLexer{r: r, delim: delim, path: path, line: 1}, nil
}

// errf builds a loud, located lexer refusal.
func (l *csvLexer) errf(format string, args ...any) error {
	loc := fmt.Sprintf("%q record %d (line %d): ", l.path, l.record, l.line)
	return errors.New(loc + fmt.Sprintf(format, args...))
}

// next lexes one record. It returns io.EOF (and no fields) at clean end of
// input. Fully-empty lines are skipped (the encoding/csv convention) EXCEPT
// when the established width is 1 — there an empty line is a one-empty-field
// record and flows through the NULL contract (ADR-0163 §5). A blank line
// before the width is established (before the first record) is skipped.
func (l *csvLexer) next() ([]csvField, error) {
	for {
		fields, hadContent, err := l.lexLine()
		if err != nil {
			return nil, err
		}
		if !hadContent {
			if fields == nil {
				return nil, io.EOF
			}
			continue // blank line: skip
		}
		return fields, nil
	}
}

// lexLine lexes one physical record. hadContent=false with non-nil fields
// means a blank line (skip); with nil fields, clean EOF.
func (l *csvLexer) lexLine() (fields []csvField, hadContent bool, err error) {
	// Detect clean EOF before starting a record.
	if _, err := l.r.Peek(1); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("%s: read %q: %w", "csv", l.path, err)
	}
	l.record++
	startLine := l.line

	var (
		buf      strings.Builder
		quoted   bool
		anyQuote bool // any field on this line was quoted → line is not blank
	)
	endField := func() error {
		f := csvField{text: buf.String(), quoted: quoted}
		if err := l.checkFieldText(f.text); err != nil {
			return err
		}
		fields = append(fields, f)
		buf.Reset()
		if quoted {
			anyQuote = true
		}
		quoted = false
		return nil
	}
	finishRecord := func() ([]csvField, bool, error) {
		if err := endField(); err != nil {
			return nil, false, err
		}
		// A truly blank line is exactly one unquoted empty field — but ONLY
		// when the established record width is not 1 (see the width field):
		// in a one-column file that shape IS a record, and it must reach the
		// NULL contract (declared '' → a NULL row; undeclared → the
		// ambiguity refusal), never a silent skip.
		blank := len(fields) == 1 && !anyQuote && fields[0].text == "" && l.width != 1
		if blank {
			l.record-- // blank lines don't consume a record ordinal
		}
		return fields, !blank, nil
	}
	// endRecord is finishRecord at an explicit newline: the record's fields
	// finish on the CURRENT line (errf must name it), then the cursor moves
	// to the next physical line for whatever follows.
	endRecord := func() ([]csvField, bool, error) {
		f, had, err := finishRecord()
		l.line++
		return f, had, err
	}

	for {
		c, rerr := l.r.ReadByte()
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				return nil, false, fmt.Errorf("%s: read %q: %w", "csv", l.path, rerr)
			}
			// EOF terminates the final record (no trailing newline).
			return finishRecord()
		}
		switch c {
		case '"':
			if buf.Len() != 0 || quoted {
				l.line = startLine
				return nil, false, l.errf("bare quote inside an unquoted field (RFC 4180 requires the whole field quoted)")
			}
			text, lines, qerr := l.lexQuoted()
			if qerr != nil {
				return nil, false, qerr
			}
			l.line += lines
			buf.WriteString(text)
			quoted = true
			// The very next byte must be a delimiter, CR LF, LF, or EOF.
			nb, nerr := l.r.ReadByte()
			if nerr != nil {
				if errors.Is(nerr, io.EOF) {
					return finishRecord()
				}
				return nil, false, fmt.Errorf("%s: read %q: %w", "csv", l.path, nerr)
			}
			switch nb {
			case l.delim:
				if err := endField(); err != nil {
					return nil, false, err
				}
			case '\n':
				return endRecord()
			case '\r':
				if err := l.expectLF(); err != nil {
					return nil, false, err
				}
				return endRecord()
			default:
				return nil, false, l.errf("unexpected %q after a closing quote (expected the delimiter or end of record)", string(nb))
			}
		case l.delim:
			if err := endField(); err != nil {
				return nil, false, err
			}
		case '\n':
			return endRecord()
		case '\r':
			if err := l.expectLF(); err != nil {
				return nil, false, err
			}
			return endRecord()
		case 0x00:
			return nil, false, l.errf("field %d %s", len(fields)+1, errNULByte.Error())
		default:
			buf.WriteByte(c)
			if buf.Len() > maxFieldBytes {
				return nil, false, l.errf("field %d exceeds %d bytes — not a delimiter-structured file?", len(fields)+1, maxFieldBytes)
			}
		}
	}
}

// lexQuoted consumes a quoted field's body after the opening quote, through
// the closing quote (handling `""` escapes). Returns the decoded text and
// how many newlines the field embedded (for line accounting).
func (l *csvLexer) lexQuoted() (text string, embeddedLines int, err error) {
	var (
		buf   strings.Builder
		lines int
	)
	for {
		c, err := l.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", 0, l.errf("unterminated quoted field (EOF before the closing quote)")
			}
			return "", 0, fmt.Errorf("%s: read %q: %w", "csv", l.path, err)
		}
		switch c {
		case '"':
			nb, nerr := l.r.Peek(1)
			if nerr == nil && nb[0] == '"' {
				_, _ = l.r.Discard(1)
				buf.WriteByte('"') // "" → one literal quote
				continue
			}
			return buf.String(), lines, nil
		case '\n':
			lines++
			buf.WriteByte(c)
		case 0x00:
			return "", 0, l.errf("quoted field %s", errNULByte.Error())
		default:
			buf.WriteByte(c)
		}
		if buf.Len() > maxFieldBytes {
			return "", 0, l.errf("quoted field exceeds %d bytes — unbalanced quote?", maxFieldBytes)
		}
	}
}

// expectLF consumes the LF of a CRLF pair; a lone CR outside quotes is
// malformed (RFC 4180 permits CR only as part of CRLF or inside quotes).
func (l *csvLexer) expectLF() error {
	nb, err := l.r.ReadByte()
	if err != nil || nb != '\n' {
		return l.errf("lone CR outside a quoted field (expected CRLF)")
	}
	return nil
}

// checkFieldText enforces the UTF-8-only encoding contract per field.
// (NUL bytes were already refused byte-wise — they are valid UTF-8, the
// UTF-16-without-BOM trap.)
func (l *csvLexer) checkFieldText(s string) error {
	if !utf8.ValidString(s) {
		return l.errf("field is not valid UTF-8 — sluice reads UTF-8 only; transcode the file first (e.g. `iconv -t UTF-8`)")
	}
	return nil
}

// stageCSV lexes the whole file and stages it: header (or col1..colN)
// naming, per-record arity enforcement, and the NULL contract per field.
func (e Engine) stageCSV(ctx context.Context, r *bufio.Reader, path string, st *stager) error {
	lex, err := newCSVLexer(r, e.delimiter(), path)
	if err != nil {
		return err
	}

	first, err := lex.next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: %q has no records (no columns can be derived)", e.Name(), path)
		}
		return err
	}

	var cols []string
	if e.opts.Header {
		cols, err = headerNames(first, path)
		if err != nil {
			return err
		}
	} else {
		cols = make([]string, len(first))
		for i := range first {
			cols[i] = fmt.Sprintf("col%d", i+1)
		}
	}
	// Establish the record width on the lexer: from here a lone empty line in
	// a ONE-column file is a record (routed through the NULL contract), not a
	// skippable blank (ADR-0163 F1).
	lex.width = len(cols)
	if err := st.createTable(ctx, cols); err != nil {
		return err
	}

	insert := func(fields []csvField) error {
		if len(fields) != len(cols) {
			return lex.errf("has %d fields; want %d (as established by the first record)", len(fields), len(cols))
		}
		vals := make([]any, len(fields))
		for i, f := range fields {
			v, verr := e.fieldValue(f)
			if verr != nil {
				return fmt.Errorf("%q record %d, column %q: %w", path, lex.record, cols[i], verr)
			}
			vals[i] = v
		}
		return st.insert(ctx, vals)
	}

	if !e.opts.Header {
		if err := insert(first); err != nil {
			return err
		}
	}
	for {
		fields, err := lex.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := insert(fields); err != nil {
			return err
		}
	}
}

// fieldValue applies the NULL contract (ADR-0163): a QUOTED field is always
// data; an UNQUOTED field equal to the declared NULL representation is SQL
// NULL; an unquoted EMPTY field with NO declaration is ambiguous and refused
// (SLUICE-E-CSV-NULL-AMBIGUOUS) — RFC 4180 has no NULL, and guessing is the
// #1 CSV silent-loss class.
func (e Engine) fieldValue(f csvField) (any, error) {
	if f.quoted {
		return f.text, nil
	}
	if e.opts.NullRepr != nil {
		if f.text == *e.opts.NullRepr {
			return nil, nil
		}
		return f.text, nil
	}
	if f.text == "" {
		return nil, sluicecode.Wrap(sluicecode.CodeCSVNullAmbiguous,
			"declare the file's NULL convention with --csv-null",
			errors.New("unquoted empty field is ambiguous (SQL NULL or empty string?) — declare the file's "+
				"convention: --csv-null='' (empty unquoted field means NULL; a quoted \"\" stays the empty string), "+
				"--csv-null='\\N' / --csv-null=NULL (that literal means NULL; empty fields are empty strings)"))
	}
	return f.text, nil
}

// headerNames validates the header record: names must be non-empty and
// unique CASE-INSENSITIVELY — staged SQLite column names are
// case-insensitive, so `a` and `A` cannot both be held (refused with a
// named message rather than the staging database's raw duplicate-column
// error). Staging quotes the names, so any other character is fine.
func headerNames(fields []csvField, path string) ([]string, error) {
	cols := make([]string, len(fields))
	seen := make(map[string]string, len(fields))
	for i, f := range fields {
		name := f.text
		if name == "" {
			return nil, fmt.Errorf("%q: header column %d is empty — every column needs a name (or use --csv-no-header)", path, i+1)
		}
		if prior, dup := seen[strings.ToLower(name)]; dup {
			if prior == name {
				return nil, fmt.Errorf("%q: duplicate header column %q — column names must be unique", path, name)
			}
			return nil, fmt.Errorf("%q: header columns %q and %q collide case-insensitively — staged SQLite column "+
				"names are case-insensitive, so both cannot be held; rename one", path, prior, name)
		}
		seen[strings.ToLower(name)] = name
		cols[i] = name
	}
	return cols, nil
}
