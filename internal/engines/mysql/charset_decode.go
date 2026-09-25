// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Change-stream values of a character column declared in a non-UTF-8
// character set (GC-37 (j)).
//
// # What was wrong, measured
//
// The bulk-copy reader asks the server for text in the connection's utf8mb4,
// so MySQL converts every column to UTF-8 before sluice sees it. The two
// change-stream transports do not: the binlog row image (MySQL 8.0 and
// MariaDB 11.4) and vttablet's VStream row event both carry a character
// column's value as the bytes STORED in the column's own character set.
// Every decoder handed those bytes on as a Go string, so a latin1 `é`
// (0xE9) became the one-byte, invalid-UTF-8 string "\xE9", a gbk `中`
// became "\xD6\xD0", and a utf16 `é` became "\x00\xE9" — measured for every
// non-UTF-8 charset MySQL ships, on all three lanes, against the server's
// own `CONVERT(col USING utf8mb4)`.
//
// What that did downstream depended on the bytes:
//   - Bytes that happen to form valid UTF-8 carried a different, valid
//     character at exit 0: latin1 `Ã©` (0xC3A9) landed as `é`, cp1251
//     `Г©` as `é`, gbk `茅` as `é`, and every swe7 value was wrong (swe7
//     is not ASCII-compatible: 0x7B is `ä`). A row keyed on such a value
//     was not even found by a later UPDATE or DELETE.
//   - Every other non-ASCII value was invalid UTF-8, which a target
//     refuses loudly — but `backup stream` and `backup incremental` JSON-
//     encode each change, and the encoder writes an invalid byte as
//     U+FFFD: the chain silently held `�` for every such value.
//
// # What the change-stream decoders do now
//
// A value of a string column whose character set is not UTF-8 is converted
// to UTF-8 by the column's declared charset before it reaches the IR,
// exactly as the server itself converts it for the bulk-copy reader. The
// conversion uses Vitess's MySQL character-set tables (every 8-bit charset,
// latin1 — which in MySQL is cp1252 with 0x81/0x8D/0x8F/0x90/0x9D mapped to
// the C1 controls — the Japanese, Korean and GB charsets, and the
// UTF-16/UTF-32/UCS-2 family), golang.org/x/text for gbk, and a sluice-owned
// table for tis620 (see [ownCharsets]). Every table is graded against the
// server's own conversion over its whole code space
// (TestColumnCharsetDecode_MatchesTheServer_*). A value the table cannot
// decode — and any non-ASCII big5 value, whose MySQL table no library
// matches — REFUSES loudly (CHARSET-NOT-DECODABLE) naming the table, column
// and charset, never a guess.

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/text/encoding/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations"
	vtcharset "vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"

	"sluicesync.dev/sluice/internal/ir"
)

// charsetNotDecodableMarker is the grep-stable marker a change-stream value
// that cannot be converted to UTF-8 faithfully ends the stream with.
const charsetNotDecodableMarker = "CHARSET-NOT-DECODABLE"

// errCharsetNotDecodable is wrapped by every refusal of this file.
var errCharsetNotDecodable = errors.New(charsetNotDecodableMarker)

// columnCharset converts one column's stored bytes to UTF-8. The zero value
// (and a nil pointer) is a UTF-8-family column: the bytes ARE the value.
type columnCharset struct {
	name string
	// vt is Vitess's MySQL charset table, when it has one.
	vt vtcharset.Charset
	// fn is a sluice-owned table for a charset neither library matches
	// MySQL on (tis620).
	fn func([]byte) ([]byte, error)
	// unsupported is a non-UTF-8 charset sluice has no faithful table for.
	unsupported bool
}

// passthroughCharset reports whether a MySQL character set's stored bytes
// are already the value's UTF-8. ascii is a strict subset (MySQL refuses a
// byte ≥ 0x80 in it); binary columns are bytes, not text, and never reach
// here as a string. An empty name — a column read without its charset —
// keeps the bytes as they are, which is what every path did before.
func passthroughCharset(name string) bool {
	switch name {
	case "", "utf8mb4", "utf8mb3", "utf8", "ascii", "binary":
		return true
	}
	return false
}

// ownCharsets are the MySQL charsets Vitess's collation environment does not
// resolve, each mapped to a table graded over its whole code space against a
// real server (TestColumnCharsetDecode_MatchesTheServer_*), which is what
// makes the mapping a fact rather than an assumption:
//   - gbk: golang.org/x/text's GBK decoder matches MySQL exactly.
//   - gb18030: Vitess ships the table; its 8.0.30 environment just does not
//     list the charset by name.
//   - tis620: neither library matches MySQL (x/text's Windows-874 maps
//     0x80 to U+20AC, MySQL to U+0080), so the table is sluice's own.
//
// big5 is deliberately ABSENT: x/text's Big5 is the WHATWG table, which
// disagrees with MySQL's in 267 measured places, so a big5 value that is not
// pure ASCII refuses (see [asciiOnlyCharsets]).
var ownCharsets = map[string]func([]byte) ([]byte, error){
	"gbk": func(b []byte) ([]byte, error) {
		out, err := simplifiedchinese.GBK.NewDecoder().Bytes(b)
		// x/text substitutes U+FFFD for a sequence it cannot map rather
		// than failing; gbk has no U+FFFD of its own, so one in the output
		// is a failed decode, not a value.
		if err == nil && bytes.ContainsRune(out, utf8.RuneError) {
			err = errors.New("unmappable byte sequence")
		}
		return out, err
	},
	"gb18030": func(b []byte) ([]byte, error) {
		return vtcharset.Convert(nil, vtcharset.Charset_utf8mb4{}, b, vtcharset.Charset_gb18030{})
	},
	"tis620": decodeTIS620,
}

// asciiOnlyCharsets are ASCII-compatible charsets sluice has no faithful
// table for: a value made only of bytes below 0x80 IS its UTF-8 (every one
// of them maps 0x00–0x7F to ASCII), anything else refuses.
var asciiOnlyCharsets = map[string]bool{"big5": true, vstreamUnknownCharset: true}

// decodeTIS620 is MySQL's tis620 (ctype-tis620), as measured: ASCII below
// 0x80, the C1 controls for 0x80–0x9F as their own code points, and the
// Thai block for 0xA1–0xFB at U+0E01 + (b − 0xA1). MySQL itself converts
// 0xA0 and the TIS-620 holes 0xDB–0xDE to U+FFFD — a replacement, not a
// value — so those, like the undefined 0xFC–0xFF, refuse rather than
// reproduce the server's own loss.
func decodeTIS620(b []byte) ([]byte, error) {
	out := make([]byte, 0, len(b)*3)
	for _, c := range b {
		switch {
		case c < 0xA0:
			out = utf8.AppendRune(out, rune(c))
		case c >= 0xA1 && c <= 0xDA, c >= 0xDF && c <= 0xFB:
			out = utf8.AppendRune(out, 0x0E01+rune(c-0xA1))
		default:
			return nil, fmt.Errorf("byte 0x%02X is not a tis620 character", c)
		}
	}
	return out, nil
}

var (
	charsetCacheMu sync.Mutex
	charsetCache   = map[string]*columnCharset{}
)

// lookupColumnCharset returns the converter for a MySQL character-set name
// (information_schema's CHARACTER_SET_NAME), or nil for a UTF-8-family one.
func lookupColumnCharset(name string) *columnCharset {
	name = strings.ToLower(strings.TrimSpace(name))
	if passthroughCharset(name) {
		return nil
	}
	charsetCacheMu.Lock()
	defer charsetCacheMu.Unlock()
	if cc, ok := charsetCache[name]; ok {
		return cc
	}
	cc := &columnCharset{name: name}
	if fn, ok := ownCharsets[name]; ok {
		cc.fn = fn
	} else if id := collationEnv().DefaultCollationForCharset(name); id != collations.Unknown {
		if coll := colldata.Lookup(id); coll != nil {
			cc.vt = coll.Charset()
		}
	}
	if cc.vt == nil && cc.fn == nil {
		cc.unsupported = true
	}
	charsetCache[name] = cc
	return cc
}

// lookupCollationCharset is [lookupColumnCharset] for a collation ID — what
// a VStream field carries.
//
// An ID that does not name a charset is NOT "no charset". vttablet sends
// collation 0 on a character field whose collation its own environment
// does not know — MEASURED on vttestserver for gbk, big5, tis620 and
// gb18030, whose cells then carried their raw bytes — so 0 means "a charset
// sluice cannot name". Every charset that arrives that way is
// ASCII-compatible, so a pure-ASCII cell is exact and anything else refuses
// ([vstreamUnknownCharset]); a non-zero ID sluice cannot name refuses too.
func lookupCollationCharset(id uint32) *columnCharset {
	if id == 0 {
		return lookupColumnCharset(vstreamUnknownCharset)
	}
	name := collationEnv().LookupCharsetName(collations.ID(id))
	if name == "" {
		return &columnCharset{name: fmt.Sprintf("<collation id %d>", id), unsupported: true}
	}
	return lookupColumnCharset(name)
}

// vstreamUnknownCharset names the charset of a VStream character field that
// carries collation 0 (see [lookupCollationCharset]).
const vstreamUnknownCharset = "<a charset vttablet does not name (gbk, big5, tis620, gb18030, …)>"

// decode converts b, stored in cc's charset, to its UTF-8 string. table and
// column name the value in a refusal.
func (cc *columnCharset) decode(b []byte, table, column string) (string, error) {
	if cc == nil {
		return string(b), nil
	}
	where := fmt.Sprintf("table %q column %q", table, column)
	if table == "" {
		where = fmt.Sprintf("column %q", column) // the VStream caller wraps the table
	}
	if cc.unsupported {
		if asciiOnlyCharsets[cc.name] && isASCII(b) {
			return string(b), nil
		}
		return "", fmt.Errorf("%w: %s is declared CHARACTER SET %s, which sluice has no faithful conversion table for, "+
			"so its non-ASCII change-stream value cannot be carried as text; the bulk copy converts it on the server, the change stream cannot. "+
			"Convert the column to utf8mb4 on the source (ALTER TABLE … MODIFY … CHARACTER SET utf8mb4), or exclude the table",
			errCharsetNotDecodable, where, cc.name)
	}
	var (
		out []byte
		err error
	)
	if cc.fn != nil {
		out, err = cc.fn(b)
	} else {
		out, err = vtcharset.Convert(nil, vtcharset.Charset_utf8mb4{}, b, cc.vt)
	}
	if err != nil || !utf8.Valid(out) {
		return "", fmt.Errorf("%w: %s (CHARACTER SET %s): the change-stream value 0x%X does not decode in its declared character set "+
			"(%v), so it cannot be carried as text without guessing. Check the stored value on the source "+
			"(SELECT HEX(%s)), or convert the column to utf8mb4", errCharsetNotDecodable, where, cc.name, b, err, column)
	}
	return string(out), nil
}

// binlogStringBytes returns the stored bytes of a binlog row-image cell for
// a character column: go-mysql hands CHAR/VARCHAR/TEXT back as a string or a
// []byte depending on the column's wire type. Anything else (NULL, or an
// integer an override decodes as text) is not charset-encoded text.
func binlogStringBytes(raw any) ([]byte, bool) {
	switch v := raw.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	}
	return nil, false
}

// vstreamCharsetError is the sentinel [decodeVStreamCell] returns as a cell
// VALUE for a character cell that does not decode in its column's charset;
// [decodeVStreamRow] turns it into a stream error naming the table.
type vstreamCharsetError struct{ cause error }

// isASCII reports whether every byte of b is below 0x80.
func isASCII(b []byte) bool {
	for _, c := range b {
		if c >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// stringColumnCharset returns the converter for an IR string type's
// declared charset, or nil for any other type or a UTF-8-family charset.
func stringColumnCharset(t ir.Type) *columnCharset {
	switch v := t.(type) {
	case ir.Char:
		return lookupColumnCharset(v.Charset)
	case ir.Varchar:
		return lookupColumnCharset(v.Charset)
	case ir.Text:
		return lookupColumnCharset(v.Charset)
	}
	return nil
}
