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
//
// Which charset: on the binlog, the one the value was WRITTEN in, from the
// TABLE_MAP ([binlogColumnCharsets]), so a replay across a charset DDL
// decodes old bytes by the old charset. Where the source records none
// (MariaDB NO_LOG), and on VStream — whose field collation vttablet reports
// as CURRENT for a replayed row (MEASURED) — by the catalog, with the
// charset-DDL guard (charset_ddl_guard.go) refusing a replay when it reaches
// the DDL. A VStream ENUM/SET cell is resolved against the
// column's labels, because vttablet sends it in two forms
// ([resolveVStreamEnumSetText]).

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"
	"golang.org/x/text/encoding/simplifiedchinese"
	"vitess.io/vitess/go/mysql/collations"
	vtcharset "vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/vt/proto/query"

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
// here as a string. An empty name keeps the bytes as they are (review F4,
// documented rather than refused): the production schema loaders always
// read CHARACTER_SET_NAME for a character column, so an empty one comes only
// from hand-built schemas — and on the binlog the TABLE_MAP's charset
// ([binlogColumnCharsets]) supersedes the catalog's whenever it is written,
// empty or not.
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
			"so its non-ASCII change-stream value cannot be carried as text; the bulk copy converts it on the server, the change stream cannot. %s",
			errCharsetNotDecodable, where, cc.name, charsetRefusalRemedy)
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
		// The value itself is deliberately NOT in the message: it is row
		// data, and a refusal lands in logs. The converter's own error is
		// dropped for the same reason (Vitess's names the input bytes).
		return "", fmt.Errorf("%w: %s (CHARACTER SET %s): a change-stream value of %d bytes does not decode in its declared character set, "+
			"so it cannot be carried as text without guessing (inspect it on the source with SELECT HEX(%s)). %s",
			errCharsetNotDecodable, where, cc.name, len(b), column, charsetRefusalRemedy)
	}
	return string(out), nil
}

// charsetRefusalRemedy is the remedy every [errCharsetNotDecodable] refusal
// ends with. It is convert-THEN-RE-SNAPSHOT, never convert-and-resume: a
// resume replays history written in the OLD charset — a big5 value that
// refuses here refuses again on the replay ([binlogColumnCharsets] decodes
// by the recorded big5), and a source without TABLE_MAP charsets, or
// VStream, would decode it by the new charset until the charset-DDL guard
// refuses at the DDL.
const charsetRefusalRemedy = "To carry it, convert the column to utf8mb4 on the source (ALTER TABLE … MODIFY … CHARACTER SET utf8mb4) " +
	"and re-snapshot the table (sync --restart-from-scratch), or exclude the table; resuming after the ALTER replays history written in the old charset"

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

// mysqlUnlistedCollations are MySQL collation IDs (SHOW COLLATION; fixed
// across 5.7 and 8.x) for the charsets Vitess's collation environment does
// not name, so a TABLE_MAP collation in one of them still resolves.
var mysqlUnlistedCollations = map[uint64]string{
	1: "big5", 84: "big5", // big5_chinese_ci, big5_bin
	28: "gbk", 87: "gbk", // gbk_chinese_ci, gbk_bin
	18: "tis620", 89: "tis620", // tis620_thai_ci, tis620_bin
	248: "gb18030", 249: "gb18030", 250: "gb18030", // gb18030_chinese_ci, _bin, _unicode_520_ci
}

// charsetNameForCollationID names the charset of a MySQL collation ID, or
// "" when sluice cannot name it.
func charsetNameForCollationID(id uint64) string {
	if name := collationEnv().LookupCharsetName(collations.ID(id)); name != "" {
		return name
	}
	return mysqlUnlistedCollations[id]
}

// loadServerCollations reads the source server's collation-ID → charset
// table: MariaDB's COLLATION_CHARACTER_SET_APPLICABILITY (the only place its
// per-charset uca1400 IDs are listed — its COLLATIONS rows for them have a
// NULL ID), else information_schema.COLLATIONS (MySQL, where every
// collation has an ID). A failure returns nil, and naming falls back to the
// static tables ([charsetNameForCollationID]) — an ID they do not name then
// refuses, and the load is retried ([CDCReader.collationCharsets]).
func loadServerCollations(ctx context.Context, db *sql.DB) map[uint64]string {
	if db == nil {
		return nil
	}
	for _, q := range []string{
		`SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY WHERE ID IS NOT NULL`,
		`SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATIONS WHERE ID IS NOT NULL`,
	} {
		if m := queryCollationIDs(ctx, db, q); len(m) > 0 {
			return m
		}
	}
	return nil
}

func queryCollationIDs(ctx context.Context, db *sql.DB, q string) map[uint64]string {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	m := map[uint64]string{}
	for rows.Next() {
		var id uint64
		var cs string
		if err := rows.Scan(&id, &cs); err != nil {
			return nil
		}
		m[id] = cs
	}
	if rows.Err() != nil {
		return nil
	}
	return m
}

// collationCharsets is the reader's naming function for TABLE_MAP collation
// IDs: the server's own table first, the static tables second.
//
// A failed load is NOT cached (review item 5): it is retried, at most once
// per [serverCollationsRetry], so a transient failure at startup does not
// leave a MariaDB MINIMAL source's uca1400 collation IDs unnamed — and every
// rows event carrying one refused — for the reader's lifetime. Only the reader goroutine calls this, so the fields need no lock.
func (r *CDCReader) collationCharsets(ctx context.Context) func(uint64) string {
	if r.serverCollations == nil && time.Since(r.serverCollationsTried) >= serverCollationsRetry {
		r.serverCollationsTried = time.Now()
		lctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		r.serverCollations = loadServerCollations(lctx, r.db)
		cancel()
	}
	server := r.serverCollations
	return func(id uint64) string {
		if cs := server[id]; cs != "" {
			return cs
		}
		return charsetNameForCollationID(id)
	}
}

// serverCollationsRetry bounds how often a failed collation-table load is
// retried, so a persistent failure costs one query per interval, not one
// per rows event.
const serverCollationsRetry = 30 * time.Second

// canonicalCharsetName folds the two spellings of the same charset that
// the catalog and the collation environment can disagree on.
func canonicalCharsetName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "utf8" {
		return "utf8mb3"
	}
	return name
}

// binlogColumnCharsets resolves, per column of one rows event, the charset
// its character values were WRITTEN in — which is what the bytes are in,
// and which the catalog may no longer say.
//
// The catalog ([tableSchema]) answers as of NOW; a resume replaying history
// recorded before a charset DDL (a warm resume after downtime, a lagging
// stream, a cold-start handoff whose copy spanned the DDL) would otherwise
// decode old bytes by the new charset — MEASURED: utf8mb4 'é' decoded as
// latin1 'Ã©', latin1 '€' as cp1251 'Ђ', both at exit 0. The TABLE_MAP that
// precedes every rows event carries each character column's collation
// (MySQL: under the default binlog_row_metadata=MINIMAL — measured), so:
//
//   - TABLE_MAP names a charset sluice can name: decode by it, whatever the
//     catalog now says. The bytes are in that charset, so the decode is
//     value-exact across a charset DDL (MEASURED: the three replay cases
//     above decode to the inserted values on MySQL 8 and MariaDB MINIMAL).
//   - TABLE_MAP names a collation ID neither the source's collation table
//     nor sluice's static ones name: refuse with the CDC-4 replay-mismatch
//     class ([errCDCSchemaReplayMismatch]) — decoding by the catalog would
//     be a guess exactly where the server said something else.
//   - TABLE_MAP carries no collations at all (MariaDB's default
//     binlog_row_metadata=NO_LOG): return nil, decode by the catalog, and
//     let the charset-DDL guard ([CDCReader.binlogCharsetDDLGuard]) catch a
//     replay when the stream reaches the DDL. Setting MINIMAL on the source
//     records charsets only for history written AFTER the change; binlogs
//     already written stay unrecorded.
//
// A nil return means "decode by the catalog" for every column.
func binlogColumnCharsets(tbl *tableSchema, tm *replication.TableMapEvent, nameOf func(uint64) string) ([]*columnCharset, error) {
	if nameOf == nil {
		nameOf = charsetNameForCollationID
	}
	if tm == nil {
		return nil, nil
	}
	collationsByCol := tm.CollationMap()
	if len(collationsByCol) == 0 {
		return nil, nil
	}
	out := make([]*columnCharset, len(tbl.Columns))
	for i, col := range tbl.Columns {
		catalog, isString := stringTypeCharset(col.Type)
		if !isString {
			continue
		}
		id, ok := collationsByCol[i]
		if !ok {
			out[i] = lookupColumnCharset(catalog)
			continue
		}
		recorded := canonicalCharsetName(nameOf(id))
		if recorded == "" {
			// The server stated the charset these bytes were written in and
			// sluice cannot name it (neither the source's collation table nor
			// the static ones list the ID): decoding by the catalog would be a
			// guess exactly where the evidence says it may be wrong.
			return nil, errCDCSchemaReplayMismatch(tbl, fmt.Sprintf(
				"column %q was recorded under collation ID %d, which neither the source's collation table nor sluice's names, "+
					"so the charset its bytes were written in is unknown", col.Name, id,
			))
		}
		// The WRITTEN charset decides, even where the catalog now declares
		// another (a replay across an ALTER … CHARACTER SET): the bytes are in
		// the charset the TABLE_MAP names, so decoding by it is value-exact —
		// including after the CONVERT TO utf8mb4 the CHARSET-NOT-DECODABLE
		// remedy recommends, which then needs no re-snapshot.
		out[i] = lookupColumnCharset(recorded)
	}
	return out, nil
}

// stringTypeCharset returns the declared charset of an IR string type, and
// whether t is one.
func stringTypeCharset(t ir.Type) (string, bool) {
	switch v := t.(type) {
	case ir.Char:
		return v.Charset, true
	case ir.Varchar:
		return v.Charset, true
	case ir.Text:
		return v.Charset, true
	}
	return "", false
}

// resolveVStreamEnumSetText decodes a VStream ENUM or SET cell of a
// non-UTF-8 column (review F2, MEASURED on vttestserver).
//
// vttablet hands the SAME column over in two forms depending on which of its
// streamers produced the row: the vstreamer (CDC, and the catch-up that runs
// between COPY chunks) renders the label as UTF-8 text from its catalog,
// while the rowstreamer (COPY) reads under `set names binary` and hands over
// the STORED bytes in the column's charset — latin1 'é' as 0xE9. The FIELD
// event is identical in both (same collation, same flags), and a catch-up
// row arrives on the same event stream as a COPY row, so the phase cannot
// be told from the caller. What does tell them apart is the field's own
// label list (column_type, UTF-8): the true value is a member of it. So the
// cell is read both ways and the reading that IS a member is taken. When
// both readings are members and differ — labels that are each other's
// mojibake, like latin1 'é' and 'Ã©' with the cell bytes C3A9 — the value is
// genuinely ambiguous and refuses (CHARSET-NOT-DECODABLE) rather than pick.
// A cell neither reading makes a member of refuses too.
//
// ok=false means the column is not a non-UTF-8 ENUM/SET (or its labels do
// not parse) and the caller keeps its existing handling.
func resolveVStreamEnumSetText(field *query.Field, raw []byte) (value string, ok bool, err error) {
	cc := lookupCollationCharset(field.GetCharset())
	if cc == nil {
		return "", false, nil
	}
	kind := "enum"
	if field.GetType() == query.Type_SET {
		kind = "set"
	}
	// No column_type means vttablet sent no label metadata (only hand-built
	// fields in unit tests lack it): nothing to resolve against, so the
	// caller keeps its UTF-8 reading. A column_type that is present but does
	// not parse is a label list sluice cannot read, and refuses.
	if field.GetColumnType() == "" {
		return "", false, nil
	}
	labels, perr := parseEnumOrSet(field.GetColumnType(), kind)
	if perr != nil || len(labels) == 0 {
		return "", true, fmt.Errorf("%w: column %q (CHARACTER SET %s, %s): its label list %q does not parse, so a value cannot be resolved against it",
			errCharsetNotDecodable, field.GetName(), cc.name, strings.ToUpper(kind), field.GetColumnType())
	}
	member := make(map[string]bool, len(labels))
	for _, l := range labels {
		member[l] = true
	}
	isMember := func(s string) bool {
		if kind == "enum" {
			// '' is ENUM index 0, the error value a non-strict INSERT IGNORE
			// of an invalid label stores; it is a value the column can hold
			// though no label spells it (review item 2 — refusing it was a
			// loud regression from v0.156.2, which carried it).
			return s == "" || member[s]
		}
		if s == "" {
			return true
		}
		for _, part := range strings.Split(s, ",") {
			if !member[part] {
				return false
			}
		}
		return true
	}
	asUTF8 := string(raw)
	cdcForm := utf8.ValidString(asUTF8) && isMember(asUTF8)
	copyText, derr := cc.decode(raw, "", field.GetName())
	copyForm := derr == nil && isMember(copyText)
	switch {
	case cdcForm && copyForm && asUTF8 != copyText:
		return "", true, fmt.Errorf("%w: column %q (CHARACTER SET %s, %s): a change-stream value is a member of the column's labels "+
			"read both as UTF-8 and as %s, and the two readings differ — vttablet sends this column in either form depending on "+
			"the streamer, so the value is ambiguous. Rename one of the labels that are each other's mis-encoding, or exclude the table",
			errCharsetNotDecodable, field.GetName(), cc.name, strings.ToUpper(kind), cc.name)
	case cdcForm:
		return asUTF8, true, nil
	case copyForm:
		return copyText, true, nil
	}
	return "", true, fmt.Errorf("%w: column %q (CHARACTER SET %s, %s): a change-stream value is not a member of the column's labels "+
		"in either of the forms vttablet sends. %s", errCharsetNotDecodable, field.GetName(), cc.name, strings.ToUpper(kind), charsetRefusalRemedy)
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
	if cs, ok := stringTypeCharset(t); ok {
		return lookupColumnCharset(cs)
	}
	return nil
}
