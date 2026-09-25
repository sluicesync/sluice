//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-37 (j): the change-stream charset conversion graded against the SERVER
// over each charset's whole code space.
//
// # The independent expected value
//
// The server's own conversion: `CONVERT(CAST(b AS CHAR CHARACTER SET cs)
// USING utf8mb4)` for every candidate byte sequence — never Vitess's or
// x/text's table, which is the thing under test. A sequence the server
// converts without substituting '?' is a value a column of that charset can
// hold, and sluice's decode must equal the server's bytes exactly. A
// sequence the server does NOT accept cannot be stored, so it is not graded
// for equality — but it must not decode to a DIFFERENT character than the
// server's substitution silently implies: sluice either refuses it or the
// cell is counted and reported. That such a sequence cannot reach sluice from
// a stored column (the server refuses to store it) is an UNVERIFIED PREMISE:
// the count is logged per charset so a change in it is visible, but no test
// asserts that a server-invalid sequence cannot be stored.

package mysql

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

// charsetCodeSpace enumerates the candidate byte sequences for one charset.
func charsetCodeSpace(cs string) [][]byte {
	var out [][]byte
	single := func(from, to int) {
		for b := from; b <= to; b++ {
			out = append(out, []byte{byte(b)})
		}
	}
	double := func(leadFrom, leadTo, trailFrom, trailTo int) {
		for l := leadFrom; l <= leadTo; l++ {
			for t := trailFrom; t <= trailTo; t++ {
				out = append(out, []byte{byte(l), byte(t)})
			}
		}
	}
	switch cs {
	case "sjis", "cp932", "euckr", "gb2312", "gbk", "big5":
		single(0x00, 0xFF)
		double(0x80, 0xFF, 0x40, 0xFF)
	case "ujis", "eucjpms":
		single(0x00, 0xFF)
		double(0x80, 0xFF, 0xA1, 0xFE)
		for a := 0xA1; a <= 0xFE; a++ {
			for b := 0xA1; b <= 0xFE; b++ {
				out = append(out, []byte{0x8F, byte(a), byte(b)})
			}
		}
	case "gb18030":
		single(0x00, 0xFF)
		double(0x81, 0xFE, 0x40, 0xFE)
		for a := 0x81; a <= 0x84; a++ {
			for b := 0x30; b <= 0x39; b++ {
				for c := 0x81; c <= 0xFE; c++ {
					for d := 0x30; d <= 0x39; d++ {
						out = append(out, []byte{byte(a), byte(b), byte(c), byte(d)})
					}
				}
			}
		}
		// A supplementary-plane sample (0x90308130 = U+10000 onward).
		for c := 0x81; c <= 0x90; c++ {
			out = append(out, []byte{0x90, 0x30, byte(c), 0x30})
		}
	case "utf16", "ucs2":
		for cp := 0; cp <= 0xFFFF; cp++ {
			out = append(out, []byte{byte(cp >> 8), byte(cp)})
		}
		if cs == "utf16" {
			out = append(out, []byte{0xD8, 0x3D, 0xDE, 0x00}, []byte{0xDB, 0xFF, 0xDF, 0xFF})
		}
	case "utf16le":
		for cp := 0; cp <= 0xFFFF; cp++ {
			out = append(out, []byte{byte(cp), byte(cp >> 8)})
		}
		out = append(out, []byte{0x3D, 0xD8, 0x00, 0xDE})
	case "utf32":
		for cp := 0; cp <= 0xFFFF; cp++ {
			out = append(out, []byte{0, 0, byte(cp >> 8), byte(cp)})
		}
		out = append(out, []byte{0, 1, 0xF6, 0x00}, []byte{0, 0x10, 0xFF, 0xFF}, []byte{0, 0x11, 0, 0})
	default: // every 8-bit charset
		single(0x00, 0xFF)
	}
	return out
}

// charsetGradeList is every non-UTF-8 charset MySQL 8.0 ships.
var charsetGradeList = []string{
	"latin1", "latin2", "latin5", "latin7", "cp1250", "cp1251", "cp1256", "cp1257",
	"cp850", "cp852", "cp866", "dec8", "geostd8", "greek", "hebrew", "hp8", "keybcs2",
	"koi8r", "koi8u", "macce", "macroman", "swe7", "armscii8", "tis620",
	"sjis", "cp932", "ujis", "eucjpms", "euckr", "gb2312", "gbk", "big5", "gb18030",
	"utf16", "utf16le", "ucs2", "utf32",
}

// gradeCharsetsAgainstServer grades every charset the server supports.
func gradeCharsetsAgainstServer(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(`CREATE TABLE cs_grade (id INT NOT NULL PRIMARY KEY, b VARBINARY(8) NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	graded := 0
	for _, cs := range charsetGradeList {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.character_sets WHERE character_set_name = ?`, cs).Scan(&n); err != nil || n == 0 {
			t.Logf("%s: not supported by this server; skipped", cs)
			continue
		}
		cc := lookupColumnCharset(cs)
		asciiOnly := cc != nil && cc.unsupported && asciiOnlyCharsets[cs]
		if cc == nil || (cc.unsupported && !asciiOnly) {
			t.Errorf("%s: sluice has no conversion table for a charset the server ships", cs)
			continue
		}
		if _, err := db.Exec(`TRUNCATE TABLE cs_grade`); err != nil {
			t.Fatal(err)
		}
		space := charsetCodeSpace(cs)
		for start := 0; start < len(space); start += 4000 {
			end := min(start+4000, len(space))
			var sb strings.Builder
			sb.WriteString("INSERT INTO cs_grade VALUES ")
			for i := start; i < end; i++ {
				if i > start {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, "(%d,X'%X')", i, space[i])
			}
			if _, err := db.Exec(sb.String()); err != nil {
				t.Fatalf("%s: seed: %v", cs, err)
			}
		}
		rows, err := db.Query(fmt.Sprintf(
			`SELECT id, HEX(CONVERT(CAST(b AS CHAR CHARACTER SET %s) USING utf8mb4)) FROM cs_grade ORDER BY id`, cs,
		))
		if err != nil {
			t.Fatalf("%s: server conversion: %v", cs, err)
		}
		var valid, mismatches, invalidDecoded int
		for rows.Next() {
			var id int
			var serverHex sql.NullString
			if err := rows.Scan(&id, &serverHex); err != nil {
				t.Fatal(err)
			}
			in := space[id]
			server, _ := hex.DecodeString(serverHex.String)
			serverValid := serverHex.Valid && len(server) > 0 &&
				(!bytes.Contains(server, []byte{'?'}) || bytes.Equal(in, []byte{'?'}))
			got, derr := cc.decode(in, "cs_grade", "b")
			if asciiOnly && !isASCII(in) {
				// No faithful table: every non-ASCII value must refuse.
				if derr == nil {
					mismatches++
					if mismatches <= 5 {
						t.Errorf("%s: 0x%X decoded to 0x%X; a charset with no faithful table must refuse it", cs, in, got)
					}
				}
				continue
			}
			if serverValid {
				valid++
				// Where the server's OWN conversion has no character to give —
				// it writes U+FFFD (tis620's holes) or bytes that are not UTF-8
				// at all (MariaDB's ucs2/utf32 surrogate code points) — the
				// bulk copy cannot carry the value either: sluice may match it
				// or refuse, but never produce something else.
				if derr != nil && (bytes.ContainsRune(server, utf8.RuneError) || !utf8.Valid(server)) {
					continue
				}
				if derr != nil || got != string(server) {
					mismatches++
					if mismatches <= 5 {
						t.Errorf("%s: 0x%X: server converts to 0x%X, sluice decodes to 0x%X (err %v)", cs, in, server, got, derr)
					}
				}
				continue
			}
			if derr == nil {
				invalidDecoded++
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if mismatches > 5 {
			t.Errorf("%s: %d mismatches in total", cs, mismatches)
		}
		// Anti-vacuity: an 8-bit charset holds most of its 256 bytes, a
		// multibyte one thousands of sequences.
		if valid < 100 && !asciiOnly {
			t.Errorf("%s: only %d server-valid sequences graded; the code space did not reach the charset", cs, valid)
		}
		t.Logf("%-9s graded %6d server-valid sequences exactly; %d server-invalid sequences sluice decodes (UNVERIFIED PREMISE: unreachable from a stored column)",
			cs, valid, invalidDecoded)
		graded++
	}
	if graded < 30 {
		t.Fatalf("only %d charsets graded; want every non-UTF-8 charset the server ships", graded)
	}
}

func TestColumnCharsetDecode_MatchesTheServer_MySQL(t *testing.T) {
	dsn, cleanup := startMySQLForCDC(t)
	defer cleanup()
	gradeCharsetsAgainstServer(t, dsn)
}

func TestColumnCharsetDecode_MatchesTheServer_MariaDB(t *testing.T) {
	dsn, cleanup := newMariaDBDedicatedForCDC(t, mariadb114Image)
	defer cleanup()
	gradeCharsetsAgainstServer(t, dsn)
}
