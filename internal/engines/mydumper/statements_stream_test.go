// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"
)

// The incremental statement scanner (stmtScanner) must split byte-
// identically to a whole-input splitMySQLChunk scan for EVERY placement
// of read-block boundaries — a wrong resume state would mis-split
// statements, the silent-loss class. splitMySQLChunk is the oracle: it
// is the original single-scan lexer, still exercised on whole schema
// files by parseSchemaFile.

// drainStream collects every statement the stream yields.
func drainStream(t *testing.T, input string, blockSize int) []string {
	t.Helper()
	stream := newStatementStream(strings.NewReader(input), blockSize)
	var got []string
	for {
		stmt, err := stream.Next()
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("block %d: Next: %v", blockSize, err)
		}
		got = append(got, stmt)
	}
}

// oracleSplit is the whole-input reference: splitMySQLChunk plus the
// trailing-fragment rule statementStream applies at EOF.
func oracleSplit(input string) []string {
	stmts, carry := splitMySQLChunk(input)
	if rest := strings.TrimSpace(carry); rest != "" {
		stmts = append(stmts, rest)
	}
	return stmts
}

// assertMatchesOracle runs input through the stream at every block size
// and compares against the whole-input oracle.
func assertMatchesOracle(t *testing.T, name, input string, blockSizes []int) {
	t.Helper()
	want := oracleSplit(input)
	for _, blockSize := range blockSizes {
		got := drainStream(t, input, blockSize)
		if len(got) != len(want) {
			t.Fatalf("%s / block %d: %d statements; oracle %d\n got: %q\nwant: %q",
				name, blockSize, len(got), len(want), got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s / block %d: statement %d diverged:\n got %q\nwant %q",
					name, blockSize, i, got[i], want[i])
			}
		}
	}
}

// TestStatementStream_DifferentialOracle pins the incremental scanner
// against the single-scan oracle for every token family the MySQL
// splitter recognises, at block sizes 1..8 (every boundary offset
// through every token) plus larger blocks. The family list mirrors
// splitMySQLChunk's dispatch: single/double-quoted strings (backslash
// and doubled-quote escapes), backtick identifiers (doubled-backtick
// escapes), `-- `/`#` line comments (and the `a--b` arithmetic
// non-comment), plain and versioned /* */ block comments — each
// carrying an embedded ';' that must NOT split, adjacent to real
// boundaries that must.
func TestStatementStream_DifferentialOracle(t *testing.T) {
	blockSizes := []int{1, 2, 3, 4, 5, 6, 7, 8, 13, 64, 4096}
	cases := map[string]string{
		"single_quoted_semicolon": "INSERT INTO t VALUES ('a;b');SELECT 1;",
		"single_quoted_backslash": `INSERT INTO t VALUES ('a\';b');SELECT 1;`,
		"single_quoted_doubled":   "INSERT INTO t VALUES ('a'';b');SELECT 1;",
		"double_quoted_semicolon": `INSERT INTO t VALUES ("a;b");SELECT 1;`,
		"double_quoted_backslash": `INSERT INTO t VALUES ("a\";b");SELECT 1;`,
		"double_quoted_doubled":   `INSERT INTO t VALUES ("a"";b");SELECT 1;`,
		"backtick_semicolon":      "INSERT INTO `a;b` VALUES (1);SELECT 1;",
		"backtick_doubled":        "INSERT INTO `a``;b` VALUES (1);SELECT 1;",
		"hash_comment":            "SELECT 1;# c;omment\nSELECT 2;",
		"dashdash_comment":        "SELECT 1;-- c;omment\nSELECT 2;",
		"dashdash_tab":            "SELECT 1;--\tc;omment\nSELECT 2;",
		"dashdash_empty":          "SELECT 1;--\nSELECT 2;",
		"dash_arithmetic":         "SELECT 1-2;SELECT 3--4;SELECT 5;",
		"triple_dash":             "SELECT 1;--- c;omment\nSELECT 2;",
		"quad_dash_no_ws":         "SELECT 1----2;SELECT 3;",
		"block_comment":           "SELECT 1;/* c;omment */SELECT 2;",
		"block_comment_stars":     "SELECT 1;/* a **;** b **/SELECT 2;",
		"block_comment_multiline": "SELECT 1;/* line1;\nline2; */SELECT 2;",
		"versioned_comment":       "/*!40101 SET NAMES binary*/;INSERT INTO t VALUES (1);",
		"versioned_partition":     "CREATE TABLE t (id int)\n/*!50100 PARTITION BY HASH (id) PARTITIONS 4 */;SELECT 1;",
		"slash_arithmetic":        "SELECT 1/2;SELECT 3;",
		"slash_then_boundary":     "SELECT 1/;SELECT 2;",
		"unterminated_string":     "INSERT INTO t VALUES ('never closed; not a boundary",
		"unterminated_backtick":   "INSERT INTO `never closed; not a boundary",
		"unterminated_block":      "SELECT 1;/* never closed; not a boundary",
		"trailing_fragment":       "SELECT 1;SELECT 2",
		"trailing_dashes":         "SELECT 1;--",
		"trailing_backslash":      `INSERT INTO t VALUES ('a\`,
		"empty_statements":        ";;;SELECT 1;;",
		"whitespace_only":         " \n\t ",
		"quote_adjacent_boundary": "INSERT INTO t VALUES ('x');INSERT INTO t VALUES ('y');",
		"mixed_realistic_chunk": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40103 SET TIME_ZONE='+00:00' */;\n" +
			"-- chunk 00001 of `db`.`t`\n" +
			"INSERT INTO `t` VALUES (1,'a;b','it''s \\'quoted\\''),(2,NULL,\"x;\\\"y\");\n" +
			"INSERT INTO `t` VALUES (3,'multi\nline;text',0x41);\n",
	}
	for name, input := range cases {
		assertMatchesOracle(t, name, input, blockSizes)
	}
}

// TestStatementStream_DifferentialOracle_Randomized composes seeded-
// random token soup from the same families and compares stream output
// against the oracle at random block sizes — the fuzz-shaped backstop
// for boundary/state combinations the hand-written corpus misses.
func TestStatementStream_DifferentialOracle_Randomized(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test corpus
	tokens := []string{
		"SELECT 1", ";", " ", "\n", "'a;b'", `'x\';'`, "'d'';'", `"q;w"`,
		"`i;d`", "`a``;`", "-- c;\n", "# h;\n", "/* b; */", "/*!40101 SET NAMES binary*/",
		"--", "-", "/", "*", "\\", "'", "`", "a--b", "1-2", "1/2", "it''s",
		"/* multi\nline; */", "INSERT INTO t VALUES (1,'v')",
	}
	for round := 0; round < 200; round++ {
		var sb strings.Builder
		for n := rng.Intn(30); n >= 0; n-- {
			sb.WriteString(tokens[rng.Intn(len(tokens))])
		}
		input := sb.String()
		blockSize := 1 + rng.Intn(17)
		want := oracleSplit(input)
		got := drainStream(t, input, blockSize)
		if len(got) != len(want) {
			t.Fatalf("round %d (block %d, input %q): %d statements; oracle %d\n got %q\nwant %q",
				round, blockSize, input, len(got), len(want), got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("round %d (block %d, input %q): statement %d diverged:\n got %q\nwant %q",
					round, blockSize, input, i, got[i], want[i])
			}
		}
	}
}

// TestStatementStream_TokenSpansManyBlocks pins the load-bearing MED-P1
// property directly: a token much larger than the read block must both
// (a) split correctly and (b) hold the lexer state across MANY blocks —
// one case per family, statement ≫ block.
func TestStatementStream_TokenSpansManyBlocks(t *testing.T) {
	// A payload full of would-be delimiters. The comment cases need it
	// free of `*/` (which would genuinely terminate the comment).
	big := strings.Repeat("payload; with -- /* tokens */ inside ", 200)
	bigComment := strings.ReplaceAll(big, "*/", "**")
	cases := map[string]struct {
		input string
		want  []string
	}{
		"giant_string": {
			input: "INSERT INTO t VALUES ('" + big + "');SELECT 1;",
			want:  []string{"INSERT INTO t VALUES ('" + big + "')", "SELECT 1"},
		},
		"giant_backtick": {
			input: "INSERT INTO `" + big + "` VALUES (1);SELECT 1;",
			want:  []string{"INSERT INTO `" + big + "` VALUES (1)", "SELECT 1"},
		},
		"giant_block_comment": {
			input: "SELECT 1;/*" + bigComment + "*/SELECT 2;",
			want:  []string{"SELECT 1", "/*" + bigComment + "*/SELECT 2"},
		},
		"giant_versioned_comment": {
			input: "/*!50100 " + bigComment + " */;SELECT 2;",
			want:  []string{"/*!50100 " + bigComment + " */", "SELECT 2"},
		},
	}
	for name, tc := range cases {
		for _, blockSize := range []int{7, 64, 512} {
			got := drainStream(t, tc.input, blockSize)
			if len(got) != len(tc.want) {
				t.Fatalf("%s / block %d: %d statements (%q); want %d", name, blockSize, len(got), got, len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("%s / block %d: statement %d = %q; want %q", name, blockSize, i, got[i], tc.want[i])
				}
			}
		}
	}
}

// TestStatementStream_TooLargeCeiling pins the loud refusal when the
// carry outgrows maxStatementBytes (an unterminated quote / non-
// statement-structured file must never buffer the whole input).
func TestStatementStream_TooLargeCeiling(t *testing.T) {
	// An endless unterminated string: the carry can never complete.
	r := io.MultiReader(
		strings.NewReader("INSERT INTO t VALUES ('"),
		&repeatReader{b: 'x', n: int64(maxStatementBytes) + (2 << 20)},
	)
	stream := newStatementStream(r, 1<<20)
	for {
		_, err := stream.Next()
		if err == nil {
			continue
		}
		if !errors.Is(err, errStatementTooLarge) {
			t.Fatalf("err = %v; want errStatementTooLarge", err)
		}
		return
	}
}

// repeatReader yields n copies of byte b.
type repeatReader struct {
	b byte
	n int64
}

func (r *repeatReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.n {
		p = p[:r.n]
	}
	for i := range p {
		p[i] = r.b
	}
	r.n -= int64(len(p))
	return len(p), nil
}
