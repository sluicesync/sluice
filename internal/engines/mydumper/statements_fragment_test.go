// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"strings"
	"testing"
)

// This file is the CLASS gate under [errNonSQLFragment]'s documented
// contract — "a pure comment/whitespace fragment is legitimate; anything
// else MUST refuse naming the bytes" — rather than a pin on the one
// fragment shape that happened to be broken.
//
// The audit-2026-07-15 CRITICAL-1 fix closed the `case "":` skip in the
// classifiers, but [stripLeadingCommentsAndSpace]'s plain-`/*` branch
// still returned "" on an UNTERMINATED comment, which made the contract
// unsatisfiable from inside: a data chunk carrying `/* note` with no
// closing `*/` lexed to an empty fragment, so errNonSQLFragment reported
// "pure comment, skip" and every INSERT after it vanished with rc=0. The
// versioned `/*!` branch three lines above already returned the bytes.
// One branch of one helper reopened the class, which is why the gate is a
// PRODUCT of prefix runs × content shapes and not a list of examples.
//
// The two directions are both load-bearing and are asserted separately:
// nothing carrying content may skip, and a genuinely comment-only
// fragment must STILL skip (a gate that refuses everything is not a gate).

// fragmentCommentPrefixes are the leading runs
// [stripLeadingCommentsAndSpace] is documented to consume. Each one must
// leave whatever follows it VISIBLE — a prefix that swallowed the rest is
// the defect this file exists for. Versioned `/*!` comments are
// deliberately absent: MySQL EXECUTES their body, so they are content,
// not prefix, and they appear below in the content table instead.
var fragmentCommentPrefixes = []struct{ name, prefix string }{
	{"none", ""},
	{"whitespace", " \t\r\n  "},
	{"dash-line-comment", "-- a note\n"},
	{"dash-line-comment-run", "-- one\n-- two\n-- three\n"},
	{"dash-line-comment-no-space", "--\n"},
	{"hash-line-comment", "# a note\n"},
	{"hash-line-comment-run", "# one\n# two\n"},
	{"mixed-line-comment-run", "-- one\n# two\n-- three\n"},
	{"block-comment", "/* a note */"},
	{"block-comment-run", "/* one */ /* two */"},
	{"block-comment-multiline", "/* a\n   note\n   spanning lines */\n"},
	{"comment-salad", "-- one\n/* two */\n# three\n/* four */"},
}

// fragmentContentShapes are the non-SQL bodies a torn / re-encoded /
// hand-edited dump can present where a statement was expected. EVERY one
// must refuse, under EVERY prefix above — that product is the pin.
var fragmentContentShapes = []struct{ name, content string }{
	{"unterminated-block-comment", "/* dangling note"},
	{"unterminated-block-comment-trailing-star", "/* dangling note *"},
	{"unterminated-block-comment-swallowing-inserts", "/* note\nINSERT INTO `t` VALUES (3);\nINSERT INTO `t` VALUES (4);\n"},
	{"unterminated-versioned-comment", "/*!40101 SET NAMES binary"},
	{"unterminated-versioned-comment-swallowing-inserts", "/*!40101 SET NAMES binary\nINSERT INTO `t` VALUES (3);\n"},
	{"bare-bom", utf8BOM},
	{"bom-glued-to-insert", utf8BOM + "INSERT INTO `t` VALUES (1)"},
	{"severed-insert-tail", "(4),(5)"},
	{"severed-insert-tail-terminated", "(4),(5);"},
	{"digit-leading-garbage", "40101 SET NAMES binary"},
	{"unterminated-string", "'a severed literal"},
	{"binary-noise", "\x00\x01\x02\xff"},
	{"utf16-re-encoded-head", "\xff\xfeI\x00N\x00S\x00"},
	{"lone-semicolon-tail", ",(6),(7)"},
}

// TestErrNonSQLFragmentRefusesEveryContentShape is the "non-empty content
// implies a refusal" half. Product of every skippable prefix × every
// content shape, so a future prefix branch that swallows its remainder
// fails here for every body rather than depending on which single one
// someone thought to add.
func TestErrNonSQLFragmentRefusesEveryContentShape(t *testing.T) {
	for _, p := range fragmentCommentPrefixes {
		for _, c := range fragmentContentShapes {
			t.Run(p.name+"/"+c.name, func(t *testing.T) {
				stmt := p.prefix + c.content
				err := errNonSQLFragment(stmt)
				if err == nil {
					t.Fatalf("errNonSQLFragment(%q) = nil; a fragment carrying content must refuse loudly, "+
						"never be skipped as a comment", stmt)
				}
				if !strings.Contains(err.Error(), "does not begin with a SQL keyword") {
					t.Fatalf("errNonSQLFragment(%q) = %v; want the keyword-less-fragment refusal", stmt, err)
				}
			})
		}
	}
}

// TestErrNonSQLFragmentRefusesBOMPrefixedContent covers the combination
// the prefix product cannot express: a BOM is NOT a skippable prefix (it
// is itself content — see [utf8BOM]), so a BOM in front of an
// unterminated comment must refuse for two independent reasons and must
// not cancel out into a skip.
func TestErrNonSQLFragmentRefusesBOMPrefixedContent(t *testing.T) {
	for _, stmt := range []string{
		utf8BOM + "/* dangling note",
		utf8BOM + "/*!40101 SET NAMES binary",
		utf8BOM + "-- a note\n/* dangling note",
		utf8BOM + "/* a note */",         // terminated comment, but the BOM remains
		utf8BOM + "\n\t" + utf8BOM,       // whitespace between two BOMs
		"-- a note\n" + utf8BOM,          // BOM after a legitimate comment run
		"/* a note */" + utf8BOM + "(4)", // BOM wedged before a severed tail
	} {
		if err := errNonSQLFragment(stmt); err == nil {
			t.Fatalf("errNonSQLFragment(%q) = nil; a BOM is content, not whitespace", stmt)
		}
	}
}

// TestErrNonSQLFragmentSkipsCommentOnlyFragments is the other direction,
// and it is what keeps the gate above from degenerating into "everything
// errors". A dump legitimately carries comment-only statements (mydumper
// writes a `-- ` header and a `/* … */` trailer), and refusing those
// would break every clean dump.
func TestErrNonSQLFragmentSkipsCommentOnlyFragments(t *testing.T) {
	commentOnly := make([]string, 0, 3*len(fragmentCommentPrefixes)+7)
	commentOnly = append(commentOnly, "", " ", "\n\t \r\n")
	for _, p := range fragmentCommentPrefixes {
		// Each prefix must skip alone, when it is the WHOLE fragment with
		// trailing whitespace, and when two of them are concatenated.
		commentOnly = append(commentOnly, p.prefix, p.prefix+"\n \t", p.prefix+p.prefix)
	}
	// End-of-file line comments: no terminating newline is legal.
	commentOnly = append(
		commentOnly,
		"-- a trailing note",
		"--",
		"# a trailing note",
		"-- one\n# two",
	)
	for _, stmt := range commentOnly {
		if err := errNonSQLFragment(stmt); err != nil {
			t.Fatalf("errNonSQLFragment(%q) = %v; a comment/whitespace-only fragment is legitimate and must skip",
				stmt, err)
		}
	}
}

// TestStripLeadingCommentsAndSpaceUnterminatedCommentsSurvive pins the
// helper's own contract directly — the ONE-LINE fix, at the level it was
// made. Both comment flavours return their bytes; neither returns "".
func TestStripLeadingCommentsAndSpaceUnterminatedCommentsSurvive(t *testing.T) {
	for _, tc := range []struct{ name, in string }{
		{"plain", "/* dangling note\nINSERT INTO `t` VALUES (1);\n"},
		{"versioned", "/*!40101 SET NAMES binary\nINSERT INTO `t` VALUES (1);\n"},
		{"plain-after-line-comments", "-- one\n# two\n/* dangling note"},
		{"plain-after-terminated-block", "/* one */ /* dangling note"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := stripLeadingCommentsAndSpace(tc.in)
			if got == "" {
				t.Fatalf("stripLeadingCommentsAndSpace(%q) = %q; an unterminated comment must be handed back "+
					"to the caller's parser, not reported as a comment-only fragment", tc.in, got)
			}
			if !strings.HasPrefix(got, "/*") {
				t.Fatalf("stripLeadingCommentsAndSpace(%q) = %q; want the unterminated comment intact", tc.in, got)
			}
		})
	}
}
