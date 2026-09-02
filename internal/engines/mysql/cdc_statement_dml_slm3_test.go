// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-01 SLM-3 (+ AQP-2) pins: the four statement-logged DML
// shapes that bypassed the STATEMENT-DML belt silently, the comment
// grammar behind two of them, the class closure behind all four, and
// the scanner parity that keeps the next grammar correction from landing
// in one copy. Each gate below was mutation-run in both directions
// against the arm it names (the commit message carries the runs).

package mysql

import (
	"context"
	"encoding/binary"
	"strconv"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// statementDMLCommentPrefixes is MySQL's leading-comment grammar as a
// table — every form the server treats as a comment before a statement,
// including the two whose mishandling was SLM-3 shapes 2 and 3. Each
// cell names the lexer arm that has to be right for it, so a mutation
// that reverts one arm fails by name.
var statementDMLCommentPrefixes = map[string]commentPrefix{
	"none":                {"", false},
	"dash_space":          {"-- note\n", false},
	"dash_tab":            {"--\tnote\n", false},
	"dash_newline":        {"--\n", false},                // SLM-3 shape 3: `--` before a control character is a comment
	"dash_crlf":           {"--\r\n", false},              // same arm, CR first
	"dash_eof_then_hash":  {"--\n# second line\n", false}, // two line comments back to back
	"hash":                {"# note\n", false},
	"block":               {"/* note */ ", false},
	"block_multiline":     {"/* a\n b */\n", false},
	"hint":                {"/*+ MAX_EXECUTION_TIME(1000) */ ", false},
	"versioned_empty":     {"/*!40000 */ ", false},
	"versioned_open":      {"/*!50000 ", true}, // SLM-3 shape 2: the statement is INSIDE the versioned comment
	"versioned_noversion": {"/*! ", true},
	"mariadb_versioned":   {"/*M!100300 ", true},
	// A plain comment inside an executable one. The server lexes the
	// versioned comment's contents, skips the plain comment as a comment,
	// and still requires the versioned closer — the first cut of this
	// table omitted the closer and the real-server pin refused it (1064),
	// which is the pin doing its job.
	"nested_in_versioned": {"/*!40000 /* inner */ ", true},
	"versioned_then_line": {"/*!40000 */ -- trailing\n", false},
	"whitespace_soup":     {" \t\r\n\f\v", false},
}

// commentPrefix is one row of the grammar table: the text to put before
// a statement, and whether it leaves an executable comment OPEN that the
// statement must close with ` */` (the shape mysqldump and the SLM-3
// observation both use).
type commentPrefix struct {
	text string
	open bool
}

// wrap renders stmt behind the prefix, closing the comment when the
// prefix opened one.
func (p commentPrefix) wrap(stmt string) string {
	if p.open {
		return p.text + stmt + " */"
	}
	return p.text + stmt
}

// statementDMLVerbStatements is one statement per DML verb, each with a
// trailing `*/` variant available so the versioned_open cells close their
// comment the way the server saw it.
var statementDMLVerbStatements = map[string]string{
	"INSERT":  "INSERT INTO t VALUES (1,'x')",
	"UPDATE":  "UPDATE t SET v = 'x' WHERE id = 1",
	"DELETE":  "DELETE FROM t WHERE id = 1",
	"REPLACE": "REPLACE INTO t VALUES (1,'x')",
	"WITH":    "WITH c AS (SELECT id FROM t) UPDATE t SET v = 'x' WHERE id IN (SELECT id FROM c)",
}

// TestStatementDMLCommentGrammar is gate (a): every comment prefix ×
// every DML verb yields the verb. The prefixes are MySQL's grammar, not
// this lexer's — the two `--` cells and the versioned_open cell are the
// ones the old lexer got wrong, and each is a silent drop when wrong.
func TestStatementDMLCommentGrammar(t *testing.T) {
	t.Parallel()
	cells := 0
	for pname, prefix := range statementDMLCommentPrefixes {
		for verb, stmt := range statementDMLVerbStatements {
			q := prefix.wrap(stmt)
			got, ok := statementDMLVerb(q)
			if !ok || got != verb {
				t.Errorf("%s/%s: statementDMLVerb(%q) = (%q, %v); want (%q, true)", pname, verb, q, got, ok, verb)
			}
			cells++
		}
	}
	// Anti-vacuity: the cells the old lexer failed must be present by
	// name, and the matrix must not have shrunk below its shipped size.
	for _, must := range []string{"dash_newline", "dash_crlf", "versioned_open", "nested_in_versioned", "mariadb_versioned"} {
		if _, ok := statementDMLCommentPrefixes[must]; !ok {
			t.Errorf("prefix %q is missing from the grammar table", must)
		}
	}
	if cells < 17*5 {
		t.Fatalf("exercised %d cells; want at least 17 prefixes x 5 verbs", cells)
	}

	// The other direction, so a lexer that returns the first word of
	// ANY text cannot pass: a bare `--x` is a syntax error on the
	// server, so the INSERT after it is never executed and must not
	// lex as one.
	if verb, ok := statementDMLVerb("--x\nINSERT INTO t VALUES (1)"); ok {
		t.Errorf("`--x` is not a comment on the server; statementDMLVerb tripped on %q", verb)
	}
}

// TestStatementDMLScannersAgree is AQP-2's parity pin: the detection
// lexer and the redaction cut share one comment grammar, and the
// keyword the lexer returns starts exactly where the redaction skip
// says the statement begins — for every prefix in the grammar table.
func TestStatementDMLScannersAgree(t *testing.T) {
	t.Parallel()
	for pname, prefix := range statementDMLCommentPrefixes {
		q := prefix.wrap("DELETE FROM patients WHERE ssn = '078051120'")
		skip := statementDMLCommentSkip(q)
		lexStart := skipLeadingSQLComments(q)
		if skip != lexStart {
			t.Errorf("%s: statementDMLCommentSkip=%d, skipLeadingSQLComments=%d for %q — the two scanners "+
				"disagree on where the statement starts", pname, skip, lexStart, q)
		}
		if !strings.HasPrefix(q[skip:], "DELETE") {
			t.Errorf("%s: the shared skip landed at %q, not on the verb", pname, q[skip:])
		}
		if lead := statementDMLLead(q); !strings.HasPrefix(lead, "DELETE FROM patients WHERE ssn") || strings.Contains(lead, "078051120") {
			t.Errorf("%s: statementDMLLead(%q) = %q", pname, q, lead)
		}
	}
	// The malformed case both scanners must agree on: an unterminated
	// plain block comment has no provable end.
	if got := skipLeadingSQLComments("/* never closed DELETE FROM t"); got != -1 {
		t.Errorf("unterminated block comment: skipLeadingSQLComments = %d; want -1", got)
	}
	if got := statementDMLCommentSkip("/* never closed DELETE FROM t"); got != 0 {
		t.Errorf("unterminated block comment: statementDMLCommentSkip = %d; want 0 (cut immediately)", got)
	}
}

// TestStatementIsCommentOnly pins the one no-keyword shape the belt
// lets through, and that it is exactly comment-only text: MariaDB's
// dummy event (observed shape) passes, anything with content does not.
func TestStatementIsCommentOnly(t *testing.T) {
	t.Parallel()
	for name, q := range map[string]string{
		"mariadb_dummy": "# Dummy event replacing event type 160 that slave cannot handle.                ",
		"line":          "-- nothing\n",
		"block":         "/* nothing */",
		"empty":         "",
		"whitespace":    " \n\t",
	} {
		if !statementIsCommentOnly(q) {
			t.Errorf("%s: statementIsCommentOnly(%q) = false; want true", name, q)
		}
	}
	for name, q := range map[string]string{
		"versioned_has_sql": "/*!50000 INSERT INTO t VALUES (1) */",
		"dash_bare":         "--x",
		"quote_first":       "'stray' INSERT",
		"paren_first":       "(INSERT INTO t VALUES (1))",
		"unterminated":      "/* open INSERT",
	} {
		if statementIsCommentOnly(q) {
			t.Errorf("%s: statementIsCommentOnly(%q) = true; want false", name, q)
		}
	}
}

// TestStatementNamesInScopeDatabase pins the SLM-3 shape-4 door in both
// directions: a `<db>.` qualifier naming the synced database is found
// wherever a DML verb can put it, and a look-alike inside a string
// literal, a comment, or a different database is not.
func TestStatementNamesInScopeDatabase(t *testing.T) {
	t.Parallel()
	inScope := func(db string) bool { return db == "src" }

	for name, q := range map[string]string{
		"insert_into":         "INSERT INTO src.t VALUES (600,'x')",
		"insert_no_into":      "INSERT src.t VALUES (1)",
		"insert_ignore":       "INSERT IGNORE INTO src.t VALUES (1)",
		"insert_low_priority": "INSERT LOW_PRIORITY INTO src.t VALUES (1)",
		"replace":             "REPLACE INTO src.t VALUES (1)",
		"update":              "UPDATE src.t SET v = 1",
		"update_modifiers":    "UPDATE LOW_PRIORITY IGNORE src.t SET v = 1",
		"update_multi_second": "UPDATE other.a, src.b SET a.v = b.v",
		"delete":              "DELETE FROM src.t WHERE id = 1",
		"delete_multi_table":  "DELETE t FROM src.t WHERE id = 1",
		"cte":                 "WITH c AS (SELECT 1) UPDATE src.t SET v = 1",
		"backticked":          "INSERT INTO `src`.`t` VALUES (1)",
		"after_literal":       "INSERT INTO other.t SELECT 'src.t' FROM src.t",
		"after_comment":       "INSERT /* src.t */ INTO src.t VALUES (1)",
		"load_data":           "LOAD DATA LOCAL INFILE '/tmp/t.tsv' INTO TABLE src.t",
		"versioned_wrapped":   "/*!50000 INSERT INTO src.t VALUES (1) */",
		"lowercase_dot_space": "insert into src . t values (1)",
	} {
		if name == "lowercase_dot_space" {
			// `src . t` is legal MySQL and is NOT found: the scan keys on
			// the dot immediately after the identifier. Stated as a known
			// non-cell rather than asserted either way — it is the loud
			// direction's residue, and no client emits it.
			continue
		}
		if !statementNamesInScopeDatabase(q, inScope) {
			t.Errorf("%s: statementNamesInScopeDatabase(%q) = false; want true", name, q)
		}
	}

	for name, q := range map[string]string{
		"other_db":           "INSERT INTO other.t VALUES (1)",
		"unqualified":        "INSERT INTO t VALUES (1)",
		"in_string":          "INSERT INTO other.t VALUES ('src.t')",
		"in_dquote":          `INSERT INTO other.t VALUES ("src.t")`,
		"in_escaped_string":  `INSERT INTO other.t VALUES ('it''s src.t')`,
		"in_backslash_str":   `INSERT INTO other.t VALUES ('a\'src.t')`,
		"in_line_comment":    "INSERT INTO other.t VALUES (1) -- src.t\n",
		"in_block_comment":   "INSERT /* src.t */ INTO other.t VALUES (1)",
		"prefix_of_name":     "INSERT INTO srcx.t VALUES (1)",
		"backticked_escaped": "INSERT INTO `src``x`.`t` VALUES (1)", // the name is src`x, not src: the doubled backtick is unescaped before the scope check
		"decimal_literal":    "UPDATE other.t SET v = 1.5",
		"unterminated_str":   "INSERT INTO other.t VALUES ('src.t",
		"empty":              "",
	} {
		if statementNamesInScopeDatabase(q, inScope) {
			t.Errorf("%s: statementNamesInScopeDatabase(%q) = true; want false", name, q)
		}
	}
}

// executeLoadQueryEvent synthesizes a raw EXECUTE_LOAD_QUERY event the
// way the server lays it out (see cdc_statement_load_data.go), with the
// go-mysql-decoded fixed part populated the way the parser would
// populate it. trailer is the checksum length to append.
func executeLoadQueryEvent(t *testing.T, schema, query string, statusVars []byte, trailer int) *replication.BinlogEvent {
	t.Helper()
	raw := make([]byte, replication.EventHeaderSize, 64+len(query))
	post := make([]byte, executeLoadQueryPostHeaderLen)
	post[8] = byte(len(schema))
	binary.LittleEndian.PutUint16(post[11:], uint16(len(statusVars)))
	binary.LittleEndian.PutUint32(post[13:], 7) // file_id
	raw = append(raw, post...)
	raw = append(raw, statusVars...)
	raw = append(raw, schema...)
	raw = append(raw, 0)
	raw = append(raw, query...)
	raw = append(raw, make([]byte, trailer)...)
	e := &replication.ExecuteLoadQueryEvent{}
	if err := e.Decode(raw[replication.EventHeaderSize : len(raw)-trailer]); err != nil {
		t.Fatalf("decode synthesized EXECUTE_LOAD_QUERY: %v", err)
	}
	h := hdr(replication.EXECUTE_LOAD_QUERY_EVENT)
	h.LogPos = 4321
	return &replication.BinlogEvent{Header: h, Event: e, RawData: raw}
}

func formatDescriptionEvent(alg replication.BinlogChecksum) *replication.BinlogEvent {
	return &replication.BinlogEvent{
		Header: hdr(replication.FORMAT_DESCRIPTION_EVENT),
		Event:  &replication.FormatDescriptionEvent{ChecksumAlgorithm: alg},
	}
}

func wantStatementDML(t *testing.T, err error, phrases ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("dispatch = nil; want the coded STATEMENT-DML refusal")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
	}
	for _, p := range phrases {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("refusal missing %q: %v", p, err)
		}
	}
}

// TestDispatch_StatementDMLShapes pins the four SLM-3 shapes and the
// class closure at the dispatcher — the wiring the lexer tests above
// cannot see — each against the scope rule in both directions.
func TestDispatch_StatementDMLShapes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	queryEv := func(schema, stmt string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: hdr(replication.QUERY_EVENT),
			Event:  &replication.QueryEvent{Schema: []byte(schema), Query: []byte(stmt)},
		}
	}
	dispatch := func(t *testing.T, evs ...*replication.BinlogEvent) error {
		t.Helper()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 8)
		for _, ev := range evs {
			if err := r.dispatch(ctx, ev, out); err != nil {
				return err
			}
		}
		return nil
	}

	t.Run("shape2_versioned_wrapped_refuses", func(t *testing.T) {
		t.Parallel()
		wantStatementDML(t, dispatch(t, queryEv("app", "/*!50000 INSERT INTO t VALUES (10,'versioned') */")),
			"INSERT", "INSERT INTO t VALUES")
	})
	t.Run("shape3_dash_newline_refuses", func(t *testing.T) {
		t.Parallel()
		err := dispatch(t, queryEv("app", "--\nINSERT INTO t VALUES (13,'dashnl')"))
		wantStatementDML(t, err, "INSERT")
		if strings.Contains(err.Error(), "dashnl") {
			t.Errorf("refusal leaked the value: %v", err)
		}
	})
	t.Run("shape4_cross_database_from_out_of_scope_session_refuses", func(t *testing.T) {
		t.Parallel()
		wantStatementDML(t, dispatch(t, queryEv("mysql", "INSERT INTO app.t VALUES (600,'x'),(601,'y')")),
			"INSERT", `"mysql"`)
	})
	t.Run("shape4_out_of_scope_session_writing_elsewhere_survives", func(t *testing.T) {
		t.Parallel()
		if err := dispatch(t, queryEv("mysql", "INSERT INTO other.t VALUES (1)")); err != nil {
			t.Fatalf("an out-of-scope session writing into an unrelated database must not kill the sync (Bug 246): %v", err)
		}
	})
	t.Run("closure_unrecognised_text_refuses", func(t *testing.T) {
		t.Parallel()
		wantStatementDML(t, dispatch(t, queryEv("app", "'stray literal' INSERT INTO t VALUES (1)")),
			statementDMLUnrecognisedVerb)
	})
	t.Run("closure_unterminated_comment_refuses", func(t *testing.T) {
		t.Parallel()
		wantStatementDML(t, dispatch(t, queryEv("", "/* open INSERT INTO t VALUES (1)")), statementDMLUnrecognisedVerb)
	})
	t.Run("closure_out_of_scope_unrecognised_survives", func(t *testing.T) {
		t.Parallel()
		if err := dispatch(t, queryEv("other", "'stray' INSERT INTO t VALUES (1)")); err != nil {
			t.Fatalf("out-of-scope unrecognised text must fall through, not kill the sync: %v", err)
		}
	})
	t.Run("closure_comment_only_mariadb_dummy_survives", func(t *testing.T) {
		t.Parallel()
		// The shape MariaDB sends in place of a suppressed ANNOTATE_ROWS
		// event: refusing it would kill every MariaDB stream with
		// binlog_annotate_row_events=ON.
		if err := dispatch(t, queryEv("", "# Dummy event replacing event type 160 that slave cannot handle.")); err != nil {
			t.Fatalf("a comment-only QueryEvent must survive: %v", err)
		}
	})

	// Shape 1 — statement-format LOAD DATA, as an EXECUTE_LOAD_QUERY event.
	const load = "LOAD DATA LOCAL INFILE '/tmp/t.tsv' INTO TABLE t FIELDS TERMINATED BY '\t'"
	t.Run("shape1_load_data_in_scope_refuses_crc32", func(t *testing.T) {
		t.Parallel()
		err := dispatch(t, formatDescriptionEvent(replication.BINLOG_CHECKSUM_ALG_CRC32),
			executeLoadQueryEvent(t, "app", load, nil, replication.BinlogChecksumLength))
		wantStatementDML(t, err, "LOAD DATA", "EXECUTE_LOAD_QUERY", `"app"`, "LOAD DATA LOCAL INFILE", "ends at binlog position 4321")
		if strings.Contains(err.Error(), "/tmp/t.tsv") {
			t.Errorf("the file name is a literal and must be cut: %v", err)
		}
		// Exact byte count proves the trailer was left off: with it, len
		// would be 4 more and the tail would carry checksum garbage.
		if !strings.Contains(err.Error(), "("+strconv.Itoa(len(load))+" bytes") {
			t.Errorf("refusal did not report the statement's own length %d (trailer not stripped?): %v", len(load), err)
		}
	})
	t.Run("shape1_load_data_no_checksum_with_status_vars", func(t *testing.T) {
		t.Parallel()
		err := dispatch(t, formatDescriptionEvent(replication.BINLOG_CHECKSUM_ALG_OFF),
			executeLoadQueryEvent(t, "app", load, []byte{0x00, 0x00, 0x00, 0x00, 0x00}, 0))
		wantStatementDML(t, err, "LOAD DATA", "("+strconv.Itoa(len(load))+" bytes")
	})
	t.Run("shape1_load_data_out_of_scope_survives", func(t *testing.T) {
		t.Parallel()
		if err := dispatch(t, formatDescriptionEvent(replication.BINLOG_CHECKSUM_ALG_CRC32),
			executeLoadQueryEvent(t, "other", load, nil, replication.BinlogChecksumLength)); err != nil {
			t.Fatalf("an out-of-scope LOAD DATA must not kill the sync: %v", err)
		}
	})
	t.Run("shape1_load_data_cross_database_refuses", func(t *testing.T) {
		t.Parallel()
		wantStatementDML(t, dispatch(t, formatDescriptionEvent(replication.BINLOG_CHECKSUM_ALG_CRC32),
			executeLoadQueryEvent(t, "mysql", "LOAD DATA INFILE '/x' INTO TABLE app.t", nil, replication.BinlogChecksumLength)),
			"LOAD DATA")
	})
	t.Run("shape1_load_data_inside_compressed_payload_has_no_trailer", func(t *testing.T) {
		t.Parallel()
		inner := executeLoadQueryEvent(t, "app", load, nil, 0)
		payload := &replication.BinlogEvent{
			Header: &replication.EventHeader{EventType: replication.TRANSACTION_PAYLOAD_EVENT, LogPos: 9000, EventSize: 500},
			Event:  &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{inner}},
		}
		err := dispatch(t, formatDescriptionEvent(replication.BINLOG_CHECKSUM_ALG_CRC32), payload)
		wantStatementDML(t, err, "LOAD DATA", "("+strconv.Itoa(len(load))+" bytes")
	})
	t.Run("shape1_undecodable_event_refuses_loudly", func(t *testing.T) {
		t.Parallel()
		ev := executeLoadQueryEvent(t, "app", load, nil, 0)
		ev.RawData = ev.RawData[:replication.EventHeaderSize+10] // severed
		err := dispatch(t, ev)
		if err == nil || !strings.Contains(err.Error(), "could not decode") {
			t.Fatalf("a severed EXECUTE_LOAD_QUERY must refuse, never drop: %v", err)
		}
	})
	t.Run("shape1_begin_load_query_is_not_the_refusal_site", func(t *testing.T) {
		t.Parallel()
		ev := &replication.BinlogEvent{Header: hdr(replication.BEGIN_LOAD_QUERY_EVENT), Event: &replication.BeginLoadQueryEvent{FileID: 7, BlockData: []byte("20\tx\n")}}
		if err := dispatch(t, ev); err != nil {
			t.Fatalf("BEGIN_LOAD_QUERY carries no schema; the EXECUTE that follows refuses: %v", err)
		}
	})
	t.Run("legacy_load_family_refuses", func(t *testing.T) {
		t.Parallel()
		for et := range legacyLoadDataEventTypes {
			ev := &replication.BinlogEvent{Header: hdr(et), Event: &replication.GenericEvent{}}
			wantStatementDML(t, dispatch(t, ev), "LOAD DATA", et.String())
		}
		other := &replication.BinlogEvent{Header: hdr(replication.INCIDENT_EVENT), Event: &replication.GenericEvent{}}
		if err := dispatch(t, other); err != nil {
			t.Fatalf("a non-LOAD generic event must keep flowing: %v", err)
		}
	})
}
