// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestStatementDMLVerb pins the tripwire's lexer in both directions:
// every row-DML verb (with the comment/whitespace shapes the server
// actually logs) trips it, and every legitimate QueryEvent the arm sees
// under ROW logging does not — the false-positive analysis from the
// file comment, as a table.
func TestStatementDMLVerb(t *testing.T) {
	t.Parallel()

	trips := map[string]string{
		"insert":           "INSERT INTO t VALUES (1)",
		"insert_lowercase": "insert into t values (1)",
		"update":           "UPDATE t SET x = 1 WHERE id = 2",
		"delete":           "DELETE FROM t WHERE id = 2",
		"replace":          "REPLACE INTO t VALUES (1)",
		"leading_ws":       "  \t\nINSERT INTO t VALUES (1)",
		"block_comment":    "/* app comment */ UPDATE t SET x = 1",
		"line_comment":     "-- generated\nDELETE FROM t",
		"hash_comment":     "# generated\nREPLACE INTO t VALUES (1)",
		"delete_from_h":    "DELETE FROM history WHERE id = 1", // HISTORY exemption must key on the token, not a prefix of the table name
	}
	for name, q := range trips {
		verb, ok := statementDMLVerb(q)
		if !ok {
			t.Errorf("%s: statementDMLVerb(%q) did not trip; want a DML verb", name, q)
			continue
		}
		if !statementDMLVerbs[verb] {
			t.Errorf("%s: verb = %q; want one of the DML verbs", name, verb)
		}
	}

	passes := map[string]string{
		"begin":                  "BEGIN",
		"create":                 "CREATE TABLE t (id INT)",
		"ctas":                   "CREATE TABLE t2 AS SELECT * FROM t", // CTAS arrives tagged CREATE; its rows follow as row events
		"alter":                  "ALTER TABLE t ADD COLUMN y INT",
		"drop":                   "DROP TABLE t",
		"truncate":               "TRUNCATE TABLE t",
		"rename":                 "RENAME TABLE t TO u",
		"savepoint":              "SAVEPOINT sp1",
		"rollback_to":            "ROLLBACK TO SAVEPOINT sp1",
		"xa_start":               "XA START X'1234',X'5678',1",
		"grant":                  "GRANT SELECT ON app.* TO 'x'@'%'",
		"analyze":                "ANALYZE TABLE t",
		"set":                    "SET @@SESSION.gtid_next = 'AUTOMATIC'",
		"mariadb_delete_history": "DELETE HISTORY FROM t BEFORE SYSTEM_TIME '2026-01-01'",
		"comment_only":           "-- INSERT swallowed by a comment",
		"hash_only":              "# UPDATE swallowed",
		"unterminated":           "/* unterminated INSERT",
		"versioned_residue":      "/*!40000 INSERT INTO t VALUES (1) */", // documented residue: versioned comments lex as comments
		"empty":                  "",
	}
	for name, q := range passes {
		if verb, ok := statementDMLVerb(q); ok {
			t.Errorf("%s: statementDMLVerb(%q) tripped on %q; want pass", name, q, verb)
		}
	}
}

// TestStatementDMLError pins the refusal's shape: the code, the verb,
// the scope note, the correlation digest, and — audit 2026-08-27 A4 —
// that the statement's PAYLOAD never reaches the error: the binlog
// text carries row values (PII) that would bypass --redact by riding
// the refusal into logs and reports.
func TestStatementDMLError(t *testing.T) {
	t.Parallel()
	long := "INSERT INTO t VALUES ('alice@example.com','555-0100')" + strings.Repeat(",(1)", 200)
	err := statementDMLError("INSERT", "app", long)
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
		t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
	}
	msg := err.Error()
	for _, phrase := range []string{
		"INSERT", `"app"`, "…", "binlog_format", "silently dropping",
		"INSERT INTO t VALUES", // the sanitized lead: verb + table stay diagnostic
		"sha256",               // the correlation digest is named
	} {
		if !strings.Contains(msg, phrase) {
			t.Errorf("message missing %q; got: %v", phrase, msg)
		}
	}
	// The payload must be withheld: no string-literal content, no
	// VALUES tuple, in either the message or the hint. (A bare `'`
	// probe would false-positive on the hint's own SQL, so the pins are
	// the quoted fragments themselves.)
	for _, leaked := range []string{"alice@example.com", "555-0100", "'alice", "(1)"} {
		if strings.Contains(msg, leaked) || strings.Contains(ce.Hint, leaked) {
			t.Errorf("refusal leaked payload fragment %q: %v (hint %q)", leaked, msg, ce.Hint)
		}
	}
	if len(msg) > 1200 {
		t.Errorf("message did not bound the echoed lead: %d bytes", len(msg))
	}
	if ce.Hint == "" || !strings.Contains(ce.Hint, "variables_by_thread") {
		t.Errorf("hint = %q; want the session-hunt remedy", ce.Hint)
	}
	if !strings.Contains(ce.Hint, "performance_schema=ON") || !strings.Contains(ce.Hint, "SHOW PROCESSLIST") {
		t.Errorf("hint = %q; want the performance_schema precondition + the MariaDB fallback (audit 2026-08-27 A6)", ce.Hint)
	}
}

// TestStatementDMLLead pins the sanitizer's cut rule in both
// directions: cut before the first string-literal quote or paren
// (whichever comes first), keep the verb + table, cap the length.
func TestStatementDMLLead(t *testing.T) {
	t.Parallel()
	cases := map[string]struct{ in, want string }{
		"paren_first":            {"INSERT INTO t VALUES (1,'x')", "INSERT INTO t VALUES…"},
		"single_quote_first":     {"UPDATE t SET v = 'secret' WHERE id = 9", "UPDATE t SET v…"},
		"double_quote_first":     {`DELETE FROM t WHERE v = "secret"`, "DELETE FROM t WHERE v…"},
		"no_assignments":         {"DELETE FROM t WHERE id IS NULL", "DELETE FROM t WHERE id IS NULL"},
		"numeric_literal_eq_cut": {"UPDATE t SET ssn=078051120", "UPDATE t SET ssn…"},
		"cap": {
			"UPDATE " + strings.Repeat("very_long_table_name_", 10) + " SET a = b",
			"UPDATE " + strings.Repeat("very_long_table_name_", 10)[:statementDMLEchoCap-len("UPDATE ")] + "…",
		},
	}
	for name, tc := range cases {
		if got := statementDMLLead(tc.in); got != tc.want {
			t.Errorf("%s: statementDMLLead(%q) = %q; want %q", name, tc.in, got, tc.want)
		}
	}
}

// TestDispatch_StatementDMLTripwire pins the belt's WIRING in the
// QueryEvent arm (the lexer tests above cannot fail if the dispatcher
// stops calling it): an in-scope statement-DML QueryEvent stops the
// stream with the coded refusal, an OUT-of-scope one falls through to
// the generic arm (Bug 246 — a statement-format writer on an unrelated
// database must not kill the sync), and a DDL QueryEvent keeps flowing.
func TestDispatch_StatementDMLTripwire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	queryEv := func(schema, stmt string) *replication.BinlogEvent {
		return &replication.BinlogEvent{
			Header: hdr(replication.QUERY_EVENT),
			Event:  &replication.QueryEvent{Schema: []byte(schema), Query: []byte(stmt)},
		}
	}

	t.Run("in_scope_dml_refuses", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		err := r.dispatch(ctx, queryEv("app", "INSERT INTO t VALUES (1)"), out)
		if err == nil {
			t.Fatal("dispatch of in-scope statement DML = nil; want the coded refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
	})

	t.Run("no_default_db_dml_refuses", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		err := r.dispatch(ctx, queryEv("", "UPDATE app.t SET x = 1"), out)
		if err == nil {
			t.Fatal("dispatch of no-default-db statement DML = nil; want the coded refusal (empty schema is in scope, matching the generic arm)")
		}
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeCDCStatementDML {
			t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
		}
	})

	t.Run("out_of_scope_dml_survives", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		if err := r.dispatch(ctx, queryEv("other", "INSERT INTO t VALUES (1)"), out); err != nil {
			t.Fatalf("dispatch of OUT-of-scope statement DML = %v; want nil (the stream must survive an unrelated database's writer)", err)
		}
	})

	t.Run("ddl_keeps_flowing", func(t *testing.T) {
		t.Parallel()
		r := newStagingReader(t, FlavorVanilla, "6a3175a8-0000-0000-0000-000000000000:1-4")
		out := make(chan ir.Change, 4)
		if err := r.dispatch(ctx, queryEv("app", "ALTER TABLE t ADD COLUMN y INT"), out); err != nil {
			t.Fatalf("dispatch of in-scope DDL = %v; want nil", err)
		}
	})
}
