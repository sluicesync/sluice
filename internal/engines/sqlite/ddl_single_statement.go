// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The C1-1 single-statement door, SQLite lane (the postgres door's
// filed sibling — audit backlog 2026-08-13). modernc's driver executes
// EVERY statement in an Exec string (pinned by
// TestModerncExecRunsMultiStatementStrings), and two surfaces here
// execute strings built from recorded content:
//
//   - the schema writer's emitted DDL, which inlines recorded
//     expression bodies verbatim (emitCheckConstraint — ADR-0134 §3's
//     loud-at-CREATE contract covers PARSE failures, not a body that
//     parses as TWO statements);
//   - the --stage-local path (stage_d1.go), which executes the D1
//     source's own preserved sqlite_master.sql — by SQLite's contract
//     one statement per object, but user-authored text that can carry
//     comments and every identifier-quoting style SQLite accepts.
//
// So this validator understands more than the postgres one: ''-doubled
// string literals, ""-doubled identifiers, [bracketed] and `backtick`
// identifiers, `--` line comments and `/* */` block comments — with
// one optional trailing semicolon. A top-level `;` with SQL after it,
// an unbalanced paren, or an unterminated quote/comment refuses with
// the shared SLUICE-E-DDL-EMIT-MULTI-STATEMENT before the driver runs
// anything.
//
// The dump-import path (dump.go) deliberately executes MULTI-statement
// scripts — that is the feature (the operator's own dump file, not
// DDL sluice built from recorded content) — and is enumerated as
// exempt at the roster gate.

// assertSingleDDLStatement reports a non-nil error when stmt is not
// structurally a single SQL statement under SQLite's lexical rules.
func assertSingleDDLStatement(stmt string) error {
	depth := 0
	i := 0
	n := len(stmt)
	for i < n {
		c := stmt[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			// All three quote forms double their own quote char to
			// escape it.
			j := i + 1
			for {
				if j >= n {
					return fmt.Errorf("unterminated %q-quoted token opened at byte %d", string(c), i)
				}
				if stmt[j] == c {
					if j+1 < n && stmt[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case c == '[':
			// Bracket identifiers have no escape; ']' ends them.
			j := i + 1
			for j < n && stmt[j] != ']' {
				j++
			}
			if j >= n {
				return fmt.Errorf("unterminated [bracketed] identifier opened at byte %d", i)
			}
			i = j + 1
		case c == '-' && i+1 < n && stmt[i+1] == '-':
			j := i + 2
			for j < n && stmt[j] != '\n' {
				j++
			}
			i = j
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			j := i + 2
			for {
				if j+1 >= n {
					return fmt.Errorf("unterminated block comment opened at byte %d", i)
				}
				if stmt[j] == '*' && stmt[j+1] == '/' {
					break
				}
				j++
			}
			i = j + 2
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced ')' at byte %d", i)
			}
			i++
		case c == ';':
			if depth != 0 {
				return fmt.Errorf("';' inside an unbalanced paren group at byte %d", i)
			}
			if rest := trimSpaceAndComments(stmt[i+1:]); rest != "" {
				return fmt.Errorf("';' at byte %d is followed by more SQL (%.40q…)", i, rest)
			}
			i = n
		default:
			i++
		}
	}
	if depth != 0 {
		return fmt.Errorf("%d unclosed '(' at end of statement", depth)
	}
	return nil
}

// trimSpaceAndComments strips leading whitespace and complete comments
// so a trailing `; -- created by tool` (a shape user-authored dump DDL
// carries) does not read as "more SQL". An unterminated block comment
// in the tail is returned as-is (non-empty → the caller refuses, which
// is the loud direction for a malformed tail).
func trimSpaceAndComments(s string) string {
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			j := i + 2
			for {
				if j+1 >= len(s) {
					return s[i:] // unterminated block comment — refuse upstream
				}
				if s[j] == '*' && s[j+1] == '/' {
					break
				}
				j++
			}
			i = j + 2
		default:
			return s[i:]
		}
	}
	return ""
}

// ddlExecer is the slice of *sql.DB / *sql.Conn the emitted-DDL sites
// execute through.
type ddlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// execEmittedDDL is the ONE road from an emitted/recorded DDL string
// to ExecContext in the schema-writing and stage-local surfaces (held
// there by TestSQLiteEmittedDDLGoesThroughTheSingleStatementDoor).
func execEmittedDDL(ctx context.Context, ex ddlExecer, stmt string) error {
	if verr := assertSingleDDLStatement(stmt); verr != nil {
		return sluicecode.Wrap(sluicecode.CodeDDLEmitMultiStatement,
			"the DDL came from recorded schema content (a backup manifest, a read source schema, or the "+
				"D1 source's own preserved object SQL); the content is corrupt or tampered — nothing was executed",
			fmt.Errorf("sqlite: refusing to execute DDL that is not a single statement (%v); "+
				"sluice's emitters and sqlite_master both only ever carry one statement per object: %.120q",
				verr, stmt))
	}
	_, err := ex.ExecContext(ctx, stmt)
	return err
}
