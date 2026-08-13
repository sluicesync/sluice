// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The C1-1 single-statement door (audit 2026-08-11).
//
// Every DDL string sluice's emitters produce is ONE statement — and the
// restore path inlines RECORDED expression bodies (CHECK constraints,
// generated columns, defaults) from a backup manifest verbatim into
// that DDL, then executes it through pgx's simple protocol, which runs
// multi-statement strings. A tampered or corrupted manifest whose
// expression body closes the surrounding syntax and appends `; DROP
// TABLE …; --` therefore executed arbitrary SQL as the restore role,
// at exit 0, with `backup verify` green (observed live by the audit:
// a canary table dropped mid-restore). Signature verification catches
// tampering when it is ENABLED; this door holds regardless, on the
// structural invariant the emitters already guarantee: one statement.
//
// The validator understands exactly the quoting the emitters produce —
// standard single-quoted literals ('' doubling; the read boundary and
// the 74299e34 escaping chunk guarantee no backslash-escape spellings
// reach the IR), double-quoted identifiers ("" doubling), and
// dollar-quoted bodies (the enum-guard DO $$ … $$ block carries
// internal semicolons, and a hostile enum LABEL containing `$$` would
// terminate that quoting early — which this validator then sees as a
// top-level `;` and refuses). One optional trailing semicolon is
// allowed; anything after it, an unbalanced paren, or an unterminated
// quote refuses with SLUICE-E-DDL-EMIT-MULTI-STATEMENT before the
// statement reaches the server.

// assertSingleDDLStatement reports a non-nil error when stmt is not
// structurally a single SQL statement under PostgreSQL quoting rules.
func assertSingleDDLStatement(stmt string) error {
	depth := 0
	i := 0
	n := len(stmt)
	for i < n {
		c := stmt[i]
		switch c {
		case '\'':
			j := i + 1
			for {
				if j >= n {
					return fmt.Errorf("unterminated string literal opened at byte %d", i)
				}
				if stmt[j] == '\'' {
					if j+1 < n && stmt[j+1] == '\'' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case '"':
			j := i + 1
			for {
				if j >= n {
					return fmt.Errorf("unterminated quoted identifier opened at byte %d", i)
				}
				if stmt[j] == '"' {
					if j+1 < n && stmt[j+1] == '"' {
						j += 2
						continue
					}
					break
				}
				j++
			}
			i = j + 1
		case '$':
			tag, ok := dollarQuoteTagAt(stmt, i)
			if !ok {
				i++
				continue
			}
			end := strings.Index(stmt[i+len(tag):], tag)
			if end < 0 {
				return fmt.Errorf("unterminated dollar-quoted body (%s) opened at byte %d", tag, i)
			}
			i += len(tag) + end + len(tag)
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("unbalanced ')' at byte %d", i)
			}
			i++
		case ';':
			if depth != 0 {
				return fmt.Errorf("';' inside an unbalanced paren group at byte %d", i)
			}
			if rest := strings.TrimSpace(stmt[i+1:]); rest != "" {
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

// dollarQuoteTagAt reports the full `$tag$` opener starting at stmt[i]
// (`$$` for the untagged form), or ok=false when the '$' does not open
// a dollar quote (e.g. a bare '$' in an emitted operator). PG tags
// follow identifier rules — the first tag byte cannot be a digit
// (that spelling is a positional parameter, which emitted DDL never
// carries).
func dollarQuoteTagAt(stmt string, i int) (tag string, ok bool) {
	j := i + 1
	for j < len(stmt) {
		c := stmt[j]
		if c == '$' {
			return stmt[i : j+1], true
		}
		isIdent := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9' && j > i+1)
		if !isIdent {
			return "", false
		}
		j++
	}
	return "", false
}

// ddlExecer is the slice of *sql.DB / *sql.Conn / *sql.Tx the emitted-
// DDL sites execute through.
type ddlExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// execEmittedDDL is the ONE road from an emitted DDL string to
// ExecContext in the schema-writing surface (held there by
// TestSchemaWriterEmittedDDLGoesThroughTheSingleStatementDoor). It
// refuses a structurally multi-statement string with the coded error
// BEFORE the server sees it, and otherwise executes it unchanged.
func execEmittedDDL(ctx context.Context, ex ddlExecer, stmt string) error {
	if verr := assertSingleDDLStatement(stmt); verr != nil {
		return sluicecode.Wrap(sluicecode.CodeDDLEmitMultiStatement,
			"the emitted DDL came from recorded schema content (a backup manifest or a read source schema); "+
				"if this is a restore, the chain's recorded schema is corrupt or tampered — verify the chain's "+
				"signature (`backup verify --verify-key …`) and take a fresh `backup full` of the live source; "+
				"nothing was executed",
			fmt.Errorf("postgres: refusing to execute emitted DDL that is not a single statement (%v); "+
				"sluice's emitters only ever produce one statement, so this content did not come from a "+
				"faithful emit: %.120q", verr, stmt))
	}
	_, err := ex.ExecContext(ctx, stmt)
	return err
}
