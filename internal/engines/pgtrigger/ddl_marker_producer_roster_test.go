// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The DDL-MARKER PRODUCER ROSTER (audit 2026-09-15 A0915-PG-MEDIUM-1).
//
// # The class
//
// Every op='X' row the change log carries is written by a capture function
// setup.go renders, and the reader grades an X row that arrives under a
// FOREIGN schema by ONE field of its payload: `captured_relid`, the OID of
// the captured relation the command concerns ([ddlMarkerRelIDKey],
// [CDCReader.rowInCaptureScope]). A producer that omits the field writes a
// marker the reader decodes to relID 0 — which, by the security rule, can
// never be in scope from a foreign schema — so that producer's markers are
// silently discarded for any captured table that has been moved with
// `ALTER TABLE … SET SCHEMA`. v0.148.2 added the field to the
// `ddl_command_end` arm and left the `sql_drop` arm without it: a moved
// captured table's DROP was discarded and the stream ran on at exit 0
// (observed on PG 16.15 and 18.6). The commit that shipped the field
// enumerated the sibling and EXEMPTED it with a reason that was false
// ("a dropped relation's marker carries the OLD identity in this reader's
// schema") — pg_event_trigger_dropped_objects().schema_name is where the
// table lived at drop time, which after a move is the foreign schema.
//
// # How the universe is derived (not hand-listed)
//
// From setup.go's AST: every function whose string literals, concatenated
// in source order with SQL `--` comments removed, contain an `INSERT INTO`
// and the op literal `'X'` is a marker producer. A producer this file does
// not classify FAILS; a classified producer must either render
// `captured_relid` into its jsonb payload or be named in the exemption
// map with a written reason. The anti-vacuity floor is the true count —
// two — so a moved render, a rename, or a comment-stripping bug that
// empties the walk fails the gate rather than greening it.
//
// # WHAT THIS GATE REACHES, stated so the name cannot be read as broader
// than the truth
//
// The RENDERED SQL of every producer in setup.go, and only that: it proves
// each producer's payload names the key. It does not prove the value is
// the RIGHT OID — that is measured on a real server by
// TestCaptureDropTier_DroppedCapturedTableRefusesAtResume (the drop arm's
// recorded OID equals the dropped table's) and by
// TestCDCReader_DDLRefusal_ForeignSchemaMarkers (the reader halts on it).
// A producer rendered outside setup.go is outside this walk; say so if
// you add one.

package pgtrigger

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// ddlMarkerProducerRender maps each producer function the roster derives
// to a render call. A producer absent here fails the roster: deciding how
// it is rendered — and therefore graded — is the point.
var ddlMarkerProducerRender = map[string]func() string{
	"renderCaptureDDLFunction":  func() string { return renderCaptureDDLFunction(testSchema, testChangeLogRef(), testMetaRef()) },
	"renderCaptureDropFunction": func() string { return renderCaptureDropFunction(testSchema, testChangeLogRef(), testMetaRef()) },
}

// ddlMarkerRelIDExempt names producers that deliberately do NOT record
// captured_relid, with the reason. Empty today: both arms know the OID
// (the ddl_command_end arm from its captured_kin lookup, the sql_drop arm
// from the dropped-object set) and there is no producer for which "no OID"
// is the truthful answer.
var ddlMarkerRelIDExempt = map[string]string{}

// sqlLineComment strips PL/pgSQL `--` comments, which mention op='X' in
// prose and would otherwise make the row capture function look like a
// producer.
var sqlLineComment = regexp.MustCompile(`(?m)--[^\n]*`)

// ddlMarkerProducers walks setup.go and returns the names of every function
// whose rendered literals write an op='X' row.
func ddlMarkerProducers(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "setup.go", nil, 0)
	if err != nil {
		t.Fatalf("parse setup.go: %v", err)
	}
	var out []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		var text strings.Builder
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatalf("%s: unquote %s: %v", fn.Name.Name, fset.Position(lit.Pos()), err)
			}
			text.WriteString(s)
			return true
		})
		body := sqlLineComment.ReplaceAllString(text.String(), "")
		if strings.Contains(body, "INSERT INTO") && strings.Contains(body, "'X'") {
			out = append(out, fn.Name.Name)
		}
	}
	sort.Strings(out)
	return out
}

func TestDDLMarkerProducerRoster_EveryProducerRecordsCapturedRelID(t *testing.T) {
	producers := ddlMarkerProducers(t)
	if len(producers) < 2 {
		t.Fatalf("the walk over setup.go found %d op='X' producer(s) %v; the true count is 2 (ddl_command_end and sql_drop). "+
			"A floor below the truth is how a gate goes vacuous — if a producer moved or the walk broke, fix the walk, not the floor",
			len(producers), producers)
	}
	key := "'" + ddlMarkerRelIDKey + "'"
	for _, name := range producers {
		t.Run(name, func(t *testing.T) {
			if reason, ok := ddlMarkerRelIDExempt[name]; ok {
				if strings.TrimSpace(reason) == "" {
					t.Fatalf("%s is exempt from recording %s with an empty reason; an exemption without a reason is a hole with a name", name, ddlMarkerRelIDKey)
				}
				t.Logf("%s: exempt — %s", name, reason)
				return
			}
			render, ok := ddlMarkerProducerRender[name]
			if !ok {
				t.Fatalf("%s writes op='X' rows but is not classified by this roster: add it to ddlMarkerProducerRender "+
					"(and make it record %s) or to ddlMarkerRelIDExempt with a reason", name, ddlMarkerRelIDKey)
			}
			sql := render()
			at := strings.Index(sql, "INSERT INTO")
			if at < 0 {
				t.Fatalf("%s's render carries no INSERT INTO although its source literals do; the walk and the render disagree", name)
			}
			values := sql[at:]
			if !strings.Contains(values, "jsonb_build_object(") {
				t.Fatalf("%s's INSERT does not build its payload with jsonb_build_object; the roster cannot grade it", name)
			}
			if !strings.Contains(values, key) {
				t.Fatalf("%s writes op='X' markers WITHOUT %s. The reader grades a marker that arrives under a foreign "+
					"schema by that OID alone, so every marker this arm writes for a captured table moved with "+
					"ALTER TABLE … SET SCHEMA is discarded and the stream runs on at exit 0 (A0915-PG-MEDIUM-1)", name, key)
			}
		})
	}
	// The classification table must not carry names the walk no longer
	// finds: a renamed producer would otherwise keep a stale entry green.
	for name := range ddlMarkerProducerRender {
		if !containsString(producers, name) {
			t.Errorf("ddlMarkerProducerRender names %q, which the walk over setup.go did not find as an op='X' producer; "+
				"a stale entry hides a rename", name)
		}
	}
	for name := range ddlMarkerRelIDExempt {
		if !containsString(producers, name) {
			t.Errorf("ddlMarkerRelIDExempt names %q, which the walk did not find; an exemption for a producer that no longer exists is noise", name)
		}
	}
}
