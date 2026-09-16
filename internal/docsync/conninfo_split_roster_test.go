// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package docsync

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// A libpq connection string may not be split on whitespace, and this is the
// gate that makes a ninth such site impossible to add quietly.
//
// # The class
//
// `password='s schema=x' dbname=real` is TWO settings to libpq and pgx: a
// password of `s schema=x`, and a dbname. A `strings.Fields` walk sees three
// tokens. Every site that then RE-JOINED the survivors deleted the middle one
// from the string handed to the driver — a truncated credential, not a working
// connection. Audit 2026-09-15 filed this as A0915-VF2-PGDSN-1 against ONE
// function; the mechanical sweep found NINE sites, of which four were
// rewriters carrying exactly that corruption, one was a detector whose
// false positive skipped an application_name stamp, and the rest were readers.
//
// The fix routes every in-engine site through the single conninfo grammar in
// `internal/engines/postgres/conninfo.go`. This gate is the ratchet: without
// it, the tenth site is written by whoever next needs to pull a value out of a
// DSN, and nothing notices until it meets a quoted value in production.
//
// # Why the floor is on the MATCHER, not on the findings
//
// The obvious anti-vacuity floor — "expect at least N connection-string
// splits" — would be exactly wrong here, because the whole point is to drive
// that number to zero. A gate that fails when its class is finally closed
// teaches people to delete the gate.
//
// So the floor is on the walk's REACH: every `strings.Fields` call site in
// non-test code, connection-string or not (22 across 12 files when this
// landed). If that collapses, the matcher broke and the gate is blind; the
// findings count is free to fall to zero, which is the success condition.
//
// # Scope, stated so the name is not read as broader than the truth
//
// This grades ONE spelling — `strings.Fields` over an expression whose source
// text names a DSN. It does not see `strings.Split(dsn, " ")`, a hand-rolled
// byte loop, or a DSN reached through a variable named something else. That
// is deliberate: the sweep that produced this roster searched those spellings
// too and found none, so a broader matcher would add false-positive surface
// for no coverage. If a future sweep finds one, widen the matcher here rather
// than filing a new gate beside it.
const connInfoSplitReachFloor = 15

// connInfoSplitRoster classifies every site that splits a connection string
// on whitespace. An entry is a REASON the site may keep doing so; a site
// absent from this map fails the build.
//
// Both entries are READERS — they extract host/port/database for a label and
// never rebuild the string — so their worst case is a wrong label, never a
// corrupted connection. They cannot use the engine's grammar for an
// architectural reason, not an oversight: `internal/pipeline` never imports a
// specific engine package (CLAUDE.md's IR-first tenet), and moving a
// Postgres-specific conninfo grammar into a neutral package to reach them
// would break the same tenet from the other side. The real fix is the one the
// v0.154.0 source-identity work already reached for: ask the ENGINE what a DSN
// names, which deleted the pipeline's other DSN knowledge. Filed as the
// follow-up to A0915-VF2-PGDSN-1.
var connInfoSplitRoster = map[string]string{
	"internal/pipeline/streamer.go::redactedHost": "READER: builds a redacted host:port label for logs and the auto-derived migration id. " +
		"Pipeline may not import an engine package; follow-up is to ask the engine (see A0915-VF2-PGDSN-1).",
	"internal/pipeline/target_schema.go::extractDSNTriple": "READER: extracts host/port/database for a target label. " +
		"Pipeline may not import an engine package; follow-up is to ask the engine (see A0915-VF2-PGDSN-1).",
}

func TestConnInfoSplitRoster_EveryDSNWhitespaceSplitClassified(t *testing.T) {
	reach, candidates := discoverConnInfoSplits(t)

	if reach < connInfoSplitReachFloor {
		t.Fatalf("anti-vacuity: the walk found only %d strings.Fields call sites in non-test code (floor %d) — "+
			"the AST matcher is broken and this gate is blind. NOTE the floor is on the matcher's REACH, not on "+
			"connection-string findings: those are free to reach zero, which is the goal.", reach, connInfoSplitReachFloor)
	}

	var unclassified []string
	for key, pos := range candidates {
		if _, ok := connInfoSplitRoster[key]; !ok {
			unclassified = append(unclassified, key+"  ("+pos+")")
		}
	}
	if len(unclassified) > 0 {
		sort.Strings(unclassified)
		t.Fatalf("connection string(s) split on whitespace at un-classified site(s):\n  %s\n\n"+
			"A libpq key/value DSN cannot be tokenised with strings.Fields: a SINGLE-QUOTED value may contain "+
			"spaces and `=`, so `password='s schema=x' dbname=real` is TWO settings, not three tokens. A site "+
			"that re-joins the survivors DELETES one from the string handed to the driver (audit "+
			"A0915-VF2-PGDSN-1 — a truncated credential).\n\n"+
			"In an engine package, use the one grammar in internal/engines/postgres/conninfo.go:\n"+
			"  reading  — parseKVFields(dsn)[\"key\"]\n"+
			"  stripping — stripSettings(dsn, \"key\") (excises the byte span; re-renders nothing)\n"+
			"  from another package — postgres.SplitSchemaFromKVDSN(dsn)\n\n"+
			"If the site genuinely cannot (internal/pipeline may not import an engine package), add it to "+
			"connInfoSplitRoster with that REASON and the follow-up.",
			strings.Join(unclassified, "\n  "))
	}

	// The reverse guard: an exemption whose site vanished is a dead
	// classification, and a roster nobody prunes becomes a permanent allow.
	for key := range connInfoSplitRoster {
		if _, ok := candidates[key]; !ok {
			t.Errorf("stale connInfoSplitRoster entry %q: no such connection-string split remains. "+
				"If it was fixed, DELETE the entry — that is the class closing.", key)
		}
	}
}

// discoverConnInfoSplits walks every non-test Go file under internal/ and
// returns (total strings.Fields call sites, connection-string ones keyed by
// "<rel>::<func>").
//
// "Connection string" is decided on the ARGUMENT's source text naming a DSN,
// which is what separates `strings.Fields(dsn)` from the tree's many innocent
// splits of log lines and command output.
func discoverConnInfoSplits(t *testing.T) (reach int, candidates map[string]string) {
	t.Helper()

	root := repoRootFromDocsync(t)
	internalDir := filepath.Join(root, "internal")
	candidates = map[string]string{}
	fset := token.NewFileSet()

	err := filepath.WalkDir(internalDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if name == "testdata" || name == "workspace" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %q: %v", path, perr)
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)

		ast.Inspect(file, func(n ast.Node) bool {
			fd, ok := n.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				return true
			}
			fn := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				fn = "(" + receiverTypeName(fd.Recv.List[0].Type) + ")." + fn
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || exprText(fset, call.Fun) != "strings.Fields" {
					return true
				}
				reach++
				if len(call.Args) != 1 || !namesADSN(exprText(fset, call.Args[0])) {
					return true
				}
				key := rel + "::" + fn
				if _, dup := candidates[key]; !dup {
					candidates[key] = fset.Position(call.Pos()).String()
				}
				return true
			})
			return false
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %q: %v", internalDir, err)
	}
	return reach, candidates
}

// namesADSN reports whether an argument's source text names a connection
// string. Deliberately textual: the alternative is type resolution, which
// would pull go/types into a gate whose whole value is being cheap enough
// that nobody deletes it.
func namesADSN(argText string) bool {
	lower := strings.ToLower(argText)
	for _, needle := range []string{"dsn", "conninfo", "connstr", "connectionstring"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}
