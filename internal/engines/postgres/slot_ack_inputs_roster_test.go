// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// GC-41 (b)'s gate. The slot ack had two inputs before GC-41: the
// consumer-released ceiling, and ADR-0020's applied-LSN tracker fed by the
// Postgres applier's commit paths. The tracker was an input only as good
// as the set of write sites that remembered to feed it, and the concurrent
// lanes' checkpoint did not — so on the default lane path it stayed at 0
// and pinned confirmed_flush_lsn at the start position for the life of the
// stream. The fix is not a report from that one site; it is that the ack
// has no input a write site must remember to feed. Its one input is the
// ceiling, raised only by ReleaseSlotAckTo, whose callers release positions
// read back from durable storage (the pipeline's slot-ack ceiling sidecar
// reads the target's control row; the backup chain releases committed
// manifest ends — both enumerated by the pipeline's
// TestSlotAckReleaseRoster_EveryStreamChangesSiteReleases).
//
// This roster holds that shape from the reader's side: every CDCReader
// field ackLSN reads must be listed here with the reason it is durable
// evidence (fail-by-default for a new one), and every write to the ceiling
// must sit in ReleaseSlotAckTo. Reach: this package's production files.

// slotAckInputs are the CDCReader fields ackLSN may read.
var slotAckInputs = map[string]string{
	"ackCeil": "the consumer-released ceiling; raised only by ReleaseSlotAckTo, from positions the " +
		"consumer holds durably",
}

// ackCeilWriters are the functions allowed to mutate ackCeil.
var ackCeilWriters = map[string]bool{"ReleaseSlotAckTo": true}

func TestSlotAckInputsRoster_EveryInputIsDurableEvidence(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var (
		foundAckLSN bool
		readFields  = map[string]bool{}
		ceilWrites  int
		badWriters  []string
	)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			if fd.Name.Name == "ackLSN" && fd.Recv != nil && len(fd.Recv.List) == 1 && len(fd.Recv.List[0].Names) == 1 {
				foundAckLSN = true
				recv := fd.Recv.List[0].Names[0].Name
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv {
							readFields[sel.Sel.Name] = true
						}
					}
					return true
				})
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				method, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch method.Sel.Name {
				case "Store", "CompareAndSwap", "Add", "Swap":
				default:
					return true
				}
				if field, ok := method.X.(*ast.SelectorExpr); ok && field.Sel.Name == "ackCeil" {
					ceilWrites++
					if !ackCeilWriters[fd.Name.Name] {
						badWriters = append(badWriters, fd.Name.Name+" ("+fset.Position(call.Pos()).String()+")")
					}
				}
				return true
			})
		}
	}

	if !foundAckLSN {
		t.Fatal("anti-vacuity: no ackLSN method found — the walk is broken or the ack moved; re-point this roster")
	}
	if !readFields["ackCeil"] {
		t.Error("ackLSN no longer reads ackCeil: the consumer-released ceiling is not bounding the slot ack (GC-41)")
	}
	if ceilWrites == 0 {
		t.Fatal("anti-vacuity: found no write to ackCeil anywhere — the matcher is broken")
	}

	var unlisted []string
	for field := range readFields {
		if _, ok := slotAckInputs[field]; !ok {
			unlisted = append(unlisted, field)
		}
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		t.Errorf("ackLSN reads CDCReader field(s) %v that slotAckInputs does not list.\n"+
			"Every input to the slot ack must be DURABLE evidence of what the consumer holds, raised from "+
			"storage rather than reported by write sites that must each remember to call it — GC-41 (b) was "+
			"exactly such an input (ADR-0020's tracker, which the lane checkpoint never fed). List the field "+
			"with that reason, or bound the ack by the ceiling instead.", unlisted)
	}
	if len(badWriters) > 0 {
		sort.Strings(badWriters)
		t.Errorf("ackCeil is written outside ReleaseSlotAckTo: %v — the ceiling must move only on a "+
			"consumer's durable release (GC-41)", badWriters)
	}
}
