// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// The broker's applied prefix — how many leading links it has already
// applied — decides two things at once: where the apply loop starts, and
// which severed-transaction findings only WARN (backup.SeveredTransactionDoor.CheckFrom).
// A wrong value either re-applies history or WARNs-and-passes a finding on a
// link the broker is about to apply. So it is pinned twice: by VALUE, against
// the order the real producer wrote the links in, and by DATA FLOW, that both
// consumers read the one value brokerAppliedPrefix returned.

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBrokerAppliedPrefix_IsTheIndexAfterLastApplied writes a full and three
// incrementals with the real IncrementalBackup writer, resolves the chain the
// way the broker does (lineage.BuildLineageChain), and checks the prefix for
// every possible last-applied link. The independent expected value is the
// writer's own order — each Run's ParentRef is the previous Run's BackupID —
// not anything derived from the chain under test.
func TestBrokerAppliedPrefix_IsTheIndexAfterLastApplied(t *testing.T) {
	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	schema, _ := ddlWindowSchemas()
	full := writeDDLWindowParent(t, store, schema)
	written := []string{full.BackupID}
	for k := 1; k <= 3; k++ {
		at := ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"slot":"sluice_slot","lsn":"0/%X"}`, 0x100+0x10*k)}
		now := time.Date(2026, 8, 7, 11, k, 0, 0, time.UTC)
		b := &IncrementalBackup{
			Source: &fakeCDCEngine{
				name:           "postgres",
				schemaSequence: []*ir.Schema{schema},
				cdcChanges: []ir.Change{
					ir.TxBegin{Position: at},
					ir.Insert{Position: at, Table: "users", Row: ir.Row{"id": int64(k)}},
					ir.TxCommit{Position: at},
				},
				cdcExpectedFromOK: true,
			},
			SourceDSN: "src", Store: store, ParentRef: written[len(written)-1],
			Window: 5 * time.Minute, ChunkChanges: 10, SluiceVersion: "test",
			Now: func() time.Time { return now }, clockNow: func() time.Time { return now },
		}
		if err := b.Run(ctx); err != nil {
			t.Fatalf("incremental %d: %v", k, err)
		}
		written = append(written, newestIncrementalChild(t, store, written[len(written)-1]))
	}

	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != len(written) {
		t.Fatalf("chain has %d links; the writer wrote %d", len(chain), len(written))
	}
	if got, err := brokerAppliedPrefix(chain, ""); err != nil || got != 0 {
		t.Fatalf("nothing applied: prefix = %d, %v; want 0", got, err)
	}
	for k, id := range written {
		got, err := brokerAppliedPrefix(chain, id)
		if err != nil {
			t.Fatalf("last applied = written[%d]: %v", k, err)
		}
		if got != k+1 {
			t.Errorf("last applied = written[%d]: prefix = %d; want %d", k, got, k+1)
		}
		// The link the apply loop starts on must be the one written next.
		if k+1 < len(written) {
			if next := lineage.ManifestBackupID(chain[got].Manifest); next != written[k+1] {
				t.Errorf("last applied = written[%d]: the loop would start on %s; the writer's next link is %s", k, next, written[k+1])
			}
		}
	}
	if _, err := brokerAppliedPrefix(chain, "not-in-this-chain"); err == nil {
		t.Error("an unknown last-applied id resolved instead of refusing")
	}
}

// newestIncrementalChild returns the BackupID of the incremental whose parent
// is parentID.
func newestIncrementalChild(t *testing.T, store *blobcodec.LocalStore, parentID string) string {
	t.Helper()
	records, err := lineage.ListAllManifestsViaWalk(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.Manifest.Kind == irbackup.BackupKindIncremental && r.Manifest.ParentBackupID == parentID {
			return r.Manifest.BackupID
		}
	}
	t.Fatalf("no incremental chains off %s", parentID)
	return ""
}

// appliedPrefixFeedsBothConsumers is the data-flow half, over the AST of
// replayNewIncrementals: the severed-transaction door's applied-prefix
// argument and the apply loop's start are the SAME identifier, and that
// identifier is assigned exactly once, from brokerAppliedPrefix. Resolved by
// name inside one function body (no type information), which is sound for
// the function as written; a shadowing declaration would also have to be a
// second assignment, which the count catches.
func appliedPrefixFeedsBothConsumers(t *testing.T) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "broker.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.BlockStmt
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "replayNewIncrementals" {
			body = fd.Body
		}
	}
	if body == nil {
		t.Fatal("replayNewIncrementals not found in broker.go — re-anchor this gate")
	}

	doorArg, loopStart := "", ""
	assigned := map[string][]ast.Expr{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.CallExpr:
			if sel, ok := s.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "refuseSeveredTransactions" && len(s.Args) == 3 {
				if id, ok := s.Args[2].(*ast.Ident); ok {
					doorArg = id.Name
				}
			}
		case *ast.ForStmt:
			if callsMethod(s.Body, "applyIncremental") {
				if as, ok := s.Init.(*ast.AssignStmt); ok && len(as.Rhs) == 1 {
					if id, ok := as.Rhs[0].(*ast.Ident); ok {
						loopStart = id.Name
					}
				}
			}
		case *ast.AssignStmt:
			for i, l := range s.Lhs {
				id, ok := l.(*ast.Ident)
				if !ok {
					continue
				}
				switch {
				case len(s.Rhs) == len(s.Lhs):
					assigned[id.Name] = append(assigned[id.Name], s.Rhs[i])
				case len(s.Rhs) == 1 && i == 0:
					assigned[id.Name] = append(assigned[id.Name], s.Rhs[0])
				default:
					assigned[id.Name] = append(assigned[id.Name], nil)
				}
			}
		case *ast.IncDecStmt:
			if id, ok := s.X.(*ast.Ident); ok {
				assigned[id.Name] = append(assigned[id.Name], nil)
			}
		}
		return true
	})
	if doorArg == "" {
		t.Fatal("replayNewIncrementals does not pass an identifier as the severed-transaction door's applied prefix")
	}
	if loopStart != doorArg {
		t.Fatalf("the apply loop starts at %q but the door is told the applied prefix is %q — the two must be one value", loopStart, doorArg)
	}
	src := assigned[doorArg]
	if len(src) != 1 {
		t.Fatalf("%s is assigned %d times in replayNewIncrementals; want exactly once, from brokerAppliedPrefix", doorArg, len(src))
	}
	call, ok := src[0].(*ast.CallExpr)
	if !ok {
		t.Fatalf("%s is not assigned from a call", doorArg)
	}
	if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "brokerAppliedPrefix" {
		t.Fatalf("%s is not assigned from brokerAppliedPrefix", doorArg)
	}
}

func callsMethod(n ast.Node, name string) bool {
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok {
			if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
				found = true
			}
		}
		return !found
	})
	return found
}
