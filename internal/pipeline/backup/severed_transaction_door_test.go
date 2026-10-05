// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// F-E1-SEVERED-TAIL-REPLAY door pins, on HAND-BUILT chains shaped the way
// an older binary wrote them (the writers in this tree can no longer produce
// either shape, so a fixture built by running them would prove nothing about
// the chains the door exists for — the 2026-07-28 self-referential-fixture
// rule). The positions mimic the real engines' conventions: Postgres rows
// carry their transaction's commit LSN, so every row of one transaction
// shares a position; MySQL GTID rows carry the pre-transaction set.

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// sevLSN is a Postgres-shaped position whose order the fake comparator
// below reads.
func sevLSN(n int) ir.Position {
	return ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":%d}`, n)}
}

// sevLSNComparator orders sevLSN positions — the stand-in for the Postgres
// engine's comparator, which the backup package cannot import.
type sevLSNComparator struct{}

func (sevLSNComparator) PrecedesOrEqual(a, b ir.Position) (bool, error) {
	pa, err := sevLSNValue(a)
	if err != nil {
		return false, err
	}
	pb, err := sevLSNValue(b)
	if err != nil {
		return false, err
	}
	return pa <= pb, nil
}

func sevLSNValue(p ir.Position) (int, error) {
	s := strings.TrimSuffix(strings.TrimPrefix(p.Token, `{"lsn":`), "}")
	return strconv.Atoi(s)
}

// sevTx is one source transaction at commit position lsn: TxBegin, its rows
// (all carrying lsn, as Postgres stamps them), TxCommit at lsn+1 (the
// post-commit position a current binary records).
func sevTx(lsn int, rows ...string) []ir.Change {
	out := []ir.Change{ir.TxBegin{Position: sevLSN(lsn)}}
	for _, r := range rows {
		out = append(out, ir.Insert{Position: sevLSN(lsn), Table: "kl", Row: ir.Row{"v": r}})
	}
	return append(out, ir.TxCommit{Position: sevLSN(lsn + 1)})
}

// sevWriteIncremental stores changes as one incremental, split into chunks
// of chunkSize changes, and returns its link.
func sevWriteIncremental(t *testing.T, store irbackup.Store, name string, chunkSize int, changes []ir.Change) lineage.SegmentRecord {
	t.Helper()
	var chunks []*irbackup.ChunkInfo
	for start := 0; start < len(changes); start += chunkSize {
		end := min(start+chunkSize, len(changes))
		var buf bytes.Buffer
		cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range changes[start:end] {
			if err := cw.WriteChange(c); err != nil {
				t.Fatal(err)
			}
		}
		if err := cw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("chunks/_changes/%s/changes-%d.jsonl.gz", name, len(chunks))
		if err := store.Put(context.Background(), path, bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, &irbackup.ChunkInfo{File: path, RowCount: cw.ChangeCount(), SHA256: cw.Hash()})
	}
	return lineage.SegmentRecord{
		Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{
			Path: "manifests/" + name + ".json",
			Manifest: &irbackup.Manifest{
				BackupID:     name,
				SourceEngine: "postgres",
				Kind:         irbackup.BackupKindIncremental,
				ChangeChunks: chunks,
			},
		},
	}
}

func sevFull() lineage.SegmentRecord {
	return lineage.SegmentRecord{
		Segment: &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{
			Path:     lineage.ManifestFileName,
			Manifest: &irbackup.Manifest{BackupID: "full", SourceEngine: "postgres", Kind: irbackup.BackupKindFull},
		},
	}
}

func sevStore(t *testing.T) *blobcodec.LocalStore {
	t.Helper()
	s, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func cat(parts ...[]ir.Change) []ir.Change {
	var out []ir.Change
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func requireSevered(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatal("door passed a chain that carries one source transaction in two incrementals")
	}
	if c, ok := sluicecode.FromError(err); !ok || c.Code != sluicecode.CodeBackupChainSeveredTransaction {
		t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeBackupChainSeveredTransaction, err)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("refusal does not name %q: %v", want, err)
	}
}

// TestSeveredTransactionDoor_SeveredTail is shape (A), the measured one: N
// ends with T open (TxBegin + some of T's rows, no TxCommit) and N+1
// re-delivers ALL of T. Refused; and the same severed incremental as the
// chain's LAST link (the live tail) passes — nothing follows it.
func TestSeveredTransactionDoor_SeveredTail(t *testing.T) {
	store := sevStore(t)
	tHead := sevTx(200, "a", "b", "c")
	tHead = tHead[:len(tHead)-1-1] // TxBegin, a, b — severed before c and the commit
	n := sevWriteIncremental(t, store, "n", 100, cat(sevTx(100, "x"), tHead))
	n1 := sevWriteIncremental(t, store, "n1", 100, cat(sevTx(200, "a", "b", "c"), sevTx(300, "y")))

	door := &SeveredTransactionDoor{Store: store}
	requireSevered(t, door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), n, n1}),
		"ends inside an open source transaction")

	// The live tail: N severed but final. Not refused.
	door = &SeveredTransactionDoor{Store: store}
	if err := door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), n}); err != nil {
		t.Fatalf("a severed FINAL incremental (the live tail) was refused: %v", err)
	}
}

// TestSeveredTransactionDoor_SeveredTailAcrossChunks: T is longer than a
// chunk, so N's last chunk is all rows with no marker and the TxBegin sits
// in an earlier chunk. The tail walk must reach it.
func TestSeveredTransactionDoor_SeveredTailAcrossChunks(t *testing.T) {
	store := sevStore(t)
	rows := make([]string, 9)
	for i := range rows {
		rows[i] = strconv.Itoa(i)
	}
	full := sevTx(200, rows...)
	severed := full[:len(full)-1] // drop only the commit
	// chunkSize 4: [Begin(100) x Commit(100) Begin(200)] [r0 r1 r2 r3] [r4 r5 r6 r7] [r8]
	n := sevWriteIncremental(t, store, "n", 4, cat(sevTx(100, "x"), severed))
	if got := len(n.Manifest.ChangeChunks); got < 3 {
		t.Fatalf("fixture: want the open transaction to span chunks, got %d chunks", got)
	}
	n1 := sevWriteIncremental(t, store, "n1", 100, full)

	door := &SeveredTransactionDoor{Store: store}
	requireSevered(t, door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), n, n1}),
		"ends inside an open source transaction")
}

// TestSeveredTransactionDoor_RedeliveredBoundary is shape (B), the pre-v0.138.0
// Postgres chain: N is complete, but its last transaction's commit position
// was recorded at the commit LSN, so N+1's resume re-delivered that whole
// transaction. Refused with the Postgres comparator; with no comparator
// (a source engine that does not order positions) (B) is not judged.
func TestSeveredTransactionDoor_RedeliveredBoundary(t *testing.T) {
	store := sevStore(t)
	n := sevWriteIncremental(t, store, "n", 100, cat(sevTx(100, "x"), sevTx(200, "a", "b")))
	n1 := sevWriteIncremental(t, store, "n1", 100, cat(sevTx(200, "a", "b"), sevTx(300, "y")))
	links := []lineage.SegmentRecord{sevFull(), n, n1}

	door := &SeveredTransactionDoor{Store: store, Comparator: sevLSNComparator{}}
	requireSevered(t, door.Check(context.Background(), links), "at or before the last rows")

	door = &SeveredTransactionDoor{Store: store}
	if err := door.Check(context.Background(), links); err != nil {
		t.Fatalf("shape (B) judged without a comparator: %v", err)
	}
}

// TestSeveredTransactionDoor_HealthyChainsPass is the false-refusal
// direction, which matters as much: every shape a CURRENT binary writes, and
// the near-misses, must pass.
func TestSeveredTransactionDoor_HealthyChainsPass(t *testing.T) {
	store := sevStore(t)
	cases := map[string][]lineage.SegmentRecord{
		// The ordinary chain: each window ends at a TxCommit, the next opens
		// with a later transaction.
		"closed windows": {
			sevFull(),
			sevWriteIncremental(t, store, "a1", 3, cat(sevTx(100, "x"), sevTx(200, "y"))),
			sevWriteIncremental(t, store, "a2", 3, cat(sevTx(300, "z"))),
			sevWriteIncremental(t, store, "a3", 3, cat(sevTx(400, "w"))),
		},
		// The keepalive-boundary edge (GC-41 (j)): N ends on a boundary-only
		// transaction at the walsender position W, and the next transaction's
		// commit record starts exactly at W, so its rows carry W. Comparing
		// against N's last END position would call this a re-delivery; the
		// door compares against N's last ROWS (lsn 100) and passes it.
		"keepalive boundary then a commit at the same LSN": {
			sevFull(),
			sevWriteIncremental(t, store, "b1", 100, cat(sevTx(100, "x"), []ir.Change{
				ir.TxBegin{Position: sevLSN(500)}, ir.TxCommit{Position: sevLSN(500)},
			})),
			sevWriteIncremental(t, store, "b2", 100, sevTx(500, "y")),
		},
		// A marker-less stream (the trigger-CDC sources): rows only, every
		// change its own boundary.
		"marker-less stream": {
			sevFull(),
			sevWriteIncremental(t, store, "c1", 2, []ir.Change{
				ir.Insert{Position: sevLSN(1), Table: "t", Row: ir.Row{"v": 1}},
				ir.Insert{Position: sevLSN(2), Table: "t", Row: ir.Row{"v": 2}},
				ir.Insert{Position: sevLSN(3), Table: "t", Row: ir.Row{"v": 3}},
			}),
			sevWriteIncremental(t, store, "c2", 2, []ir.Change{
				ir.Insert{Position: sevLSN(4), Table: "t", Row: ir.Row{"v": 4}},
			}),
		},
		// A segment's full between two incrementals breaks the pair: the
		// rotation overlap (P_N, S] is snapshot-versus-change, not this door's.
		"rotation full between incrementals": {
			sevFull(),
			sevWriteIncremental(t, store, "d1", 100, sevTx(200, "a")),
			sevFull(),
			sevWriteIncremental(t, store, "d2", 100, sevTx(200, "a")),
		},
		// An incremental with no rows at all (only a boundary pair) between
		// two healthy ones.
		"empty middle": {
			sevFull(),
			sevWriteIncremental(t, store, "e1", 100, sevTx(100, "x")),
			sevWriteIncremental(t, store, "e2", 100, []ir.Change{ir.TxBegin{Position: sevLSN(150)}, ir.TxCommit{Position: sevLSN(150)}}),
			sevWriteIncremental(t, store, "e3", 100, sevTx(200, "y")),
		},
	}
	for name, links := range cases {
		t.Run(name, func(t *testing.T) {
			door := &SeveredTransactionDoor{Store: store, Comparator: sevLSNComparator{}}
			if err := door.Check(context.Background(), links); err != nil {
				t.Fatalf("healthy chain refused: %v", err)
			}
		})
	}
}

// TestSeveredTransactionDoor_CachesPerLink pins the broker's cost claim: a
// second Check over the same chain plus one new link decodes only the new
// link (the store would refuse a re-read of a deleted chunk).
func TestSeveredTransactionDoor_CachesPerLink(t *testing.T) {
	store := sevStore(t)
	a := sevWriteIncremental(t, store, "a", 100, sevTx(100, "x"))
	b := sevWriteIncremental(t, store, "b", 100, sevTx(200, "y"))
	door := &SeveredTransactionDoor{Store: store, Comparator: sevLSNComparator{}}
	if err := door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), a, b}); err != nil {
		t.Fatal(err)
	}
	for _, l := range []lineage.SegmentRecord{a, b} {
		for _, c := range l.Manifest.ChangeChunks {
			if err := store.Delete(context.Background(), c.File); err != nil {
				t.Fatal(err)
			}
		}
	}
	c := sevWriteIncremental(t, store, "c", 100, sevTx(300, "z"))
	if err := door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), a, b, c}); err != nil {
		t.Fatalf("a cached link was re-read (its chunks are gone): %v", err)
	}
	// And a decode failure is loud, not a silent pass.
	d := sevWriteIncremental(t, store, "d", 100, sevTx(400, "w"))
	for _, ch := range d.Manifest.ChangeChunks {
		_ = store.Delete(context.Background(), ch.File)
	}
	err := (&SeveredTransactionDoor{Store: store}).Check(context.Background(), []lineage.SegmentRecord{sevFull(), d, c})
	if err == nil || !strings.Contains(err.Error(), "severed-transaction door") {
		t.Fatalf("an unreadable chunk passed the door: %v", err)
	}
}

// TestVerifySeveredTransactions pins `backup verify`'s run of the door: a
// severed chain is reported with the code restore refuses with, and an
// encrypted chain verified WITHOUT key material skips the check (it cannot
// decode the chunks) instead of reading as a pass — the skip is the WARN.
func TestVerifySeveredTransactions(t *testing.T) {
	store := sevStore(t)
	head := sevTx(200, "a", "b")
	n := sevWriteIncremental(t, store, "n", 100, cat(sevTx(100, "x"), head[:len(head)-1]))
	n1 := sevWriteIncremental(t, store, "n1", 100, sevTx(200, "a", "b"))
	chain := []lineage.SegmentRecord{sevFull(), n, n1}

	requireSevered(t, verifySeveredTransactions(context.Background(), store, chain, true, false, false, &chunkAuthProber{}),
		"ends inside an open source transaction")
	if err := verifySeveredTransactions(context.Background(), store, chain, true, true, false, &chunkAuthProber{}); err != nil {
		t.Fatalf("an unkeyed encrypted chain must skip (it cannot decode), got %v", err)
	}
}

// TestVerifyBackupScanRunsTheSeveredDoor holds the WIRING: verifyBackupScan
// calls verifySeveredTransactions, so verify cannot drift from restore again
// (the Bug 217/218 shape) by losing the call.
func TestVerifyBackupScanRunsTheSeveredDoor(t *testing.T) {
	src, err := os.ReadFile("restore.go")
	if err != nil {
		t.Fatal(err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), "restore.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "verifyBackupScan" {
			return true
		}
		ast.Inspect(fd.Body, func(m ast.Node) bool {
			if call, ok := m.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "verifySeveredTransactions" {
					found = true
				}
			}
			return true
		})
		return false
	})
	if !found {
		t.Fatal("verifyBackupScan no longer calls verifySeveredTransactions: `backup verify` would report healthy a chain restore refuses with SLUICE-E-BACKUP-CHAIN-SEVERED-TRANSACTION")
	}
}

// sevWithFill marks a link as carrying an ADD COLUMN fill (v0.156.1+): the
// capture lanes append the fill as complete transactions AFTER the window,
// every one of its rows at the window's EndPosition.
func sevWithFill(l lineage.SegmentRecord) lineage.SegmentRecord {
	l.Manifest.SchemaDelta = []*irbackup.SchemaDeltaEntry{{
		Kind:          irbackup.SchemaDeltaAlterTable,
		Table:         "kl",
		AddColumnFill: &irbackup.AddColumnFill{Columns: []string{"extra"}, Rows: 1},
	}}
	return l
}

func sevFillTx(pos int) []ir.Change {
	return []ir.Change{
		ir.TxBegin{Position: sevLSN(pos)},
		ir.Update{Position: sevLSN(pos), Table: "kl", Before: ir.Row{"v": "a"}, After: ir.Row{"v": "a", "extra": 1}},
		ir.TxCommit{Position: sevLSN(pos)},
	}
}

// TestSeveredTransactionDoor_FillAfterASeveredWindow: a v0.156.1–v0.156.11
// stop severed the window and the rollover then appended its ADD COLUMN fill,
// so the incremental's LAST marker is the fill's TxCommit while the window's
// transaction is still open. The tail rule alone would pass it; the in-order
// scan catches the TxBegin that arrives with a transaction open.
func TestSeveredTransactionDoor_FillAfterASeveredWindow(t *testing.T) {
	store := sevStore(t)
	head := sevTx(200, "a", "b")
	n := sevWithFill(sevWriteIncremental(t, store, "n", 2, cat(sevTx(100, "x"), head[:len(head)-1], sevFillTx(200))))
	n1 := sevWriteIncremental(t, store, "n1", 100, sevTx(200, "a", "b"))
	door := &SeveredTransactionDoor{Store: store, Comparator: sevLSNComparator{}}
	requireSevered(t, door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), n, n1}),
		"ends inside an open source transaction")
}

// TestSeveredTransactionDoor_FillDoesNotForgeARedelivery is the false-refusal
// direction for the fill: a clean window whose trailing fill rows sit at its
// EndPosition W, followed by a link whose first transaction's commit record
// starts exactly at W (possible after a keepalive boundary). Comparing the
// fill row would call that a re-delivery; a fill-bearing link is not judged
// for shape (B).
func TestSeveredTransactionDoor_FillDoesNotForgeARedelivery(t *testing.T) {
	store := sevStore(t)
	n := sevWithFill(sevWriteIncremental(t, store, "n", 100, cat(sevTx(100, "x"),
		[]ir.Change{ir.TxBegin{Position: sevLSN(500)}, ir.TxCommit{Position: sevLSN(500)}}, sevFillTx(500))))
	n1 := sevWriteIncremental(t, store, "n1", 100, sevTx(500, "y"))
	door := &SeveredTransactionDoor{Store: store, Comparator: sevLSNComparator{}}
	if err := door.Check(context.Background(), []lineage.SegmentRecord{sevFull(), n, n1}); err != nil {
		t.Fatalf("a clean fill-bearing link was refused: %v", err)
	}
}
