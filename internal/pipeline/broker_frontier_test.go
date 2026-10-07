// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// frontierEvents is a three-chunk incremental over the shapes the frontier
// stamps: transactions, a change OUTSIDE a transaction (a MySQL TRUNCATE), and
// a transaction spanning a chunk boundary. Ordinals in the comments.
func frontierEvents() [][]ir.Change {
	p := tailLSN
	return [][]ir.Change{
		{
			ir.TxBegin{Position: p(10)}, // 0
			ir.Insert{Position: p(10), Table: "t", Row: ir.Row{"id": int64(1)}, ApplyID: ir.ApplyID{TxID: "T1", Seq: 1}}, // 1
			ir.Insert{Position: p(10), Table: "t", Row: ir.Row{"id": int64(2)}, ApplyID: ir.ApplyID{TxID: "T1", Seq: 2}}, // 2
			ir.TxCommit{Position: p(11)}, // 3
		},
		{
			ir.Truncate{Position: p(12), Table: "u"}, // 4 — outside a transaction
			ir.TxBegin{Position: p(13)},              // 5
			ir.Update{Position: p(13), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(9)}, ApplyID: ir.ApplyID{TxID: "T2", Seq: 1}}, // 6
		},
		{
			ir.Delete{Position: p(13), Table: "t", Before: ir.Row{"id": int64(2)}, ApplyID: ir.ApplyID{TxID: "T2", Seq: 2}}, // 7
			ir.TxCommit{Position: p(14)}, // 8
		},
	}
}

// frontierWant is, per ordinal of frontierEvents, the boundary its token must
// name: -1 is the parent (the start of the first transaction).
var frontierWant = []int64{-1, -1, -1, 3, 4, 4, 4, 4, 8}

// frontierLink writes chunks as an incremental's change chunks.
func frontierLink(t *testing.T, store irbackup.Store, name string, chunks [][]ir.Change) lineage.SegmentRecord {
	t.Helper()
	m := &irbackup.Manifest{BackupID: name, SourceEngine: "postgres", Kind: irbackup.BackupKindIncremental}
	var last ir.Position
	for i, changes := range chunks {
		var buf bytes.Buffer
		cw, err := blobcodec.NewChangeChunkWriter(&buf, nil, blobcodec.CodecGzip, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range changes {
			if err := cw.WriteChange(c); err != nil {
				t.Fatal(err)
			}
			last = c.Pos()
		}
		if err := cw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("chunks/_changes/%s/changes-%d.jsonl.gz", name, i)
		if err := store.Put(context.Background(), path, bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		m.ChangeChunks = append(m.ChangeChunks, &irbackup.ChunkInfo{File: path, RowCount: cw.ChangeCount(), SHA256: cw.Hash()})
	}
	m.StartPosition, m.EndPosition = tailLSN(1), last
	return lineage.SegmentRecord{
		Segment:        &lineage.Segment{Codec: blobcodec.CodecGzip},
		ManifestRecord: lineage.ManifestRecord{Path: "manifests/" + name + ".json", Manifest: m},
	}
}

// emitted runs the broker's producer over link from a frontier skipping
// through skip, and returns what reached the applier.
func emitted(t *testing.T, store irbackup.Store, link *lineage.SegmentRecord, skip int64) ([]ir.Change, error) {
	t.Helper()
	b := &SyncFromBackup{Store: store, ChainURL: "test://f"}
	out := make(chan ir.Change, 64)
	err := b.streamIncrementalWithPosition(context.Background(), link, newBrokerFrontier("test://f", "P", link.Manifest, skip), out)
	close(out)
	var got []ir.Change
	for c := range out {
		got = append(got, c)
	}
	return got, err
}

// boundaryOf decodes the frontier a token names (-1 = the parent).
func boundaryOf(t *testing.T, p ir.Position) int64 {
	t.Helper()
	tok, err := decodeBrokerPosition(p)
	if err != nil {
		t.Fatalf("decode %q: %v", p.Token, err)
	}
	if tok.LastAppliedBackupID != "P" {
		t.Fatalf("token %q names last-applied %q; every token of the in-flight incremental must name its parent", p.Token, tok.LastAppliedBackupID)
	}
	if tok.InProgress == nil {
		return -1
	}
	return tok.InProgress.Through
}

// TestBroker_FrontierTokenAtEveryBoundary is ADR-0191 §9 P3: a row carries the
// frontier of its transaction's START, a TxCommit and a change outside a
// transaction their own ordinal, and the ordinals count the RAW stream — the
// same numbers on a resumed run that drops a prefix as on a full one. The
// expected values are written out per ordinal (frontierWant), not derived
// from the stamper.
func TestBroker_FrontierTokenAtEveryBoundary(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := frontierLink(t, store, "X", frontierEvents())
	for _, skip := range []int64{-1, 3, 5} {
		t.Run(fmt.Sprintf("resumed through %d", skip), func(t *testing.T) {
			got, err := emitted(t, store, &link, skip)
			if err != nil {
				t.Fatal(err)
			}
			if want := len(frontierWant) - int(skip+1); len(got) != want {
				t.Fatalf("emitted %d events; want %d (those after ordinal %d)", len(got), want, skip)
			}
			for i, c := range got {
				ord := int(skip+1) + i
				if b := boundaryOf(t, c.Pos()); b != frontierWant[ord] {
					t.Errorf("event %d (%T) carries the frontier %d; want %d", ord, c, b, frontierWant[ord])
				}
			}
		})
	}

	// The reverse: a marker-less stream (the trigger sources) has no
	// transactions, so every change is its own boundary.
	t.Run("marker-less: every change its own boundary", func(t *testing.T) {
		ml := frontierLink(t, store, "ML", [][]ir.Change{{
			ir.Insert{Position: tailLSN(1), Table: "t", Row: ir.Row{"id": int64(1)}},
			ir.Insert{Position: tailLSN(2), Table: "t", Row: ir.Row{"id": int64(2)}},
			ir.Update{Position: tailLSN(3), Table: "t", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(3)}},
		}})
		got, err := emitted(t, store, &ml, -1)
		if err != nil {
			t.Fatal(err)
		}
		for i, c := range got {
			if b := boundaryOf(t, c.Pos()); b != int64(i) {
				t.Errorf("marker-less change %d carries the frontier %d; want its own ordinal", i, b)
			}
		}
	})
}

// unreadableChunkStore fails every Get of one path (a chunk the frontier skips).
type unreadableChunkStore struct {
	irbackup.Store
	path string
}

func (s unreadableChunkStore) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	if p == s.path {
		return nil, errors.New("injected: this chunk cannot be read")
	}
	return s.Store.Get(ctx, p)
}

// TestBroker_ResumeSkipsThroughTheFrontier is ADR-0191 §9 P4: a resume through
// e emits exactly the events after e — not one more, not one fewer — and the
// skipped prefix is still fetched and verified: a chunk wholly inside the
// skipped prefix that is corrupt (bytes differing from the manifest's SHA) or
// unreadable fails the resume instead of being stepped over.
func TestBroker_ResumeSkipsThroughTheFrontier(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := frontierLink(t, store, "X", frontierEvents())
	for skip, wantFirst := range map[int64]int64{3: 4, 4: 5, 8: 9} {
		got, err := emitted(t, store, &link, skip)
		if err != nil {
			t.Errorf("resume through %d: %v", skip, err)
			continue
		}
		if n := int64(len(frontierWant)) - wantFirst; int64(len(got)) != n {
			t.Errorf("resume through %d emitted %d events; want %d (ordinals %d..%d)", skip, len(got), n, wantFirst, len(frontierWant)-1)
		}
	}
	// Ordinal 4 is the TRUNCATE outside any transaction: what a resume through
	// 3 must emit first, and a resume through 4 must not.
	if got, _ := emitted(t, store, &link, 3); len(got) == 0 {
		t.Fatal("a resume through 3 emitted nothing")
	} else if _, ok := got[0].(ir.Truncate); !ok {
		t.Errorf("a resume through 3 emitted %T first; want ordinal 4, the Truncate", got[0])
	}

	t.Run("a corrupt chunk inside the skipped prefix still refuses", func(t *testing.T) {
		first := link.Manifest.ChangeChunks[0].File
		rc, err := store.Get(context.Background(), first)
		if err != nil {
			t.Fatal(err)
		}
		orig, _ := io.ReadAll(rc)
		_ = rc.Close()
		bad := append([]byte(nil), orig...)
		bad[len(bad)/2] ^= 0xFF
		if err := store.Put(context.Background(), first, bytes.NewReader(bad)); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = store.Put(context.Background(), first, bytes.NewReader(orig)) }()
		if _, err := emitted(t, store, &link, 3); err == nil {
			t.Fatal("a resume through 3 stepped over chunk 0 without verifying it: its bytes no longer match the manifest's SHA-256")
		}
	})
	t.Run("an unreadable chunk inside the skipped prefix still refuses", func(t *testing.T) {
		fs := unreadableChunkStore{Store: store, path: link.Manifest.ChangeChunks[0].File}
		if _, err := emitted(t, fs, &link, 3); err == nil {
			t.Fatal("a resume through 3 never read chunk 0")
		}
	})
}

// TestBroker_RewrittenIncrementalRefuses is ADR-0191 §9 P5. The frontier is a
// count into one stream; a rewritten incremental (smart compaction writes new
// bytes under the SAME chunk file names) is a different stream, and a resume
// must refuse with SLUICE-E-BROKER-INCREMENTAL-REWRITTEN rather than skip by
// ordinal into it. Also a stream shorter than the frontier, and a frontier
// naming an incremental that no longer follows the parent. The reverse: the
// same bytes MOVED under new names (naive compaction) resume normally.
func TestBroker_RewrittenIncrementalRefuses(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := frontierLink(t, store, "X", frontierEvents())
	at := func(id, digest string, through int64) ir.Position {
		return encodeBrokerFrontier("test://f", "P", &brokerInProgress{BackupID: id, Chunks: digest, Through: through})
	}
	digest := chunkListDigest(link.Manifest)

	// Smart compaction's shape: the same chunk files, different bytes.
	collapsed := frontierEvents()
	collapsed[0] = collapsed[0][:2] // a collapsed first transaction (no commit kept here)
	// A second store: the rewrite lands on the SAME paths, which would
	// otherwise clobber the original the later subtests read.
	rewrittenStore, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rewritten := frontierLink(t, rewrittenStore, "X", collapsed)
	if rewritten.Manifest.ChangeChunks[0].File != link.Manifest.ChangeChunks[0].File {
		t.Fatal("fixture: the rewrite must keep the chunk file names, as smart compaction does")
	}

	for _, tc := range []struct {
		name string
		m    *irbackup.Manifest
		pos  ir.Position
	}{
		{"rewritten under the same chunk names", rewritten.Manifest, at("X", digest, 3)},
		{"another incremental follows the parent", link.Manifest, at("Y", digest, 3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := resumeSkip(tc.pos, true, tc.m, "P")
			ce, ok := sluicecode.FromError(err)
			if !ok || ce.Code != sluicecode.CodeBrokerIncrementalRewritten || !strings.Contains(err.Error(), BrokerIncrementalRewrittenMarker) {
				t.Fatalf("resumeSkip = %v; want %s", err, sluicecode.CodeBrokerIncrementalRewritten)
			}
		})
	}
	t.Run("a stream shorter than the frontier", func(t *testing.T) {
		_, err := emitted(t, store, &link, int64(len(frontierWant))) // through one past the last event
		if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeBrokerIncrementalRewritten {
			t.Fatalf("a frontier past the stream's end returned %v; want %s", err, sluicecode.CodeBrokerIncrementalRewritten)
		}
	})
	t.Run("reverse: the same bytes moved under new names resume", func(t *testing.T) {
		moved := *link.Manifest
		moved.ChangeChunks = nil
		for i, c := range link.Manifest.ChangeChunks {
			cc := *c
			cc.File = fmt.Sprintf("seg-merged/chunks/_changes/X/changes-%d.jsonl.gz", i)
			moved.ChangeChunks = append(moved.ChangeChunks, &cc)
		}
		skip, err := resumeSkip(at("X", digest, 3), true, &moved, "P")
		if err != nil || skip != 3 {
			t.Fatalf("a naively compacted incremental (same chunk bytes, new names) resumed as (%d, %v); want (3, nil)", skip, err)
		}
	})
	t.Run("no frontier: from the start", func(t *testing.T) {
		if skip, err := resumeSkip(encodeBrokerPosition("test://f", "P"), true, link.Manifest, "P"); err != nil || skip != -1 {
			t.Fatalf("a token between incrementals resumed as (%d, %v); want (-1, nil)", skip, err)
		}
	})
}

// TestBrokerToken_RoundTrip is the token's codec-checklist row: every field
// the frontier carries survives the position table's text column byte-exact,
// including an ordinal above 2^53 and identifiers carrying JSON-special and
// non-ASCII text, on both sentinels the decoder reads; and a frontier no
// writer produces is refused rather than resumed from.
func TestBrokerToken_RoundTrip(t *testing.T) {
	in := &brokerInProgress{BackupID: `inc"\ ü-1`, Chunks: strings.Repeat("ab", 32), Through: 1<<53 + 1}
	p := encodeBrokerFrontier(`s3://b/"chain"`, "P ", in)
	tok, err := decodeBrokerPosition(ir.Position{Engine: "postgres", Token: p.Token})
	if err != nil {
		t.Fatal(err)
	}
	if *tok.InProgress != *in || tok.LastAppliedBackupID != "P " || tok.ChainURL != `s3://b/"chain"` {
		t.Errorf("round trip changed the token: %+v (in_progress %+v)", tok, tok.InProgress)
	}
	classic := ir.Position{Token: `{"_engine":"backup-broker","chain_url":"x","last_applied_backup_id":"P"}`}
	if !isBrokerToken(classic) {
		t.Error("a classic token a pre-ADR-0191 broker wrote is not read as a broker token; an upgraded broker would refuse its own row")
	}
	for _, bad := range []string{
		`{"_engine":"backup-broker","last_applied_backup_id":"P","in_progress":{"backup_id":"X","chunks":"` + strings.Repeat("ab", 32) + `","through":1}}`,
		`{"_engine":"backup-broker-v2","last_applied_backup_id":"P","in_progress":{"backup_id":"","chunks":"` + strings.Repeat("ab", 32) + `","through":1}}`,
		`{"_engine":"backup-broker-v2","last_applied_backup_id":"P","in_progress":{"backup_id":"X","chunks":"short","through":1}}`,
		`{"_engine":"backup-broker-v2","last_applied_backup_id":"P","in_progress":{"backup_id":"X","chunks":"` + strings.Repeat("ab", 32) + `","through":-2}}`,
		`{"_engine":"backup-broker-v2","last_applied_backup_id":"P","in_progress":{"backup_id":"X","chunks":"` + strings.Repeat("ab", 32) + `","through":1.5}}`,
	} {
		if _, err := decodeBrokerPosition(ir.Position{Token: bad}); err == nil {
			t.Errorf("decoded a frontier no writer produces: %s", bad)
		}
	}
}

// ---- the v0.156.11 broker-token decoder, FROZEN ----
//
// Copied verbatim from `git show v0.156.11:internal/pipeline/broker.go`
// (the struct, decodeBrokerPosition's sentinel test and isBrokerToken), NOT
// derived from today's code: a downgrade gate whose fixture is the current
// decoder proves only that this build agrees with itself (CLAUDE.md, the
// item-104 corollary).

const backupBrokerPositionEngineV015611 = "backup-broker"

type brokerPositionTokenV015611 struct {
	Engine              string `json:"_engine,omitempty"`
	ChainURL            string `json:"chain_url,omitempty"`
	LastAppliedBackupID string `json:"last_applied_backup_id"`
}

func isBrokerTokenV015611(pos ir.Position) bool {
	if pos.Token == "" {
		return false
	}
	var tok brokerPositionTokenV015611
	if err := json.Unmarshal([]byte(pos.Token), &tok); err != nil {
		return false
	}
	return tok.Engine == backupBrokerPositionEngineV015611
}

// TestBrokerToken_DowngradeIsLoud is ADR-0191 §9 P13: every token this binary
// writes — standing inside an incremental, and between incrementals (Q3:
// always the v2 sentinel) — is refused by the v0.156.11 decoder as a
// non-broker row. That broker's Run then exits "owned by a non-broker writer"
// instead of reading last_applied_backup_id and re-applying the in-flight
// incremental with no marks — which, before v0.156.11's keyless door, was a
// silent keyless duplicate.
func TestBrokerToken_DowngradeIsLoud(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := frontierLink(t, store, "X", frontierEvents())
	got, err := emitted(t, store, &link, -1)
	if err != nil {
		t.Fatal(err)
	}
	written := make([]ir.Position, 0, 1+len(got))
	written = append(written, encodeBrokerPosition("test://f", "X")) // writePositionDirect, a clean boundary
	for _, c := range got {
		written = append(written, c.Pos()) // every frontier the applier may persist
	}
	inside := 0
	for _, p := range written {
		if isBrokerTokenV015611(ir.Position{Engine: "postgres", Token: p.Token}) {
			t.Errorf("the v0.156.11 decoder reads %s as its own token: a downgraded broker would resume from it with no marks", p.Token)
		}
		if strings.Contains(p.Token, `"in_progress"`) {
			inside++
		}
	}
	if inside == 0 {
		t.Fatal("no token stood inside the incremental: the cell grades only the clean-boundary half")
	}
}

// recordingFrontierApplier records each change the broker hands it and
// presents a configured persisted position.
type recordingFrontierApplier struct {
	capturingApplier
	mu       sync.Mutex
	resume   ir.Position
	found    bool
	received []ir.Change
}

func (a *recordingFrontierApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return a.resume, a.found, nil
}

func (a *recordingFrontierApplier) ApplyBatch(_ context.Context, _ string, ch <-chan ir.Change, _ int) error {
	for c := range ch {
		a.mu.Lock()
		a.received = append(a.received, c)
		a.mu.Unlock()
	}
	return nil
}

// TestBroker_ApplyIncrementalResumesFromTheTarget is the wiring half of P4:
// applyIncremental reads the frontier from the TARGET (not from memory), skips
// through it, and then writes the incremental's own id as fully applied.
func TestBroker_ApplyIncrementalResumesFromTheTarget(t *testing.T) {
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link := frontierLink(t, store, "X", frontierEvents())
	app := &recordingFrontierApplier{
		resume: encodeBrokerFrontier("test://f", "P", &brokerInProgress{BackupID: "X", Chunks: chunkListDigest(link.Manifest), Through: 4}),
		found:  true,
	}
	b := &SyncFromBackup{Store: store, ChainURL: "test://f", StreamID: "s"}
	if _, err := b.applyIncremental(context.Background(), app, &link, 10, "P"); err != nil {
		t.Fatal(err)
	}
	if len(app.received) != len(frontierWant)-5 {
		t.Fatalf("the applier received %d events; want %d (ordinals 5..%d)", len(app.received), len(frontierWant)-5, len(frontierWant)-1)
	}
	if _, ok := app.received[0].(ir.TxBegin); !ok {
		t.Errorf("first event after the frontier is %T; want ordinal 5, the TxBegin", app.received[0])
	}
	if n := len(app.written); n == 0 {
		t.Fatal("no position written after the incremental")
	} else if tok, err := decodeBrokerPosition(app.written[n-1]); err != nil || tok.LastAppliedBackupID != "X" || tok.InProgress != nil {
		t.Errorf("the final position is %+v (%v); want X fully applied, no frontier", tok, err)
	}
}
