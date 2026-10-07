// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// identityStampedRows wraps a seed rows function so every row change carries
// the identity a reader would have stamped: one transaction per TxBegin, the
// per-table ordinal counted from 1 (ADR-0190 §1).
func identityStampedRows(inner smartCompactRowsFn) smartCompactRowsFn {
	return func(startLSN uint64, incrIdx int) ([]ir.Change, uint64) {
		events, next := inner(startLSN, incrIdx)
		var tx string
		seq := map[string]uint64{}
		for i, c := range events {
			switch v := c.(type) {
			case ir.TxBegin:
				tx = fmt.Sprintf("pg:1:1:%s", v.Position.Token)
				seq = map[string]uint64{}
			case ir.Insert:
				seq[v.Table]++
				v.ApplyID = ir.ApplyID{TxID: tx, Seq: seq[v.Table]}
				events[i] = v
			case ir.Update:
				seq[v.Table]++
				v.ApplyID = ir.ApplyID{TxID: tx, Seq: seq[v.Table]}
				events[i] = v
			case ir.Delete:
				seq[v.Table]++
				v.ApplyID = ir.ApplyID{TxID: tx, Seq: seq[v.Table]}
				events[i] = v
			}
		}
		return events, next
	}
}

// collapsingAndPassThroughRows is one incremental that smart compaction both
// COLLAPSES (an INSERT+UPDATE chain per key on the keyed `users`) and PASSES
// THROUGH (inserts into the keyless `audit_log`, which it never collapses) —
// the two ways an event leaves the compactor. A pin with only collapsed events
// cannot see a rewrite that keeps a pass-through event's identity: the
// collapse rebuilds its events from scratch and drops the identity on its own
// (measured: the first cut of this test, which had only `users`, stayed green
// with the strip removed).
func collapsingAndPassThroughRows(startLSN uint64, incrIdx int) (events []ir.Change, end uint64) {
	collapsing, next := smartCompactRowsHappyPath(startLSN, incrIdx)
	passing, end := smartCompactRowsNoPKTable(next, incrIdx)
	return append(collapsing, passing...), end
}

// usersAndAuditSchema is usersSchema plus the keyless audit_log.
func usersAndAuditSchema() *ir.Schema {
	s := usersSchema()
	s.Tables = append(s.Tables, noPKSchema().Tables...)
	return s
}

// flagEveryIncremental stamps ApplyIdentity on every incremental the catalog
// lists, as the capture lanes do for an identity-stamping reader.
func flagEveryIncremental(t *testing.T, store irbackup.Store) {
	t.Helper()
	ctx := context.Background()
	cat, _, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range cat.Segments {
		segStore := lineage.NewPrefixedStore(store, seg.Dir)
		for _, ip := range seg.Incrementals {
			im, err := lineage.ReadManifestAt(ctx, segStore, ip)
			if err != nil {
				t.Fatal(err)
			}
			im.ApplyIdentity = true
			if err := lineage.WriteManifestAt(ctx, segStore, ip, im); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// compactedIdentities reports, over every incremental of the compacted chain,
// how many manifests carry ApplyIdentity and how many row changes carry an
// `aid`, out of the totals.
func compactedIdentities(t *testing.T, store irbackup.Store) (flagged, incrementals, withAID, rows, passThrough int) {
	t.Helper()
	ctx := context.Background()
	chain, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, link := range chain {
		if lineage.CanonicalKind(link.Manifest.Kind) != irbackup.BackupKindIncremental {
			continue
		}
		incrementals++
		if link.Manifest.ApplyIdentity {
			flagged++
		}
		for idx, ch := range link.Manifest.ChangeChunks {
			src, err := blobcodec.FetchChunkVerified(ctx, link.Segment.Store(store), ch.File, ch.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			cr, err := blobcodec.NewChangeChunkReader(src, ch.SHA256, nil, link.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(link.Manifest, ch, idx))
			if err != nil {
				t.Fatal(err)
			}
			for {
				c, err := cr.ReadChange()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				switch v := c.(type) {
				case ir.Insert, ir.Update, ir.Delete:
					rows++
					if !ir.ApplyIDOf(c).IsZero() {
						withAID++
					}
					if ins, ok := v.(ir.Insert); ok && ins.Table == "audit_log" {
						passThrough++
					}
				}
			}
			if err := cr.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	return flagged, incrementals, withAID, rows, passThrough
}

// TestCompaction_ApplyIdentityFlag is ADR-0191 §9 P11: naive compaction MOVES
// an incremental's chunks verbatim and carries its manifest, so the flag and
// every `aid` survive; smart compaction REWRITES the chunks of every
// incremental in a merge group, collapsing a key's changes across
// transactions, so it clears the flag and strips every identity (Q9) — a
// collapsed event cannot keep an identity naming a change it no longer is.
//
// The independent expected value is the decoded chunks themselves, read back
// through the codec after the compaction, not the compaction's own tallies.
func TestCompaction_ApplyIdentityFlag(t *testing.T) {
	for _, smart := range []bool{false, true} {
		name := "naive"
		if smart {
			name = "smart"
		}
		t.Run(name, func(t *testing.T) {
			store := newMemStore()
			now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
			seedSmartCompactLineageWithSchemaAndEnc(t, store, now, usersAndAuditSchema(), nil, identityStampedRows(collapsingAndPassThroughRows))
			flagEveryIncremental(t, store)
			if f, n, a, r, _ := compactedIdentities(t, store); f != n || a != r || r == 0 {
				t.Fatalf("the seeded chain carries flag %d/%d, aid %d/%d; want every incremental and row identified", f, n, a, r)
			}

			opts := CompactOpts{
				MergeWindow:  2 * time.Hour,
				Now:          func() time.Time { return now.Add(10 * time.Hour) },
				newSegmentID: func() string { return "merged-" + name },
			}
			if smart {
				opts.SmartCompaction = true
				opts.PKStrategy = PKStrategyPK
			}
			res, err := CompactChain(context.Background(), store, opts)
			if err != nil {
				t.Fatalf("CompactChain: %v", err)
			}
			if res.GroupsMerged != 1 {
				t.Fatalf("GroupsMerged = %d; the chain was not compacted, so the cell grades nothing", res.GroupsMerged)
			}
			flagged, incrementals, withAID, rows, passThrough := compactedIdentities(t, store)
			t.Logf("%s: ApplyIdentity on %d/%d incrementals, aid on %d/%d row changes", name, flagged, incrementals, withAID, rows)
			if rows == 0 || incrementals == 0 || passThrough == 0 {
				t.Fatalf("the compacted chain carries %d row changes, %d of them passed through uncollapsed: the cell needs both", rows, passThrough)
			}
			if smart {
				if flagged != 0 {
					t.Errorf("smart compaction kept ApplyIdentity on %d of %d rewritten incrementals; it must clear it (ADR-0191 Q9)", flagged, incrementals)
				}
				if withAID != 0 {
					t.Errorf("smart compaction kept an identity on %d of %d rewritten row changes; it must strip them all", withAID, rows)
				}
				return
			}
			if flagged != incrementals {
				t.Errorf("naive compaction dropped ApplyIdentity from %d of %d incrementals; it moves them verbatim", incrementals-flagged, incrementals)
			}
			if withAID != rows {
				t.Errorf("naive compaction lost the identity of %d of %d row changes; it moves chunk bytes verbatim", rows-withAID, rows)
			}
		})
	}
}
