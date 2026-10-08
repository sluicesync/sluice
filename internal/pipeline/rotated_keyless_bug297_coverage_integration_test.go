//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// r297ChainRecorded returns the values of col the chain RECORDS for table —
// every row of every segment full plus every Insert of every incremental,
// decoded from the chunks of the links restore walks. It reads the backup,
// not the target, so it is what a restore of the chain owes; the fixtures
// below use it to know the chain covers the source before they compare a
// restore against the source.
func r297ChainRecorded(ctx context.Context, store irbackup.Store, table, col string) (map[int64]bool, error) {
	links, err := lineage.BuildLineageChain(ctx, store, nil)
	if err != nil {
		return nil, err
	}
	out := map[int64]bool{}
	add := func(row ir.Row) error {
		v, ok := row[col]
		if !ok {
			return fmt.Errorf("a %s row carries no %q: %v", table, col, row)
		}
		n, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		if err != nil {
			return err
		}
		out[n] = true
		return nil
	}
	for i := range links {
		l := &links[i]
		ss, codec := l.Segment.Store(store), l.Segment.CodecOrDefault()
		for _, tm := range l.Manifest.Tables {
			if tm.Name != table {
				continue
			}
			for _, c := range tm.Chunks {
				src, err := blobcodec.FetchChunkVerified(ctx, ss, c.File, c.SHA256)
				if err != nil {
					return nil, err
				}
				cr, err := blobcodec.NewChunkReader(src, c.SHA256, nil, codec, irbackup.ChunkAADFor(l.Manifest, c, tm.Schema, tm.Name))
				if err != nil {
					return nil, err
				}
				for {
					row, err := cr.ReadRow()
					if errors.Is(err, io.EOF) {
						break
					}
					if err == nil {
						err = add(row)
					}
					if err != nil {
						_ = cr.Close()
						return nil, err
					}
				}
				_ = cr.Close()
			}
		}
		for idx, c := range l.Manifest.ChangeChunks {
			src, err := blobcodec.FetchChunkVerified(ctx, ss, c.File, c.SHA256)
			if err != nil {
				return nil, err
			}
			cr, err := blobcodec.NewChangeChunkReader(src, c.SHA256, nil, codec, irbackup.ChangeChunkAADFor(l.Manifest, c, idx))
			if err != nil {
				return nil, err
			}
			for {
				ch, err := cr.ReadChange()
				if errors.Is(err, io.EOF) {
					break
				}
				if ins, ok := ch.(ir.Insert); err == nil && ok && ins.Table == table {
					err = add(ins.Row)
				}
				if err != nil {
					_ = cr.Close()
					return nil, err
				}
			}
			_ = cr.Close()
		}
	}
	return out, nil
}

// r297AwaitChainCovers blocks until the chain records every value of col the
// SOURCE holds in table, for each table→col pair, so the stream can be
// stopped knowing the chain holds the whole source.
//
// Why (v0.157.0 CI, TestBug297_RotatedKeylessChain_PG): these fixtures used
// to stop the stream a fixed 4s after the last write and then grade the
// restore against the live SOURCE. A chain that had not captured the last
// write by then failed as "restore lost a row" — indistinguishable from a
// silent restore loss, which is the one thing this suite must never leave
// ambiguous. Now a stream that does not capture the source in time, or that
// exits on its own, fails as THAT, and a restore that disagrees with the
// source is a restore that disagrees with its chain.
func r297AwaitChainCovers(t *testing.T, side r297Side, store irbackup.Store, src string, streamErr <-chan error, cols map[string]string) {
	t.Helper()
	ctx := context.Background()
	missing := func() map[string][]int64 {
		gaps := map[string][]int64{}
		for table, col := range cols {
			recorded, err := r297ChainRecorded(ctx, store, table, col)
			if err != nil {
				// The stream is writing the chain under us; a partial read
				// is retried, never treated as coverage.
				gaps[table] = []int64{-1}
				continue
			}
			for _, v := range r297Ints(t, side, src, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", col, table, col)) {
				if !recorded[v] {
					gaps[table] = append(gaps[table], v)
				}
			}
		}
		return gaps
	}
	deadline := time.Now().Add(90 * time.Second)
	for {
		select {
		case err := <-streamErr:
			t.Fatalf("fixture: the stream exited (err=%v) before its chain recorded the whole source (missing %v)", err, missing())
		default:
		}
		gaps := missing()
		if len(gaps) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for table := range gaps {
				slices.Sort(gaps[table])
			}
			t.Fatalf("fixture: after 90s the stream's chain still does not record %v of the source: "+
				"the stream stalled (this is not a restore result)", gaps)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
