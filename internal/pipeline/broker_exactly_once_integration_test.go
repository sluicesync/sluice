//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
)

// firstRowIdentity decodes the data incremental and returns the identity of
// its first row change — the transaction the broker delivers first.
func firstRowIdentity(t *testing.T, c *fe1Chain) ir.ApplyID {
	t.Helper()
	ctx := context.Background()
	for idx, chunk := range c.incr.Manifest.ChangeChunks {
		src, err := blobcodec.FetchChunkVerified(ctx, c.incr.Segment.Store(c.store), chunk.File, chunk.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		cr, err := blobcodec.NewChangeChunkReader(src, chunk.SHA256, nil, c.incr.Segment.CodecOrDefault(), irbackup.ChangeChunkAADFor(c.incr.Manifest, chunk, idx))
		if err != nil {
			t.Fatal(err)
		}
		for {
			ch, err := cr.ReadChange()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if id := ir.ApplyIDOf(ch); !id.IsZero() {
				_ = cr.Close()
				return id
			}
		}
		_ = cr.Close()
	}
	t.Fatal("the incremental carries no identified row change: the chain was written without identities")
	return ir.ApplyID{}
}

// plantPotentMark writes, under streamID, an apply mark on k's row 1 naming
// tx with an ordinal past every change of it: a mark that, if trusted, SKIPS
// the replayed insert of row 1. It is what a stale mark becomes when its
// transaction id recurs (a source whose history was reset), the case the
// cold-start clear exists for.
func plantPotentMark(t *testing.T, dsn, streamID, tx string) {
	t.Helper()
	ctx := context.Background()
	pgEng, _ := engines.Get("postgres")
	app, err := pgEng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cl, ok := app.(io.Closer); ok {
			_ = cl.Close()
		}
	}()
	if err := app.EnsureControlTable(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	key, _ := applymarks.KeyDigest(ir.Row{"id": int64(1)}, []string{"id"})
	if _, err := db.Exec(`INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest)
		VALUES ($1, 'public.k', $2, $3, 1000000, 'planted', '')`, streamID, key, tx); err != nil {
		t.Fatalf("plant the mark: %v", err)
	}
}

// TestBroker_ColdStartClearsAStaleMarkThatWouldSkip is the real-server half of
// ADR-0191 §9 P10 (the unit half, TestBroker_ColdStartsClearTheStreamsMarks,
// grades the order of the clear on both cold starts). A mark that names the
// first transaction the broker will deliver, with an ordinal past its
// changes, SKIPS the replayed insert of row 1 when it is trusted. The control
// arm proves the planted mark is that potent: a WARM resume (no cold start,
// so no clear) over it loses row 1. The graded arm plants the same mark and
// cold-starts with --at-chain-id: the clear removes it, and the target
// converges to the SOURCE — the independent expected value.
//
// The --reset-target-data clear is graded by the unit half only: its restore
// applies the chain under the restore's own stream, so a broker-stream mark
// is consulted only by an incremental that lands after the reset, whose
// identity a test cannot know in advance (a Postgres transaction id is its
// commit LSN).
func TestBroker_ColdStartClearsAStaleMarkThatWouldSkip(t *testing.T) {
	c := fe1SetupWith(t, `
		CREATE TABLE k (id INT PRIMARY KEY, note TEXT);
		INSERT INTO k VALUES (-1, 'seed');
	`, func(t *testing.T, src string) {
		applyDDL(t, src, `INSERT INTO k SELECT g, 'r'||g FROM generate_series(1, 300) g;`)
	}, nil)
	first := firstRowIdentity(t, c)
	want := fe1Q(t, c.src, `SELECT count(*)::text || '/' || sum(id)::text FROM k`)
	got := func() string { return fe1Q(t, c.dst, `SELECT count(*)::text || '/' || sum(id)::text FROM k`) }

	t.Run("control: a warm resume trusts the planted mark", func(t *testing.T) {
		applyDDL(t, c.dst, `TRUNCATE k; INSERT INTO k VALUES (-1, 'seed');`)
		const stream = "p10-control"
		plantPotentMark(t, c.dst, stream, first.TxID)
		pgEng, _ := engines.Get("postgres")
		app, err := pgEng.OpenChangeApplier(context.Background(), c.dst)
		if err != nil {
			t.Fatal(err)
		}
		if err := app.(ir.PositionWriter).WritePosition(context.Background(), stream, encodeBrokerPosition("test://"+stream, c.fullID)); err != nil {
			t.Fatal(err)
		}
		_ = app.(io.Closer).Close()
		if err := fe1RunToTailThenCancel(t, c, c.broker(stream, 1, "")); err != nil {
			t.Fatal(err)
		}
		if g := got(); g == want {
			t.Fatalf("the planted mark skipped nothing on a warm resume (target %s = source): it is not potent, so the graded arm proves nothing", g)
		}
		if n := fe1Q(t, c.dst, `SELECT count(*)::text FROM k WHERE id = 1`); n != "0" {
			t.Fatalf("the warm resume diverged (%s vs source %s) but not by skipping row 1", got(), want)
		}
	})

	t.Run("--at-chain-id clears it", func(t *testing.T) {
		applyDDL(t, c.dst, `TRUNCATE k; INSERT INTO k VALUES (-1, 'seed');`)
		const stream = "p10-at-chain"
		plantPotentMark(t, c.dst, stream, first.TxID)
		if err := fe1RunToTailThenCancel(t, c, c.broker(stream, 1, c.fullID)); err != nil {
			t.Fatal(err)
		}
		if g := got(); g != want {
			t.Fatalf("after an --at-chain-id cold start over a stale mark the target holds %s; the source %s (row 1 present: %s)",
				g, want, fe1Q(t, c.dst, `SELECT count(*)::text FROM k WHERE id = 1`))
		}
		if n := fe1Q(t, c.dst, fmt.Sprintf(`SELECT count(*)::text FROM sluice_cdc_apply_marks WHERE stream_id = '%s'`, stream)); n != "0" {
			t.Errorf("%s apply marks of stream %s survive the run", n, stream)
		}
	})
}
