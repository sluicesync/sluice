//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses is the F-E1 in-run
// retry pin against a real Postgres: the first chunk's COPY really commits,
// and the connection then "drops" before the acknowledgement (the test-only
// copyChunkAckLossHook) — the committed-but-unacked branch.
//
//   - "sur" is keyed on `id` in the recorded table but, on the target, only
//     on a bigserial `sid` the rows never carry. Before the fix the retry
//     gate judged the recorded table alone, re-COPIED the chunk, drew fresh
//     `sid` values and returned nil with the chunk doubled. It must now
//     refuse with SLUICE-E-COPY-RETRY-AMBIGUOUS-KEYLESS naming the target
//     judgment.
//   - "kpk" is the control, keyed on `id` on both sides: the re-COPY is
//     licensed and collides (23505) — loud, not refused as keyless.
//
// The independent expected value is the target's own count(*), read from
// Postgres: exactly one chunk, never two.
func TestColdCopyRetry_SurrogateKeyedTarget_AckLostRefuses(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	applyDDL(t, dsn, `
		CREATE TABLE sur (sid bigserial PRIMARY KEY, id bigint NOT NULL, v text);
		CREATE TABLE kpk (id bigint PRIMARY KEY, v text);`)
	recorded := func(name string) *ir.Table {
		return &ir.Table{
			Name: name,
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "v", Type: ir.Text{}, Nullable: true},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}
	}
	const chunk, total = 50, 120
	rows := make([]ir.Row, total)
	for i := range rows {
		rows[i] = ir.Row{"id": int64(i), "v": "r"}
	}

	for _, c := range []struct {
		table   string
		refused bool
	}{{"sur", true}, {"kpk", false}} {
		t.Run(c.table, func(t *testing.T) {
			withFastPGCopyBackoff(t)
			withSmallPGChunk(t, chunk)
			rwIface, err := Engine{}.OpenRowWriter(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer closeIf(rwIface)
			rw := rwIface.(*RowWriter)
			rw.SetGrowGate(&recordingGrowGate{})
			lost := 0
			rw.copyChunkAckLossHook = func(attempt int) error {
				if lost == 0 && attempt == 1 {
					lost++
					return &pgconn.PgError{Code: "08006", Message: "connection failure (ack lost after commit)"}
				}
				return nil
			}

			err = rw.WriteRows(ctx, recorded(c.table), feedRows(rows))
			if lost != 1 {
				t.Fatalf("the ack-loss injection fired %d times; the chunked COPY path was not taken", lost)
			}
			if err == nil {
				t.Fatalf("WriteRows returned nil after a committed chunk lost its ack; target holds %d rows for %d",
					tableCount(t, ctx, rw.db, c.table), total)
			}
			ce, coded := sluicecode.FromError(err)
			isKeyless := coded && ce.Code == sluicecode.CodeCopyRetryAmbiguousKeyless
			if c.refused {
				if !isKeyless || !strings.Contains(err.Error(), irbackup.ReplayKeylessTarget.Describe()) {
					t.Fatalf("want the target-judgment refusal; got: %v", err)
				}
			} else if isKeyless || !strings.Contains(err.Error(), "23505") {
				t.Fatalf("a target keyed on a supplied column must replay and collide (23505), not refuse; got: %v", err)
			}
			if got := tableCount(t, ctx, rw.db, c.table); got != chunk {
				t.Errorf("target holds %d rows; want exactly the one committed chunk (%d)", got, chunk)
			}
		})
	}
}
