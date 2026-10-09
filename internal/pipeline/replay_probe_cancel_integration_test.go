//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestFindReplayKeylessTables_RealPostgresCancelIsTheCancel is the
// environmental premise behind [migcore.ErrOrCancel], against a real
// server: a context that ends while the keyless door is probing a Postgres
// target comes back from the driver in shapes that are NOT the context error
// (pgx's write-path `i/o timeout`, database/sql's `driver: bad connection` on
// a connection an earlier cancel closed). Thousands of probes whose deadline
// lands anywhere inside the call; every failure must unwrap to the context's
// error, as the broker's clean-stop rule needs.
//
// What it reaches, stated so it is not read as broader: the count of failures
// the helper re-attributed is logged, NOT floored, because how often the
// race lands is the scheduler's choice — a forensic loop of bare
// ProbeReplayKey calls saw 24 in 6,000, this loop usually sees none, and its
// one catch so far was the dial-timeout shape [migcore.ErrOrCancel]'s
// deadline check exists for. A floor would be a flake; its job is to fail on
// any NEW shape. The deterministic pins are
// TestFindReplayKeylessTables_ProbeFailureUnderCancelIsTheCancel (migcore) and
// TestBrokerKeylessDoor_CancelMidProbeIsTheCancel.
func TestFindReplayKeylessTables_RealPostgresCancelIsTheCancel(t *testing.T) {
	_, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, tgt, `CREATE TABLE su (id INT PRIMARY KEY, u TEXT, v TEXT)`)
	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	rw, err := eng.OpenRowWriter(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	defer migcore.CloseIf(rw)
	su := &ir.Table{
		Name:       "su",
		Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}, {Name: "u", Type: ir.Text{}}, {Name: "v", Type: ir.Text{}}},
		PrimaryKey: &ir.Index{Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	judge := func(ctx context.Context) error {
		_, err := migcore.FindReplayKeylessTables(ctx, rw, []*ir.Table{su}, migcore.ReplayJudgeOptions{ProbeTarget: true, OnlyNonEmpty: true})
		return err
	}
	// The deadline budget adapts toward the point where half the calls
	// complete, so deadlines keep landing INSIDE a call however long one
	// takes here (a cancel closes the connection, so the next call dials).
	budget := 2 * time.Millisecond
	r := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // a spread of deadlines, not a secret
	var completed, cancelled, attributed int
	for i := 0; i < 6000; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int63n(int64(budget))))
		err := judge(ctx)
		ctxErr := ctx.Err()
		if d, ok := ctx.Deadline(); ok && ctxErr == nil && !time.Now().Before(d) {
			ctxErr = context.DeadlineExceeded // past its deadline; its timer has not fired yet
		}
		cancel()
		switch {
		case err == nil:
			completed++
			budget = budget * 97 / 100
		case ctxErr != nil && errors.Is(err, ctxErr):
			cancelled++
			budget = budget * 103 / 100
			if strings.Contains(err.Error(), "while the probe was in flight") {
				attributed++
			}
		default:
			t.Fatalf("probe %d failed as %v with the run's context %v: a cancelled door must report the cancellation", i, err, ctxErr)
		}
	}
	if completed == 0 || cancelled == 0 {
		t.Fatalf("the deadline spread missed the probe (completed=%d cancelled=%d): no deadline landed inside a call", completed, cancelled)
	}
	t.Logf("%d probes completed, %d cancelled, of which %d needed re-attribution", completed, cancelled, attributed)
}
