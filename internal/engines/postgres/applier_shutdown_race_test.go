// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"testing"
)

// TestApplierErrorOrShutdown pins the shutdown-race mapping the v0.123.0 tag
// run caught: database/sql auto-rolls-back a BeginTx(ctx) transaction when
// ctx cancels, so a graceful stop landing between BeginTx and Commit
// surfaces as sql.ErrTxDone — an error the streamer's clean-stop check
// (errors.Is Canceled/DeadlineExceeded) does not match, which turned an
// ordinary stop into a reported apply failure. The mapping reports the
// cancellation as the cause when ctx is terminated (nothing persisted — the
// safe, re-delivered direction) and stays out of the way otherwise.
func TestApplierErrorOrShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	underlying := errors.New("sql: transaction has already been committed or rolled back")

	if err := applierErrorOrShutdown(ctx, underlying); !errors.Is(err, context.Canceled) {
		t.Fatalf("with a cancelled ctx the mapping must surface the cancellation (clean stop); got: %v", err)
	}

	live := context.Background()
	err := applierErrorOrShutdown(live, underlying)
	if errors.Is(err, context.Canceled) {
		t.Fatal("with a live ctx the mapping must NOT invent a cancellation")
	}
	if err == nil || !errors.Is(err, underlying) && err.Error() == "" {
		t.Fatalf("with a live ctx the original failure must survive classification; got: %v", err)
	}
}
