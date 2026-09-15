// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// TestCaptureBackupPosition_NekiAnswersUnavailableWithoutQuerying pins the
// Neki branch of CaptureBackupPosition: a reader that knows it is talking to
// a router answers ErrPositionUnavailable BEFORE issuing pg_current_wal_lsn()
// — the router does not implement it (NK013, measured 2026-09-15 by the
// nekiverify backup arm, where a full backup FROM a sharded Neki copied every
// row and died at the finalize phase on this read).
//
// The pool below points at a port nothing listens on, so if the Neki branch
// is skipped the call returns a connection error rather than the sentinel —
// which is what makes this a pin on the ORDER of the two checks, not just on
// the sentinel's existence.
func TestCaptureBackupPosition_NekiAnswersUnavailableWithoutQuerying(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("pgx", "postgres://nobody@127.0.0.1:1/nowhere?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := &SchemaReader{db: db, schema: "public", isNeki: true}
	_, err = r.CaptureBackupPosition(ctx, "")
	if !errors.Is(err, irbackup.ErrPositionUnavailable) {
		t.Fatalf("a Neki reader must answer ErrPositionUnavailable without querying; got: %v", err)
	}

	// Anti-vacuity: the same reader with the flavor OFF must NOT answer the
	// sentinel — it must try the query (and fail on the dead port here).
	off := &SchemaReader{db: db, schema: "public"}
	_, err = off.CaptureBackupPosition(ctx, "")
	if err == nil || errors.Is(err, irbackup.ErrPositionUnavailable) {
		t.Fatalf("an ordinary reader must query the server, not answer the sentinel; got: %v", err)
	}
}
