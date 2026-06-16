//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0093: VStream purged-GTID resume → reactive cold-start re-snapshot.
//
// The self-hosted binlog source recovers from a purged resume position
// via a PRE-FLIGHT gtid_purged ⊆ resume check (cdc_reader.go) that returns
// ir.ErrPositionInvalid; the streamer's ADR-0022 fall-through re-enters
// cold-start. vtgate exposes no single authoritative gtid_purged to
// pre-flight against, so the VStream path can only discover a purged
// position REACTIVELY — vtgate rejects the position on the stream and the
// pump's Recv surfaces "the source/master ... purged required binary
// logs". ADR-0093 classifies that reactive error as ir.ErrPositionInvalid
// (reader_errors.go: isVStreamPurgedGTIDError) and routes it to a bounded
// one-shot cold-start re-snapshot (default), or a loud terminal error
// under --no-auto-resnapshot.
//
// This test boots the REAL multi-process Vitess cluster (the same harness
// the chaos suite uses), captures a VStream position, advances the
// underlying primary tablet's gtid_purged PAST that position (FLUSH +
// PURGE BINARY LOGS on the tablet's mysqld), then re-opens a VStream from
// the stale position and asserts the reader surfaces an error that
// errors.Is(ir.ErrPositionInvalid) — proving the classifier carve-out
// fires against a genuine vtgate purged rejection (not a synthesised
// string). The streamer-level recovery (re-snapshot vs loud opt-out) is
// pinned by the unit tests in internal/pipeline (the pipeline package
// owns Run/runWithRetry); this engine-level test pins the source signal
// the recovery depends on.

package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// primaryTabletSocket is mysqlctl's per-tablet UNIX socket inside the
// vttablet container (uid 100 → vt_0000000100). The tablet's mysqld
// listens on the socket only (no exposed TCP), so SQL against it goes
// through `compose exec`.
const primaryTabletSocket = "/vt/vtdataroot/vt_0000000100/mysql.sock"

// tabletMySQLExec runs SQL against the PRIMARY tablet's mysqld via its
// mysqlctl socket, inside the vttablet container. Used to manipulate the
// tablet's binlogs directly (advance gtid_purged) — something neither
// vtgate nor vtctldclient exposes.
func tabletMySQLExec(t *testing.T, cc *chaosCluster, sqlText string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := cc.runCompose(ctx, "exec", "-T", svcTabletPrimary, "sh", "-c",
		fmt.Sprintf("mysql -u root --socket=%s -e %q", primaryTabletSocket, sqlText))
	if err != nil {
		t.Fatalf("tablet mysql exec %q: %v\n%s", sqlText, err, out)
	}
	return out
}

// TestVitessCluster_PurgedGTID_ReactiveColdStart pins ADR-0093: a VStream
// resume from a position older than the tablet's retained binlogs surfaces
// a reactive error that classifyReaderError maps to ir.ErrPositionInvalid.
func TestVitessCluster_PurgedGTID_ReactiveColdStart(t *testing.T) {
	cc := startChaosCluster(t)
	defer cc.cleanup()

	const table = "purged_t"
	const seedRows = 50
	chaosSeedTable(t, cc.mysqlDSN, table)
	chaosInsertBatch(t, cc.mysqlDSN, table, 1, seedRows)
	// Let the tablet schema engine pick the table up before COPY opens.
	time.Sleep(3 * time.Second)

	eng := Engine{Flavor: FlavorPlanetScale}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	// ---- Phase 1: cold-start COPY → capture the post-snapshot VStream
	// position (this is the resume position a later run would persist). ----
	stream, err := eng.OpenSnapshotStream(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	rowsCh, err := stream.Rows.ReadRows(ctx, chaosTable(table))
	if err != nil {
		_ = stream.Close()
		t.Fatalf("ReadRows: %v", err)
	}
	snap := 0
	for range rowsCh {
		snap++
	}
	if rerr := stream.Rows.Err(); rerr != nil {
		_ = stream.Close()
		t.Fatalf("snapshot Rows.Err = %v; want nil", rerr)
	}
	if snap != seedRows {
		_ = stream.Close()
		t.Fatalf("snapshot copied %d; want %d", snap, seedRows)
	}
	resumePos := stream.Position
	_ = stream.Close()
	if resumePos.Token == "" {
		t.Fatal("captured VStream resume position is empty after snapshot")
	}
	t.Logf("captured resume position: %s", resumePos.Token)

	// ---- Phase 2: advance the tablet's gtid_purged PAST the captured
	// position. Generate more transactions, rotate twice so PURGE has a
	// non-active file to remove, then PURGE everything but the latest —
	// which drops the binlogs covering the captured GTIDs, advancing
	// gtid_purged past resumePos. (Mirrors the binlog purged test recipe,
	// applied directly to the tablet's mysqld.) ----
	chaosInsertBatch(t, cc.mysqlDSN, table, seedRows+1, 20)
	tabletMySQLExec(t, cc, "FLUSH BINARY LOGS")
	chaosInsertBatch(t, cc.mysqlDSN, table, seedRows+100, 20)
	tabletMySQLExec(t, cc, "FLUSH BINARY LOGS")
	// Purge all but the active binlog — includes the file(s) the captured
	// position references.
	tabletMySQLExec(t, cc, "PURGE BINARY LOGS BEFORE NOW()")
	purged := string(tabletMySQLExec(t, cc, "SELECT @@global.gtid_purged"))
	t.Logf("tablet gtid_purged after PURGE: %s", strings.TrimSpace(purged))

	// ---- Phase 3: re-open a VStream FROM the now-stale resume position.
	// vtgate must reject it; the pump's Recv surfaces the purged error,
	// which classifyReaderError maps to ir.ErrPositionInvalid. ----
	reader, err := eng.OpenCDCReader(ctx, chaosVStreamDSN(cc))
	if err != nil {
		t.Fatalf("OpenCDCReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	changes, err := reader.StreamChanges(ctx, resumePos)
	if err != nil {
		// Some transports surface the rejection synchronously from
		// StreamChanges rather than via the pump's Err(); accept either.
		assertPurgedInvalidPosition(t, err)
		return
	}

	// Drain until the channel closes (the pump exits on the purged error),
	// then read the reader's terminal Err().
	drainCtx, drainCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer drainCancel()
	drained := false
	for !drained {
		select {
		case _, ok := <-changes:
			if !ok {
				drained = true
			}
		case <-drainCtx.Done():
			t.Fatal("timed out waiting for the purged-position stream to surface its error")
		}
	}

	rerr := readerErr(reader)
	if rerr == nil {
		t.Fatal("purged-position resume produced NO reader error; ADR-0093 requires it surface ir.ErrPositionInvalid (restart-loop regression)")
	}
	assertPurgedInvalidPosition(t, rerr)
}

// assertPurgedInvalidPosition asserts err is the ADR-0093 classified
// purged-position signal: it must errors.Is(ir.ErrPositionInvalid) (so the
// streamer routes it to cold-start) and must NOT be classified retriable
// (retrying the same purged position spins forever).
func assertPurgedInvalidPosition(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("purged-position error does not wrap ir.ErrPositionInvalid (ADR-0093 classifier did not fire): %v", err)
	}
	var re ir.RetriableError
	if errors.As(err, &re) {
		t.Fatalf("purged-position error classified RETRIABLE; it must be terminal-but-cold-start-recoverable: %v", err)
	}
	t.Logf("purged-position correctly classified as ir.ErrPositionInvalid: %v", err)
}
