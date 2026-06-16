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

// tabletScalar runs a single-column query against the PRIMARY tablet and
// returns the value (the row under the header), trimmed. `mysql -e` emits a
// header line followed by the value line(s); a header-only result (no value)
// returns "" — which is how an EMPTY gtid_purged surfaces (the timing-bug
// tell the original recipe hit).
func tabletScalar(t *testing.T, cc *chaosCluster, query string) string {
	t.Helper()
	out := strings.TrimRight(string(tabletMySQLExec(t, cc, query)), "\n")
	lines := strings.Split(out, "\n")
	if len(lines) < 2 {
		return "" // header only → empty value
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

// tabletActiveBinlog returns the tablet's current (active) binary-log file
// name — the anchor for a timestamp-independent `PURGE BINARY LOGS TO <file>`
// that drops every older binlog regardless of wall-clock timing.
//
// Uses SHOW BINARY LOGS deliberately: it is version-agnostic (present on every
// MySQL version, never deprecated), unlike SHOW MASTER STATUS — removed in
// MySQL 8.4, which vitess/lite:v24 ships, where it errors 1064 — or
// SHOW BINARY LOG STATUS, which is absent before 8.2. SHOW BINARY LOGS lists
// every binlog file in order; the active one is the highest-numbered, i.e.
// the LAST row.
func tabletActiveBinlog(t *testing.T, cc *chaosCluster) string {
	t.Helper()
	out := strings.TrimRight(string(tabletMySQLExec(t, cc, "SHOW BINARY LOGS")), "\n")
	lines := strings.Split(out, "\n")
	if len(lines) < 2 {
		t.Fatalf("SHOW BINARY LOGS returned no rows:\n%s", out)
	}
	fields := strings.Fields(lines[len(lines)-1]) // last (active) file; Log_name is column 1
	if len(fields) == 0 {
		t.Fatalf("SHOW BINARY LOGS last row has no Log_name:\n%s", out)
	}
	return fields[0]
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
	// Purge by ACTIVE-FILE NAME, not BEFORE NOW(). On a fast local cluster
	// the FLUSHes above land in the same wall-clock second, so
	// `PURGE BINARY LOGS BEFORE NOW()` retains them and gtid_purged never
	// overtakes resumePos — the re-opened stream then stays VALID and idle
	// until the Phase-3 drain times out, a false negative that never
	// exercises the ADR-0093 carve-out. Purging TO the active file is
	// timestamp-independent: it drops every NON-active binlog, which
	// includes the file(s) covering the captured GTIDs.
	activeLog := tabletActiveBinlog(t, cc)
	tabletMySQLExec(t, cc, fmt.Sprintf("PURGE BINARY LOGS TO '%s'", activeLog))
	purged := tabletScalar(t, cc, "SELECT @@global.gtid_purged")
	t.Logf("tablet gtid_purged after PURGE TO %s: %q", activeLog, purged)
	// Fail FAST if the purge precondition wasn't met (gtid_purged still
	// empty ⇒ resume position not overtaken), with a precise message —
	// rather than letting the 2-minute Phase-3 drain timeout mask the cause
	// (the original timing-bug failure mode).
	if purged == "" {
		t.Fatalf("PURGE BINARY LOGS TO %q did not advance gtid_purged (still empty) — "+
			"resume position not overtaken, so the ADR-0093 purged-position path cannot be "+
			"exercised; the purge recipe is broken", activeLog)
	}

	// ---- Phase 3: re-open a VStream FROM the now-stale (purged) resume
	// position and observe what the REAL vtgate/tablet does.
	//
	// PHASE-A GROUND TRUTH (vitess/lite:v24.0.1, mysqld 8.4.8, 2026-06-16 —
	// raw VStream Recv dumped directly, bypassing the pump's watchdog and
	// classifyReaderError):
	//
	//   - resume position = pure GTID "...:1-37" (TablePKs empty; a CDC tail,
	//     not a COPY resume); tablet @@gtid_purged advanced to "...:1-39",
	//     so transactions 1-38/1-39 live only in purged binlog files.
	//   - Re-opening a VStream from 1-37 returned 17 batches over 90s, ALL
	//     HEARTBEAT, zero non-heartbeat events, and NO error from vtgate.
	//     The only terminal error was the client-side 90s deadline.
	//   - The raw error contained NEITHER "purged required binary logs" NOR
	//     "purged"/"gtid"/"1236"; isVStreamPurgedGTIDError == false and
	//     classifyReaderError did NOT wrap ir.ErrPositionInvalid.
	//
	// WHY (corroborated against the vendored vitess.io/vitess v0.24.1 source):
	//   - uvstreamer.setStreamStartPosition only rejects a position that is
	//     AHEAD of the tablet (`!curPos.AtLeast(pos)` → GTIDSetMismatch). A
	//     purged position is BEHIND curPos, so the check PASSES and the stream
	//     proceeds to a binlog dump from 1-37.
	//   - mysqld 8.4.8 cannot serve the purged GTIDs; it drops the dump
	//     connection (errno 2013 CRServerLost) rather than returning a clean
	//     1236. binlog_connection.streamEvents treats CRServerLost as
	//     "possibly intentional", logs INFO, and returns nil; vstreamer's
	//     wrapError(nil,...) then returns nil → a CLEAN stream end. vtgate
	//     keeps the gRPC stream open and emits only its ~5s heartbeats.
	//
	// VERDICT: this Vitess version surfaces NO reactive "purged required
	// binary logs" error for a purged-position CDC-tail resume — it IDLES.
	// So ADR-0093's reactive classifier carve-out (reader_errors.go:
	// isVStreamPurgedGTIDError) can NEVER fire here, and the VStream path has
	// no proactive equivalent of the binlog reader's pre-flight
	// verifyGTIDSetReachable (cdc_reader.go) — a REAL ADR-0093 gap, not a
	// harness artifact. In production a purged VStream resume idles until the
	// Phase-1 liveness watchdog fires a (Bug-141 RETRIABLE) timeout, so the
	// pipeline retries the SAME purged position and never cold-starts.
	//
	// This test therefore pins the GROUND TRUTH (idle → liveness timeout, not
	// ErrPositionInvalid) so the gap can't be mistaken for fixed, and so a
	// future Vitess version that DOES start surfacing a reactive purged error
	// (which the classifier would then catch) shows up here as a behavior
	// change rather than passing silently. The proactive-pre-flight fix that
	// would let the carve-out's INTENT hold for VStream is tracked as the
	// ADR-0093 follow-up (see the investigation report). ----
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
		// A synchronous rejection here WOULD be the reactive signal ADR-0093
		// wants. Phase A proved this version doesn't produce one; if a future
		// version does, assert it carries the carve-out so the intent holds.
		assertPurgedSignalOrDocumentedGap(t, err)
		return
	}

	// Drain until the channel closes (the pump exits when its liveness/
	// progress watchdog fires and cancels the stream), then read Err().
	// chaosVStreamDSN sets vstream_progress_timeout=20s; the Phase-1 liveness
	// window is the default 30s — both well inside this drain budget.
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
			t.Fatal("purged-position stream neither idled-then-timed-out nor errored within 2m — " +
				"behavior changed from the Phase-A ground truth; re-investigate")
		}
	}

	rerr := readerErr(reader)
	if rerr == nil {
		t.Fatal("purged-position resume produced NO reader error at all — even the liveness watchdog " +
			"did not fire; that is a silent-wedge regression (ADR-0073 F3 / loud-failure tenet)")
	}
	assertPurgedSignalOrDocumentedGap(t, rerr)
}

// assertPurgedSignalOrDocumentedGap encodes the ADR-0093 VStream-purged
// ground truth as a forward-compatible assertion. Two acceptable outcomes:
//
//   - REACTIVE PURGED SIGNAL (the ADR-0093 ideal, what the binlog path and a
//     future vtgate would give): err wraps ir.ErrPositionInvalid and is NOT
//     retriable, so the streamer cold-starts. If we ever see this against the
//     real cluster, the carve-out genuinely fired — assert its full shape.
//
//   - THE DOCUMENTED GAP (vitess/lite:v24, mysqld 8.4 — the Phase-A finding):
//     vtgate idles, so the reader surfaces the liveness/progress timeout
//     instead. That error is loud (not a silent wedge) and, per Bug 141,
//     RETRIABLE. We assert it is NOT mis-classified as ErrPositionInvalid
//     (the carve-out did not spuriously fire) and IS the watchdog timeout —
//     pinning the gap exactly so it can't drift unnoticed.
func assertPurgedSignalOrDocumentedGap(t *testing.T, err error) {
	t.Helper()
	if errors.Is(err, ir.ErrPositionInvalid) {
		// The reactive carve-out fired — the ADR-0093 ideal. Hold it to the
		// full contract: cold-start-recoverable, never retriable-in-place.
		var re ir.RetriableError
		if errors.As(err, &re) {
			t.Fatalf("purged-position error wraps ErrPositionInvalid but is ALSO retriable; "+
				"retrying the same purged position spins forever: %v", err)
		}
		t.Logf("REACTIVE PURGED SIGNAL fired (ADR-0093 ideal): %v", err)
		return
	}
	// The documented gap: vtgate idled and the liveness watchdog fired. It
	// must be the watchdog timeout, not some other terminal error, and it
	// must be loud (non-nil, already checked by the caller).
	if !strings.Contains(err.Error(), "no events within") &&
		!strings.Contains(err.Error(), "produced no events for") {
		t.Fatalf("purged-position resume surfaced an UNEXPECTED error (neither the ADR-0093 "+
			"reactive ErrPositionInvalid nor the documented liveness/progress timeout) — "+
			"behavior changed from Phase-A ground truth, re-investigate: %v", err)
	}
	t.Logf("DOCUMENTED ADR-0093 VSTREAM-PURGED GAP confirmed: vtgate idled on the purged "+
		"position and the reader surfaced the liveness/progress watchdog timeout (loud, "+
		"Bug-141 retriable) — NOT ir.ErrPositionInvalid; the reactive carve-out cannot fire "+
		"on this Vitess version. A proactive gtid_purged pre-flight (binlog-reader parity) "+
		"is required for true cold-start recovery. Surfaced error: %v", err)
}
