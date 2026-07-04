//go:build integration && vitesscluster && chaos

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0093 / Bug 146: VStream purged-GTID resume → PROACTIVE pre-flight →
// cold-start re-snapshot.
//
// The binlog source recovers from a purged resume position via a pre-flight
// gtid_purged ⊆ resume check (cdc_reader.go verifyGTIDSetReachable) that
// returns ir.ErrPositionInvalid; the streamer's ADR-0022 fall-through
// re-enters cold-start. The VStream path was expected to discover a purged
// position REACTIVELY (vtgate rejecting the stream with "purged required
// binary logs", classified by reader_errors.go isVStreamPurgedGTIDError) —
// but a Phase-A investigation proved Vitess 24 does NOT emit that reactive
// error: vtgate accepts the (behind) position, mysqld drops the dump with
// errno 2013, vtgate treats it as a clean end and only heartbeats, so the
// stream idles into a (Bug-141 retriable) liveness timeout and never
// cold-starts. Bug 146 closes that with a PROACTIVE pre-flight on the
// VStream open path (cdc_vstream.go verifyVStreamPositionReachable): before
// opening the stream, query GTID_SUBSET(@@global.gtid_purged, resume) —
// routed at the SAME tablet type the stream binds to (gtid_purged is
// tablet-type-routed by vtgate) — and return ir.ErrPositionInvalid when the
// resume is unreachable. The reactive classifier remains as defence-in-depth
// for any source that DOES surface the error.
//
// This test boots the REAL multi-process Vitess cluster (the same harness
// the chaos suite uses), captures a VStream position, advances the
// underlying primary tablet's gtid_purged PAST that position (FLUSH +
// PURGE BINARY LOGS on the tablet's mysqld; chaosVStreamDSN streams from the
// PRIMARY, the tablet purged here), then re-opens a VStream from the stale
// position and asserts StreamChanges refuses SYNCHRONOUSLY with an error that
// errors.Is(ir.ErrPositionInvalid) and is NOT retriable — proving the
// proactive pre-flight fires before the stream opens. The streamer-level
// recovery (re-snapshot vs loud opt-out) is pinned by the unit tests in
// internal/pipeline; this engine-level test pins the source signal the
// recovery depends on.

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

// TestVitessChaos_PurgedGTID_ReactiveColdStart pins ADR-0093: a VStream
// resume from a position older than the tablet's retained binlogs surfaces
// a reactive error that classifyReaderError maps to ir.ErrPositionInvalid.
func TestVitessChaos_PurgedGTID_ReactiveColdStart(t *testing.T) {
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

	changes, serr := reader.StreamChanges(ctx, resumePos)
	if serr == nil {
		// The pre-flight should have refused synchronously. If the stream
		// opened instead, that's the Bug 146 regression: it would idle into a
		// retriable liveness timeout and never cold-start. Tear the stream
		// down so a pump goroutine doesn't linger, then fail.
		_ = changes
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		t.Fatal("purged-position resume OPENED the stream instead of being refused by the " +
			"gtid_purged pre-flight (Bug 146 regression) — vtgate would idle and the stream " +
			"would never cold-start")
	}
	assertProactivePurgedRefusal(t, serr)
}

// assertProactivePurgedRefusal asserts the Bug 146 pre-flight refused the
// purged resume with the cold-start signal: the error wraps
// ir.ErrPositionInvalid (so the streamer's ADR-0022 fall-through re-snapshots)
// and is NOT retriable (retrying the same purged position would spin forever).
func assertProactivePurgedRefusal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("purged-position pre-flight error does not wrap ir.ErrPositionInvalid "+
			"(ADR-0022 cold-start won't fire): %v", err)
	}
	var re ir.RetriableError
	if errors.As(err, &re) {
		t.Fatalf("purged-position error wraps ErrPositionInvalid but is ALSO retriable; "+
			"retrying the same purged position spins forever: %v", err)
	}
	t.Logf("Bug 146 proactive pre-flight fired: purged resume refused with the cold-start "+
		"signal (ir.ErrPositionInvalid, non-retriable): %v", err)
}
