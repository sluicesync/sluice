// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Native-MySQL concurrent multi-table cold-copy (ADR-0101). The classic
// binlog-flavor snapshot (cdc_snapshot.go) copies tables SERIALLY from ONE
// pinned REPEATABLE-READ transaction — the Track-D bottleneck (~0.66 MB/s
// vanilla MySQL → PS-MySQL vs ~26 MB/s for Vitess, whose cross-table
// concurrency is driven by the VStream-only vstream_copy_table_parallelism
// knob).
//
// This file brings cross-table concurrent cold-copy to NATIVE (non-Vitess)
// MySQL sources. Native MySQL has no exported/shareable snapshot (can't
// share an InnoDB read-view across connections, unlike PG) and no VStream,
// so the consistent multi-table snapshot is built with the proven
// mydumper / `mysqldump --single-transaction --master-data` pattern:
//
//  1. FLUSH TABLES WITH READ LOCK (FTWRL) — briefly freeze writes globally.
//  2. On EACH of N reader connections, under the lock:
//     SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
//     START TRANSACTION WITH CONSISTENT SNAPSHOT — so all N snapshots pin
//     the SAME consistent cut.
//  3. Record ONE binlog position (SHOW BINARY LOG STATUS) — the single
//     CDC-resume anchor.
//  4. UNLOCK TABLES — writes resume; the N reader transactions keep their
//     frozen snapshots.
//  5. Partition the in-scope tables into N disjoint groups and surface them
//     via ir.ConcurrentCopyPartitioner; the ADR-0100 pipeline consumer
//     (runConcurrentTableCopy) drives W = N read→write pipelines, each
//     reading its group from its own pinned connection.
//  6. CDC resumes from the single recorded position — no per-table stitch,
//     because all N readers share one consistent point (unlike VStream's
//     set-min stitch, ADR-0099 §4).
//
// FTWRL needs RELOAD/FLUSH_TABLES and is blocked on some managed MySQL
// (RDS/Aurora). If it is unavailable, this opener FALLS BACK to the serial
// single-snapshot path (openBinlogSnapshotStreamShared) with a LOUD WARN —
// NEVER silently producing an inconsistent multi-table snapshot (opening N
// independent snapshots without the lock could let a commit interleave
// between the 1st and Nth START TRANSACTION, landing in some readers'
// snapshots but not others, with no single position naming a cut consistent
// with all N). See ADR-0101 §4.
//
// Since the perf-parity gap-3 chunk, the concurrent path is the DEFAULT for
// a multi-table cold-copy (defaultNativeCopyTableParallelism = 4, matching
// migrate's cross-table auto); copy_table_parallelism=1 (DSN) or
// --copy-table-parallelism=1 (CLI) is the serial opt-out.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// defaultNativeCopyTableParallelism is the native-binlog cold-copy reader
// count when the operator sets neither the CLI flag nor the DSN knob. It is
// 4 — the SAME auto value migrate's cross-table pool defaults to
// (pipeline defaultTableParallelism, ADR-0076) — so a MySQL user's first
// `sync` cold-start is no longer ~4× slower than the `migrate` they just ran
// against the same database (perf-parity matrix gap 3, roadmap item 54).
//
// Why the flip is safe here but NOT on the VStream path (which keeps
// defaultCopyTableParallelism = 1): the FTWRL-coordinated N-snapshot is
// consistency-IDENTICAL to the serial path (one cut, one recorded position,
// no stitch — ADR-0101 §3; strictly stronger than migrate's own
// per-connection-snapshot parallel readers), the serial opener already
// attempts the same FTWRL on every cold start, FTWRL-denied sources
// (RDS/Aurora) fall back to serial with a LOUD WARN (ADR-0101 §4), and the
// native path has no cross-process resume whose table→reader partition a
// changed default could destabilize (in-memory cursors only). ADR-0101 §1's
// serial default was cost caution, not correctness — see the ADR's
// implementation note. resolveCopyTableParallelism still clamps to
// min(N, nTables, maxCopyTableParallelism), so a small schema never
// over-opens; an explicit 1 (CLI or DSN) is the serial opt-out.
//
// Zero-value-safety (the v0.99.51 trap): this default lives in the RESOLVER
// chain below (CLI override → DSN param → this constant), never in a struct
// field — every construction that reaches the table-scoped opener resolves
// the same value, and the Go zero of the CLI override (0 = unset) falls
// through rather than forcing serial.
const defaultNativeCopyTableParallelism = 4

// nativeCopyTableParallelismFromDSN reads the optional copy_table_parallelism
// source-DSN parameter — the number of CONCURRENT pinned-snapshot reader
// connections the native-binlog cold-copy opens (ADR-0101). It is the
// native-MySQL analogue of vstreamCopyTableParallelismFromDSN (and reuses the
// SAME resolveCopyTableParallelism resolver + clamp); the distinct DSN key
// keeps it independent of the VStream knob (a self-managed MySQL source has
// no VStream). Absent ⇒ defaultNativeCopyTableParallelism (auto: 4, clamped
// to the table count — NOT the VStream default of 1; see the constant's
// rationale). A malformed value is a LOUD error (the loud-failure tenet: an
// operator who set the knob deserves to know it didn't parse), NOT a silent
// fallback to serial.
//
// ADR-0118 finding 4 precedence: an explicit --copy-table-parallelism CLI flag
// (the per-instance cliOverride, value > 0 — formerly the
// SetNativeCopyTableParallelismOverride global, now
// engineOptions.copyTableParallelism, task 2.5) WINS over the DSN param. The DSN
// form is still read + validated loudly when no CLI override is set, so existing
// DSN-only setups are byte-identical.
func nativeCopyTableParallelismFromDSN(cfg *gomysql.Config, cliOverride int) (int, error) {
	if cliOverride > 0 {
		return cliOverride, nil
	}
	v := cfg.Params["copy_table_parallelism"]
	if v == "" {
		return defaultNativeCopyTableParallelism, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf(
			"mysql: invalid copy_table_parallelism %q (want a positive integer, e.g. 4; 0 or 1 means serial single-snapshot): %w",
			v, err,
		)
	}
	return n, nil
}

// openBinlogSnapshotStreamConcurrent is the ADR-0101 concurrent analogue of
// [Engine.openBinlogSnapshotStreamShared]: it opens N reader connections,
// each pinned to its own REPEATABLE-READ consistent-snapshot transaction,
// ALL under one held FTWRL, records ONE binlog position, then unlocks — so
// the N snapshots share one consistent cut = the recorded CDC anchor.
//
// n is the resolved (already-clamped) reader count; the caller resolves it
// from copy_table_parallelism. tables is the in-scope unqualified table
// allowlist (len(tables) > 1 — the caller gates on it). On FTWRL
// unavailability it falls back to the serial single-snapshot opener (LOUD
// WARN), never proceeding with N independent snapshots.
//
// The returned stream's Position is recorded ONCE inside the FTWRL window
// and never mutated during copy, so the cold-start→CDC handoff reads the
// correct anchor after the copy errgroup joins (no WaitCopyCompleteFn
// needed — ADR-0101 §7).
func (e Engine) openBinlogSnapshotStreamConcurrent(ctx context.Context, dsn string, n int, tables []string) (*ir.SnapshotStream, error) {
	if n <= 1 || len(tables) <= 1 {
		// Defensive: the caller gates on n > 1 && len(tables) > 1. A
		// degenerate call collapses to the serial path (byte-identical,
		// the zero-value-safe floor) rather than opening a 1-reader
		// "concurrent" copy.
		return e.openBinlogSnapshotStreamShared(ctx, dsn, false)
	}

	// ADR-0153 read-fidelity exemption: snapshot ROW-DATA reads keep the
	// binary protocol (FLOAT text display-rounding — see OpenRowReader).
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// Per-sync zero-date policy (ADR-0127): the concurrent cold-copy honors the
	// same source-DSN `zero_date` override as the serial path and the CDC
	// reader. Resolved before openDB so an invalid value refuses loudly. The DSN
	// param wins; absent, the engine's --zero-date default applies.
	zeroDate, err := readerZeroDateMode(cfg)
	if err != nil {
		return nil, err
	}
	zeroDate = e.resolveReaderZeroDate(zeroDate)
	// ADR-0109 §A: raise net_write_timeout / net_read_timeout on the
	// concurrent snapshot pool too — every one of the N FTWRL-coordinated
	// reader connections inherits it at session init, so a target stall
	// backpressuring any reader doesn't trip the source's default 60s
	// net_write_timeout. Bounded (10 min), operator-override-respecting.
	applySourceReadSessionTimeouts(cfg)
	db, err := openDB(ctx, cfg, e.opts.sqlMode)
	if err != nil {
		return nil, err
	}

	// Acquire the consistent N-snapshot (FTWRL → N pinned CONSISTENT SNAPSHOT
	// conns → record ONE binlog position P → UNLOCK). Extracted so the
	// ADR-0111 re-snapshot recovery re-runs the EXACT same sequence on a fresh
	// pool. errFTWRLUnavailable is the no-RELOAD signal → fall back to the
	// SERIAL single-snapshot path (LOUD WARN, ADR-0101 §4); any other error
	// already cleaned up the pool inside the helper.
	conns, file, pos, serverUUID, err := acquireConsistentSnapshot(ctx, db, n)
	if err != nil {
		if errors.Is(err, errFTWRLUnavailable) {
			slog.WarnContext(ctx,
				"mysql: snapshot(concurrent): FLUSH TABLES WITH READ LOCK failed; "+
					"falling back to the SERIAL single-snapshot cold-copy — cross-table "+
					"concurrency DISABLED, consistency preserved. The N-way concurrent "+
					"cold-copy (the default since the perf-parity gap-3 chunk) needs the "+
					"RELOAD privilege; grant it to the source user to re-enable, or set "+
					"--copy-table-parallelism=1 to opt into serial and silence this.",
				slog.Int("requested_parallelism", n),
				slog.String("error", err.Error()))
			_ = db.Close()
			return e.openBinlogSnapshotStreamShared(ctx, dsn, false)
		}
		_ = db.Close()
		return nil, err
	}

	// Paired CDC reader (separate connection + binlog-dump protocol), same
	// as the single-reader path.
	cdcReader, err := e.openCDCReaderForSnapshot(ctx, dsn, false)
	if err != nil {
		// Lock already released; just roll back + close the readers + pool.
		for _, c := range conns {
			_, _ = c.ExecContext(context.Background(), "ROLLBACK")
			_ = c.Close()
		}
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot(concurrent): build cdc reader: %w", err)
	}

	// The ORIGINAL CDC anchor P. It is stamped onto stream.Position here and
	// recorded on the reader; the ADR-0111 re-snapshot recovery NEVER advances
	// it (the value-fidelity invariant). CopyCursors is empty at open.
	anchor := binlogPos{
		Mode:       positionModeFilePos,
		File:       file,
		Pos:        pos,
		ServerUUID: serverUUID,
	}
	position, err := encodeBinlogPos(anchor)
	if err != nil {
		_ = cdcReader.(closer).Close()
		for _, c := range conns {
			_, _ = c.ExecContext(context.Background(), "ROLLBACK")
			_ = c.Close()
		}
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot(concurrent): encode position: %w", err)
	}

	// Partition the in-scope tables into n disjoint groups (the SAME
	// deterministic pure function ADR-0099 uses; coverage/disjointness/
	// determinism inherited + unit-pinned). v1 wires no size estimator, so
	// it uses the deterministic round-robin floor.
	groups := partitionTablesForStreams(tables, n, nil)

	// Build one inner RowReader per pinned connection, then the
	// multi-snapshot router that dispatches each table's ReadRows to its
	// group's connection (ADR-0101 §6). Each connection serves exactly one
	// group (disjoint), and the ADR-0100 consumer drains each group serially
	// within one goroutine, so each connection runs at most ONE SELECT at a
	// time; the n goroutines run n SELECTs across n DISTINCT connections.
	rows := newConcurrentBinlogRows(conns, groups, cfg.DBName, db, zeroDate)

	// ADR-0111: record the original anchor + wire the re-snapshot recovery so a
	// classified source-read drop resumes incomplete tables from their cursors
	// instead of restarting the whole copy from row 0. The resnapshot closure
	// re-runs the EXACT FTWRL→N-CONSISTENT-SNAPSHOT→position→UNLOCK sequence on
	// a FRESH pool (a dropped connection's read-view cannot be recreated), and
	// hands the fresh conns back to the reader. It captures dsn/n only — never
	// the anchor — so it cannot accidentally advance P.
	rows.anchor = anchor
	rows.anchorToken = position.Token
	rows.anchorSet = true
	rows.resnapshot = func(rctx context.Context) ([]*sql.Conn, *sql.DB, string, uint32, error) {
		// Same ADR-0153 read-fidelity exemption as the initial open.
		rcfg, perr := parseDSN(dsn)
		if perr != nil {
			return nil, nil, "", 0, perr
		}
		applySourceReadSessionTimeouts(rcfg)
		rdb, derr := openDB(rctx, rcfg, e.opts.sqlMode)
		if derr != nil {
			return nil, nil, "", 0, derr
		}
		rconns, rfile, rpos, _, aerr := acquireConsistentSnapshot(rctx, rdb, n)
		if aerr != nil {
			_ = rdb.Close()
			return nil, nil, "", 0, aerr
		}
		return rconns, rdb, rfile, rpos, nil
	}

	// Honest connection-budget surfacing (no false auto-clamp — ADR-0101 §5 /
	// ADR-0102 §3). readers = W = the cross-table reader pipelines; each fans
	// its active table across D = --copy-fanout-degree plain-INSERT writers
	// (ADR-0102), so the target write concurrency is W × D and the operator
	// owns the budget (MySQL has no connection-slot prober on this path).
	slog.InfoContext(ctx,
		"mysql: native concurrent cold-copy: opened consistent multi-table snapshot",
		slog.Int("readers", n),
		slog.Int("groups", len(groups)),
		slog.String("contract", "operator ensures W(readers) × D(--copy-fanout-degree) ≤ --max-target-connections and readers ≤ source max_connections"))

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rows,
		Changes:  cdcReader,
	}

	// Lifecycle: COMMIT + close all N pinned connections + the pool, once,
	// first-error-wins (ADR-0101 §8). It closes the reader's CURRENT connection
	// set — after an ADR-0111 re-snapshot recovery those are the FRESH P′ conns
	// (the recovery already closed the dropped originals), so the lifecycle
	// always cleans up whatever the reader holds NOW, not the open-time pool.
	released := false
	releaseRows := func() error {
		if released {
			return nil
		}
		released = true
		return rows.commitAndCloseConns()
	}
	stream.ReleaseRowsFn = releaseRows
	stream.CloseFn = func() error {
		var firstErr error
		if c, ok := cdcReader.(closer); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := releaseRows(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return stream, nil
}

// errFTWRLUnavailable signals that FLUSH TABLES WITH READ LOCK failed (no
// RELOAD privilege, or a managed MySQL that blocks it). The INITIAL open maps
// it to the SERIAL single-snapshot fallback (ADR-0101 §4); the ADR-0111
// recovery treats it as a non-transient terminal failure (the source's
// FTWRL capability won't appear mid-run, so retrying is pointless).
var errFTWRLUnavailable = errors.New("mysql: snapshot(concurrent): FLUSH TABLES WITH READ LOCK unavailable")

// acquireConsistentSnapshot runs the consistency-lynchpin sequence on db:
// FTWRL → N pinned REPEATABLE-READ CONSISTENT SNAPSHOT connections → record
// ONE binlog position (file, pos) → UNLOCK → read @@server_uuid. All N
// snapshots pin the SAME cut because the FTWRL is held across every START
// TRANSACTION and the position read (ADR-0101 §2). It is the ONE place this
// sequence lives so the initial open AND the ADR-0111 re-snapshot recovery
// run byte-identical logic on their respective pools.
//
// On FTWRL failure it returns errFTWRLUnavailable (and cleans up). On any
// other failure before UNLOCK it releases the lock + closes the pinned conns
// (NOT db — the caller owns the pool) and returns the wrapped error. On
// success the N pinned connections are live (each in its consistent-snapshot
// transaction) and the caller owns their lifecycle.
func acquireConsistentSnapshot(ctx context.Context, db *sql.DB, n int) (conns []*sql.Conn, file string, pos uint32, serverUUID string, err error) {
	conn0, err := db.Conn(ctx)
	if err != nil {
		return nil, "", 0, "", fmt.Errorf("mysql: snapshot(concurrent): pin conn 0: %w", err)
	}
	if _, err := conn0.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn0.Close()
		return nil, "", 0, "", fmt.Errorf("mysql: snapshot(concurrent): set isolation: %w", err)
	}
	// FTWRL is the consistency lynchpin — never proceed with N independent
	// snapshots if it fails (silent-inconsistency class, ADR-0101 §4).
	if _, err := conn0.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		_ = conn0.Close()
		return nil, "", 0, "", fmt.Errorf("%w: %w", errFTWRLUnavailable, err)
	}

	// From here the FTWRL is HELD. Every failure path before UNLOCK releases it
	// and closes the pinned conns accumulated so far.
	conns = []*sql.Conn{conn0}
	failUnlock := func(wrap error) ([]*sql.Conn, string, uint32, string, error) {
		_, _ = conn0.ExecContext(context.Background(), "UNLOCK TABLES")
		for _, c := range conns {
			_ = c.Close()
		}
		return nil, "", 0, "", wrap
	}

	if _, err := conn0.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): start tx (reader 0): %w", err))
	}
	// Open the remaining N-1 reader connections + their snapshot transactions
	// while the FTWRL is held — so they pin the SAME cut.
	for i := 1; i < n; i++ {
		c, err := db.Conn(ctx)
		if err != nil {
			return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): pin conn %d: %w", i, err))
		}
		conns = append(conns, c)
		if _, err := c.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
			return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): set isolation (reader %d): %w", i, err))
		}
		if _, err := c.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
			return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): start tx (reader %d): %w", i, err))
		}
	}

	// Record the ONE binlog position inside the lock — the single CDC-resume
	// anchor every reader shares (ADR-0101 §2 step 3).
	file, pos, err = snapshotMasterStatus(ctx, conn0)
	if err != nil {
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): capture position: %w", err))
	}
	// Release the write freeze — snapshot views + position are captured.
	if _, err := conn0.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): unlock tables: %w", err))
	}
	// Instance identity (Track 1c floor) — non-fatal on failure.
	if uerr := conn0.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&serverUUID); uerr != nil {
		serverUUID = ""
	}
	return conns, file, pos, serverUUID, nil
}

// concurrentBinlogRows is the multi-snapshot RowReader (ADR-0101 §6): it
// owns N pinned connections (each on its own consistent-snapshot
// transaction) and routes each table's [ReadRows] to the connection owning
// that table's group. It implements [ir.RowReader] and
// [ir.ConcurrentCopyPartitioner] so the ADR-0100 pipeline consumer drives
// W = N read→write pipelines over the disjoint partition.
//
// It deliberately does NOT implement [ir.IdempotentCopyReader]: the binlog
// snapshot is gap-free + overlap-free (each table read exactly once from a
// frozen REPEATABLE-READ view), so the cold-copy uses the plain-INSERT
// concurrent path, not the upsert one. The pipeline's concurrent-partition
// guard is widened to allow a non-idempotent gap-free reader onto the plain
// concurrent path (ADR-0101 §6).
// Compile-time guarantee that the native multi-snapshot reader satisfies the
// work-stealing surface (roadmap 21a) in addition to the static partition one
// — its N connections share one FTWRL cut, so any can read any table. It also
// satisfies the CHUNKED work-stealing surface (roadmap 21b, ADR-0119): any
// connection can read any PK-RANGE of any table, so a large table can be split
// across several idle readers at the tail. And it implements ir.RowCounter so
// the progress ticker gets a per-table ETA on the concurrent path (the inner
// snapshot readers cannot — closer == nil no-ops their CountRows — so the count
// runs on the side metaDB pool instead).
var (
	_ ir.WorkStealingCopyReader        = (*concurrentBinlogRows)(nil)
	_ ir.ChunkedWorkStealingCopyReader = (*concurrentBinlogRows)(nil)
	_ ir.RowCounter                    = (*concurrentBinlogRows)(nil)
)

type concurrentBinlogRows struct {
	// connMu guards the SWAPPABLE inner-connection set (byTable, readers,
	// metaDB) against the ADR-0111 re-snapshot-from-cursor recovery, which
	// replaces all three under it. The copy path reads via pickReader (RLock);
	// recovery writes via swapConnections (Lock) — so a read never observes a
	// half-swapped connection set.
	connMu sync.RWMutex

	// byTable maps an unqualified table name to the inner RowReader owning its
	// group; byTableIdx maps it to that group's INDEX (so the resumable read
	// can re-resolve the owner after a swap, where the *RowReader pointers
	// change but the index is stable). Built at open from the disjoint
	// partition; rebuilt onto the fresh snapshot by the ADR-0111 recovery.
	byTable    map[string]*RowReader
	byTableIdx map[string]int

	// readers is the N inner RowReaders (one per connection), kept for Close,
	// the work-stealing ReadRowsOn, and pickReader. Each wraps one *sql.Conn
	// (snapshot mode: closer nil — the SnapshotStream owns the lifecycle).
	// Swapped by the ADR-0111 recovery (guarded by connMu).
	readers []*RowReader

	// groups is the disjoint table partition surfaced via
	// ConcurrentCopyGroups (one group per inner reader, same index).
	groups [][]string

	// metaDB is the source connection POOL the N snapshot connections were
	// pinned from (retained for the copy's lifetime — releaseRows closes it).
	// CountRows runs the cheap information_schema.TABLE_ROWS estimate on a
	// FRESH pooled connection here, NOT on a pinned snapshot connection: the
	// inner snapshot readers (closer == nil) deliberately no-op CountRows
	// because a concurrent metadata query on a connection that is actively
	// streaming a table's rows would deadlock/error (one query per *sql.Conn).
	// The estimate is catalog metadata — it does not need the snapshot — so a
	// non-snapshot pooled connection is both correct and collision-free. The
	// pool has no MaxOpenConns cap (openDB), so this never starves behind the
	// N pinned connections. Swapped by the recovery (guarded by connMu).
	metaDB *sql.DB
	dbName string

	// zeroDate is the per-sync zero/partial-date policy (ADR-0127) stamped onto
	// every inner RowReader at construction AND re-stamped onto the fresh
	// readers the ADR-0111 recovery builds in swapConnections, so a re-snapshot
	// never silently reverts the cold-copy to the global default.
	zeroDate zeroDateMode

	// --- ADR-0111 resumable cold-copy state (see cdc_snapshot_concurrent_resume.go) ---

	// anchor is the ORIGINAL CDC anchor P recorded at open (file/pos + uuid),
	// and anchorToken is its encoded token. The re-snapshot recovery NEVER
	// advances either (the ADR-0111 §3 value-fidelity invariant); the recovery
	// re-encodes anchor and asserts it still equals anchorToken as a runtime
	// guard against an accidental advance.
	anchor      binlogPos
	anchorToken string
	anchorSet   bool

	// resnapshot re-establishes a fresh consistent N-snapshot at P′ for the
	// recovery. Supplied by the opener; nil disables recovery (a drop becomes
	// terminal, never a silent wrong-point read).
	resnapshot resnapshotFn

	// cursMu guards the IN-MEMORY per-table cursor map (NOT persisted — see the
	// ADR-0111 scope note in cdc_snapshot_concurrent_resume.go). Distinct from
	// connMu so a cursor update on the row path never blocks (or is blocked by)
	// a connection swap.
	cursMu  sync.Mutex
	cursors map[string]*tableCursor

	// recoveryMu serialises the re-snapshot recovery across the W concurrent
	// read pipelines. recoveryGen counts COMPLETED recoveries (bumped once per
	// successful re-snapshot, under recoveryMu). It coalesces peers: a pipeline
	// captures the generation it began its read at (currentRecoveryGen) and, on
	// a drop, only re-snapshots if recoveryGen has NOT advanced past that
	// captured value — otherwise a peer already re-snapshotted this drop and it
	// just resumes on the swapped-in fresh connections. So a single grow window
	// triggers ONE FTWRL re-snapshot, not W.
	recoveryMu  sync.Mutex
	recoveryGen int

	// errMu / err is the reader's OWN sticky error (the resumable path owns
	// error reporting now; the inner readers' errors are consumed inside the
	// per-table read). Mirrors RowReader.Err's contract for the orchestrator's
	// post-drain check.
	errMu sync.Mutex
	err   error
}

// newConcurrentBinlogRows builds the router from the N pinned connections
// and their disjoint groups. conns[i] serves groups[i]; len(conns) ==
// len(groups). dbName is the source database (the single-database snapshot
// path; reads are unqualified, byte-identical to the serial reader). The
// ADR-0111 resumable state (anchor, resnapshot fn) is attached by the opener
// after construction. zeroDate is the per-sync zero-date policy (ADR-0127)
// stamped onto every inner reader.
func newConcurrentBinlogRows(conns []*sql.Conn, groups [][]string, dbName string, metaDB *sql.DB, zeroDate zeroDateMode) *concurrentBinlogRows {
	readers := make([]*RowReader, len(conns))
	byTable := make(map[string]*RowReader)
	byTableIdx := make(map[string]int)
	for i, c := range conns {
		rr := &RowReader{
			q:      c,
			schema: dbName,
			// Single-database snapshot: unqualified SELECTs, byte-identical
			// to the serial binlog reader. closer nil — the SnapshotStream's
			// Close/ReleaseRows owns the connection lifecycle.
			qualifyBySchema: false,
			closer:          nil,
			zeroDate:        zeroDate,
		}
		readers[i] = rr
		if i < len(groups) {
			for _, t := range groups[i] {
				byTable[t] = rr
				byTableIdx[t] = i
			}
		}
	}
	return &concurrentBinlogRows{
		byTable:    byTable,
		byTableIdx: byTableIdx,
		readers:    readers,
		groups:     groups,
		metaDB:     metaDB,
		dbName:     dbName,
		zeroDate:   zeroDate,
		cursors:    make(map[string]*tableCursor),
	}
}

// setErr records the reader's sticky error (first-wins is not enforced — the
// last classified terminal error is the one the orchestrator surfaces, which
// matches RowReader.setErr). Safe to call from the per-table read goroutines.
func (r *concurrentBinlogRows) setErr(err error) {
	if err == nil {
		return
	}
	r.errMu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.errMu.Unlock()
}

// ReadRows dispatches the table to the inner reader (pinned connection)
// owning its group and streams its rows from that connection's snapshot
// transaction. A table not present in any group is refused LOUDLY rather
// than silently read from a wrong/zero connection — that would be a
// partition/scope mismatch (a table the engine surfaced no reader for),
// the silent-loss class the loud-failure tenet forbids (ADR-0101 §6).
func (r *concurrentBinlogRows) ReadRows(ctx context.Context, table *ir.Table) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql: concurrent ReadRows: table is nil")
	}
	idx, ok := r.byTableIdx[table.Name]
	if !ok {
		return nil, fmt.Errorf(
			"mysql: concurrent ReadRows: table %q is not in the concurrent-copy partition "+
				"(no reader owns it — a partition/scope mismatch); refusing to read from an arbitrary connection",
			table.Name,
		)
	}
	// ADR-0111: read through the resumable wrapper, pinned to the table's
	// statically-assigned group connection (idx). The wrapper rides out a
	// classified source-read drop via re-snapshot-from-cursor recovery,
	// producing ONE continuous channel; the CDC anchor stays at the original P.
	// Whole-table read: no PK-range bounds, cursor keyed on the table name.
	return r.readResumable(ctx, table, nil, nil, table.Name, idx)
}

// ConcurrentCopyGroups returns the disjoint table partition the cold-start
// bulk copy may write CONCURRENTLY (ir.ConcurrentCopyPartitioner) — one
// consumer pipeline per group, each group's tables drained serially through
// its owning connection. Only ever >1 group here (the opener gates on
// n > 1 && len(tables) > 1), so the pipeline always engages the W-goroutine
// consumer for this reader.
func (r *concurrentBinlogRows) ConcurrentCopyGroups() [][]string {
	return r.groups
}

// ConcurrentReaderCount implements [ir.WorkStealingCopyReader]: the number of
// pinned connections. Every connection is a consistent-snapshot transaction
// from the SAME FTWRL cut (ADR-0101), so any can read any in-scope table and
// see identical data — which is what makes work-stealing correct (roadmap
// item 21a). >1 here by construction (the opener gates on n > 1).
func (r *concurrentBinlogRows) ConcurrentReaderCount() int {
	return len(r.readers)
}

// ReadRowsOn implements [ir.WorkStealingCopyReader]: read table on the pinned
// connection at index `reader`, rather than the table's statically-assigned
// owner ([ReadRows] via byTable). Correct for ANY index because all N
// connections share the one FTWRL snapshot cut. The work-stealing consumer
// guarantees at most one in-flight ReadRowsOn per index, so each connection
// still serves one query at a time (the same invariant the static partition
// gave for free). A table not present in the schema still produces a valid
// SELECT; an out-of-range index is refused LOUDLY (a caller bug, never a
// silent wrong-connection read).
func (r *concurrentBinlogRows) ReadRowsOn(ctx context.Context, table *ir.Table, reader int) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql: concurrent ReadRowsOn: table is nil")
	}
	if reader < 0 || reader >= len(r.readers) {
		return nil, fmt.Errorf(
			"mysql: concurrent ReadRowsOn: reader index %d out of range [0,%d) — caller bug",
			reader, len(r.readers),
		)
	}
	// ADR-0111: read through the resumable wrapper, pinned to the requested
	// connection index. After a re-snapshot the index still selects a valid
	// fresh connection (the reader count is preserved across recovery).
	// Whole-table read: no PK-range bounds, cursor keyed on the table name.
	return r.readResumable(ctx, table, nil, nil, table.Name, reader)
}

// ReadRowsRangeOn implements [ir.ChunkedWorkStealingCopyReader]: read the
// half-open PK range (lowerPK, upperPK] of table on the pinned connection at
// index `reader` (ADR-0119, roadmap 21b). Correct for ANY index because all N
// connections share the one FTWRL snapshot cut, and correct for ANY range
// because the upper clip is pushed into SQL in the column's native collation
// (the Bug-74 contract) — so the M chunks of a table tile its rows with no gap
// and no overlap. chunkIndex disambiguates the per-(table,chunk) resume cursor
// (Decision 4) so concurrent chunks of one table never alias on the shared
// cursor map: cursorKey = "table#chunkIndex" (vs table.Name for a whole-table
// read). An out-of-range index is refused LOUDLY (a caller bug, never a silent
// wrong-connection read), mirroring [ReadRowsOn].
func (r *concurrentBinlogRows) ReadRowsRangeOn(ctx context.Context, table *ir.Table, lowerPK, upperPK []any, chunkIndex, reader int) (<-chan ir.Row, error) {
	if table == nil {
		return nil, errors.New("mysql: concurrent ReadRowsRangeOn: table is nil")
	}
	if reader < 0 || reader >= len(r.readers) {
		return nil, fmt.Errorf(
			"mysql: concurrent ReadRowsRangeOn: reader index %d out of range [0,%d) — caller bug",
			reader, len(r.readers),
		)
	}
	return r.readResumable(ctx, table, lowerPK, upperPK, workItemCursorKey(table.Name, chunkIndex), reader)
}

// workItemCursorKey is the per-work-item key into the in-memory resume cursor
// map (ADR-0119 Decision 4). A whole-table item (chunkIndex < 0) keys on the
// bare table name — sharing the tier-(a) whole-table cursor space; a chunk keys
// on "table#chunkIndex". A table is read EITHER whole OR as chunks (never both),
// so the two key spaces never overlap, and CONCURRENT chunks of one table get
// DISTINCT entries — no collision on the shared cursor map under W readers.
func workItemCursorKey(tableName string, chunkIndex int) string {
	if chunkIndex < 0 {
		return tableName
	}
	return fmt.Sprintf("%s#%d", tableName, chunkIndex)
}

// RangeBounds implements [ir.RangeBoundsQuerier] for the chunked work-stealing
// boundary HINT (ADR-0119 Decision 2): it delegates to a metaDB-backed
// [RowReader] (a FRESH pooled connection, NOT a pinned snapshot conn — the same
// collision-free side pool CountRows uses), so the MIN/MAX query never races an
// in-flight snapshot read. The result is a partition hint, not a
// consistency-bearing read: the chunk ranges tile (-inf, +inf] regardless of
// how fresh the bounds are. A missing pool/dbName yields (nil, nil, nil) — an
// honest "no hint" that collapses the table to a single whole-table chunk.
func (r *concurrentBinlogRows) RangeBounds(ctx context.Context, table *ir.Table, pkColumn string) (minVal, maxVal any, err error) {
	mr := r.metaReader()
	if mr == nil {
		return nil, nil, nil
	}
	return mr.RangeBounds(ctx, table, pkColumn)
}

// SampleKeysetBoundaries implements [ir.KeysetSampler] for the chunked
// work-stealing boundary HINT (non-integer / composite PK, ADR-0119 Decision
// 2). Like [RangeBounds] it delegates to a metaDB-backed [RowReader] on the
// side pool. A missing pool/dbName yields (nil, nil) — no boundaries → the
// table collapses to a single whole-table chunk.
func (r *concurrentBinlogRows) SampleKeysetBoundaries(ctx context.Context, table *ir.Table, pkColumns []string, n int) ([][]any, error) {
	mr := r.metaReader()
	if mr == nil {
		return nil, nil
	}
	return mr.SampleKeysetBoundaries(ctx, table, pkColumns, n)
}

// metaReader builds a non-snapshot [RowReader] over the side metaDB pool for
// the boundary-HINT queries (RangeBounds / SampleKeysetBoundaries). The closer
// is set to the pool so the reader is NOT classified snapshot-pinned (those
// methods refuse a closer==nil pinned reader, since a boundary query on a
// streaming snapshot conn would conflict) — RangeBounds / SampleKeysetBoundaries
// never call Close, so wiring the pool as closer only satisfies that guard. The
// pool reference is read under connMu so it never observes a half-swapped set
// across an ADR-0111 recovery; nil pool/dbName ⇒ nil reader (graceful no-hint).
func (r *concurrentBinlogRows) metaReader() *RowReader {
	r.connMu.RLock()
	db, dbName := r.metaDB, r.dbName
	r.connMu.RUnlock()
	if db == nil || dbName == "" {
		return nil
	}
	// zeroDate is deliberately left at zeroDateInherit here (ADR-0127): this is
	// the ONLY data-reader-typed site not stamped with the per-sync mode, and
	// it is safe — metaReader serves boundary-HINT queries only
	// (RangeBounds / SampleKeysetBoundaries), never carried row data, so a
	// temporal PK holding a zero/partial date would resolve to the GLOBAL policy
	// and fail in the LOUD direction (refuse) rather than carry a wrong value.
	return &RowReader{q: db, schema: dbName, qualifyBySchema: false, closer: db}
}

// CountRows implements [ir.RowCounter] so the progress ticker gets a per-table
// ETA on the concurrent path. The inner snapshot readers deliberately no-op
// CountRows (closer == nil — a metadata query on a connection actively
// streaming a table would be a concurrent query on one *sql.Conn), so the
// estimate runs on a FRESH connection from the side metaDB pool: it reads
// information_schema.TABLE_ROWS — catalog metadata, the SAME cheap estimate the
// serial RowReader uses (NOT an exact COUNT(*) scan), needing no snapshot and
// never touching the pinned connections. A missing pool/dbName yields (0, nil):
// an honest "no estimate" (the ticker shows rows-copied without a %/ETA) rather
// than a wrong total.
func (r *concurrentBinlogRows) CountRows(ctx context.Context, table *ir.Table) (int64, error) {
	if table == nil || r.metaDB == nil || r.dbName == "" {
		return 0, nil
	}
	const q = `SELECT COALESCE(TABLE_ROWS, 0)
	      FROM information_schema.tables
	      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`
	var n int64
	if err := r.metaDB.QueryRowContext(ctx, q, r.dbName, table.Name).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: concurrent CountRows estimate for %q: %w", table.Name, err)
	}
	return n, nil
}

// Close releases the inner readers. In snapshot mode each inner reader's
// closer is nil (the SnapshotStream's CloseFn/ReleaseRowsFn does the
// COMMIT + conn.Close), so this is a no-op for the connections — it exists
// so concurrentBinlogRows satisfies the io.Closer the orchestrator may call
// on stream.Rows, mirroring the single-reader RowReader.Close contract.
func (r *concurrentBinlogRows) Close() error {
	var firstErr error
	for _, rr := range r.readers {
		if err := rr.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Err returns the reader's sticky error (valid after the channels drain).
// Mirrors RowReader.Err for the orchestrator's post-drain check. ADR-0111:
// the resumable read path (readResumable) owns error reporting now — it
// consumes each inner read's error inside the per-table loop, rides out the
// classified-drop class via re-snapshot recovery, and records only a TERMINAL
// failure here (a real decode/query fault, or recovery exhaustion / binlog
// purge). So a transient that recovery absorbed never surfaces, while a true
// terminal cause does.
func (r *concurrentBinlogRows) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}
