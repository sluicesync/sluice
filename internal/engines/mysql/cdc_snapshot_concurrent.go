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

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// nativeCopyTableParallelismFromDSN reads the optional copy_table_parallelism
// source-DSN parameter — the number of CONCURRENT pinned-snapshot reader
// connections the native-binlog cold-copy opens (ADR-0101). It is the
// native-MySQL analogue of vstreamCopyTableParallelismFromDSN (and reuses the
// SAME resolveCopyTableParallelism resolver + clamp); the distinct DSN key
// keeps it independent of the VStream knob (a self-managed MySQL source has
// no VStream). Absent ⇒ defaultCopyTableParallelism (1, serial). A malformed
// value is a LOUD error (the loud-failure tenet: an operator who set the knob
// deserves to know it didn't parse), NOT a silent fallback to serial.
func nativeCopyTableParallelismFromDSN(cfg *gomysql.Config) (int, error) {
	v := cfg.Params["copy_table_parallelism"]
	if v == "" {
		return defaultCopyTableParallelism, nil
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

	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Pin connection 0 and acquire the global write freeze on it. The lock
	// is held across ALL N START TRANSACTION calls AND the position read, so
	// every snapshot pins the same logical cut (ADR-0101 §2, silent-loss
	// guard #1).
	conn0, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot(concurrent): pin conn 0: %w", err)
	}
	if _, err := conn0.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn0.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot(concurrent): set isolation: %w", err)
	}

	// FTWRL is the consistency lynchpin. If it fails (no RELOAD privilege,
	// or a managed MySQL that blocks it), we MUST NOT proceed with N
	// independent snapshots (silent-inconsistency class). Fall back to the
	// SERIAL single-snapshot path — consistent by construction, concurrency
	// disabled — with a loud WARN. (ADR-0101 §4.)
	if _, err := conn0.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		slog.WarnContext(ctx,
			"mysql: snapshot(concurrent): FLUSH TABLES WITH READ LOCK failed; "+
				"falling back to the SERIAL single-snapshot cold-copy — cross-table "+
				"concurrency DISABLED, consistency preserved. Grant RELOAD to the "+
				"source user to enable copy_table_parallelism>1.",
			slog.Int("requested_parallelism", n),
			slog.String("error", err.Error()))
		_ = conn0.Close()
		_ = db.Close()
		return e.openBinlogSnapshotStreamShared(ctx, dsn, false)
	}

	// From here the FTWRL is HELD. Every failure path before UNLOCK must
	// release it (and close conn0 + db). conns accumulates the pinned reader
	// connections (conn0 first) so a partway failure unwinds all of them.
	conns := []*sql.Conn{conn0}
	failUnlock := func(wrap error) (*ir.SnapshotStream, error) {
		// Release the lock on conn0 (still open), then close every pinned
		// conn and the pool. Best-effort on the cleanup; return the
		// triggering error.
		_, _ = conn0.ExecContext(context.Background(), "UNLOCK TABLES")
		for _, c := range conns {
			_ = c.Close()
		}
		_ = db.Close()
		return nil, wrap
	}

	// conn0's own snapshot transaction.
	if _, err := conn0.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): start tx (reader 0): %w", err))
	}

	// Open the remaining N-1 reader connections + their snapshot
	// transactions, all while the FTWRL is held — so they pin the SAME cut.
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

	// Record the ONE binlog position inside the lock, on conn0's snapshot
	// transaction — the single CDC-resume anchor every reader shares
	// (ADR-0101 §2 step 3, silent-loss guard #3).
	file, pos, err := snapshotMasterStatus(ctx, conn0)
	if err != nil {
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): capture position: %w", err))
	}

	// Release the write freeze — both the snapshot views (all N) and the
	// binlog position are captured. The open transactions keep their views
	// alive for the per-group COPY reads that follow.
	if _, err := conn0.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		// UNLOCK failed but the snapshots + position are already captured.
		// Treat as fatal (a stuck global lock would block the source) and
		// unwind everything — but the UNLOCK we issue in failUnlock is a
		// best-effort retry.
		return failUnlock(fmt.Errorf("mysql: snapshot(concurrent): unlock tables: %w", err))
	}

	// Instance identity (Track 1c floor) — non-fatal on failure.
	var serverUUID string
	if err := conn0.QueryRowContext(ctx, "SELECT @@global.server_uuid").Scan(&serverUUID); err != nil {
		serverUUID = ""
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

	position, err := encodeBinlogPos(binlogPos{
		Mode:       positionModeFilePos,
		File:       file,
		Pos:        pos,
		ServerUUID: serverUUID,
	})
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
	rows := newConcurrentBinlogRows(conns, groups, cfg.DBName)

	// Honest connection-budget surfacing (no false auto-clamp — ADR-0101 §5).
	slog.InfoContext(ctx,
		"mysql: native concurrent cold-copy: opened consistent multi-table snapshot",
		slog.Int("readers", n),
		slog.Int("groups", len(groups)),
		slog.String("contract", "operator ensures readers × write-fanout ≤ --max-target-connections and readers ≤ source max_connections"))

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rows,
		Changes:  cdcReader,
	}

	// Lifecycle: COMMIT + close all N pinned connections + the pool, once,
	// first-error-wins (ADR-0101 §8). Same shape as the single-reader
	// releaseRows, scaled to N. The FTWRL is already released by now (held
	// only during open).
	released := false
	releaseRows := func() error {
		if released {
			return nil
		}
		released = true
		var firstErr error
		for _, c := range conns {
			if _, err := c.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
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
type concurrentBinlogRows struct {
	// byTable maps an unqualified table name to the inner RowReader (one per
	// pinned connection) that owns its group. Built once at open from the
	// disjoint partition; read-only thereafter, so concurrent ReadRows from
	// the W consumer goroutines need no lock to look up their reader.
	byTable map[string]*RowReader

	// readers is the N inner RowReaders (one per connection), kept for
	// Close. Each wraps one *sql.Conn (snapshot mode: closer nil — the
	// SnapshotStream owns the lifecycle, mirroring the single-reader path).
	readers []*RowReader

	// groups is the disjoint table partition surfaced via
	// ConcurrentCopyGroups (one group per inner reader, same index).
	groups [][]string
}

// newConcurrentBinlogRows builds the router from the N pinned connections
// and their disjoint groups. conns[i] serves groups[i]; len(conns) ==
// len(groups). dbName is the source database (the single-database snapshot
// path; reads are unqualified, byte-identical to the serial reader).
func newConcurrentBinlogRows(conns []*sql.Conn, groups [][]string, dbName string) *concurrentBinlogRows {
	readers := make([]*RowReader, len(conns))
	byTable := make(map[string]*RowReader)
	for i, c := range conns {
		rr := &RowReader{
			q:      c,
			schema: dbName,
			// Single-database snapshot: unqualified SELECTs, byte-identical
			// to the serial binlog reader. closer nil — the SnapshotStream's
			// Close/ReleaseRows owns the connection lifecycle.
			qualifyBySchema: false,
			closer:          nil,
		}
		readers[i] = rr
		if i < len(groups) {
			for _, t := range groups[i] {
				byTable[t] = rr
			}
		}
	}
	return &concurrentBinlogRows{
		byTable: byTable,
		readers: readers,
		groups:  groups,
	}
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
	rr, ok := r.byTable[table.Name]
	if !ok {
		return nil, fmt.Errorf(
			"mysql: concurrent ReadRows: table %q is not in the concurrent-copy partition "+
				"(no reader owns it — a partition/scope mismatch); refusing to read from an arbitrary connection",
			table.Name,
		)
	}
	return rr.ReadRows(ctx, table)
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

// Err returns the first error across the inner readers (sticky, valid after
// the channels drain). Mirrors RowReader.Err for the orchestrator's
// post-drain error check; each inner reader tracks its own table's scan
// error, so the first non-nil across them is the copy's failure cause.
func (r *concurrentBinlogRows) Err() error {
	for _, rr := range r.readers {
		if err := rr.Err(); err != nil {
			return err
		}
	}
	return nil
}
