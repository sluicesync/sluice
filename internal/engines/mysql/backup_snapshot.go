// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// OpenBackupSnapshot implements [irbackup.SnapshotOpener]. It captures
// a consistent MySQL snapshot via `START TRANSACTION WITH CONSISTENT
// SNAPSHOT`, returning a snapshot-pinned RowReader the full-backup
// orchestrator drives the table sweep against.
//
// MySQL's REPEATABLE READ snapshot is per-session and not shareable
// across connections (ADR-0019), so it cannot be lazily imported like
// PG's exported snapshot. Two shapes:
//
//   - Serial (the floor): one pinned conn; all table reads run on it
//     sequentially. Used when opts.ReaderParallelism <= 1, on a
//     Vitess/PlanetScale source, or whenever the coordinated open
//     below fails (FTWRL denied, conn error) — falling back with a
//     loud INFO. Byte-identical to the pre-ADR-0088 behaviour.
//   - Coordinated parallel (ADR-0088): when opts.ReaderParallelism > 1
//     on a vanilla source, [openBackupSnapshotCoordinated] opens N
//     reader transactions whose consistent snapshots COINCIDE under a
//     brief `FLUSH TABLES WITH READ LOCK` window (mydumper's
//     mechanism), returned as Rows + [irbackup.Snapshot.ExtraReaders]
//     so the cross-table backup pool can sweep across them.
//
// Either way the cross-table consistency property holds and the
// EndPosition is anchored at the snapshot's start point — closing the
// during-backup write-window gap (v0.18.0).
//
// opts.SlotName is accepted for [irbackup.SnapshotOpener] interface
// uniformity and ignored — MySQL has no slot concept on the source
// side. opts.PersistChainSlot is a loud no-op for the same reason
// (binlog retention is server-configured via
// binlog_expire_logs_seconds, not a per-consumer slot). The captured
// EndPosition is `@@global.gtid_executed` (when GTID mode is on) or
// `(file, pos)` (when off), captured INSIDE the snapshot transaction
// so the recorded position refers to the snapshot's logical clock.
//
// Caller closes the returned snapshot to commit the snapshot tx,
// release the pinned conn, and close the underlying DB pool.
//
// PlanetScale flavor (v0.44.0, GitHub issue #16): delegates to
// [openBackupSnapshotVStream] which rides VStream COPY mode for
// both data and position capture. The pinned-conn + binlog-position
// path below stays for vanilla MySQL only — applying it to a Vitess
// source produces a binlog-shape position the VStream-based
// continuous-sync path can't decode, breaking incremental + stream-
// run chain-resume entirely. The pre-v0.44.0 PS-MySQL backup-via-
// pinned-conn path "worked" against single-shard keyspaces only in
// the data-read sense — operators couldn't actually chain
// incrementals onto those backups because the encoded position
// shape was wrong.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn string, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: backup snapshot not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	warnChainSlotNoOp(ctx, e, opts)
	if e.Flavor.usesVStream() {
		// Whole-keyspace COPY (nil tables). The table-scoped variant lives
		// on OpenBackupSnapshotForTables (irbackup.TableScopedBackupSnapshotOpener).
		return e.openBackupSnapshotVStream(ctx, dsn, nil)
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	// ADR-0088: with a cross-table parallelism request, try the
	// coordinated open — N reader transactions whose consistent
	// snapshots COINCIDE under a brief FLUSH TABLES WITH READ LOCK
	// window, returned as Rows + ExtraReaders so the cross-table backup
	// pool can sweep across them. On a permission error (no RELOAD) or
	// any coordinated-open failure, snap is nil and the serial floor
	// below takes over (a loud INFO already named the reason). N <= 1
	// skips the coordinated path entirely — serial is byte-identical.
	if opts.ReaderParallelism > 1 {
		if snap := e.openBackupSnapshotCoordinated(ctx, cfg, opts.ReaderParallelism); snap != nil {
			return snap, nil
		}
	}

	return e.openBackupSnapshotSerial(ctx, cfg)
}

// openBackupSnapshotSerial opens today's single-reader consistent
// snapshot: one pinned conn running REPEATABLE READ + START
// TRANSACTION WITH CONSISTENT SNAPSHOT, position captured inside the
// tx. It is the floor every coordinated-open failure path falls back
// to, and the only path when ReaderParallelism <= 1.
func (e Engine) openBackupSnapshotSerial(ctx context.Context, cfg *gomysql.Config) (*irbackup.Snapshot, error) {
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: pin conn: %w", err)
	}

	// REPEATABLE READ + WITH CONSISTENT SNAPSHOT in two statements is
	// the canonical InnoDB snapshot capture (mirrors
	// [openBinlogSnapshotStream]). The session isolation is set
	// explicitly first so the behaviour doesn't depend on the
	// server's tx_isolation default.
	if err := startConsistentReaderTx(ctx, conn); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, err
	}

	// Capture the position INSIDE the snapshot tx so it refers to the
	// snapshot's logical clock (the same shape openBinlogSnapshotStream
	// uses). Prefer GTID when gtid_mode is on; fall back to file/pos.
	position, err := captureBackupPositionInTx(ctx, conn)
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: backup snapshot: capture position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.DBName,
		closer: nil, // BackupSnapshot owns the lifecycle
	}

	closed := false
	closeFn := func() error {
		if closed {
			return nil
		}
		closed = true
		// Order matters: COMMIT (releases the read view), close the
		// pinned conn back to the pool, close the underlying DB pool.
		// context.Background so a cancelled parent ctx doesn't prevent
		// cleanup.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	return &irbackup.Snapshot{
		Position: position,
		Rows:     rowReader,
		CloseFn:  closeFn,
	}, nil
}

// startConsistentReaderTx puts conn into the canonical InnoDB
// consistent-read posture: explicit REPEATABLE READ isolation, then
// START TRANSACTION WITH CONSISTENT SNAPSHOT. Factored out so the
// serial path and each of the N coordinated readers start their read
// view identically (ADR-0088).
func startConsistentReaderTx(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return fmt.Errorf("mysql: backup snapshot: set isolation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return fmt.Errorf("mysql: backup snapshot: start tx: %w", err)
	}
	return nil
}

// backupReadersOpenedHook is a TEST-ONLY seam (ADR-0088 consistency
// oracle): when non-nil it fires the instant all N coordinated reader
// transactions have started their consistent snapshot AND the FTWRL
// has been released — i.e. the readers' views are pinned and writes
// have resumed. The consistency oracle test installs a hook that
// INSERTs into the source while it runs; because every reader's
// snapshot predates those writes, the backup artifact must contain
// none of them, proving the N readers share one consistent point. nil
// in production (a single nil check).
var backupReadersOpenedHook func()

// openBackupSnapshotCoordinated opens N reader transactions whose
// consistent snapshots COINCIDE under a brief FLUSH TABLES WITH READ
// LOCK window (ADR-0088, mydumper's mechanism). It returns a populated
// [irbackup.Snapshot] on success, or nil to signal the caller to fall
// back to the serial floor — every failure path (FTWRL permission
// denied, conn-open error, tx-start error, position-capture error)
// closes everything it opened, logs a loud INFO naming the reason, and
// returns nil. It NEVER returns an error: a coordinated-open failure is
// not a backup failure, it is a degrade to serial.
//
// The algorithm (vanilla MySQL only — the caller has already excluded
// usesVStream flavors):
//
//  1. open a dedicated coordinator conn C; FLUSH TABLES WITH READ LOCK
//     freezes all writes globally.
//  2. on each of N reader conns: REPEATABLE READ + START TRANSACTION
//     WITH CONSISTENT SNAPSHOT. Because C holds the lock, no write
//     occurs between the first and last START, so all N read views are
//     byte-identical.
//  3. capture the EndPosition on R[0] WHILE the lock is held — it
//     refers to the frozen instant.
//  4. UNLOCK TABLES + close C; writes resume. Each R[i] keeps its view
//     via its open REPEATABLE-READ tx.
//  5. return Rows: R[0], ExtraReaders: R[1..N-1], CloseFn closing all.
func (e Engine) openBackupSnapshotCoordinated(ctx context.Context, cfg *gomysql.Config, n int) *irbackup.Snapshot {
	db, err := openDB(ctx, cfg)
	if err != nil {
		// A pool-open failure is not specific to the coordinated path;
		// let the serial floor surface the same error loudly.
		slog.InfoContext(
			ctx, "mysql: backup snapshot: coordinated parallel open could not connect; falling back to serial reader",
			slog.String("err", err.Error()),
		)
		return nil
	}

	// Coordinator conn: holds FTWRL for the duration of the reader-tx
	// opening + position capture, then is released.
	coord, err := db.Conn(ctx)
	if err != nil {
		slog.InfoContext(
			ctx, "mysql: backup snapshot: coordinated parallel open could not pin coordinator conn; falling back to serial reader",
			slog.String("err", err.Error()),
		)
		_ = db.Close()
		return nil
	}
	if _, err := coord.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		// The common managed-tier case: the role lacks RELOAD /
		// FLUSH_TABLES. Serial is the safe, correct fallback (no LOCK
		// TABLES in v1). Loud INFO names the reason.
		slog.InfoContext(
			ctx, "mysql: backup snapshot: FLUSH TABLES WITH READ LOCK denied or failed; falling back to serial single-reader backup (the source role likely lacks the RELOAD privilege)",
			slog.Int("requested_readers", n),
			slog.String("err", err.Error()),
		)
		_ = coord.Close()
		_ = db.Close()
		return nil
	}

	// Open the N reader transactions while the lock is held. On any
	// failure, release the lock + everything opened and fall back.
	conns := make([]*sql.Conn, 0, n)
	abort := func(reason string, cause error) *irbackup.Snapshot {
		slog.InfoContext(
			ctx, "mysql: backup snapshot: "+reason+"; falling back to serial single-reader backup",
			slog.Int("requested_readers", n),
			slog.String("err", cause.Error()),
		)
		for _, c := range conns {
			_, _ = c.ExecContext(context.Background(), "ROLLBACK")
			_ = c.Close()
		}
		_, _ = coord.ExecContext(context.Background(), "UNLOCK TABLES")
		_ = coord.Close()
		_ = db.Close()
		return nil
	}
	for i := 0; i < n; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			return abort("could not pin reader conn", err)
		}
		if err := startConsistentReaderTx(ctx, conn); err != nil {
			_ = conn.Close()
			return abort("could not start reader snapshot tx", err)
		}
		conns = append(conns, conn)
	}

	// Capture the position on R[0] while FTWRL is still held — it refers
	// to the frozen instant shared by all N readers.
	position, err := captureBackupPositionInTx(ctx, conns[0])
	if err != nil {
		return abort("could not capture snapshot position", err)
	}

	// Release the global lock + drop the coordinator conn. Writes resume
	// now; each reader keeps its consistent view via its open tx.
	if _, err := coord.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		return abort("could not release FLUSH TABLES WITH READ LOCK", err)
	}
	_ = coord.Close()

	// Test-only seam (consistency oracle): readers pinned, writes
	// resumed. The hook INSERTs into the source; none of those writes
	// may appear in the backup.
	if backupReadersOpenedHook != nil {
		backupReadersOpenedHook()
	}

	readers := make([]ir.RowReader, len(conns))
	for i, conn := range conns {
		readers[i] = &RowReader{
			q:      conn,
			schema: cfg.DBName,
			closer: nil, // BackupSnapshot owns the lifecycle
		}
	}

	closed := false
	closeFn := func() error {
		if closed {
			return nil
		}
		closed = true
		// COMMIT (releases each read view), then close each pinned conn,
		// then close the pool. context.Background so a cancelled parent
		// ctx doesn't prevent cleanup. The pool is closed exactly once,
		// here — ExtraReaders are owned by this closure, not the pool
		// that pops them.
		var firstErr error
		for _, conn := range conns {
			if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := conn.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	slog.InfoContext(
		ctx, "mysql: backup snapshot: opened coordinated parallel consistent view (FTWRL-aligned)",
		slog.Int("readers", len(readers)),
		slog.String("position_token", position.Token),
	)

	return &irbackup.Snapshot{
		Position:     position,
		Rows:         readers[0],
		ExtraReaders: readers[1:],
		CloseFn:      closeFn,
	}
}

// OpenBackupSnapshotForTables implements
// [irbackup.TableScopedBackupSnapshotOpener]: it opens a backup snapshot whose
// COPY is scoped to the unqualified table names in tables (an empty slice
// means "all tables", identical to [Engine.OpenBackupSnapshot]).
//
// It branches on flavor EXACTLY like [Engine.OpenSnapshotStreamForTables]:
//
//   - FlavorPlanetScale → [openBackupSnapshotVStream] with the table
//     allowlist, so vtgate's COPY scans only those tables and a large
//     unrelated table in the same keyspace is never streamed/buffered
//     (the ADR-0071 multi-table-interleaving buffer overflow). This is
//     the over-stream the base whole-keyspace path hits on a scoped
//     PlanetScale backup.
//   - vanilla/binlog → delegate to the base [Engine.OpenBackupSnapshot].
//     Its per-table pinned-conn reader reads one table at a time and
//     never over-streams, so the table scope is a no-op there (mirrors
//     the OpenSnapshotStreamForTables comment).
func (e Engine) OpenBackupSnapshotForTables(ctx context.Context, dsn string, opts irbackup.SnapshotOptions, tables []string) (*irbackup.Snapshot, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: backup snapshot not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	if e.Flavor.usesVStream() {
		warnChainSlotNoOp(ctx, e, opts)
		return e.openBackupSnapshotVStream(ctx, dsn, tables)
	}
	// Vanilla/binlog flavor: its snapshot RowReader already reads per-table,
	// so it never over-streams; the table scope is a no-op there. Delegate
	// to the base whole-snapshot path (opts are ignored there too).
	return e.OpenBackupSnapshot(ctx, dsn, opts)
}

// warnChainSlotNoOp surfaces --chain-slot as a loud no-op on engines
// without a slot concept (the #26 loud-no-op discipline): the chain
// story on MySQL needs no provisioning — the binlog IS the retention
// mechanism — but silently accepting the flag would let an operator
// believe something was set up.
func warnChainSlotNoOp(ctx context.Context, e Engine, opts irbackup.SnapshotOptions) {
	if !opts.PersistChainSlot {
		return
	}
	slog.WarnContext(
		ctx, "backup: --chain-slot is a no-op on this engine — there is no replication-slot concept; incremental chaining rides the binlog directly",
		slog.String("engine", e.Name()),
		slog.String("note", "ensure binlog retention (binlog_expire_logs_seconds) covers your incremental cadence"),
	)
}

// captureBackupPositionInTx queries the source-side cursor against the
// pinned snapshot connection and encodes it the same way the standalone
// CDC reader emits via [encodeBinlogPos]. The function is the conn-
// scoped sibling of [SchemaReader.CaptureBackupPosition].
func captureBackupPositionInTx(ctx context.Context, conn *sql.Conn) (ir.Position, error) {
	useGTID, err := gtidModeOnConn(ctx, conn)
	if err != nil {
		return ir.Position{}, fmt.Errorf("detect gtid mode: %w", err)
	}
	if useGTID {
		set, err := executedGTIDSetConn(ctx, conn)
		if err != nil {
			return ir.Position{}, fmt.Errorf("read @@gtid_executed: %w", err)
		}
		return encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
	}
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		return ir.Position{}, fmt.Errorf("master status: %w", err)
	}
	return encodeBinlogPos(binlogPos{Mode: positionModeFilePos, File: file, Pos: pos})
}

// gtidModeOnConn is the *sql.Conn variant of gtidModeOn (cdc_reader.go).
// Used so the snapshot's gtid_mode probe runs INSIDE the snapshot tx —
// reading the SHOW VARIABLES result outside the tx would lose the
// snapshot read-view alignment.
func gtidModeOnConn(ctx context.Context, conn *sql.Conn) (bool, error) {
	var name, value string
	err := conn.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'gtid_mode'").Scan(&name, &value)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == "ON" || value == "ON_PERMISSIVE", nil
}

// executedGTIDSetConn is the *sql.Conn variant of executedGTIDSet.
func executedGTIDSetConn(ctx context.Context, conn *sql.Conn) (string, error) {
	var set string
	err := conn.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&set)
	if err != nil {
		return "", err
	}
	return set, nil
}
