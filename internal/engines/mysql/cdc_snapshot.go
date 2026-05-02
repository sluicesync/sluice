package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// OpenSnapshotStream opens a consistent MySQL snapshot and returns a
// paired RowReader (pinned to the snapshot transaction) and CDCReader
// (configured to start from the captured binlog position). The bulk-
// copy phase reads from the same connection that captured the
// position, so the bulk-copy view is exactly as-of the position; CDC
// resumes from immediately after, with no gap and no overlap.
//
// PlanetScale and other CDCNone flavors return ErrNotImplemented.
//
// Caller closes the returned stream to release: the snapshot tx,
// the pinned connection, the underlying schema-DB pool, and the CDC
// reader's resources.
func (e Engine) OpenSnapshotStream(ctx context.Context, dsn string) (*ir.SnapshotStream, error) {
	if e.Capabilities().CDC == ir.CDCNone {
		return nil, fmt.Errorf("%s: snapshot+CDC not supported by this flavor: %w", e.Name(), ErrNotImplemented)
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Pin a single connection. All snapshot-pinned reads will run on
	// this conn; the snapshot transaction is bound to it.
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: pin conn: %w", err)
	}

	// REPEATABLE READ + WITH CONSISTENT SNAPSHOT in a single statement
	// is the canonical InnoDB snapshot capture. The session isolation
	// is set explicitly first so the behaviour doesn't depend on the
	// server's tx_isolation default.
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: set isolation: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: start tx: %w", err)
	}

	// Capture the position INSIDE the same transaction so it is
	// guaranteed to refer to the snapshot's logical clock. SHOW BINARY
	// LOG STATUS is the 8.4+ spelling; SHOW MASTER STATUS is the
	// pre-8.4 fallback. Same shape as the standalone CDC reader.
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: capture position: %w", err)
	}

	// The CDC reader uses an entirely separate connection and protocol
	// (binlog dump). Construct it with the same DSN so it parses the
	// host/port/credentials itself.
	cdcReader, err := e.OpenCDCReader(ctx, dsn)
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: build cdc reader: %w", err)
	}

	position, err := encodeBinlogPos(binlogPos{
		Mode: positionModeFilePos,
		File: file,
		Pos:  pos,
	})
	if err != nil {
		_ = cdcReader.(closer).Close()
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysql: snapshot: encode position: %w", err)
	}

	rowReader := &RowReader{
		q:      conn,
		schema: cfg.DBName,
		// Snapshot mode: SnapshotStream.Close handles cleanup.
		closer: nil,
	}

	stream := &ir.SnapshotStream{
		Position: position,
		Rows:     rowReader,
		Changes:  cdcReader,
	}
	stream.CloseFn = func() error {
		// Order matters: stop the CDC reader first (in case the
		// caller has it streaming), then commit the snapshot tx
		// (release the read view), then close the conn back to the
		// pool, then close the schema DB.
		var firstErr error
		if c, ok := cdcReader.(closer); ok {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
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
	return stream, nil
}

// closer is the local view of io.Closer for the CDC reader cleanup
// path. Avoids importing io for this single use.
type closer interface{ Close() error }

// snapshotMasterStatus is the single-conn variant of the standalone
// CDC reader's masterStatus helper. We can't reuse that one directly
// because it operates on *sql.DB; here we need to run on the pinned
// *sql.Conn so the position is captured inside the snapshot tx.
func snapshotMasterStatus(ctx context.Context, conn *sql.Conn) (file string, pos uint32, err error) {
	for _, q := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		file, pos, err = scanMasterStatusOnConn(ctx, conn, q)
		if err == nil {
			return file, pos, nil
		}
	}
	return "", 0, errors.New("mysql: snapshot: SHOW BINARY LOG STATUS / SHOW MASTER STATUS both failed (binlog disabled?)")
}

// scanMasterStatusOnConn mirrors scanMasterStatus from cdc_reader.go,
// adapted for *sql.Conn. The query may return additional columns
// after (file, position) which we discard.
func scanMasterStatusOnConn(ctx context.Context, conn *sql.Conn, q string) (file string, pos uint32, err error) {
	rows, err := conn.QueryContext(ctx, q)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", 0, err
		}
		return "", 0, errors.New("master status returned no rows")
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	dest := make([]any, len(cols))
	holders := make([]any, len(cols))
	for i := range dest {
		holders[i] = &dest[i]
	}
	if err := rows.Scan(holders...); err != nil {
		return "", 0, err
	}
	f, ok := scanString(dest[0])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected file type %T", dest[0])
	}
	p, ok := scanUint32(dest[1])
	if !ok {
		return "", 0, fmt.Errorf("master status: unexpected position type %T", dest[1])
	}
	return f, p, nil
}
