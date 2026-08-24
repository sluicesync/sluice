// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/engines/internal/triggercdc"
	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// This file is the TRANSPORT SEAM (ADR-0136). The trigger engine's setup +
// CDC-reader logic is identical for a local SQLite FILE and a live Cloudflare
// D1 database — the setup DDL, the trigger bodies, the change-log/meta/
// fingerprint schema, the poll SQL, the watermark, and the MAX(id) snapshot
// anchor are all the same (ADR-0135). ONLY the executor differs: a local
// *sql.DB vs the D1 `/query` HTTP API. So all that logic runs against the small
// [executor] interface below, with two implementations:
//
//   - [localExecutor] — the Phase-1 local *sql.DB path, byte-identical to the
//     shipped v0.99.148 engine (its SQL + scan code moved here verbatim).
//   - [d1Executor] — the Phase-2 D1-over-HTTP path, backed by [sqlite.D1Conn]
//     (the same transport the cold-start `d1` reader uses).
//
// [backend] bundles the three transport-specific pieces the shared logic needs
// (the cold-start engine to delegate schema/row reads to, the captured-cell
// decoder, and an executor factory) so Setup / OpenCDCReader / OpenSnapshotStream
// are written ONCE over it.

// executor is the transport the shared trigger logic runs against. execDDL runs
// one non-SELECT setup/teardown statement; the query methods return
// transport-neutral results (the local path scans *sql.Rows, the D1 path
// reconstructs from JSON).
type executor interface {
	execDDL(ctx context.Context, stmt string) error
	pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error)
	readFingerprints(ctx context.Context) ([]fingerprintRow, error)
	changeLogExists(ctx context.Context) (bool, error)
	// tableColumns returns the column NAMES of `table`, or an empty slice when
	// no such table exists. It is the shape half of the change-log adoption
	// refusal (roadmap item 149b): existence alone cannot distinguish sluice's
	// own change log from a user table that happens to carry the name, because
	// a legitimate re-`setup` finds the table there every time.
	tableColumns(ctx context.Context, table string) ([]string, error)
	// captureTriggerSQL returns the INSTALLED CREATE-statement text of every
	// sluice capture trigger (sqlite_master.sql), keyed by trigger name. The
	// CDC reader's capture-shape door compares it against what THIS binary's
	// setup would install ([captureTriggerCreateSQL]) so a stale install —
	// notably one whose REAL capture still uses the pre-fix lossy
	// format('%.17g') rendering — or a dropped/edited capture trigger refuses
	// loudly at stream start instead of mis-capturing silently.
	captureTriggerSQL(ctx context.Context) (map[string]string, error)
	// realRenderProbe runs [sqlite.RealRenderProbeSQL] on the connected engine
	// and returns the rendered text. The CDC-open render-fidelity door
	// ([verifyRealRenderHonoured]) grades it, so a SQLite that ignores the `!`
	// alternate-form-2 precision flag — where the capture expression would
	// silently clamp every REAL to 16 digits — refuses at stream start instead
	// of streaming altered floats. On the D1 transport this probes the SAME
	// engine that fires the triggers; on the local transport it probes the
	// poller's own connection (see sqlite.CapturedValueExpr for the residual).
	realRenderProbe(ctx context.Context) (string, error)
	maxChangeLogID(ctx context.Context) (int64, error)
	// changeLogAllocation reports the change log's id-allocation state — the
	// floor below which it can never issue a new id, and whether the table is
	// declared AUTOINCREMENT. Both CDC doors grade it against the resume
	// watermark; see [verifyChangeLogWatermark] for why the id-reuse it
	// detects is a silent-loss class rather than a cosmetic one.
	changeLogAllocation(ctx context.Context) (changeLogAllocation, error)
	discoverTriggers(ctx context.Context) ([]string, error)
	// pruneChangeLogBatch DELETEs one bounded keyset step of change-log rows —
	// floor < id <= upper — and returns the number deleted (ADR-0137, batched
	// per P-1). The `<=` upper bound is load-bearing exactly as the old
	// single-shot form's `id <= cut` was: the caller guarantees upper never
	// exceeds the durably-applied cut, so id == upper is itself durably
	// applied and safe to remove.
	pruneChangeLogBatch(ctx context.Context, floor, upper int64) (deleted int64, err error)
	// minChangeLogID returns MIN(id) of the change-log (0 when empty) — the
	// prune loop's keyset floor. Indexed (id is the PK) and cheap.
	minChangeLogID(ctx context.Context) (int64, error)
	// pruneBatchSize is the transport's DELETE batch bound; see the
	// per-transport constants for the rationale (P-1).
	pruneBatchSize() int64
	// maxPollBatch is the transport's poll-batch ceiling; 0 means no ceiling.
	// The reader clamps its poll batch to it at open (P-3).
	maxPollBatch() int
	// checkpointWAL bounds the source WAL under sustained CDC (Bug 167). On the
	// local SQLite path it issues `PRAGMA wal_checkpoint(TRUNCATE)` so the
	// continuously-churned change-log WAL is reset on a cadence instead of
	// growing without bound; a BUSY result (a reader/writer momentarily held the
	// WAL) is not an error — the next cadence retries. The D1 path is a no-op:
	// D1 polls over HTTP against Cloudflare-managed storage with no local pager
	// or WAL file. It is pure WAL-file management — it must never change the
	// read/apply path or the watermark.
	checkpointWAL(ctx context.Context) error
	// vacuum reclaims file space after a prune (SQLite/D1 only; PG uses
	// autovacuum). Behind the operator's --vacuum opt-in — VACUUM rewrites the
	// whole database.
	vacuum(ctx context.Context) error
	// changeLogStats returns the post-prune MIN(id) (0 on an empty change-log)
	// and total row count, for the operator-facing prune report.
	changeLogStats(ctx context.Context) (minID, count int64, err error)
	// upsertConsumer records/refreshes one consumer's durably-applied frontier
	// in the source-side registry (roadmap item 115).
	upsertConsumer(ctx context.Context, consumerID string, appliedID int64) error
	// consumerRegistryState reports whether the registry table exists and the
	// change log's recorded schema_version — the two halves of the fail-closed
	// migration gate. ver is 0 when the meta row is absent.
	consumerRegistryState(ctx context.Context) (exists bool, ver int, err error)
	// readConsumers snapshots the registry, with each row's age measured by the
	// SOURCE's own clock (immune to skew between the hosts running the syncs).
	readConsumers(ctx context.Context) ([]triggercdc.Consumer, error)
	close() error
}

// rawChangeRow is one decoded `sluice_change_log` row, transport-neutral. before
// / after carry the captured (typeof, text/hex) JSON image text (a NULL image is
// !Valid); the reader reconstructs the faithful ir.Row from it.
type rawChangeRow struct {
	id         int64
	op         string
	tbl        string
	before     sql.NullString
	after      sql.NullString
	capturedAt sql.NullString
}

// fingerprintRow is one (tbl, columns) row from `sluice_change_log_columns` —
// the captured-column fingerprint the startup drift check compares to the live
// schema.
type fingerprintRow struct {
	tbl     string
	columns string
}

// backend supplies the transport-specific pieces the shared trigger logic needs.
// The local-file backend ([localBackend]) and the D1 backend ([d1Backend]) each
// construct one; everything above it (setup/poll/drift/anchor) is shared.
type backend struct {
	// driver is the user-facing source-driver name woven into the operator
	// recovery hints (e.g. "run sluice trigger setup --source-driver <driver>").
	driver string
	// dsn is the source DSN (the SQLite file path, or the d1:// form), passed to
	// the cold-start engine's schema/row readers.
	dsn string
	// coldStart is the engine the schema + row reads delegate to (the validated
	// `sqlite` file reader or the `d1` HTTP reader), so the captured column set,
	// types, and snapshot rows match the cold-start exactly.
	coldStart ir.Engine
	// newDecoder builds the captured-cell decoder (date/bool policy resolved from
	// the source DSN), shared with the cold-start reader so a captured change
	// decodes byte-identically to a snapshot row.
	newDecoder func() (*sqlite.CapturedCellDecoder, error)
	// openExec opens an executor. readOnly is honoured by the local path (the
	// poller opens a read-only connection, setup/teardown a writable one); the
	// D1 `/query` endpoint executes both, so it ignores the flag.
	openExec func(ctx context.Context, readOnly bool) (executor, error)
}

// localBackend is the Phase-1 local SQLite-FILE backend (ADR-0135). It preserves
// the shipped engine's behaviour exactly: the `sqlite` cold-start engine, the
// DSN-resolved decoder, and a *sql.DB executor opened via [sqlite.OpenFile].
func localBackend(dsn string) backend {
	return backend{
		driver:    EngineName,
		dsn:       dsn,
		coldStart: sqlite.Engine{},
		newDecoder: func() (*sqlite.CapturedCellDecoder, error) {
			return sqlite.NewCapturedCellDecoderForDSN(dsn)
		},
		openExec: func(ctx context.Context, readOnly bool) (executor, error) {
			db, _, err := sqlite.OpenFile(ctx, dsn, readOnly)
			if err != nil {
				return nil, err
			}
			if readOnly {
				// Bug 167: the CDC poller's read connection must NOT linger idle
				// in the pool. An idle pooled connection retains a stale WAL
				// read-mark, which pins SQLite's checkpoint (the app writer's
				// auto-checkpoint AND our own wal_checkpoint(TRUNCATE)) from ever
				// resetting the WAL — so under sustained change-log churn the
				// source WAL (and modernc's mmap of it, hence process RSS) grows
				// without bound. Closing the connection after each poll releases
				// the mark, so the checkpoint can reset the WAL. Ground-truthed:
				// default idle pool grew 69→158 MB in 12 s; SetMaxIdleConns(0)
				// held flat at ~8 MB. Read-only autocommit reads remain correct
				// (each opens its own consistent snapshot); exactly-once and the
				// watermark are unaffected.
				db.SetMaxIdleConns(0)
			}
			return &localExecutor{db: db}, nil
		},
	}
}

// d1Backend is the Phase-2 live-Cloudflare-D1 backend (ADR-0136): the `d1`
// cold-start engine, the D1-DSN-resolved decoder, and a [d1Executor] over the
// shared `/query` transport. Credentials are resolved (and refused loudly if
// absent) at [sqlite.OpenD1Conn]; reachability is verified on the first
// openExec via Ping.
func d1Backend(dsn string) (backend, error) {
	conn, err := sqlite.OpenD1Conn(dsn)
	if err != nil {
		return backend{}, err
	}
	return backend{
		driver:    EngineNameD1,
		dsn:       dsn,
		coldStart: sqlite.NewD1Engine(),
		newDecoder: func() (*sqlite.CapturedCellDecoder, error) {
			return conn.CellDecoder(), nil
		},
		openExec: func(ctx context.Context, _ bool) (executor, error) {
			// Verify the token/account/database at open so a credential or
			// reachability problem fails before any DDL or poll (ADR-0136).
			if err := conn.Ping(ctx); err != nil {
				return nil, err
			}
			return &d1Executor{conn: conn}, nil
		},
	}, nil
}

// --- shared catalog SQL (byte-identical across both transports) -------------

const (
	// changeLogExistsSQL probes for the change-log table by exact name.
	changeLogExistsSQL = `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`
	// changeLogCreateSQL reads the change log's own CREATE statement back out
	// of the catalog, verbatim — the only way to see whether the id column was
	// declared AUTOINCREMENT (there is no pragma for it: PRAGMA table_info
	// reports `pk` and the declared type, never the rowid-reuse policy).
	changeLogCreateSQL = `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`
	// changeLogSeqSQL reads the AUTOINCREMENT high-water counter. It is a LEFT
	// JOIN off a one-row subquery rather than the obvious
	// `SELECT seq FROM sqlite_sequence WHERE name = ?` so an ABSENT row still
	// returns a row (with 0), which is exactly how SQLite reads it — the
	// alternative returns no rows and would have to be special-cased into the
	// same answer at every call site.
	changeLogSeqSQL = `SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = ?), 0) AS seq`
	// discoverTriggersSQL lists every sluice-installed capture trigger.
	discoverTriggersSQL = `SELECT name FROM sqlite_master WHERE type = 'trigger' ` +
		`AND name LIKE 'sluice\_capture\_%' ESCAPE '\' ORDER BY name`
	// captureTriggerSQLQuery reads each installed capture trigger's verbatim
	// CREATE statement for the capture-shape door. The projection spelling
	// (`SELECT name, sql`) is distinct from the change-log allocation probe's
	// `SELECT sql FROM sqlite_master` on purpose — the D1 test mock dispatches
	// on those substrings.
	captureTriggerSQLQuery = `SELECT name, sql FROM sqlite_master WHERE type = 'trigger' ` +
		`AND name LIKE 'sluice\_capture\_%' ESCAPE '\' ORDER BY name`
	// readFingerprintsSQL reads the per-table captured-column fingerprints.
	readFingerprintsSQL = `SELECT tbl, columns FROM "` + ChangeLogColumnsTable + `" ORDER BY tbl`
	// tableExistsSQL probes for any table by exact name (the change-log probe
	// generalised — the consumer registry uses the same shape).
	tableExistsSQL = changeLogExistsSQL

	// upsertConsumerSQL records/refreshes one consumer's frontier (item 115).
	// applied_id is OVERWRITTEN, never max()'d forward: the writer of a row is
	// the consumer itself, so there is no rogue writer to defend against —
	// while a frontier that legitimately moves BACKWARD (an operator restored
	// an older target and resumed the stream) must be able to lower the
	// registry, or the next prune would cut above what that target has applied.
	upsertConsumerSQL = `INSERT INTO "` + ChangeLogConsumersTable + `" (consumer_id, applied_id, updated_at) ` +
		`VALUES (?, ?, strftime('%Y-%m-%d %H:%M:%f', 'now')) ` +
		`ON CONFLICT (consumer_id) DO UPDATE SET applied_id = excluded.applied_id, updated_at = excluded.updated_at`

	// readConsumersSQL snapshots the registry. applied_id and the age are
	// projected as exact TEXT so the D1 transport never routes an integer it
	// keys on through a JSON number (ADR-0132); the local path parses the same
	// text, so both transports run byte-identical SQL. The age is the SOURCE
	// clock's (julianday('now') - julianday(updated_at)), floored at 0.
	readConsumersSQL = `SELECT consumer_id, CAST(applied_id AS TEXT) AS applied_id, ` +
		`CAST(CAST(max(0, (julianday('now') - julianday(updated_at)) * 86400) AS INTEGER) AS TEXT) AS age_seconds ` +
		`FROM "` + ChangeLogConsumersTable + `"`

	// readSchemaVersionSQL reads the change-log schema-version pin.
	readSchemaVersionSQL = `SELECT CAST(schema_version AS TEXT) AS v FROM "` +
		ChangeLogMetaTable + `" WHERE singleton_pk = 1`
)

// tableColumnsSQL renders the catalog read behind [executor.tableColumns].
//
// `PRAGMA table_info(<literal>)` rather than the table-valued
// `pragma_table_info(?)` form deliberately: the PRAGMA spelling is the one the
// SHIPPED, live-validated D1 cold-start schema reader already runs over the
// same `/query` endpoint (`sqlite/d1_schema.go` reads `PRAGMA table_xinfo` /
// `index_list` / `foreign_key_list` that way), so one query serves both
// transports on evidence rather than on hope. PRAGMA takes no bind parameter,
// hence the quoted string literal — the argument is always one of the engine's
// own constant table names, and it is quoted regardless.
//
// table_info (not table_xinfo) is right here: the engine's own tables have no
// generated columns, and a user table whose VIRTUAL generated column is
// therefore omitted reports FEWER columns, which lands on the refusing side.
func tableColumnsSQL(table string) string {
	return "PRAGMA table_info(" + quoteSQLString(table) + ")"
}

// --- localExecutor: the Phase-1 *sql.DB transport ---------------------------

// localExecutor runs the trigger SQL against a local SQLite file's *sql.DB. Its
// SQL and scan code are the shipped v0.99.148 engine's verbatim, so the local
// path is byte-identical after the executor refactor.
type localExecutor struct {
	db *sql.DB
}

func (e *localExecutor) execDDL(ctx context.Context, stmt string) error {
	_, err := e.db.ExecContext(ctx, stmt)
	return err
}

func (e *localExecutor) pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error) {
	const q = "SELECT id, op, tbl, before, after, captured_at FROM " +
		`"` + ChangeLogTable + `" WHERE id > ? ORDER BY id ASC LIMIT ?`
	rows, err := e.db.QueryContext(ctx, q, sinceID, batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []rawChangeRow
	for rows.Next() {
		var rc rawChangeRow
		if err := rows.Scan(&rc.id, &rc.op, &rc.tbl, &rc.before, &rc.after, &rc.capturedAt); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		out = append(out, rc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter rows: %w", err)
	}
	return out, nil
}

func (e *localExecutor) readFingerprints(ctx context.Context) ([]fingerprintRow, error) {
	rows, err := e.db.QueryContext(ctx, readFingerprintsSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []fingerprintRow
	for rows.Next() {
		var fr fingerprintRow
		if err := rows.Scan(&fr.tbl, &fr.columns); err != nil {
			return nil, fmt.Errorf("sqlite-trigger: scan captured-column fingerprint: %w", err)
		}
		out = append(out, fr)
	}
	return out, rows.Err()
}

func (e *localExecutor) changeLogExists(ctx context.Context) (bool, error) {
	var name string
	err := e.db.QueryRowContext(ctx, changeLogExistsSQL, ChangeLogTable).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (e *localExecutor) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, tableColumnsSQL(table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var (
			cid       int
			name      string
			declType  string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &declType, &notNull, &dfltValue, &pk); err != nil {
			return nil, fmt.Errorf("sqlite-trigger: scan table_info(%q): %w", table, err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (e *localExecutor) maxChangeLogID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MAX(id) FROM "`+ChangeLogTable+`"`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (e *localExecutor) changeLogAllocation(ctx context.Context) (changeLogAllocation, error) {
	var createSQL sql.NullString
	if err := e.db.QueryRowContext(ctx, changeLogCreateSQL, ChangeLogTable).Scan(&createSQL); err != nil {
		return changeLogAllocation{}, err
	}
	maxID, err := e.maxChangeLogID(ctx)
	if err != nil {
		return changeLogAllocation{}, err
	}
	var seq int64
	// A database that has never held an AUTOINCREMENT table has no
	// sqlite_sequence table at all, and the query is then a hard "no such
	// table" error rather than an empty result. That is not a failure to
	// report: it means the counter is 0, which is what the DDL arm is there
	// to explain.
	//
	// SUBSTRING CLASSIFIER, ENUMERATED AND EXEMPT (audit backlog C-1's sweep).
	// The MySQL/PG missing-table classifiers were moved onto the server's error
	// CODE; this one and its D1 twin below cannot follow, because the two
	// transports have no COMMON code to classify on — the local path could read
	// modernc's extended result code, but the D1 path receives only an HTTP
	// error body with no SQLite code in it, and a fix that reached one
	// transport and not the other is the sibling failure this project keeps
	// repeating. It is exempt rather than fixed on top of that: the answer
	// gates a counter default (seq = 0), not a refusal, and the query is a
	// fixed literal against `sqlite_sequence`, so the misclassification surface
	// is one table name rather than an arbitrary user query. If D1 ever exposes
	// a SQLite result code, fix both.
	if err := e.db.QueryRowContext(ctx, changeLogSeqSQL, ChangeLogTable).Scan(&seq); err != nil {
		if !strings.Contains(err.Error(), "no such table") {
			return changeLogAllocation{}, err
		}
		seq = 0
	}
	return changeLogAllocation{
		floor:         maxChangeLogInt64(maxID, seq),
		autoincrement: createSQL.Valid && changeLogDeclaresAutoincrement(createSQL.String),
	}, nil
}

func maxChangeLogInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (e *localExecutor) discoverTriggers(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, discoverTriggersSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (e *localExecutor) captureTriggerSQL(ctx context.Context) (map[string]string, error) {
	rows, err := e.db.QueryContext(ctx, captureTriggerSQLQuery)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var name string
		var createSQL sql.NullString
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, err
		}
		if createSQL.Valid {
			out[name] = createSQL.String
		}
	}
	return out, rows.Err()
}

func (e *localExecutor) realRenderProbe(ctx context.Context) (string, error) {
	var rendered string
	if err := e.db.QueryRowContext(ctx, sqlite.RealRenderProbeSQL()).Scan(&rendered); err != nil {
		return "", err
	}
	return rendered, nil
}

// localPruneBatchSize bounds one local-file prune DELETE (P-1). SQLite is
// single-writer: the SOURCE APPLICATION's writes stall for the duration of any
// DELETE we run, so a monolithic backlog DELETE would hold its writer hostage.
// ~20k id-ordered PK deletes is milliseconds on a local file — larger than
// D1's bound (no per-query CPU ceiling here) but short enough that the app's
// writer interleaves between batches.
const localPruneBatchSize = 20_000

func (e *localExecutor) pruneChangeLogBatch(ctx context.Context, floor, upper int64) (int64, error) {
	res, err := e.db.ExecContext(ctx,
		`DELETE FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`, floor, upper)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (e *localExecutor) minChangeLogID(ctx context.Context) (int64, error) {
	var id sql.NullInt64
	if err := e.db.QueryRowContext(ctx, `SELECT MIN(id) FROM "`+ChangeLogTable+`"`).Scan(&id); err != nil {
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
}

func (e *localExecutor) pruneBatchSize() int64 { return localPruneBatchSize }

// maxPollBatch: no transport ceiling — the local *sql.DB path streams rows
// with no response-size cap, so the reader keeps the shared default batch.
func (e *localExecutor) maxPollBatch() int { return 0 }

func (e *localExecutor) checkpointWAL(ctx context.Context) error {
	// PRAGMA wal_checkpoint returns one row: (busy, log_frames, checkpointed).
	// busy=1 means a reader/writer held the WAL so it could not fully truncate
	// this round — NOT an error; the next cadence retries. On a non-WAL database
	// it returns (0,-1,-1) with no error, so this is harmless if the operator's
	// source is not in WAL mode. With the poller pool's idle connection released
	// (SetMaxIdleConns(0)) there is no stale read-mark of our own to pin it.
	var busy, logFrames, checkpointed int
	if err := e.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").
		Scan(&busy, &logFrames, &checkpointed); err != nil {
		return err
	}
	if busy != 0 {
		slog.DebugContext(ctx, "sqlite-trigger: WAL checkpoint busy; retrying next cadence",
			slog.Int("log_frames", logFrames), slog.Int("checkpointed", checkpointed))
	} else {
		slog.DebugContext(ctx, "sqlite-trigger: WAL checkpoint(TRUNCATE) ok",
			slog.Int("log_frames", logFrames), slog.Int("checkpointed", checkpointed))
	}
	return nil
}

func (e *localExecutor) vacuum(ctx context.Context) error {
	_, err := e.db.ExecContext(ctx, "VACUUM")
	return err
}

func (e *localExecutor) changeLogStats(ctx context.Context) (minID, count int64, err error) {
	var m sql.NullInt64
	if scanErr := e.db.QueryRowContext(ctx, `SELECT MIN(id), COUNT(*) FROM "`+ChangeLogTable+`"`).Scan(&m, &count); scanErr != nil {
		return 0, 0, scanErr
	}
	if m.Valid {
		minID = m.Int64
	}
	return minID, count, nil
}

func (e *localExecutor) upsertConsumer(ctx context.Context, consumerID string, appliedID int64) error {
	_, err := e.db.ExecContext(ctx, upsertConsumerSQL, consumerID, appliedID)
	return err
}

func (e *localExecutor) consumerRegistryState(ctx context.Context) (exists bool, ver int, err error) {
	var name string
	err = e.db.QueryRowContext(ctx, tableExistsSQL, ChangeLogConsumersTable).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	var verText string
	err = e.db.QueryRowContext(ctx, readSchemaVersionSQL).Scan(&verText)
	if errors.Is(err, sql.ErrNoRows) {
		return true, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	if ver, err = strconv.Atoi(verText); err != nil {
		return false, 0, fmt.Errorf("change-log schema_version %q is not an integer: %w", verText, err)
	}
	return true, ver, nil
}

func (e *localExecutor) readConsumers(ctx context.Context) ([]triggercdc.Consumer, error) {
	rows, err := e.db.QueryContext(ctx, readConsumersSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []triggercdc.Consumer
	for rows.Next() {
		var id, appliedText, ageText string
		if err := rows.Scan(&id, &appliedText, &ageText); err != nil {
			return nil, err
		}
		c, err := decodeConsumerRow(id, appliedText, ageText)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (e *localExecutor) close() error {
	if e.db == nil {
		return nil
	}
	err := e.db.Close()
	e.db = nil
	return err
}

// --- d1Executor: the Phase-2 D1 /query HTTP transport -----------------------

// d1Executor runs the SAME trigger SQL against a live Cloudflare D1 database
// over the `/query` HTTP API (ADR-0136). The catalog queries are byte-identical
// to the local path; the poll and MAX(id) project the `id` as exact TEXT
// (CAST AS TEXT) and bind the watermark as a STRING param so the watermark is
// never rounded through a JSON number — the ADR-0132 no-JSON-number-for-an-
// integer-we-key-on discipline, kept uniform even though the change-log
// AUTOINCREMENT id is bounded well under 2^53.
type d1Executor struct {
	conn *sqlite.D1Conn
}

func (e *d1Executor) execDDL(ctx context.Context, stmt string) error {
	return e.conn.Exec(ctx, stmt)
}

func (e *d1Executor) pollChangeLog(ctx context.Context, sinceID int64, batch int) ([]rawChangeRow, error) {
	// LIMIT is a trusted in-process int (embedded, like the d1 row reader's
	// pagination); only the watermark crosses as a bound param, sent as a string.
	q := `SELECT CAST(id AS TEXT) AS id, op, tbl, before, after, captured_at FROM "` +
		ChangeLogTable + `" WHERE id > ? ORDER BY id ASC LIMIT ` + strconv.Itoa(batch)
	rows, err := e.conn.Query(ctx, q, strconv.FormatInt(sinceID, 10))
	if err != nil {
		return nil, err
	}
	out := make([]rawChangeRow, 0, len(rows))
	for _, row := range rows {
		idText, ok, err := d1CellString(row["id"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode change-log id: %w", err)
		}
		if !ok {
			return nil, errors.New("d1-trigger: change-log row has a NULL/absent id")
		}
		id, perr := strconv.ParseInt(idText, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id %q is not a valid int64: %w", idText, perr)
		}
		op, _, err := d1CellString(row["op"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode op: %w", id, err)
		}
		tbl, _, err := d1CellString(row["tbl"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode tbl: %w", id, err)
		}
		// The before/after cells ARE captured row images, so an SQT-1
		// unrepresentable-text refusal here gets the Bug 245 trigger-lane
		// recovery too. Whether this transport can actually surface the
		// raw bytes is the unmeasured D1-verbatim premise (backlog
		// 2026-08-12) — if D1 mangles server-side the guard cannot fire
		// here; the extension is correct in either world.
		before, err := d1NullString(row["before"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode before: %w%s",
				id, err, capturedImageRemedy(err, id))
		}
		after, err := d1NullString(row["after"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode after: %w%s",
				id, err, capturedImageRemedy(err, id))
		}
		capturedAt, err := d1NullString(row["captured_at"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: change-log id=%d decode captured_at: %w", id, err)
		}
		out = append(out, rawChangeRow{
			id: id, op: op, tbl: tbl,
			before: before, after: after, capturedAt: capturedAt,
		})
	}
	return out, nil
}

func (e *d1Executor) readFingerprints(ctx context.Context) ([]fingerprintRow, error) {
	rows, err := e.conn.Query(ctx, readFingerprintsSQL)
	if err != nil {
		return nil, err
	}
	out := make([]fingerprintRow, 0, len(rows))
	for _, row := range rows {
		tbl, _, err := d1CellString(row["tbl"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: scan captured-column fingerprint tbl: %w", err)
		}
		columns, _, err := d1CellString(row["columns"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: scan captured-column fingerprint columns: %w", err)
		}
		out = append(out, fingerprintRow{tbl: tbl, columns: columns})
	}
	return out, nil
}

func (e *d1Executor) changeLogExists(ctx context.Context) (bool, error) {
	rows, err := e.conn.Query(ctx, changeLogExistsSQL, ChangeLogTable)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}

func (e *d1Executor) tableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := e.conn.Query(ctx, tableColumnsSQL(table))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, ok, err := d1CellString(row["name"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode table_info(%q).name: %w", table, err)
		}
		if !ok {
			// A PRAGMA row without a name is not a shape this can interpret;
			// refusing beats silently shortening the column set, which is the
			// direction that would ADOPT a foreign table.
			return nil, fmt.Errorf("d1-trigger: table_info(%q) returned a row with a NULL name", table)
		}
		out = append(out, name)
	}
	return out, nil
}

func (e *d1Executor) maxChangeLogID(ctx context.Context) (int64, error) {
	rows, err := e.conn.Query(ctx, `SELECT CAST(MAX(id) AS TEXT) AS m FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	text, ok, err := d1CellString(rows[0]["m"])
	if err != nil {
		return 0, err
	}
	if !ok { // MAX(id) over an empty change-log is NULL.
		return 0, nil
	}
	id, perr := strconv.ParseInt(text, 10, 64)
	if perr != nil {
		return 0, fmt.Errorf("d1-trigger: MAX(id) text %q is not a valid int64: %w", text, perr)
	}
	return id, nil
}

// changeLogAllocation mirrors the local implementation over the /query API.
// `sqlite_master` and `sqlite_sequence` are ordinary tables on D1 too, so the
// hazard and the check port unchanged; the counter is projected as exact TEXT
// for the same ADR-0132 reason the poll's ids are.
func (e *d1Executor) changeLogAllocation(ctx context.Context) (changeLogAllocation, error) {
	createRows, err := e.conn.Query(ctx, changeLogCreateSQL, ChangeLogTable)
	if err != nil {
		return changeLogAllocation{}, err
	}
	var createSQL string
	if len(createRows) > 0 {
		s, ok, cerr := d1CellString(createRows[0]["sql"])
		if cerr != nil {
			return changeLogAllocation{}, fmt.Errorf("d1-trigger: decode change-log CREATE statement: %w", cerr)
		}
		if ok {
			createSQL = s
		}
	}
	maxID, err := e.maxChangeLogID(ctx)
	if err != nil {
		return changeLogAllocation{}, err
	}
	var seq int64
	seqRows, err := e.conn.Query(ctx,
		`SELECT CAST(COALESCE((SELECT seq FROM sqlite_sequence WHERE name = ?), 0) AS TEXT) AS seq`,
		ChangeLogTable)
	if err != nil {
		// Same "the database has never held an AUTOINCREMENT table" case as
		// the local path: a missing sqlite_sequence means the counter is 0.
		// Substring-classified, enumerated and exempt for the reason spelled out
		// on the local twin above — over HTTP there is no SQLite result code to
		// classify on at all.
		if !strings.Contains(err.Error(), "no such table") {
			return changeLogAllocation{}, err
		}
	} else if len(seqRows) > 0 {
		text, ok, cerr := d1CellString(seqRows[0]["seq"])
		if cerr != nil {
			return changeLogAllocation{}, fmt.Errorf("d1-trigger: decode sqlite_sequence.seq: %w", cerr)
		}
		if ok {
			seq, cerr = strconv.ParseInt(text, 10, 64)
			if cerr != nil {
				return changeLogAllocation{}, fmt.Errorf(
					"d1-trigger: sqlite_sequence.seq text %q is not a valid int64: %w", text, cerr,
				)
			}
		}
	}
	return changeLogAllocation{
		floor:         maxChangeLogInt64(maxID, seq),
		autoincrement: changeLogDeclaresAutoincrement(createSQL),
	}, nil
}

func (e *d1Executor) discoverTriggers(ctx context.Context) ([]string, error) {
	rows, err := e.conn.Query(ctx, discoverTriggersSQL)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		name, ok, err := d1CellString(row["name"])
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, name)
		}
	}
	return out, nil
}

func (e *d1Executor) captureTriggerSQL(ctx context.Context) (map[string]string, error) {
	rows, err := e.conn.Query(ctx, captureTriggerSQLQuery)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(rows))
	for _, row := range rows {
		name, ok, err := d1CellString(row["name"])
		if err != nil || !ok {
			if err != nil {
				return nil, err
			}
			continue
		}
		createSQL, ok, err := d1CellString(row["sql"])
		if err != nil {
			return nil, err
		}
		if ok {
			out[name] = createSQL
		}
	}
	return out, nil
}

func (e *d1Executor) realRenderProbe(ctx context.Context) (string, error) {
	rows, err := e.conn.Query(ctx, sqlite.RealRenderProbeSQL())
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errors.New("d1-trigger: the REAL render probe returned no rows")
	}
	rendered, ok, err := d1CellString(rows[0]["p"])
	if err != nil {
		return "", fmt.Errorf("d1-trigger: decode the REAL render probe: %w", err)
	}
	if !ok {
		return "", errors.New("d1-trigger: the REAL render probe returned NULL")
	}
	return rendered, nil
}

// d1PruneBatchSize bounds one D1 prune DELETE (P-1). D1 enforces a per-query
// CPU ceiling (a too-big statement fails with the HTTP 400 code-7500 class —
// the same limit that broke `--infer-types` GLOBs, ADR-0145), and a failed
// tick retries an even BIGGER delete next interval — prune would fall behind
// permanently, the exact failure ADR-0137 Phase B exists to prevent. 2k
// id-ordered PK deletes stays comfortably under the ceiling; the batching loop
// makes throughput a function of the tick budget, not statement size.
const d1PruneBatchSize = 2_000

func (e *d1Executor) pruneChangeLogBatch(ctx context.Context, floor, upper int64) (int64, error) {
	// The D1 `/query` transport doesn't surface rows-affected, so count the
	// to-be-deleted range first. This is exact and concurrency-safe: only rows
	// with id <= upper (<= the durably-applied cut) are deleted, the
	// AUTOINCREMENT id only ever grows, and the loop steps disjoint
	// (floor, upper] ranges, so no row can enter the range between this count
	// and the DELETE (the same property that makes pruning safe while a sync
	// is live). COUNT is projected as TEXT so a JSON-number cell never reaches
	// d1CellString.
	floorStr := strconv.FormatInt(floor, 10)
	upperStr := strconv.FormatInt(upper, 10)
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COUNT(*) AS TEXT) AS n FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`,
		floorStr, upperStr)
	if err != nil {
		return 0, err
	}
	var deleted int64
	if len(rows) > 0 {
		text, ok, cerr := d1CellString(rows[0]["n"])
		if cerr != nil {
			return 0, fmt.Errorf("d1-trigger: decode prune count: %w", cerr)
		}
		if ok {
			if deleted, err = strconv.ParseInt(text, 10, 64); err != nil {
				return 0, fmt.Errorf("d1-trigger: prune count %q is not a valid int64: %w", text, err)
			}
		}
	}
	if err := e.conn.Exec(ctx,
		`DELETE FROM "`+ChangeLogTable+`" WHERE id > ? AND id <= ?`, floorStr, upperStr); err != nil {
		return 0, err
	}
	return deleted, nil
}

func (e *d1Executor) minChangeLogID(ctx context.Context) (int64, error) {
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COALESCE(MIN(id), 0) AS TEXT) AS mn FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	id, err := d1IntCell(rows[0]["mn"])
	if err != nil {
		return 0, fmt.Errorf("d1-trigger: decode change-log MIN(id): %w", err)
	}
	return id, nil
}

func (e *d1Executor) pruneBatchSize() int64 { return d1PruneBatchSize }

// d1PollBatchSize clamps the change-log poll batch on the D1 transport (P-3).
// Cloudflare caps a /query response at ~1 MB — the same limit that sizes the
// cold-copy row reader's page at 1000 (see sqlite.d1PageSize) — and change
// rows are HEAVIER than data rows (each carries full before/after JSON
// images), so the shared defaultBatchSize (10000) can overflow the cap on a
// catch-up poll. 1000 matches d1PageSize's reasoning; the pump's full-batch
// fast-repoll keeps catch-up throughput unthrottled.
const d1PollBatchSize = 1000

func (e *d1Executor) maxPollBatch() int { return d1PollBatchSize }

// checkpointWAL is a no-op on the D1 transport: D1 polls over the `/query` HTTP
// API against Cloudflare-managed storage — there is no local pager or WAL file
// for sluice to checkpoint (Bug 167 is local-SQLite-only).
func (e *d1Executor) checkpointWAL(context.Context) error { return nil }

func (e *d1Executor) vacuum(ctx context.Context) error {
	return e.conn.Exec(ctx, "VACUUM")
}

func (e *d1Executor) changeLogStats(ctx context.Context) (minID, count int64, err error) {
	rows, err := e.conn.Query(ctx,
		`SELECT CAST(COALESCE(MIN(id), 0) AS TEXT) AS mn, CAST(COUNT(*) AS TEXT) AS cnt FROM "`+ChangeLogTable+`"`)
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 {
		return 0, 0, nil
	}
	minID, err = d1IntCell(rows[0]["mn"])
	if err != nil {
		return 0, 0, fmt.Errorf("d1-trigger: decode change-log MIN(id): %w", err)
	}
	count, err = d1IntCell(rows[0]["cnt"])
	if err != nil {
		return 0, 0, fmt.Errorf("d1-trigger: decode change-log count: %w", err)
	}
	return minID, count, nil
}

func (e *d1Executor) upsertConsumer(ctx context.Context, consumerID string, appliedID int64) error {
	// applied_id crosses as exact TEXT (never a JSON number); the column's
	// INTEGER affinity converts it on the way in.
	return e.conn.Exec(ctx, upsertConsumerSQL, consumerID, strconv.FormatInt(appliedID, 10))
}

func (e *d1Executor) consumerRegistryState(ctx context.Context) (exists bool, ver int, err error) {
	rows, err := e.conn.Query(ctx, tableExistsSQL, ChangeLogConsumersTable)
	if err != nil {
		return false, 0, err
	}
	if len(rows) == 0 {
		return false, 0, nil
	}
	verRows, err := e.conn.Query(ctx, readSchemaVersionSQL)
	if err != nil {
		return false, 0, err
	}
	if len(verRows) == 0 {
		return true, 0, nil
	}
	verText, ok, err := d1CellString(verRows[0]["v"])
	if err != nil {
		return false, 0, err
	}
	if !ok {
		return true, 0, nil
	}
	if ver, err = strconv.Atoi(verText); err != nil {
		return false, 0, fmt.Errorf("d1-trigger: change-log schema_version %q is not an integer: %w", verText, err)
	}
	return true, ver, nil
}

func (e *d1Executor) readConsumers(ctx context.Context) ([]triggercdc.Consumer, error) {
	rows, err := e.conn.Query(ctx, readConsumersSQL)
	if err != nil {
		return nil, err
	}
	out := make([]triggercdc.Consumer, 0, len(rows))
	for _, row := range rows {
		id, _, err := d1CellString(row["consumer_id"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode consumer_id: %w", err)
		}
		appliedText, _, err := d1CellString(row["applied_id"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode consumer applied_id: %w", err)
		}
		ageText, _, err := d1CellString(row["age_seconds"])
		if err != nil {
			return nil, fmt.Errorf("d1-trigger: decode consumer age: %w", err)
		}
		c, err := decodeConsumerRow(id, appliedText, ageText)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// decodeConsumerRow turns one registry row's exact-TEXT projection into a
// [triggercdc.Consumer]. Shared by both transports so the local path and the D1
// path can never disagree on what a registry row MEANS — the same reason the SQL
// itself is shared. An unparseable frontier is refused loudly rather than
// defaulted: a consumer whose frontier we cannot read must not be silently
// treated as one at id 0 (harmless) or dropped from the MIN (silent loss).
func decodeConsumerRow(id, appliedText, ageText string) (triggercdc.Consumer, error) {
	applied, err := strconv.ParseInt(appliedText, 10, 64)
	if err != nil {
		return triggercdc.Consumer{}, fmt.Errorf(
			"change-log consumer %q has an unreadable applied_id %q: %w", id, appliedText, err,
		)
	}
	age, err := strconv.ParseInt(ageText, 10, 64)
	if err != nil {
		// The age is advisory (it only drives a staleness WARN); an
		// unreadable one must not take the whole prune down.
		age = 0
	}
	return triggercdc.Consumer{ID: id, AppliedID: applied, AgeSeconds: age}, nil
}

// d1IntCell decodes a CAST(... AS TEXT) integer cell to int64. A NULL/absent
// cell decodes to 0 (the COALESCE in the stats query already guards MIN(id);
// this is a defensive fallback).
func d1IntCell(raw json.RawMessage) (int64, error) {
	text, ok, err := d1CellString(raw)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(text, 10, 64)
}

// close is a no-op: the D1 HTTP transport has no pool/file to release.
func (e *d1Executor) close() error { return nil }

// d1CellString extracts a Go string from a JSON string cell. ok is false for a
// JSON null/absent value (so the caller can distinguish absent from ""); a
// non-string, non-null JSON value is an error.
//
// It delegates to the sqlite package's GUARDED extraction (audit SQT-1): the
// before/after cells this transport pulls from the change log are the
// captured row images, and an unguarded json.Unmarshal here silently
// rewrote invalid UTF-8 to U+FFFD one layer ABOVE the guarded storage-value
// decode — which then saw valid UTF-8 and passed the mangle. The catalog/
// bookkeeping call sites (id/op/tbl/fingerprints/registry names) are ASCII
// by construction and can never trip the guard.
func d1CellString(raw json.RawMessage) (s string, ok bool, err error) {
	return sqlite.JSONStringValue(raw)
}

// d1NullString maps a JSON string/null cell to a sql.NullString, matching the
// local path's nullable change-log columns (before / after / captured_at).
func d1NullString(raw json.RawMessage) (sql.NullString, error) {
	s, ok, err := d1CellString(raw)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: s, Valid: ok}, nil
}
