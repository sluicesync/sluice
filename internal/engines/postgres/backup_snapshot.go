// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// ensureChainSlotPublication is the --chain-slot publication step: the
// G2 Door-4 UNLOGGED census first (capture-completeness 2026-08-26 —
// rationale + door roster on [refuseUnloggedTables] /
// [preflightChainSlotUnlogged]), then the FOR ALL TABLES ensure the
// chain's incrementals decode through.
func (e Engine) ensureChainSlotPublication(ctx context.Context, db *sql.DB, schema string, inScopeTables []string) error {
	if err := preflightChainSlotUnlogged(ctx, db, schema, inScopeTables); err != nil {
		return err
	}
	// The SECOND caller of ensureAllTablesPublication, and the one the
	// A2-4b warning missed on its first cut (VF review of v0.141.0, HIGH-1).
	//
	// A FOR ALL TABLES publication stops Postgres accepting UPDATE and
	// DELETE on every permanent logged table in the database that has no
	// replica identity -- not only the ones this backup reads. INSERT keeps
	// working, so the breakage is partial and surfaces inside whatever
	// application owns those tables.
	//
	// It is WORSE here than on the sync path, which is why it is not enough
	// to say "pre-existing". --chain-slot deliberately PERSISTS this
	// publication so the chain's incrementals can decode through it, so the
	// exposure outlives the run that caused it.
	//
	// Nothing is passed as covered: unlike the sync cold start, this path
	// runs no replica-identity REFUSAL at all, so no table here is graded by
	// anything else. Adding that refusal is a behaviour change on a shipped
	// path and is deliberately not made here; warning is purely additive.
	warnPublicationExposure(ctx, db, nil)
	if err := ensureAllTablesPublication(ctx, db, e.publicationName()); err != nil {
		return classifyStandbyReadOnly(fmt.Errorf("postgres: backup snapshot: --chain-slot: ensure publication: %w", err))
	}
	return nil
}

// warnPublicationExposure reports the tables a FOR ALL TABLES publication is
// about to stop accepting UPDATE and DELETE on, and is engine-local because
// its callers hold a *sql.DB rather than a Streamer.
//
// Both of them pass a nil predicate, so the reported set is EVERY at-risk
// table in the database -- including tables the run itself reads. The message
// says "across this whole database" for that reason. It used to say "tables
// it does not read", which was a sentence about the SYNC path's set pasted
// onto a path that has no refusal to take a complement of, and it was found
// by a post-tag review rather than by anyone reading it.
//
// Advisory by construction: every failure is swallowed to DEBUG. A catalog
// read that cannot run must not fail a backup that would otherwise succeed,
// which would turn an advisory into the refusal this deliberately is not.
func warnPublicationExposure(ctx context.Context, db *sql.DB, covered func(namespace, table string) bool) {
	query := func(ctx context.Context, q string, args ...any) (*catalogRows, error) {
		return catalogQueryOn(ctx, db, q, args...)
	}
	exposed, err := auditPublicationExposure(ctx, query, covered)
	if err != nil {
		slog.DebugContext(ctx, "publication exposure audit skipped", "error", err)
		return
	}
	if len(exposed) == 0 {
		return
	}
	slog.WarnContext(
		ctx,
		"UNSELECTED-NAMESPACE-EXPOSURE: this run's publication will stop UPDATE and DELETE on tables across this whole database",
		"tables", exposed,
		"count", len(exposed),
		"why", "a chain slot needs a database-wide publication, so it is created FOR ALL TABLES and reaches every "+
			"table in the database; Postgres refuses UPDATE and DELETE on a published table that has no replica "+
			"identity, while INSERT keeps working -- so the failure surfaces inside whatever application owns them, "+
			"and --chain-slot keeps the publication after the run",
		"remedy", "give each listed table a PRIMARY KEY or REPLICA IDENTITY FULL, or drop this backup's publication "+
			"when the chain is finished with it",
	)
}

// backupSnapshotSlotPrefix is the prefix the backup-anchor temporary
// slot is named with. Each call appends a Unix-nanosecond timestamp so
// concurrent backups against the same source don't fight for the same
// slot name. The slot is protocol-TEMPORARY (Bug 137) — the server
// drops it when the creating replication conn closes, including on
// hard process death — so the timestamp is purely for
// collision-avoidance during the run. The timestamp doubles as the
// age signal the resume-time orphan sweep uses to clean up persistent
// anchors leaked by pre-fix binaries (see backup_anchor_sweep.go).
const backupSnapshotSlotPrefix = "sluice_backup_anchor_"

// OpenBackupSnapshot implements [irbackup.SnapshotOpener]. It captures
// a consistent Postgres snapshot anchored at a logical-replication
// slot's `consistent_point` LSN, returning a snapshot-pinned RowReader
// the full-backup orchestrator drives the table sweep against.
//
// Two anchor shapes, selected by opts.PersistChainSlot:
//
//   - Default (false): the anchor slot is protocol-TEMPORARY (named
//     with a timestamp prefix; the server drops it when the creating
//     replication conn closes — graceful Close AND hard process death
//     both qualify, which is the Bug 137 fix: a SIGKILLed backup can
//     no longer leak a persistent slot that pins WAL forever) —
//     distinct from the chain-handoff slot recorded in the manifest's
//     EndPosition. The exported snapshot only needs the replication
//     conn alive until the SQL conn below has run SET TRANSACTION
//     SNAPSHOT; after that the pinned tx stands alone, and nothing
//     ever consumes the anchor slot — so tying the slot's life to the
//     session costs nothing. The
//     chain-handoff slot is the operator's responsibility to maintain
//     (created via `sluice sync start` or manually, BEFORE this
//     backup — a slot created after it cannot serve the WAL in
//     between; see [Engine.PreflightChainResume]).
//   - --chain-slot (true): the PERSISTENT chain slot itself (named
//     opts.SlotName) is created and used as the anchor, so its
//     consistent point IS the recorded EndPosition and `backup
//     incremental` chains with zero gap by construction. The slot is
//     kept only when the orchestrator calls [irbackup.Snapshot.Commit]
//     — since task #42 (ADR-0085) that happens once the run's
//     in-progress manifest durably records the anchor, so an
//     interrupted-but-resumable run keeps the slot for resume adoption;
//     a run that fails before that point Closes uncommitted and drops
//     it. The publication the CDC reader decodes through is
//     ensured here too — pgoutput evaluates publication membership
//     with a HISTORIC catalog snapshot, so a publication created
//     after the anchor cannot decode the chain's first window.
//
// Caller closes the returned snapshot to release the snapshot tx, the
// pinned SQL conn(s), the slot-creation replication conn, the anchor
// slot (unless committed), and the underlying DB pool.
func (e Engine) OpenBackupSnapshot(ctx context.Context, dsn string, opts irbackup.SnapshotOptions) (*irbackup.Snapshot, error) {
	chainSlotName := opts.SlotName
	if chainSlotName == "" {
		chainSlotName = defaultSlot
	}
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Bug 263 (v0.139.0 regression cycle): the standby check runs BEFORE
	// checkWALLevel, because on a hot standby at wal_level=replica — the
	// Postgres default, and what a plain streaming read replica actually
	// runs — BOTH conditions hold, and whichever fires first is the error
	// the caller sees. wal_level-first was actively misleading: it told the
	// operator to set wal_level=logical on a server that inherits the
	// setting from its primary and cannot change it, and on the backup path
	// it also meant the coded standby refusal never reached the fallback
	// door, so the run copied the whole database before dying at the
	// position capture. A standby is never a valid CDC or backup-anchor
	// source at any wal_level, so it is the more specific answer.
	if err := checkNotStandby(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := checkWALLevel(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	// Resolve the anchor slot. Default shape: a fresh timestamped
	// protocol-TEMPORARY anchor, auto-dropped when its replication
	// conn closes. --chain-slot shape: the persistent chain slot
	// itself.
	anchorSlot := fmt.Sprintf("%s%d", backupSnapshotSlotPrefix, time.Now().UnixNano())
	anchorIsTemporary := !opts.PersistChainSlot
	if opts.PersistChainSlot {
		anchorSlot = chainSlotName
	}

	// dropAnchorBestEffort cleans up a half-created PERSISTENT anchor
	// on the open-failure paths below. A temporary anchor needs no SQL
	// drop — the closeReplConnGraceful that always follows releases it
	// server-side (and a cross-session pg_drop_replication_slot on a
	// temporary slot would fail anyway: temporary slots stay owned by
	// their creating session for their whole life).
	dropAnchorBestEffort := func() {
		if anchorIsTemporary {
			return
		}
		_, _ = db.ExecContext(ctx, "SELECT pg_catalog.pg_drop_replication_slot($1)", anchorSlot)
	}

	// A pre-existing slot at the anchor name is refused loudly. For
	// the timestamped default this is near-impossible and indicates a
	// stale leak; for --chain-slot it is the load-bearing guard: an
	// existing slot's consistent point is NOT this backup's anchor, so
	// silently reusing it would record a position the slot may not be
	// able to serve gap-free.
	info, err := slotInfo(ctx, db, anchorSlot)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if info != nil {
		_ = db.Close()
		return nil, anchorSlotExistsErr(anchorSlot, opts.PersistChainSlot)
	}

	// --chain-slot: ensure the publication the chain's incrementals
	// will decode through exists BEFORE the slot is created. pgoutput
	// resolves publication membership with a historic catalog snapshot
	// at each WAL record's LSN, so the publication must predate the
	// anchor or the chain's first window cannot be decoded (loud
	// "publication does not exist" at incremental time — observed live
	// in the 2026-06-10 backup benchmark). FOR ALL TABLES matches the
	// CDC reader's own no-scope ensure and is superset-safe.
	if opts.PersistChainSlot {
		if err := e.ensureChainSlotPublication(ctx, db, cfg.schema, opts.InScopeTables); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	// Open a replication connection dedicated to slot creation. We
	// keep it alive for the lifetime of the BackupSnapshot so the
	// exported snapshot stays valid through the row sweep. Once we
	// drop the slot in Close the conn is released too.
	replConn, err := openReplicationConn(ctx, cfg.dsn, cfg.appID)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: open replication conn: %w", err)
	}

	// EXPORT_SNAPSHOT under createLogicalReplicationSlot's PG-version-
	// adaptive helper. Default shape: protocol-TEMPORARY (Bug 137) —
	// the slot's only job is to pin the exported snapshot for this
	// run, and the server auto-drops it with the replication conn, so
	// a hard-killed run leaves nothing behind. --chain-slot shape:
	// persistent + FAILOVER true on PG 17+ — that slot is intended to
	// live across failovers, so FAILOVER is exactly right there (and
	// the server refuses TEMPORARY+FAILOVER combined anyway).
	consistentPoint, snapshotName, err := createLogicalReplicationSlot(ctx, db, replConn, anchorSlot, slotCreateOptions{
		exportSnapshot: true,
		temporary:      anchorIsTemporary,
	})
	if err != nil {
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: %w", err)
	}
	if snapshotName == "" {
		// Drop the slot we just made so we don't leak it.
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, errors.New("postgres: backup snapshot: server returned empty snapshot_name; expected EXPORT_SNAPSHOT to populate it")
	}

	// Pin a regular SQL connection and import the exported snapshot.
	// SET TRANSACTION SNAPSHOT must be the first statement after
	// BEGIN — the docs are explicit about this.
	conn, err := db.Conn(ctx)
	if err != nil {
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: pin sql conn: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: BEGIN: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", quoteSnapshotName(snapshotName))); err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: SET TRANSACTION SNAPSHOT: %w", err)
	}

	// Encode the position with the CHAIN-HANDOFF slot name (not the
	// anchor slot) so a Phase 3 incremental against this manifest
	// opens CDC against the slot the operator manages. On the
	// --chain-slot shape the two are the same slot, so the recorded
	// name is right either way. The recorded LSN is the snapshot's
	// consistent_point: every write before that LSN is captured by the
	// row sweep; every write after it is captured by the chain's next
	// link's CDC stream from this LSN forward.
	position, err := encodePGPos(pgPos{Slot: chainSlotName, LSN: consistentPoint})
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		dropAnchorBestEffort()
		closeReplConnGraceful(replConn)
		_ = db.Close()
		return nil, fmt.Errorf("postgres: backup snapshot: encode position: %w", err)
	}

	if !opts.PersistChainSlot {
		// The chain prerequisites (standing slot + publication) are NOT
		// provisioned on this shape — say so once, at the moment the
		// operator can still act on it, instead of letting the first
		// `backup incremental` discover it the hard way.
		slog.InfoContext(
			ctx, "backup: snapshot anchor slot is temporary; to chain incrementals off this backup, the chain slot must retain WAL from this point",
			slog.String("chain_slot", chainSlotName),
			slog.String("hint", "re-run with --chain-slot to provision it at the anchor, or run continuous `sluice sync start`"),
		)
	}

	// estimatorDSN + estimatorExactCount (ADR-0149): the backup
	// orchestrator's within-table chunk DECISION probes this reader's
	// EstimateRowCount pre-stream. The reader is pinned (closer == nil),
	// so the probe runs reltuples on a FRESH off-snapshot conn — and on
	// the never-ANALYZEd sentinel resolves via an exact COUNT(*) on that
	// SAME fresh conn (the 59c55e27 estimate/bounds split: the decision
	// is a size estimate with no consistency requirement; the chunk
	// bounds and row streams stay on the pinned conn). Without the DSN
	// the estimate reports 0 and every large table would silently stream
	// single-reader.
	rowReader := &RowReader{
		q:      conn,
		schema: cfg.schema,
		closer: nil, // BackupSnapshot owns the lifecycle

		snapshotPinned:      true,
		estimatorDSN:        cfg.dsn,
		estimatorAppID:      cfg.appID,
		estimatorExactCount: true,
	}

	closed := false
	committed := false
	closeFn := func() error {
		if closed {
			return nil
		}
		closed = true
		// Order matters: commit (or rollback) the snapshot tx → drop
		// the anchor slot via SQL → close the replication conn → close
		// the DB pool. The anchor slot drop must happen on the regular
		// SQL conn (not the replication conn) because the replication
		// conn is in REPLICATION mode and pg_drop_replication_slot()
		// is a regular SQL function. context.Background so a cancelled
		// parent ctx doesn't prevent cleanup.
		var firstErr error
		if _, err := conn.ExecContext(context.Background(), "COMMIT"); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if anchorIsTemporary {
			// Protocol-TEMPORARY anchor (Bug 137): the server drops the
			// slot itself when replConn closes below — including on hard
			// process death, which is the entire point. No explicit SQL
			// drop is attempted: a temporary slot stays owned (active)
			// by its creating session for its whole life, so a
			// cross-session pg_drop_replication_slot here would fail
			// with "replication slot is active for PID …" rather than
			// drop anything.
		} else if committed {
			// --chain-slot run that completed: the chain slot is now a
			// durable resource anchored at this backup's EndPosition —
			// keeping it is the entire point. Skip the drop.
			slog.InfoContext(
				context.Background(), "postgres: backup snapshot: chain slot persisted at the backup's anchor position",
				slog.String("slot", anchorSlot),
				slog.String("consistent_point", consistentPoint),
				slog.String("note", "the slot retains WAL until the next `backup incremental` consumes it; drop via `sluice slot drop` if you abandon the chain"),
			)
		} else if _, err := db.ExecContext(context.Background(), "SELECT pg_catalog.pg_drop_replication_slot($1)", anchorSlot); err != nil {
			// Slot drop failure is logged but doesn't escalate — the
			// backup itself is durable, and a leaked anchor slot is
			// recoverable via `sluice slot drop` or manual SQL.
			slog.WarnContext(
				context.Background(), "postgres: backup snapshot: drop anchor slot failed; manual cleanup may be required",
				slog.String("slot", anchorSlot),
				slog.String("err", err.Error()),
			)
			if !isSlotAlreadyGoneErr(err) && firstErr == nil {
				firstErr = err
			}
		}
		if err := replConn.Close(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}

	snap := &irbackup.Snapshot{
		Position:     position,
		Rows:         rowReader,
		CloseFn:      closeFn,
		SnapshotName: snapshotName,
	}
	if opts.PersistChainSlot {
		snap.CommitFn = func(context.Context) error {
			committed = true
			return nil
		}
	}
	return snap, nil
}

// anchorSlotExistsErr builds the loud refusal for a pre-existing slot at
// the anchor name. On --chain-slot it carries the task #42 (ADR-0085)
// recovery wording — a crashed --chain-slot backup's slot is the
// WAL-retention guarantee for a sound resume, so the operator must
// re-run (adopt) rather than drop; on the timestamped-default shape a
// collision is near-impossible and signals a stale leak.
func anchorSlotExistsErr(anchorSlot string, persistChainSlot bool) error {
	if persistChainSlot {
		return fmt.Errorf(
			"postgres: backup snapshot: --chain-slot: replication slot %q already exists. "+
				"It may belong to a running `sluice sync` (which already retains WAL for chaining — omit --chain-slot and chain off its position), "+
				"an interrupted --chain-slot backup (re-run the SAME `backup full` command against the same destination — resume adopts the slot and its anchor; "+
				"do NOT drop the slot to recover, that releases the WAL the resume needs), "+
				"or another consumer (pass a different --slot-name). "+
				"Only for a deliberate fresh start: drop it via `sluice slot drop %s` and pass --force-overwrite",
			anchorSlot, anchorSlot,
		)
	}
	return fmt.Errorf(
		"postgres: backup snapshot: anchor slot %q already exists; this should be impossible (timestamped name) — drop manually and retry",
		anchorSlot,
	)
}

// isSlotAlreadyGoneErr reports whether err is the "slot does not exist"
// answer from pg_drop_replication_slot — SQLSTATE 42704, undefined_object.
// The drop call uses an idempotent intent, so finding the slot already gone
// (manual drop, automatic cleanup on connection drop, etc.) is success.
//
// It matches on the SQLSTATE, never the text. The text form —
// `strings.Contains(msg, "does not exist")`, unqualified — is the audit-backlog
// C-1 shape, and this is a SWALLOW site: both callers treat a true answer as
// "nothing to clean up". "does not exist" is PostgreSQL's house phrasing for a
// whole family of conditions, so the unqualified match read
// `database "app" does not exist` (3D000, which a pooled *sql.DB surfaces after
// a re-dial) and `function pg_drop_replication_slot(unknown) does not exist`
// (42883, an under-privileged or pre-9.4 server) as "the slot is already gone".
// Both are the leak direction: the slot survives, keeps pinning WAL on the
// SOURCE, and the operator is told cleanup succeeded. A leaked anchor slot is
// exactly the failure whose warning the caller above suppresses.
// Scope, because 42704 is undefined_OBJECT and not undefined_slot: PG raises it
// for a missing type, collation, or role (from DROP ROLE / GRANT — SET ROLE
// answers 22023 instead; both measured on a real PG 16) as well as for a
// missing replication slot. That breadth is safe HERE and only here, because
// the sole statement this classifies is `SELECT pg_drop_replication_slot($1)`,
// which names no type, collation or role — so 42704 from it can only be the
// slot. Do not lift this helper to a statement that references other objects.
func isSlotAlreadyGoneErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "42704"
	}
	return false
}

// avoid an unused-import warning when sql is referenced indirectly.
var _ = sql.ErrNoRows
