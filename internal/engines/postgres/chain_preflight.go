// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// PreflightChainResume implements [ir.ChainResumePreflighter]: before
// the incremental-backup orchestrator opens CDC at the parent
// manifest's terminal position, verify the named slot can actually
// serve the chain from that LSN. Two loud refusals replace two bad
// outcomes:
//
//   - Slot missing → without this check the stream open fails with a
//     generic "slot no longer exists" that reads like WAL pruning.
//     The slot most likely NEVER existed: the chain anchor records
//     where the next incremental must start, but only a standing slot
//     retains the WAL to serve it. The refusal names the fix
//     (`backup full --chain-slot`, or a slot created before the full).
//   - Slot's confirmed_flush_lsn AHEAD of the parent position → the
//     silent-loss shape. A slot created (or advanced by another
//     consumer) after the parent backup cannot replay the WAL in
//     between; PostgreSQL's walsender silently fast-forwards
//     START_REPLICATION to confirmed_flush_lsn, so without this check
//     the incremental SUCCEEDS while the chain silently misses every
//     write in (parent, confirmed_flush]. Same hazard class the
//     live add-table preflight guards via [Engine.ReadSlotPosition]
//     ("events in [snapshot-LSN, confirmed_flush_lsn] would be
//     silently dropped"); this is the backup-chain counterpart.
//
// A confirmed_flush_lsn at or behind the parent position is healthy:
// equal is the steady-state chain shape (the slot sits exactly at the
// last committed window end), behind happens when the previous run's
// final ack didn't land before its connection closed (the WAL is
// still retained, PostgreSQL replays from the requested LSN, and the
// chunk writer's dedup is not needed because the recorded EndPosition
// is what the orchestrator restarts from).
func (e Engine) PreflightChainResume(ctx context.Context, dsn string, from ir.Position) error {
	decoded, ok, err := decodePGPos(from)
	if err != nil {
		return fmt.Errorf("postgres: chain preflight: %w", err)
	}
	if !ok || decoded.Slot == "" {
		// Position isn't ours / carries no slot — nothing to verify
		// here; the stream open will validate what it can.
		return nil
	}

	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	const q = `SELECT COALESCE(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1`
	var confirmedFlush string
	switch err := db.QueryRowContext(ctx, q, decoded.Slot).Scan(&confirmedFlush); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf(
			"postgres: chain preflight: replication slot %q does not exist on the source — it may never have been created. "+
				"The chain's parent backup recorded position %s on this slot, but only a standing slot retains the WAL to serve it; "+
				"creating the slot NOW would silently skip every write since the parent backup, so that is refused too. "+
				"To chain incrementals: take a fresh full with `backup full --chain-slot` (provisions the slot at the backup's anchor), "+
				"or create the slot before the full backup runs",
			decoded.Slot, decoded.LSN,
		)
	case err != nil:
		return fmt.Errorf("postgres: chain preflight: read slot %q: %w", decoded.Slot, err)
	}
	if confirmedFlush == "" {
		// Logical slots have confirmed_flush_lsn set from creation;
		// empty means we cannot prove anything either way — fall
		// through to the stream open's own validation rather than
		// refusing on a shape we can't assess.
		return nil
	}

	fromLSN, err := pglogrepl.ParseLSN(decoded.LSN)
	if err != nil {
		return fmt.Errorf("postgres: chain preflight: parse parent LSN %q: %w", decoded.LSN, err)
	}
	flushLSN, err := pglogrepl.ParseLSN(confirmedFlush)
	if err != nil {
		return fmt.Errorf("postgres: chain preflight: parse confirmed_flush_lsn %q: %w", confirmedFlush, err)
	}
	if flushLSN > fromLSN {
		return fmt.Errorf(
			"postgres: chain preflight: slot %q confirmed_flush_lsn %s is AHEAD of the parent backup's terminal position %s — "+
				"the WAL in between is not retained by the slot, and starting the stream would silently skip it (PostgreSQL fast-forwards to confirmed_flush_lsn). "+
				"This happens when the slot was created after the parent backup, or another consumer (e.g. `sluice sync`) advanced it. "+
				"Take a fresh full backup (consider `backup full --chain-slot` so the slot is anchored at the backup's exact position)",
			decoded.Slot, confirmedFlush, decoded.LSN,
		)
	}
	return nil
}
