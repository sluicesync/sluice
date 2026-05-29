// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Postgres-side implementation of the orchestrator's XID-wraparound
// preflight prober surface (see
// `internal/pipeline/xid_wraparound_preflight.go` for the operator-facing
// rationale).
//
// PG transaction IDs are 32-bit and wrap around at ~2.15B. As the
// `age(datfrozenxid)` of a database approaches the wraparound horizon,
// autovacuum will start emergency anti-wraparound work and — at the
// hard limit — the database stops accepting new writes globally to
// prevent data corruption (PG enters "single-user mode for VACUUM
// FREEZE" recovery). A migration / long-running CDC stream against
// such a source either runs into the global write-block mid-migration
// or actively contributes to the problem (a long-held replication
// xmin prevents autovacuum from advancing the relfrozenxid). The
// preflight catches both classes UPFRONT and refuses with VACUUM
// FREEZE guidance, mirroring pgcopydb PR #17.
//
// Implemented on [SchemaReader] so the same handle the orchestrator
// uses to read the source schema also drives this probe — no new
// connection plumbing.
//
// The probe reads `pg_database`, which is world-readable, so it needs
// no special privilege — it works for the unprivileged sluice role the
// preflight is designed to catch (same posture as the REPLICATION
// preflight in `replication_preflight.go`).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SourceXIDWraparoundHorizon reports the current `age(datfrozenxid)`
// for the connecting database — the canonical PG measure of how close
// the database is to transaction-ID wraparound (and therefore to
// either emergency autovacuum or the hard write-block at the
// wraparound horizon). The database name is returned so the
// orchestrator can name it in the refusal message.
//
// Implements the orchestrator's `xidWraparoundProber` surface. PG's
// per-database `datfrozenxid` is the oldest unfrozen XID in any table
// of the database; `age()` returns its distance from the current XID
// counter. The hard wraparound horizon is ~2^31 (2,147,483,647); PG
// stops accepting writes well before that to leave VACUUM FREEZE
// recovery headroom.
func (r *SchemaReader) SourceXIDWraparoundHorizon(ctx context.Context) (age int64, datname string, err error) {
	return probeSourceXIDWraparoundHorizon(ctx, r.db)
}

// probeSourceXIDWraparoundHorizon issues `SELECT age(datfrozenxid),
// datname FROM pg_database WHERE datname = current_database()`. Returns
// the (age, datname) pair plus any error.
//
// `pg_database` is the world-readable catalog of databases on the
// server; reading the row for `current_database()` needs no extra
// privilege.
func probeSourceXIDWraparoundHorizon(ctx context.Context, db *sql.DB) (age int64, datname string, err error) {
	const q = `SELECT age(datfrozenxid), datname FROM pg_database WHERE datname = current_database()`
	switch err := db.QueryRowContext(ctx, q).Scan(&age, &datname); {
	case errors.Is(err, sql.ErrNoRows):
		// `current_database()` is always present in pg_database by
		// construction; no-rows means a catalog mismatch worth surfacing
		// rather than silently treating as "no wraparound risk" (which
		// would defer to the raw mid-migration write-block error the
		// preflight exists to replace).
		return 0, "", errors.New("postgres: probe XID wraparound horizon: pg_database row for current_database() not found")
	case err != nil:
		return 0, "", fmt.Errorf("postgres: probe XID wraparound horizon: %w", err)
	}
	return age, datname, nil
}
