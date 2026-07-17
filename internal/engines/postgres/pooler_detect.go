// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// Pooler-strip detection for the replication-protocol path (roadmap
// item 69e, live-probed against Supabase Supavisor 2026-07-15).
//
// Most connection poolers in front of Postgres (Supabase's Supavisor,
// transaction/statement-mode pgbouncer) accept the replication
// connection but STRIP the `replication=database` startup parameter,
// so the first replication-protocol command sluice sends —
// CREATE_REPLICATION_SLOT — reaches a normal backend as plain SQL and
// is rejected with SQLSTATE 42601 (`syntax error at or near
// "CREATE_REPLICATION_SLOT"`). The failure is loud and leaves zero
// partial state, but the raw error reads like a sluice bug; this
// classifier turns the exact signature into a coded, remedy-bearing
// error naming the pooler and the direct endpoint.
//
// The strip is provider-DEPENDENT, not universal: modern pgbouncer
// (>= 1.24) forwards replication connections 1:1, and Vultr's managed
// pools carried slot creation, streaming, and warm resume end-to-end
// (live-verified 2026-07-16). Those setups never produce the 42601
// signature, so this classifier stays truthful either way — it fires
// only on the OBSERVED strip, never on a host pattern.
//
// The match is deliberately narrow — SQLSTATE 42601 structurally via
// [pgconn.PgError] AND the replication command name in the server's
// message — so a genuine syntax error elsewhere never mis-classifies.
// Slot creation is the first replication-protocol command sluice
// issues on a fresh stream (the wal_level and publication probes run
// over plain SQL, which a pooler proxies fine), so this chokepoint in
// [createLogicalReplicationSlot] covers the CDC cold start, the
// snapshot+CDC handoff, and the backup-anchor path alike.

// poolerStripHint is the concise remedy carried as the coded error's
// machine-readable hint.
const poolerStripHint = "point --source at the direct database endpoint; this pooler stripped the replication parameter, so logical replication cannot traverse it (Supabase: the direct endpoint is IPv6-only — the IPv4 add-on is required from IPv4-only networks)"

// classifyPoolerStripError wraps err in the SLUICE-E-CDC-POOLER-ENDPOINT
// coded error when it carries the pooler-strip signature; every other
// error (including nil) is returned unchanged.
func classifyPoolerStripError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	if pgErr.Code != "42601" || !strings.Contains(pgErr.Message, "CREATE_REPLICATION_SLOT") {
		return err
	}
	return &sluicecode.CodedError{
		Code: sluicecode.CodeCDCPoolerEndpoint,
		Hint: poolerStripHint,
		Err: fmt.Errorf(
			"postgres: cdc: the server rejected CREATE_REPLICATION_SLOT as a SQL syntax error (SQLSTATE 42601) — "+
				"the source appears to be a connection pooler (Supavisor / transaction-mode pgbouncer) that "+
				"stripped the replication=database startup parameter, so the replication-protocol command "+
				"reached a normal backend as plain SQL; logical replication requires the DIRECT database "+
				"endpoint here (on Supabase the direct endpoint is IPv6-only — the IPv4 add-on is required "+
				"from IPv4-only networks): %w",
			err,
		),
	}
}
