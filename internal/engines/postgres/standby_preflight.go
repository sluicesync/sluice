// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Standby / read-replica CDC-source preflight (Bug 197, Supabase
// read-replica probe 2026-07-17). CDC has to manage the sluice
// publication on the source, and CREATE/ALTER PUBLICATION cannot run on
// a hot standby — pre-fix, `sync start` against a replica died at
// publication ensure with a raw, uncoded `SQLSTATE 25006 cannot execute
// CREATE PUBLICATION in a read-only transaction` that read like a
// sluice bug and steered nowhere. The preflight names the source as a
// standby and steers to the primary BEFORE anything touches the source;
// the 25006 classifier is the belt for a source that flips into
// recovery between the preflight and the write.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// standbyRemedyHint is the concise remedy carried as CodedError
// metadata by both the preflight refusal and the 25006 belt.
const standbyRemedyHint = "point --source at the primary endpoint (a standby/replica remains fine for bulk migrate)"

// checkNotStandby refuses a CDC source that is in recovery — a hot
// standby / read replica — with the coded SLUICE-E-CDC-STANDBY-SOURCE
// refusal. Runs alongside [checkWALLevel] at every CDC start (stream,
// snapshot+CDC handoff, backup CDC anchor) plus at the streamer
// cold-start's publication ensure, the first source write.
//
// Why refuse rather than degrade: publications cannot be created or
// altered on a standby at all, and while a PG 16+ standby CAN host a
// logical slot (live-proven on a Supabase PG 17 replica), creation
// blocks on the primary's next xl_running_xacts record and managed
// platforms gate the nudge (pg_log_standby_snapshot() is often
// superuser-retained) — so the primary is the supported CDC source.
// The refusal says so, and says what a replica IS still good for
// (bulk migrate: the parallel snapshot-pinned copy works unreduced on
// PG 16+ standbys — see snapshot_exporter.go).
func checkNotStandby(ctx context.Context, db *sql.DB) error {
	var inRecovery bool
	if err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return fmt.Errorf("postgres: read pg_is_in_recovery: %w", err)
	}
	if !inRecovery {
		return nil
	}
	return sluicecode.Wrap(sluicecode.CodeCDCStandbySource, standbyRemedyHint,
		errors.New("postgres: cdc: the source is a read-only hot standby / read replica (pg_is_in_recovery() = true) — "+
			"point --source at the PRIMARY endpoint (e.g. Supabase: db.<ref>.supabase.co, not the -rr- replica host). "+
			"CDC manages a publication on the source and CREATE/ALTER PUBLICATION cannot run on a standby; PG 16+ "+
			"standbys can technically host logical slots, but slot creation waits on the primary's next running-xacts "+
			"record and managed platforms gate the nudge, so the primary is the supported CDC source. "+
			"A standby/replica remains a fine source for bulk `sluice migrate`"))
}

// classifyStandbyReadOnly is the belt behind [checkNotStandby]: when a
// source-side publication write fails with SQLSTATE 25006 ("cannot
// execute ... in a read-only transaction" — the standby signature),
// the raw driver error gains the same coded standby steer instead of
// surfacing bare. Fires only if the preflight somehow read false —
// e.g. the source was promoted-then-demoted, or a load balancer routed
// the write to a different host than the preflight's connection. Any
// other error (and nil) passes through unchanged.
func classifyStandbyReadOnly(err error) error {
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "25006" {
		return err
	}
	return sluicecode.Wrap(sluicecode.CodeCDCStandbySource, standbyRemedyHint,
		fmt.Errorf("postgres: cdc: the source refused a publication write in a read-only transaction (SQLSTATE 25006) — "+
			"it is a hot standby / read replica; point --source at the PRIMARY endpoint "+
			"(a standby/replica remains a fine source for bulk `sluice migrate`): %w", err))
}
