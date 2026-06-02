// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// StaleBackend describes one sluice-labelled server-side connection the
// stale-backend detector found orphaned on the target (connection-
// resilience Phase 2, item 2). It is the cross-engine report shape the
// orchestrator acts on; the catalog query that produces it is target-
// engine knowledge living in the engine package (today: Postgres against
// pg_stat_activity / pg_locks).
//
// "Orphaned" means the backend is sluice's own (its application_name
// carries the `sluice/` prefix), owned by the connecting role, is not
// the current session, and shows an orphan signal: it is idle in a
// transaction, or it holds a lock on a relation in a schema sluice is
// about to write. The classic instance is a SIGKILL'd run whose server-
// side `COPY <table> FROM STDIN` backend kept running, still holding the
// target table's lock and consuming a connection slot — blocking the
// next run's cold-start DROP/CREATE.
type StaleBackend struct {
	// PID is the server-side backend process id (pg_stat_activity.pid).
	PID int

	// ApplicationName is the backend's application_name verbatim — the
	// `sluice/<role>/<id>` label the detector matched on. Carried so the
	// operator-facing report can show which sluice subsystem (snapshot /
	// applier / cdc-reader / …) the orphan belongs to.
	ApplicationName string

	// State is the backend's pg_stat_activity.state ("idle in
	// transaction", "active", …). Empty when the catalog reported NULL.
	State string

	// Age is now() - state_change at probe time: how long the backend has
	// sat in its current state. A large Age on an "idle in transaction"
	// backend is the strong orphan signal.
	Age time.Duration

	// LockRelation is the qualified relation (schema.table) the backend
	// holds a lock on within a sluice-written schema, or "" when the
	// orphan signal was idle-in-transaction rather than a held lock.
	LockRelation string

	// LockMode is the held lock's mode (e.g. "AccessExclusiveLock") when
	// LockRelation is set; "" otherwise.
	LockMode string
}

// StaleBackendReport is the detector's verdict for one target preflight.
// Backends is the list of orphans found (empty when none). Reaped is set
// only on the opt-in path, listing the PIDs actually terminated; on the
// report-only (default) path it is nil even when Backends is non-empty.
//
// ProbeFailed mirrors the connection-budget prober's degrade contract: a
// catalog quirk or a permission gap on pg_stat_activity / pg_locks sets
// ProbeFailed with a populated Warning and the orchestrator proceeds
// (the detector must never be the thing that breaks an otherwise-working
// migration). Only a connection-open failure surfaces as a non-nil error.
type StaleBackendReport struct {
	Backends []StaleBackend
	Reaped   []int

	ProbeFailed bool
	Warning     string
}

// TargetStaleBackendReaper is implemented by target engines that can
// detect (and, on opt-in, terminate) sluice's own orphaned backends on a
// target (today: Postgres, via pg_stat_activity + pg_locks). The
// orchestrator discovers it structurally with a type assertion in the
// cold-start preflight, alongside the connection-budget prober; engines
// without a backend model (today: MySQL) omit it and the step is a clean
// no-op for them.
//
// schemas is the set of schema namespaces sluice is about to write into
// (the source-schema target tables plus the control/state schema). A
// backend is reported orphaned if it holds a lock on any relation in one
// of these schemas — the lock that would block sluice's own DROP/CREATE.
//
// reap selects the action: false (default) detects and reports only,
// never terminating; true additionally calls pg_terminate_backend on
// each detected orphan. Termination is always scoped to sluice's own,
// own-role, non-self backends — a non-superuser may terminate its own
// backends with no extra grant, and the scoping never reaches another
// role's or a non-sluice session.
type TargetStaleBackendReaper interface {
	DetectStaleBackends(ctx context.Context, dsn string, schemas []string, reap bool) (StaleBackendReport, error)
}
