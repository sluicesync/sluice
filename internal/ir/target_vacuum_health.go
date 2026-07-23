// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// VacuumHealth is a point-in-time view of a Postgres TARGET's table-
// maintenance pressure: how far autovacuum is trailing the write load
// (dead tuples) and how much transaction-ID wraparound headroom the
// database has left. A sluice bulk copy or a long CDC catch-up IS a
// sustained high-write workload — exactly the shape where dead tuples
// outrun vacuum — so the threshold alerter surfaces these as ADVISORY
// signals (roadmap 2026-07-22, the ADR-0107 item-36 rule-family
// extension). Advisory only: sluice's writes stay correct regardless;
// this exists to tell the operator their target is accumulating bloat
// before it becomes a stall.
//
// The struct lives in IR because the optional reporter interface needs
// a return type the orchestrator can threshold-test without importing
// engine packages (the [SlotHealth] precedent). Postgres is the only
// engine that produces these values today; MySQL/InnoDB purge lag has a
// different shape (history-list length) and is a separate task if ever
// demanded.
type VacuumHealth struct {
	// WorstTable is the schema-qualified user table with the highest
	// dead-tuple ratio among tables above the probe's noise floor
	// (small tables are excluded so a 10-row scratch table cannot
	// page). Empty means no user table cleared the floor — a genuinely
	// healthy reading (DeadTupleRatio 0), NOT "unobserved": the rule
	// must still re-arm on it after a recovery.
	WorstTable string

	// DeadTuples / LiveTuples are pg_stat_user_tables.n_dead_tup /
	// n_live_tup for WorstTable; DeadTupleRatio is
	// dead / (dead + live) in [0, 1]. All zero when WorstTable is "".
	DeadTuples     int64
	LiveTuples     int64
	DeadTupleRatio float64

	// LastAutovacuum is when autovacuum last completed on WorstTable
	// (zero time ⇒ never since stats reset) and AutovacuumCount its
	// completion count — carried for the alert body so the operator can
	// distinguish "autovacuum is running but losing" from "autovacuum
	// never reached this table".
	LastAutovacuum  time.Time
	AutovacuumCount int64

	// XIDAge is age(datfrozenxid) for the connected database — the
	// transaction-ID wraparound headroom consumed so far. Postgres
	// forces a shutdown as the age approaches ~2.1B; autovacuum's
	// freeze cycles normally keep it near autovacuum_freeze_max_age
	// (default 200M). Datname names the database for the alert body.
	XIDAge  int64
	Datname string
}

// TargetVacuumHealthReporter is the optional TARGET-side surface exposing
// autovacuum / dead-tuple / wraparound pressure for the threshold alerter's
// vacuum rule family. The Postgres [ChangeApplier] implements it over the
// pool it already holds; consumers type-assert from the applier and treat a
// missing implementation as "rules inert on this target" (WARN once, never
// an error — the alerter is advisory).
//
// The boolean return mirrors [SlotSpillReporter]: ok=false means the probe
// could not produce a usable reading this tick (e.g. the stats view is
// unavailable to the connecting role) and the caller skips the tick —
// no fire, no re-arm — per the *Known honesty contract. A healthy target
// with zero dead tuples is ok=true with a zero-value reading, which is a
// real observation and DOES re-arm a fired rule.
type TargetVacuumHealthReporter interface {
	TargetVacuumHealth(ctx context.Context) (health VacuumHealth, ok bool, err error)
}
