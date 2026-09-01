// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import "time"

// openProbeTimeout bounds every open-path probe this package runs before
// a stream (or setup) may proceed — the change-log existence check, the
// gap-free sequence grade, the capture-shape door, and the two
// capture-blindness WARNs (audit 2026-08-27 A5). 15s matches the sibling
// cap on the Postgres slot-create probe (preparedXactProbeTimeout); the
// MySQL binlog preflights use 30s for the same shape.
//
// Why the cap exists: warnSluiceRelayShape reads a count(*) off the
// USER-visible sluice_cdc_state table, and in Postgres a merely-QUEUED
// ACCESS EXCLUSIVE request (an operator's ALTER TABLE/VACUUM FULL, or
// wraparound autovacuum, parked behind the upstream sync's long apply
// tx) blocks every new ACCESS SHARE acquirer behind it — so an unbounded
// probe parks in the lock queue and a WARN-only detector wedges the very
// open it exists to make legible. The catalog-only probes cannot be
// wedged by user-table locks, but an unbounded read on a half-dead
// pooled connection hangs the open just the same, so all of them carry
// the cap (enforced by internal/engines' probe-timeout roster gate).
//
// Instrument choice — context.WithTimeout, not SET LOCAL
// statement_timeout: pgx (under database/sql) watches the context and on
// expiry delivers an out-of-band PG CancelRequest, which interrupts a
// lock-queue wait exactly as statement_timeout would (both abort the
// parked statement server-side); if cancel delivery itself fails, pgconn
// force-closes the connection, so the ctx ALSO bounds transport-level
// stalls that a server-side statement_timeout can never see. A SET LOCAL
// would additionally need a transaction plus its own unbounded
// round-trip to install, and a session-level SET would leak onto the
// pooled connection the poll loop reuses. Deriving the deadline at the
// top of each probe function matches the MySQL/Postgres siblings and is
// what the roster gate asserts.
//
// # What the caps COMPOSE to, because each one only bounds itself
//
// openCDCReader runs its probes in SEQUENCE, so the bound an operator
// actually experiences is count x cap, not cap. Seven probes derive this
// deadline today — change-log existence, gap-free sequence grade,
// capture-posture read, capture-shape door, replica-role shape dispatch,
// the DDL-detection WARN, the SEC-1 insecure-definer WARN — so the
// **worst case is 105 seconds of added latency per CDC open**, paid on
// cold start and on every warm-resume reopen. Two of those seven can
// only WARN unconditionally (warnDDLDetectionAbsent,
// warnInsecureCaptureFunctions) and a third is WARN-only in the default
// posture (checkReplicaRoleCaptureShapes refuses only under
// --capture-replicated-writes), so 30–45s of the bound is spent by
// detectors that cannot stop anything — including, inside that tier, the
// one probe that reads a lockable USER table and can therefore genuinely
// park. While an operator's stuck ALTER TABLE sits in the lock queue,
// every reopen re-pays that to re-emit a warning it already emitted.
//
// This is DOCUMENTED rather than optimized, and the three alternatives
// are recorded so the next reader does not re-derive them (audit
// 2026-08-31 C-6, filed Low):
//
//   - Parallelizing the WARN-only tier buys time only in the pathological
//     case — one where the operator's real problem is the lock, not the
//     15s — and pays for it with concurrent probe output and an errgroup
//     on an open path whose current failure mode is a plain returned
//     error.
//   - Shortening the WARN tier's cap to ~5s would clip the one probe that
//     legitimately can be slow (warnSluiceRelayShape's count(*) on a
//     large control table under load), converting "the relay shape
//     exists" into "the probe failed" — degrading the signal to buy
//     latency on a path that is already bounded.
//   - Skipping the WARN probes on REOPEN is the tempting one and the
//     wrong one: their answers can change mid-stream (a subscription
//     created, a relay sync pointed at this database), so it would be a
//     door that stops reaching a path it used to cover.
//
// It stays Low because the composition is BOUNDED, not runaway, and the
// two consumers bound it differently — stated rather than generalized
// from one of them: on the backup-stream path a reopen failure is
// terminal (internal/pipeline/stream.go returns migcore.PhaseConnect from
// the transient-reopen arm, verified), so a wedged source stops the
// stream. On the sync path the ADR-0038 retry is a budget, not a loop —
// Streamer.ApplyRetryAttempts, CLI default 8 — so the worst case
// multiplies by that budget rather than repeating forever. Neither is
// unbounded; neither is free.
//
// The number is not left as prose: internal/engines'
// TestOpenPathProbesDeriveBoundedContexts derives the probe count itself,
// reads this constant out of the source, and fails if the product exceeds
// the ceiling recorded in its roster — so a seventh-plus serial probe, or
// a raised cap, has to re-justify the bound here.
const openProbeTimeout = 15 * time.Second
