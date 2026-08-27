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
const openProbeTimeout = 15 * time.Second
