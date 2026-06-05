// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// defaultVStreamLivenessWindow is the wall-clock window a VStream pump
// waits for the first NON-HEARTBEAT VEvent — a VGTID / FIELD / ROW / DDL /
// COPY_COMPLETED / JOURNAL, i.e. any event that proves a tablet is
// actually SERVING the keyspace — after the stream opens, before it
// surfaces a LOUD reader error instead of hanging.
//
// This is the loud-failure-tenet fix for the primary-only-cluster wedge
// (ADR-0073 (b2)): sluice's CDC tail requests a REPLICA tablet by
// default, and against a primary-only Vitess cluster (PlanetScale
// development branches, minimal self-hosted setups) vtgate has no REPLICA
// tablet to serve the VStream. vtgate logs `failed to find a REPLICA
// tablet for VStream in <ks>/<shard>` — but, crucially, it KEEPS SENDING
// HEARTBEATS every ~5s (the request's HeartbeatInterval=5) while never
// emitting a single data/position event. So the pump's Recv keeps
// returning, but only ever HEARTBEAT events; no rows, no VGTID, no
// progress — Err() stays nil forever: a SILENT hang. This watchdog
// converts that into a loud, actionable error.
//
// PHASE-A GROUND TRUTH (primary-only `vitesscluster` harness, Vitess 24):
//   - DEAD stream (no REPLICA tablet): Recv returns `[HEARTBEAT]` every
//     ~5s, NOTHING ELSE, indefinitely.
//   - HEALTHY stream: the VERY FIRST Recv returns `[VGTID OTHER]`
//     immediately on attach (vtgate stamps the starting position before
//     any row and before the first heartbeat), even when the source is
//     completely idle.
//
// That is the clean discriminator: a HEARTBEAT does NOT prove a serving
// tablet exists (vtgate sends it regardless), but ANY non-heartbeat event
// does. So the watchdog keys on the absence of any NON-HEARTBEAT event —
// not the absence of all events. A healthy idle source disarms it within
// the first second (its initial VGTID), so legitimate long-idle workloads
// never false-time-out; only the no-tablet wedge (heartbeats-only) trips
// it. 30s ≈ 6× the 5s heartbeat: generous enough to ride out a slow
// vtgate cold-start, short enough to fail loudly rather than wedge a sync.
//
// Overridable per-DSN via vstream_liveness_timeout (a Go duration string);
// 0 or negative disables the watchdog entirely (an explicit opt-out for
// pathological setups).
const defaultVStreamLivenessWindow = 30 * time.Second

// eventsProveLiveness reports whether a Recv batch contains at least one
// VEvent that proves a tablet is actually SERVING the stream — i.e. any
// event that is NOT a bare heartbeat. vtgate emits HEARTBEAT events on the
// configured cadence even when it has no serving tablet for the keyspace
// (the primary-only wedge; ADR-0073 (b2)), so a heartbeat alone must NOT
// disarm the liveness watchdog. Every other event type (VGTID, FIELD,
// ROW, DDL, BEGIN/COMMIT, COPY_COMPLETED, JOURNAL, …) only flows when a
// tablet is serving, so any one of them is liveness proof. An empty batch
// proves nothing.
func eventsProveLiveness(evs []*binlogdata.VEvent) bool {
	for _, ev := range evs {
		if ev.GetType() != binlogdata.VEventType_HEARTBEAT {
			return true
		}
	}
	return false
}

// vstreamLiveness is the resettable first-event watchdog shared by the
// three VStream pumps (the standalone CDC tail, the snapshot COPY pump,
// and the snapshot post-COPY CDC pump). It runs a single timer goroutine:
//
//   - The pump calls [observe] on every successful Recv (before
//     dispatch). The FIRST observe stops the timer permanently — once any
//     event has flowed, the stream has proven a serving tablet exists, so
//     the no-tablet wedge cannot be the failure mode anymore (later
//     stalls are caught by Recv's own error path / context cancellation).
//   - If the window elapses before the first observe, [onTimeout] fires
//     with a loud, actionable error and the pump is unblocked (the caller
//     cancels the stream so the parked Recv returns).
//
// Keying on the FIRST event only (rather than resetting on every event)
// is deliberate and sufficient: the wedge is "no serving tablet ⇒ no
// events EVER", so proving liveness once is proving it for the no-tablet
// class. It also means a healthy stream pays the watchdog cost for at
// most one window and never interacts with the steady-state hot path.
type vstreamLiveness struct {
	window  time.Duration
	timer   *time.Timer
	stopped chan struct{}
}

// startVStreamLiveness arms the first-event watchdog. onTimeout runs in
// the watchdog goroutine if window elapses before the first [observe];
// it should record a loud error and cancel the stream so the pump's
// parked Recv unblocks. A window <= 0 disables the watchdog (returns a
// no-op whose observe/stop are safe to call).
func startVStreamLiveness(ctx context.Context, window time.Duration, onTimeout func()) *vstreamLiveness {
	l := &vstreamLiveness{window: window, stopped: make(chan struct{})}
	if window <= 0 {
		// Disabled: a closed stopped channel makes observe/stop no-ops and
		// no goroutine is spawned.
		close(l.stopped)
		return l
	}
	l.timer = time.NewTimer(window)
	go func() {
		defer l.timer.Stop()
		select {
		case <-l.stopped:
			return
		case <-ctx.Done():
			return
		case <-l.timer.C:
			onTimeout()
		}
	}()
	return l
}

// observe records that an event has flowed. The first call disarms the
// watchdog permanently; subsequent calls are cheap no-ops. Safe to call
// from the pump goroutine on every Recv.
func (l *vstreamLiveness) observe() {
	select {
	case <-l.stopped:
		// Already disarmed (first event seen, or watchdog disabled).
	default:
		close(l.stopped)
	}
}

// stop disarms the watchdog without recording an event — used on pump
// teardown so the goroutine exits even if no event ever arrived (the
// onTimeout path already fired, or the pump is shutting down). Idempotent.
func (l *vstreamLiveness) stop() { l.observe() }

// vstreamLivenessTimeoutError builds the loud, actionable error the
// watchdog records when no event flows within the window. It names the
// tablet type, the keyspace, and the remediation (vstream_tablet_type)
// so an operator hitting the primary-only wedge gets a one-line fix
// rather than a silent hang. Terminal by construction (not wrapped as an
// ir.RetriableError): a missing tablet does not heal on retry.
func vstreamLivenessTimeoutError(window time.Duration, tabletType topodata.TabletType, keyspace string, shards []string) error {
	return fmt.Errorf(
		"mysql/vstream: no events within %s of opening the stream; vtgate may have no %s tablet for keyspace %q shards %v "+
			"(if the cluster is primary-only — e.g. a PlanetScale dev branch or a minimal self-hosted Vitess — set vstream_tablet_type=primary in the DSN)",
		window, tabletType, keyspace, shards,
	)
}

// vstreamLivenessWindowFromDSN reads the optional vstream_liveness_timeout
// DSN parameter (a Go duration string, e.g. "45s"). Absent ⇒ the default
// window. A 0/negative duration disables the watchdog. A malformed value
// is a loud error rather than a silent fallback (the loud-failure tenet:
// an operator who set the knob deserves to know it didn't parse).
func vstreamLivenessWindowFromDSN(cfg *gomysql.Config) (time.Duration, error) {
	v := cfg.Params["vstream_liveness_timeout"]
	if v == "" {
		return defaultVStreamLivenessWindow, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("mysql/vstream: invalid vstream_liveness_timeout %q (want a Go duration like 45s, or 0 to disable): %w", v, err)
	}
	return d, nil
}

// vstreamTabletTypeFromDSN reads the optional vstream_tablet_type DSN
// parameter and maps it to the proto tablet type for the pure-CDC-tail
// VStream request. Valid values: primary | replica | rdonly (default
// replica — unchanged for PlanetScale production, which reads from
// replicas). An unrecognized value is a LOUD error (the loud-failure
// tenet) rather than a silent fallback to the default.
//
// This is the usability half of the primary-only fix: a primary-only
// cluster works via vstream_tablet_type=primary. The COPY-resume PRIMARY
// override in [buildVStreamRequest] still wins when a cursor is present —
// this only selects the tablet for pure CDC tailing.
func vstreamTabletTypeFromDSN(cfg *gomysql.Config) (topodata.TabletType, error) {
	switch cfg.Params["vstream_tablet_type"] {
	case "", "replica":
		return topodata.TabletType_REPLICA, nil
	case "primary":
		return topodata.TabletType_PRIMARY, nil
	case "rdonly":
		return topodata.TabletType_RDONLY, nil
	default:
		return topodata.TabletType_UNKNOWN, fmt.Errorf(
			"mysql/vstream: unknown vstream_tablet_type %q (want primary, replica, or rdonly)",
			cfg.Params["vstream_tablet_type"],
		)
	}
}
