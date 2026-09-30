// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import "time"

// ControlTimestampInFutureMarker is the grep-stable marker every consumer
// that ages a stream's control-table timestamp prints when that timestamp
// is later than the reader's clock by more than
// [ControlTimestampSkewTolerance] (GC-40 (c)).
//
// A future-dated row is not a fresh one. Before v0.156.5 a Postgres target
// whose database time zone is east of UTC stored its session zone's
// wall-clock digits in the naive updated_at column (GC-39 item 2), so a row
// such a binary wrote reads hours in the future — measured −32,395 s on
// Asia/Tokyo. The first position write by a current binary corrects the
// row, but a stream stalled since before the upgrade never writes one, and
// every reader that treated the negative age as "just applied" reported
// that stalled stream healthy: the stall alarm failing open.
const ControlTimestampInFutureMarker = "CONTROL-TIMESTAMP-IN-FUTURE"

// ControlTimestampSkewTolerance is how far in the future a control-table
// timestamp may read and still be aged normally.
//
// The value separates the two populations a negative age comes from. Clock
// skew between the host running the reader and the target's server is
// milliseconds under NTP and seconds to tens of seconds on an unsynced VM,
// so it stays inside 60 s. The pre-v0.156.5 wall-clock write is off by the
// database zone's whole UTC offset, and every zone east of UTC is at least
// an hour ahead, so it lands at least 60× past the bound. A row inside the
// tolerance is the reader's clock trailing the server's, not a stale row.
const ControlTimestampSkewTolerance = 60 * time.Second

// ControlTimestampInFutureRemedy is the cause-and-remedy text that goes with
// [ControlTimestampInFutureMarker] on every surface that prints it.
const ControlTimestampInFutureRemedy = "the stream's control-table timestamp is later than this host's clock, so how long ago it last applied cannot be read from it: " +
	"either a sluice older than v0.156.5 last wrote the row on a Postgres target whose database time zone is east of UTC (those binaries stored local wall-clock digits), " +
	"or the target's clock and this host's differ by more than 60s. " +
	"The stream's next position write corrects the row; a stream that is stalled or stopped never writes one, " +
	"so restart it (`sluice sync start` with its usual flags) and check again, and check NTP on both hosts"

// ControlTimestampAge ages a control-table timestamp against now. readable is
// false when at is further in the future than [ControlTimestampSkewTolerance]:
// the age is then no evidence of freshness, and the caller must report it as
// unknown under [ControlTimestampInFutureMarker] rather than as healthy. The
// raw age is returned either way so it can be shown.
//
// Every reader that turns sluice_cdc_state.updated_at into a freshness
// verdict goes through here (sync health, sync status and the fleet view,
// sluice_seconds_since_last_apply, the live panel);
// TestControlTimestampAge_EveryAgeConsumerRoutesThroughIt holds them to it.
func ControlTimestampAge(now, at time.Time) (age time.Duration, readable bool) {
	age = now.Sub(at)
	return age, age >= -ControlTimestampSkewTolerance
}
