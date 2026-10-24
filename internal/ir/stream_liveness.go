// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"time"
)

// # Stream liveness (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED)
//
// A drained `sluice schema add-table` is exact only while the stream is not
// running: the stream's restart replays the add's window from its position.
// Nothing on the control row used to say whether a stream was running.
// stop_requested_at says only that a stop is pending, and updated_at moves
// only when the stream writes a position (on Postgres) or when anything
// changes the row (on a MySQL-family target, where it is ON UPDATE
// CURRENT_TIMESTAMP), so a running stream can read exactly like a stopped
// one.
//
// So the stream records whether it is running in its own column, written
// only by what applies changes under the stream id: every position write
// stamps it running in the same statement, every attempt stamps it running
// as it starts, and the stream stamps a stopped marker when it exits. A
// drained add passes only on that marker (or on no row at all). A stream
// that crashed never wrote the marker, and the add refuses until the stream
// has been started and stopped again: nothing distinguishes a crashed
// stream's record from an idle running one's, however old the stamp.
// Every stamp is the control-table server's clock: no host's clock is ever
// compared with another's.

// StreamLiveness is what a stream's control row says about whether the
// stream is running, read with the server's clock.
type StreamLiveness struct {
	// Found: the stream's control row exists.
	Found bool
	// Recorded: a release that records liveness has stamped this row. False
	// for a row only an older release has run (or whose control table lacks
	// the column).
	Recorded bool
	// Stopped: the stream's last stamp is the stopped marker — it stopped
	// and recorded that it did.
	Stopped bool
	// AliveAt is the last running stamp, when Recorded and not Stopped.
	AliveAt time.Time
	// Now is the control-table server's clock when the row was read.
	Now time.Time
}

// Age is how long ago, by the server's clock, the stream last stamped that it
// was running. Reported in refusals; it decides nothing, because an idle
// running stream's stamp ages exactly like a crashed one's.
func (l StreamLiveness) Age() time.Duration { return l.Now.Sub(l.AliveAt) }

// StreamLivenessStore is the optional [ChangeApplier] surface that records
// and reads a stream's liveness on its control row. Both shipping appliers
// (MySQL, Postgres) implement it.
type StreamLivenessStore interface {
	// StampStreamAlive stamps the stream running at the server's now,
	// overwriting a stopped marker, and reports whether its row took the
	// stamp. It may first add the column to a control table an older
	// release created. A missing row or control table is not an error, only
	// not stamped. Once a stamp succeeds, the applier's own position writes
	// stamp too.
	StampStreamAlive(ctx context.Context, streamID string) (stamped bool, err error)

	// RecordStreamStopped stamps the stopped marker.
	RecordStreamStopped(ctx context.Context, streamID string) error

	// ReadStreamLiveness reads the stream's liveness and the server's now.
	// A missing control table or row is Found false; a control table
	// without the column is Found true, Recorded false.
	ReadStreamLiveness(ctx context.Context, streamID string) (StreamLiveness, error)
}
