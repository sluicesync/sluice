// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPostgresSnapshotRowsDoNotDisplayRoundFloats pins the premise
// behind one line of the stopped-cold-start resume's scope statement
// (internal/pipeline/streamer_coldstart_resume.go).
//
// The A0909-STOP-1 filing named the post-copy FLOAT exact re-read as a
// phase the resume must finish, on the reasoning that "the copy path's
// floats are display-rounded, so skipping it is silent value loss".
// That is true of the VStream COPY reader and the mydumper reader, and
// it is NOT true here: the repair phase is gated on the snapshot reader
// implementing [ir.LossyFloatCopyReader], which this engine's reader
// does not, so on a PostgreSQL source the phase is inert and the resume
// has nothing to finish.
//
// That is an argument from a capability, so the capability gets a
// check. If this engine's reader ever DOES start display-rounding
// floats, the resume's ladder acquires a phase it does not run, and
// this test is what says so — rather than the claim quietly becoming
// false in a comment nobody re-reads.
func TestPostgresSnapshotRowsDoNotDisplayRoundFloats(t *testing.T) {
	var reader any = (*RowReader)(nil)
	if _, ok := reader.(ir.LossyFloatCopyReader); ok {
		t.Fatal("the postgres RowReader now implements ir.LossyFloatCopyReader, so a PostgreSQL cold start's " +
			"copy display-rounds single-precision floats and the post-copy exact re-read is live. The " +
			"stopped-cold-start resume (internal/pipeline/streamer_coldstart_resume.go) states that phase is " +
			"inert on this source and does not run it — that statement is now FALSE, and a resumed cold start " +
			"would leave rounded floats on the target. Add the re-read to the resume ladder before relaxing this.")
	}
}
