// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// # Source-reader error classification (GitHub issue #19)
//
// PG-side mirror of the MySQL [classifyReaderError]. The CDC reader's
// pump calls [setErr] on transient SQLSTATE / network shapes; the
// streamer probes for the reader's Err() after the changes channel
// closes and surfaces it back into the ADR-0038 retry loop. Without
// classification, the loop never sees a [ir.RetriableError] shape
// and exits clean on what was actually a transient.
//
// See [classifyApplierError] for the shared shape table — reader-
// side transients overlap entirely with applier-side transients on
// PG (40001 / 40P01 / 57P0x / 08* / driver.ErrBadConn / network
// text).

// classifyReaderError wraps a source-side reader error in
// [ir.RetriableError] when err matches one of the documented
// transient shapes. Returns err unchanged otherwise. nil in → nil
// out. Delegates to [classifyApplierError].
//
// # It delegates to the APPLIER classifier, not the copy one, deliberately
//
// Its five call sites are all in the CDC pump's walreceiver loop
// ([cdc_reader.go]: standby status update, receive, server error, parse
// xlogdata, and the decode path) — so this is a CDC classifier, and CDC keeps
// the retriable verdict for schema drift.
//
// This line was briefly [classifyCopyError] during the Bug-285 fix, which made
// schema drift terminal here as a side effect nobody chose: the commit that
// did it is titled "schema drift is retriable for CDC and TERMINAL for a copy"
// and this is CDC, so the change contradicted its own stated scope. Reverted
// 2026-09-13 and pinned by TestCDCReaderKeepsTheRetriableSchemaDriftVerdict,
// because the whole point of splitting the two classifiers was that the split
// is a decision, and a decision that can be made by an unnoticed one-word edit
// is not being made.
//
// Whether the CDC READER *should* eventually take the terminal verdict is a
// real question — its errors arrive from the SOURCE, where "an operator is
// about to create the missing relation" is a much weaker premise than it is on
// the target — but that is a behaviour change owed a measurement and a
// deliberate commit, not a patch-release side effect.
func classifyReaderError(err error) error {
	return classifyApplierError(err)
}
