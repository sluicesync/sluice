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
func classifyReaderError(err error) error {
	return classifyApplierError(err)
}
