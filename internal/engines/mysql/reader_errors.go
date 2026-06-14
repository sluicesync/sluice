// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// # Source-reader error classification (GitHub issue #19)
//
// The CDC reader pumps in this engine call [setErr] when their
// underlying transport (binlog client, VStream gRPC, etc.) returns a
// non-cancellation error. The streamer probes the reader for an
// Err() method after the changes channel closes; v0.45.x and earlier
// surfaced that error as plain text and returned nil from the
// applier (since the channel-close path is the normal EOF signal).
// The pipeline retry loop (ADR-0038) never saw a retriable shape —
// so a transient `read tcp ... read: connection reset by peer`
// from the source mid-stream produced a clean exit, not a retry.
//
// v0.46.0 closes the gap by classifying source-reader errors with
// the same [ir.RetriableError] mechanism the applier already uses.
// The transient shapes overlap perfectly — connection resets, EOF,
// bad-connection pool returns, vttablet rpc Aborted /
// Unavailable / ResourceExhausted — so this file is a thin
// delegating wrapper over [classifyApplierError]. Keeping the
// reader-side entry point named distinctly makes the source vs.
// target retry surfaces self-documenting at the call sites.

// classifyReaderError wraps a source-side reader error in
// [ir.RetriableError] when err matches one of the documented
// transient shapes. Returns err unchanged otherwise. nil in → nil
// out.
//
// The reader classifier is a SUPERSET of [classifyApplierError]: it
// first honors native gRPC status codes (a reader-only shape — the
// SQL applier never sees a VStream stream error), then delegates the
// remaining SQL / driver / text shapes to the applier classifier
// (server-restart 1105 / vttablet transients, driver-level bad-conn /
// EOF, network-error text), which are identical on both surfaces.
//
// # The gRPC-status gap (operator report: `Unavailable: connector reset by peer`)
//
// A VStream cold-start or CDC stream drop surfaces from
// Vitess_VStreamClient.Recv as a NATIVE gRPC status error, not a
// MySQL 1105 wrapper. [classifyApplierError] only recognizes Vitess
// transients when they arrive as a `1105 (HY000)` payload or match a
// handful of raw-text shapes (`connection reset by peer`, …). The
// gRPC desc wording varies across the transport stack — "transport is
// closing", "connection reset by peer", "connector reset by peer",
// "error reading from server: EOF", "the connection is draining" —
// so text matching is fragile and let a real transient fall through
// as TERMINAL, failing a large-table cold-start instead of retrying.
//
// Honoring the structured code is robust regardless of wording:
// Unavailable (transport reset / draining / not-serving), Aborted
// (tx-killer / failover), Unknown (vttablet internal transients), and
// ResourceExhausted (throttler) are exactly the transient set
// ADR-0038 already retries on the 1105 path. All other codes
// (InvalidArgument, NotFound, FailedPrecondition, PermissionDenied,
// …) stay terminal — the operator's request is wrong, and retrying
// would mask it.
func classifyReaderError(err error) error {
	if err == nil {
		return nil
	}
	// Source-side schema-resolution gap (Bug F9). The vstreamer
	// resolves each row event against the table schema for the replay
	// position; right after a DDL cutover (or when the Vitess schema
	// historian is off — `track_schema_versions` is disabled by
	// default on PlanetScale), that lookup can transiently miss with
	// `unknown table <t> in schema` / `no schema found for table`.
	// This arrives as a plain VStream error string (NOT a gRPC status,
	// NOT a 1105 wrapper), so neither the gRPC-code check below nor the
	// applier classifier catches it — it fell through TERMINAL and
	// killed the stream on a window that clears itself once the
	// historian catches up. Classify it retriable with an actionable
	// message so the ADR-0038 backoff rides out the cutover window in
	// process. sluice's own ADR-0049 CDC schema history covers the
	// decode/apply side; this is purely the source-reader error shape.
	if isVStreamSchemaResolutionError(err) {
		return &retriableMySQLError{err: fmt.Errorf(
			"source vstream could not resolve a table's schema for the replay position (likely a DDL cutover, or the Vitess schema historian / track_schema_versions is off) — retrying from the last position; if it persists, resume from current (cold-start) to skip the unresolvable window: %w", err,
		)}
	}
	// status.FromError reports ok=true only when err is — or wraps
	// (errors.As, so the pump's `recv: %w` wrap resolves) — a real
	// gRPC status. A non-gRPC error yields ok=false and falls through
	// to the SQL classifier; it is NOT misread as codes.Unknown.
	if st, ok := status.FromError(err); ok && isRetriableGRPCCode(st.Code()) {
		return &retriableMySQLError{err: err}
	}
	return classifyApplierError(err)
}

// isVStreamSchemaResolutionError reports whether err is the source
// vstreamer's "can't resolve this table's schema at the replay
// position" shape. These surface as free-text errors from the VStream
// pump (no gRPC status, no MySQL 1105 code), so the match is
// substring-based on the two known wordings:
//
//	"unknown table <name> in schema"  — historian has no row for the
//	                                    position's schema version yet
//	"no schema found for table <name>" — schema engine reload race
//
// Kept as a named helper (not inlined) so [TestClassifyReaderError_SchemaResolution]
// pins the exact wording set — a vstreamer wording change then fails
// the pin rather than silently reverting to TERMINAL.
func isVStreamSchemaResolutionError(err error) bool {
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "unknown table") && strings.Contains(msg, "in schema")) ||
		strings.Contains(msg, "no schema found for table")
}

// isRetriableGRPCCode reports whether a gRPC status code is one of the
// ADR-0038 transient set. Kept as a named function (not an inline
// switch) so [TestClassifyReaderError_GRPCStatusCodes] pins the exact
// code set — widening the retry surface then fails the pin rather than
// slipping in silently.
func isRetriableGRPCCode(c codes.Code) bool {
	switch c {
	case codes.Unavailable, // transport reset / draining / tablet not serving
		codes.Aborted,           // tx-killer rollback / primary failover
		codes.Unknown,           // vttablet internal transients (matches the 1105 set)
		codes.ResourceExhausted: // throttler engaged / pool full
		return true
	default:
		return false
	}
}
