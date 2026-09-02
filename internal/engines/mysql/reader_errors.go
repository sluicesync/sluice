// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/nettransient"
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
	// ADR-0093: VStream purged-GTID resume. When the persisted resume
	// position is older than the source's retained binlogs (gtid_purged
	// advanced past it — routine on PlanetScale's ~3-day retention
	// window), vtgate rejects the position REACTIVELY on the stream and
	// the pump's Recv surfaces it here. The binlog source catches this
	// with a pre-flight gtid_purged ⊆ resume check that returns
	// [ir.ErrPositionInvalid]; vtgate exposes no single authoritative
	// gtid_purged to pre-flight against, so the reactive error is the
	// only reliable signal. Classify it as [ir.ErrPositionInvalid] so
	// the streamer routes it to a cold-start re-snapshot (ADR-0022
	// parity), at the reactive layer the VStream path needs.
	//
	// Checked FIRST — before the gRPC-code and applier-classifier
	// branches: a purged error can arrive carrying codes.Unknown (in the
	// ADR-0038 retriable set), and retrying the SAME purged position
	// spins forever. The position is invalid, not transient.
	// MariaDB domain-GTID purged-position resume (ADR-0170). MariaDB
	// exposes no SQL surface to pre-flight GTID reachability
	// (@@gtid_binlog_state is newest-per-domain, not a purged floor; there
	// is no GTID_SUBSET / @@gtid_purged), so — unlike the MySQL binlog path
	// which pre-checks proactively — the authoritative signal is the
	// stream's reactive error 1236 on the pump's first GetEvent. Classify
	// it as ir.ErrPositionInvalid so the streamer routes it to a cold-start
	// re-snapshot (ADR-0022 parity), exactly as the VStream purged path
	// does. Checked here (before the retriable classifier) so the invalid
	// position is not mistaken for a transient and retried forever against
	// the same purged GTID.
	if isMariaDBPurgedGTIDError(err) {
		return fmt.Errorf(
			"source mariadb cannot resume: the persisted domain-GTID position is older than the source's retained binlogs (required binlog files have been purged); a fresh cold-start re-snapshot is required: %w (%w)", ir.ErrPositionInvalid, err,
		)
	}
	// The sibling 1236: a domain-GTID the source has NEVER had — a fresh,
	// reset or rebuilt instance, or a position captured on a different
	// server (audit 2026-09-01 SLM-2's MariaDB arm, measured live). Same
	// route: the position is not resumable here, so cold-start.
	if isMariaDBForeignGTIDError(err) {
		return fmt.Errorf(
			"source mariadb cannot resume: the persisted domain-GTID position was never executed on this source (a fresh, reset, or replaced instance, or a position from a different server); a fresh cold-start re-snapshot is required: %w (%w)", ir.ErrPositionInvalid, err,
		)
	}
	// Oversized gRPC message. Checked BEFORE the retriable classifier for the
	// same reason the purged-position branches are: the text carries
	// `code = ResourceExhausted`, which vitessRetriableSubstrings matches as
	// a throttler/pool transient — and this one is DETERMINISTIC. The same
	// row is re-sent on every reconnect, so retrying spins the reconnect
	// budget (10 attempts) and then the copy's retry budget (8 more) against
	// an error that cannot clear, turning an actionable failure into a slow
	// one with a misleading trail.
	//
	// The message also reads like a server-side rejection and is not one:
	// nothing in MySQL or vtgate refused the row. It is the gRPC CLIENT
	// declining to receive a message above its ceiling, so the remedy is a
	// client setting, and the error now says so.
	if isVStreamMessageTooLargeError(err) {
		return fmt.Errorf(
			"source vstream: a row exceeded this client's gRPC receive ceiling, so the copy cannot proceed: "+
				"nothing on the server rejected the row — sluice's own client declined to receive a message "+
				"larger than its limit. VStream ships whole rows and cannot split one across messages, so a "+
				"single wide row (a large mediumtext/longtext or json column) trips this. Raise the ceiling "+
				"with the `vstream_max_recv_bytes=<bytes>` source-DSN parameter (default %d); if that does "+
				"not clear it, the row also exceeds vtgate's own --grpc_max_message_size and only the server "+
				"side can raise it. This is NOT retriable — the same row is re-sent on every reconnect: %w",
			defaultVStreamMaxRecvBytes, err,
		)
	}
	if isVStreamPurgedGTIDError(err) {
		// Both the original error AND ir.ErrPositionInvalid are wrapped
		// (%w, Go 1.20 multi-error): the streamer routes on
		// errors.Is(ErrPositionInvalid), while the original vtgate text
		// stays reachable on the chain for diagnostics and for the
		// retriable-classifier identity checks.
		return fmt.Errorf(
			"source vstream cannot resume: the persisted GTID position is older than the source's retained binlogs (gtid_purged advanced past it — common on PlanetScale's binlog retention window); a fresh cold-start re-snapshot is required: %w (%w)", ir.ErrPositionInvalid, err,
		)
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
	// FIELD-cache miss after a DDL boundary (roadmap item 81, observed live
	// 2026-07-26). dispatchDDL invalidates the field cache; vtgate only
	// re-emits a FIELD event for the table whose shape actually changed, so a
	// ROW event on a table whose entry was dropped without a re-announcement
	// finds an empty cache and trips the loud floor in dispatchRow. That is
	// how a `CREATE TABLE` on one table killed a stream whose fatal error
	// named a DIFFERENT, long-established table.
	//
	// The invalidation is no longer a BLANKET clear — this comment said it was
	// for one release after it stopped being true (audit 2026-07-26 QUAL-4;
	// `cd369576` wrote the claim, `9fe4dd23` changed the mechanism).
	// invalidateFieldsForDDL now drops only the DDL's target table across
	// every shard, falling back to the blanket clear only when it cannot
	// attribute the statement. The carve-out below is still needed: the
	// fallback path still exists, a warm resume replays the DDL, and an
	// attributable ALTER legitimately drops the altered table's entry — so a
	// miss remains reachable and must stay retriable rather than terminal.
	//
	// Retriable, not terminal, and NOT a weakening of the floor: nothing here
	// decodes a row with guessed or stale metadata. A reconnect re-opens the
	// stream from the last position, where the VStream protocol emits a FIELD
	// event ahead of the first ROW event for every table in the filter — so
	// the reader re-learns the CURRENT shape and resumes correctly. If the
	// FIELD genuinely never arrives, the retry budget still exhausts loudly
	// rather than looping forever.
	if isVStreamMissingFieldEventError(err) {
		return &retriableMySQLError{err: fmt.Errorf(
			"source vstream sent a row event before the table's FIELD event — the field cache was invalidated by a DDL on the keyspace and this table's shape has not been re-announced yet; reconnecting from the last position to re-learn it: %w", err,
		)}
	}
	// vttablet's own lineage refusal (audit 2026-09-01 SLM-2, VStream arm,
	// measured on the cluster rig 2026-09-02): uvstreamer checks the resume
	// set ⊆ its gtid_executed and returns codes.InvalidArgument "GTIDSet
	// Mismatch" for a position from a different cluster/shard lineage. The
	// status switch below keeps InvalidArgument TERMINAL, so without this arm
	// a foreign position exited the stream with no cold-start route — and
	// when vtgate had fewer than three candidate tablets it did not surface
	// at all (vtgate marked the refusing tablet ignorable and blocked). The
	// pre-flight in verifyVStreamPositionReachable now refuses first; this is
	// the reactive twin for the shapes that reach the stream.
	if isVStreamGTIDSetMismatchError(err) {
		return fmt.Errorf(
			"source vstream refused the resume position: its GTID set is not contained in the shard's executed set (a fresh, reset, rebuilt or replaced keyspace/shard — a different lineage); a fresh cold-start re-snapshot is required: %w (%w)", ir.ErrPositionInvalid, err,
		)
	}
	// GRACEFUL HTTP/2 DRAIN (roadmap item 79, observed live 2026-07-24). The
	// server sent a GOAWAY carrying ErrCode=NO_ERROR — "this connection is
	// going away, reconnect" — which on PlanetScale accompanies routine
	// edge-pod rotation. grpc-go surfaces it as codes.InvalidArgument
	// ("protocol error: incomplete envelope: http2: server sent GOAWAY …"),
	// because the code describes the ENVELOPE-PARSE failure, not the
	// operator's request. The switch below deliberately keeps
	// InvalidArgument terminal (a genuinely malformed request must not be
	// retried), so without this arm a graceful drain killed the stream and an
	// unattended sync stayed down until a human restarted it.
	//
	// Checked BEFORE the status block so it catches the shape whether or not
	// it arrives wrapped in a gRPC status, and AFTER the purged-position arms
	// above so an invalid position is never mistaken for a retriable blip.
	// The conjunction (GOAWAY *and* NO_ERROR) lives in nettransient — a
	// GOAWAY carrying a real error code stays terminal there and here.
	if nettransient.IsGracefulGoAway(err) {
		return &retriableMySQLError{err: fmt.Errorf(
			"source vstream connection was gracefully drained by the server (HTTP/2 GOAWAY, ErrCode=NO_ERROR — routine on managed platforms that rotate edge pods); reconnecting from the last position: %w", err,
		)}
	}
	// status.FromError reports ok=true only when err is — or wraps
	// (errors.As, so the pump's `recv: %w` wrap resolves) — a real
	// gRPC status. A non-gRPC error yields ok=false and falls through
	// to the SQL classifier; it is NOT misread as codes.Unknown.
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Canceled, codes.DeadlineExceeded:
			// The source VStream being torn down on an operator `sync stop`
			// (or a Ctrl-C / outer-ctx cancel) surfaces from Recv as a gRPC
			// Canceled / DeadlineExceeded status — NOT a fault. Normalize it
			// to the standard context sentinel so the engine-neutral streamer's
			// errors.Is(context.Canceled) ctx-termination check recognizes it
			// as a clean stop. Without this the raw gRPC status is treated as
			// terminal: the stop-flag clear is skipped and `sync stop --wait`
			// reports a false drain timeout even though the stream did stop.
			// The original status stays on the chain (%w) for diagnostics.
			if st.Code() == codes.DeadlineExceeded {
				return fmt.Errorf("source vstream deadline exceeded: %w (%w)", context.DeadlineExceeded, err)
			}
			return fmt.Errorf("source vstream cancelled: %w (%w)", context.Canceled, err)
		}
		if isRetriableGRPCCode(st.Code()) {
			return &retriableMySQLError{err: err}
		}
		// Transport-level abnormal stream close arrives as codes.Internal, which
		// is deliberately NOT in the retriable code set (a genuine vtgate
		// Internal fault must stay terminal). See
		// [isGRPCAbnormalStreamCloseError] for why the narrow wording match is
		// the right discriminator here.
		if isGRPCAbnormalStreamCloseError(st) {
			return &retriableMySQLError{err: err}
		}
		// Post-SwitchTraffic primary-routable window (roadmap item 72(b),
		// observed live on the extended-suites reshard leg). When the Streamer
		// FOLLOWS a 1->2 reshard, reopenAfterReshard rebuilds the CDC tail
		// against the new layout and its FIRST Recv can race the documented
		// window in which the resharded shard's PRIMARY is not yet routable —
		// vtgate answers with codes.NotFound and the wording `tablet uid:N is
		// either down or nonexistent`. The primaries become routable a beat
		// AFTER SwitchTraffic's RPC returns (see waitReshardPrimariesRoutable),
		// so this is transient: a reconnect from the persisted position lands
		// on the now-serving PRIMARY.
		//
		// codes.NotFound stays TERMINAL wholesale (see isRetriableGRPCCode and
		// the switch above deliberately NOT listing it) — a genuinely bogus
		// keyspace/shard or a real missing table must fail loudly, not spin.
		// Only the SPECIFIC tablet-unroutable wording is carved out, so a
		// keyspace-not-found / table-not-found NotFound is unaffected. Retriable,
		// not terminal, and BOUNDED: if the tablet were permanently gone the
		// ADR-0038 reconnect budget still exhausts loudly rather than looping.
		if isReshardPrimaryUnroutableError(st) {
			return &retriableMySQLError{
				reshardPrimaryWindow: true,
				err: fmt.Errorf(
					"source vstream reopen after a reshard hit a tablet that is transiently unroutable "+
						"(a resharded shard's PRIMARY becomes routable a beat after SwitchTraffic returns — the "+
						"documented post-SwitchTraffic window); reconnecting from the last position to ride it out: %w", err,
				),
			}
		}
	}
	return classifyApplierError(err)
}

// isGRPCAbnormalStreamCloseError reports whether a gRPC status is the
// TRANSPORT-level abnormal-close shape — the HTTP/2 stream died mid-flight
// without a clean trailer/EOF handshake. grpc-go surfaces these as
// codes.Internal, NOT as one of the [isRetriableGRPCCode] transients.
//
// # Why this is a separate matcher and not `codes.Internal` in the code set
//
// codes.Internal is overloaded: it is BOTH "the transport broke" and "the
// server hit a genuine internal fault". Adding Internal wholesale to
// [isRetriableGRPCCode] would retry real vtgate faults forever, masking them —
// exactly what that function's doc warns against. So the transport subset is
// discriminated by its (grpc-go-generated, not vtgate-authored) wording.
//
// # Ground truth (2026-07-22)
//
// A multi-day soak against real PlanetScale died with:
//
//	rpc error: code = Internal desc = server closed the stream without sending trailers
//
// after ~17h of healthy streaming — a routine long-lived-VStream drop. It fell
// through as TERMINAL, so the ADR-0038 pipeline retry loop never saw a
// retriable shape and the sync EXITED instead of reopening from its persisted
// position. Data was never at risk (the position is durable and the restart
// warm-resumed cleanly), but a continuous sync that dies on every routine
// stream drop is not operationally usable. Classifying it retriable lets the
// existing retry loop reopen the pump in process.
//
// Kept as a named helper (not inlined) so [TestClassifyReaderError_GRPCAbnormalStreamClose]
// pins the exact wording set — a grpc-go rewording then fails the pin rather
// than silently reverting to a fatal exit on a routine drop.
func isGRPCAbnormalStreamCloseError(st *status.Status) bool {
	if st.Code() != codes.Internal {
		return false
	}
	msg := strings.ToLower(st.Message())
	return strings.Contains(msg, "server closed the stream without sending trailers") ||
		strings.Contains(msg, "unexpected eof") ||
		strings.Contains(msg, "stream terminated by rst_stream")
}

// isReshardPrimaryUnroutableError reports whether a gRPC status is the
// TRANSIENT "the resharded shard's PRIMARY is not routable yet" shape that
// reopenAfterReshard's first Recv can hit in the documented post-SwitchTraffic
// window. vtgate surfaces it as codes.NotFound with the tablet-health wording:
//
//	failed to get tablet connection to zone1-0000000201: target:
//	commerce.80-.primary: tablet uid:201 is either down or nonexistent
//
// # Why the wording, not the code
//
// codes.NotFound is overloaded and stays TERMINAL wholesale (see
// [isRetriableGRPCCode], which deliberately excludes it): a bogus keyspace, a
// dropped shard, or a real missing table are all NotFound and MUST fail loudly
// rather than spin. Only the tablet-routability flavour is transient, and it is
// discriminated by the vtgate-authored phrase `is either down or nonexistent`
// (paired with `tablet`, the two-fragment discipline this file uses elsewhere).
// A keyspace-not-found / table-not-found NotFound carries neither fragment and
// stays terminal.
//
// Retries are BOUNDED by the ADR-0038 reconnect budget, so a tablet that is
// permanently gone (not merely settling) still exhausts loudly rather than
// looping forever — the same safety the schema-resolution and abnormal-close
// arms rely on.
//
// Kept as a named helper (not inlined) so
// [TestClassifyReaderError_ReshardPrimaryUnroutable] pins the exact wording set
// AND the narrowness (a broadening to all-NotFound reddens the terminal cases);
// a vtgate rewording then fails the pin rather than silently reverting the
// reshard-follow reopen to a fatal exit on the routability window.
func isReshardPrimaryUnroutableError(st *status.Status) bool {
	if st.Code() != codes.NotFound {
		return false
	}
	msg := strings.ToLower(st.Message())
	return strings.Contains(msg, "tablet") &&
		strings.Contains(msg, "is either down or nonexistent")
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

// isVStreamMissingFieldEventError reports whether err is the
// "row event … without preceding FIELD event" shape raised by
// [vstreamCDCReader.dispatchRow] (and its concurrent-COPY twin) when the
// field cache has no entry for a ROW event's (shard, table).
//
// Matched on BOTH fragments together, never on either alone: "field event"
// appears in unrelated diagnostics, and "row event" is generic. Kept as a
// named helper — not inlined — so the wording set is pinned by
// [TestClassifyReaderError_MissingFieldEvent]; a message change then fails
// the pin rather than silently reverting this to TERMINAL and re-introducing
// the item-81 stream kill.
func isVStreamMissingFieldEventError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "row event for") &&
		strings.Contains(msg, "without preceding field event")
}

// isMariaDBPurgedGTIDError reports whether err is MariaDB's domain-GTID
// "resume position is older than the retained binlogs" shape — server
// error 1236 with the GTID-specific wording (ground-truthed live against
// mariadb:11.4 and mariadb:10.11):
//
//	"Could not find GTID state requested by slave in any binlog files.
//	 Probably the slave state is too old and required binlog files have
//	 been purged."
//
// This is DISTINCT from MySQL's file/pos 1236 ("the master has purged
// required binary logs", caught by isVStreamPurgedGTIDError's
// "purged required binary logs" substring) — MariaDB's GTID wording
// shares no such substring, so it needs its own matcher. The
// discriminating phrase "could not find gtid state requested" is unique to
// the MariaDB GTID-purge case and does not collide with the file/pos
// wording. Kept as a named helper (not inlined) so
// [TestClassifyReaderError_MariaDBPurgedGTID] pins the exact wording — a
// server-side rewording then fails the pin rather than silently reverting
// to a restart loop against an unreachable position.
func isMariaDBPurgedGTIDError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "could not find gtid state requested")
}

// isVStreamGTIDSetMismatchError reports whether err is vttablet's lineage
// refusal of a resume position (uvstreamer setStreamStartPosition:
// "GTIDSet Mismatch", carried as codes.InvalidArgument). Measured verbatim
// on the real cluster rig 2026-09-02 resuming a position captured on a
// different cluster. The phrase is unique to that check.
func isVStreamGTIDSetMismatchError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "gtidset mismatch")
}

// isMariaDBForeignGTIDError reports whether err is MariaDB's OTHER
// GTID-resume 1236 — a position the server has never had at all, as
// opposed to one it once had and purged (ground-truthed live against
// mariadb:11.4, 2026-09-02, resuming instance A's position on unrelated
// fresh instance B; audit SLM-2's MariaDB sibling):
//
//	"Error: connecting slave requested to start from GTID 0-1-3, which is
//	 not in the master's binlog"
//
// Same consequence as the purged shape — the position is not resumable on
// this source — so it takes the same ir.ErrPositionInvalid route to the
// ADR-0022/ADR-0093 cold-start fall-through. Before this matcher the
// refusal was LOUD but TERMINAL: the stream exited instead of taking the
// fall-through the MariaDB arm's pre-check comment promises. The
// discriminating phrase is unique to this server message.
func isMariaDBForeignGTIDError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "which is not in the master's binlog")
}

// isVStreamPurgedGTIDError reports whether err is the source's
// "resume position is older than the retained binlogs" shape (ADR-0093).
// Both wordings sluice can see carry the discriminating substring
// `purged required binary logs`:
//
//	"the master has purged required binary logs ..."  — MySQL error 1236,
//	                                                    raw binlog stream
//	"the source purged required binary logs ..."       — Vitess's inclusive
//	                                                    rewording on vtgate
//
// Matching that one substring covers both. Kept as a named helper (not
// inlined) so [TestClassifyReaderError_PurgedGTID] pins the exact
// wording — a vtgate wording change then fails the pin rather than
// silently reverting to a restart loop.
func isVStreamPurgedGTIDError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "purged required binary logs")
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

// isVStreamMessageTooLargeError reports whether err is gRPC's
// received-message-too-large rejection.
//
// Matched on the DISTINCTIVE phrase rather than on `code = ResourceExhausted`
// alone, because that code legitimately covers vttablet's throttler and pool
// exhaustion — both genuinely transient and both correctly retried by
// vitessRetriableSubstrings. Only the size flavour is deterministic, so only
// the size flavour is carved out.
//
// gRPC emits THREE receive-side size rejections, verified against the pinned
// google.golang.org/grpc@v1.66.0 rather than assumed — an earlier version of
// this comment listed two of them, which is the kind of "stable across the
// forms" claim that is true until it is not:
//
//	grpc: received message larger than max (N vs. M)
//	grpc: received message after decompression larger than max (N vs. M)
//	grpc: received message larger than max length allowed on current machine (N vs. M)
//
// All three carry both substrings matched below, so the pair covers the set.
// The SEND-side rejections ("trying to send message larger than max") are
// deliberately NOT matched: those are sluice exceeding its own send ceiling,
// a different failure with a different remedy, and folding them in here would
// hand an operator the receive-side advice for a send-side problem.
func isVStreamMessageTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "grpc: received message") &&
		strings.Contains(msg, "larger than max")
}
