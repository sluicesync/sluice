// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sluicesync.dev/sluice/internal/ir"
)

// TestClassifyReaderError_DelegatesToApplierClassifier asserts the
// reader-side classifier matches the applier-side shapes 1:1. The
// v0.46.0 wiring relies on this identity — the streamer's
// retry loop (ADR-0038) classifies source errors and applier errors
// against the same [ir.RetriableError] interface, so the underlying
// transient table must agree. Divergence would be a silent regression
// where one surface retries on a shape the other treats as terminal.
//
// GitHub issue #19.
func TestClassifyReaderError_DelegatesToApplierClassifier(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"nil passes through", nil},
		{"plain non-retriable error", errors.New("schema mismatch")},
		{"InnoDB deadlock (1213)", &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}},
		{"Vitess tx-killer Aborted (1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Aborted desc = tx killer"}},
		{"Vitess Unavailable (1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = tablet not serving"}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"io.EOF", io.EOF},
		{"connection reset by peer", errors.New("read tcp: connection reset by peer")},
		{"duplicate key (explicit non-retriable)", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotReader := classifyReaderError(c.err)
			gotApplier := classifyApplierError(c.err)

			// Identity check: when the applier classifier returned the
			// input unchanged (non-retriable), reader must too.
			//
			//nolint:errorlint // identity comparison is the assertion
			if (gotReader == c.err) != (gotApplier == c.err) {
				t.Errorf("reader/applier classifier disagree on identity-preservation for %q", c.name)
			}

			// Both must agree on RetriableError satisfaction.
			var reReader, reApplier ir.RetriableError
			retriableR := errors.As(gotReader, &reReader)
			retriableA := errors.As(gotApplier, &reApplier)
			if retriableR != retriableA {
				t.Errorf("reader/applier classifier disagree on retriable shape for %q: reader=%v applier=%v",
					c.name, retriableR, retriableA)
			}
		})
	}
}

// TestClassifyReaderError_SchemaResolution pins the source-side
// schema-resolution carve-out (Bug F9): the vstreamer's "can't resolve
// this table's schema at the replay position" shapes arrive as free-text
// (no gRPC status, no 1105 wrapper) right after a DDL cutover or when the
// Vitess historian is off, and used to fall through TERMINAL — killing
// the stream on a window that clears itself. They must classify retriable
// so the ADR-0038 backoff rides out the cutover window.
//
// Pin-the-class: both known wordings are asserted retriable (each wrapped
// as the pump wraps it), the underlying error stays reachable, and a
// near-miss ("unknown table" with no "in schema", which is a genuine
// terminal DROP/typo) is asserted to STAY terminal so the substring match
// can't widen into masking real schema errors.
func TestClassifyReaderError_SchemaResolution(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			"unknown table in schema (historian gap)",
			errors.New("unknown table soak_events in schema"),
			true,
		},
		{
			"no schema found for table (reload race)",
			errors.New("vstreamer: no schema found for table soak_events"),
			true,
		},
		{
			// Near-miss: a bare "unknown table" with no "in schema" is the
			// terminal shape (DROP / typo on the source) and must NOT be
			// swept into the retriable carve-out.
			"bare unknown table stays terminal",
			errors.New("Error 1146: Table 'db.gone' doesn't exist: unknown table gone"),
			false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Wrap as the VStream pump does.
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", c.err)
			got := classifyReaderError(wrapped)

			var re ir.RetriableError
			gotRetriable := errors.As(got, &re)
			if gotRetriable != c.retriable {
				t.Errorf("classifyReaderError(%q) retriable=%v, want %v", c.name, gotRetriable, c.retriable)
			}
			// The underlying error must stay reachable on the chain.
			if !errors.Is(got, c.err) {
				t.Errorf("classifyReaderError(%q) lost the underlying error from the chain", c.name)
			}
		})
	}
}

// TestClassifyReaderError_PurgedGTID pins the ADR-0093 carve-out: a
// VStream/PlanetScale resume from a purged GTID position (gtid_purged
// advanced past the persisted position) surfaces REACTIVELY from the
// pump's Recv. classifyReaderError must map it to an error that
// errors.Is(ir.ErrPositionInvalid) so the streamer routes it to a
// cold-start re-snapshot (ADR-0022 parity) — and must NOT classify it
// retriable (retrying the same purged position spins forever; the
// PlanetScale-flavored vtgate error can carry codes.Unknown, which IS in
// the retriable gRPC set, so the purged check has to win FIRST).
//
// Pin-the-class: both known wordings (MySQL 1236 "the master has purged
// required binary logs" and Vitess's "the source purged required binary
// logs"), including a gRPC-status-wrapped Unknown variant, are asserted
// invalid-position-and-not-retriable; a near-miss (bare "purged", no
// "required binary logs") is asserted to STAY terminal/unchanged so the
// substring match can't widen into masking unrelated errors.
func TestClassifyReaderError_PurgedGTID(t *testing.T) {
	cases := []struct {
		name            string
		err             error
		invalidPosition bool
	}{
		{
			"mysql 1236 master purged",
			errors.New("Error 1236 (HY000): the master has purged required binary logs and replication is required"),
			true,
		},
		{
			"vitess source purged",
			errors.New("vstreamer: the source purged required binary logs needed to resume"),
			true,
		},
		{
			// MariaDB domain-GTID purge (ADR-0170): the wording shares no
			// "purged required binary logs" substring with the MySQL/Vitess
			// cases above, so isMariaDBPurgedGTIDError's distinct
			// "could not find gtid state requested" matcher must catch it —
			// else a purged MariaDB resume falls through terminal and never
			// cold-starts. Ground-truthed verbatim against mariadb:11.4/10.11.
			"mariadb 1236 gtid state not found",
			errors.New("ERROR 1236 (HY000): Could not find GTID state requested by slave in any binlog files. " +
				"Probably the slave state is too old and required binlog files have been purged."),
			true,
		},
		{
			// MariaDB's OTHER 1236: a domain-GTID this server never had (a
			// fresh/reset/replaced instance, or a position from another
			// server). Measured verbatim on mariadb:11.4 resuming instance
			// A's position on fresh instance B (audit 2026-09-01 SLM-2's
			// MariaDB arm). Before isMariaDBForeignGTIDError existed this
			// was loud but TERMINAL — the stream exited instead of taking
			// the cold-start fall-through the arm's own comment promises.
			"mariadb 1236 gtid not in the master's binlog",
			errors.New("mysql: cdc: get event: ERROR 1236 (HY000): Error: connecting slave requested to start from GTID 0-1-3, " +
				"which is not in the master's binlog"),
			true,
		},
		{
			// vttablet's lineage refusal of a resume position from a different
			// cluster/shard (uvstreamer setStreamStartPosition), measured on the
			// real cluster rig 2026-09-02 as codes.InvalidArgument. The status
			// switch keeps InvalidArgument TERMINAL, so without its own arm this
			// exited the stream with no cold-start route.
			"vstream GTIDSet Mismatch carried as gRPC InvalidArgument",
			status.Error(codes.InvalidArgument, "vstream: rpc error: code = InvalidArgument desc = target: commerce.0.replica: "+
				"vttablet: rpc error: code = InvalidArgument desc = GTIDSet Mismatch, requested: MySQL56/58e74464-8f3f-11f0-9d2c-0242ac110002:1-11, current: MySQL56/b8b646a3-8f3f-11f0-9d2c-0242ac110003:1-5"),
			true,
		},
		{
			// PlanetScale-flavored: the purged error arrives as a gRPC
			// status carrying codes.Unknown (in the ADR-0038 retriable
			// set). The purged check MUST win before isRetriableGRPCCode,
			// or this would be (wrongly) retried forever.
			"purged carried as gRPC Unknown (must not be retriable)",
			status.Error(codes.Unknown, "vttablet: the source purged required binary logs"),
			true,
		},
		{
			// Near-miss: "purged" alone, without the discriminating
			// "required binary logs", is some other error and must stay
			// terminal/unchanged (not swept into ErrPositionInvalid).
			"bare purged stays terminal",
			errors.New("the throttler purged a stale entry"),
			false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Wrap as the VStream pump does.
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", c.err)
			got := classifyReaderError(wrapped)

			if errors.Is(got, ir.ErrPositionInvalid) != c.invalidPosition {
				t.Errorf("classifyReaderError(%q): errors.Is(ErrPositionInvalid)=%v, want %v (got %v)",
					c.name, errors.Is(got, ir.ErrPositionInvalid), c.invalidPosition, got)
			}
			// A purged position is NEVER retriable — retrying spins forever.
			var re ir.RetriableError
			if c.invalidPosition && errors.As(got, &re) {
				t.Errorf("classifyReaderError(%q) classified a purged position as retriable; it must be ErrPositionInvalid (terminal-but-recoverable-via-cold-start)", c.name)
			}
			// The underlying error must stay reachable for diagnostics.
			if !errors.Is(got, c.err) {
				t.Errorf("classifyReaderError(%q) lost the underlying error from the chain", c.name)
			}
		})
	}
}

// TestClassifyReaderError_GRPCStatusCodes pins the gRPC-status branch
// the reader classifier adds on top of the SQL-path delegation — the
// reader-only shape a VStream stream Recv produces on a connection
// drop (operator report: `Unavailable: connector reset by peer` failing
// a cold-start, which the text/1105 matchers missed).
//
// Pin-the-class, not the representative: every code in the retriable
// set AND a spread of terminal codes are asserted, each wrapped EXACTLY
// as the pump wraps it (`fmt.Errorf("mysql/vstream: recv: %w", …)`) so
// the test also guards that status.FromError still unwraps the `%w`
// chain on a grpc dependency bump. A widening of [isRetriableGRPCCode]
// (or a regression in unwrapping) fails here rather than silently.
func TestClassifyReaderError_GRPCStatusCodes(t *testing.T) {
	cases := []struct {
		name      string
		code      codes.Code
		retriable bool
	}{
		{"Unavailable (transport reset/draining)", codes.Unavailable, true},
		{"Aborted (tx-killer/failover)", codes.Aborted, true},
		{"Unknown (vttablet internal transient)", codes.Unknown, true},
		{"ResourceExhausted (throttler)", codes.ResourceExhausted, true},
		{"InvalidArgument (terminal)", codes.InvalidArgument, false},
		{"NotFound (terminal)", codes.NotFound, false},
		{"FailedPrecondition (terminal)", codes.FailedPrecondition, false},
		{"PermissionDenied (terminal)", codes.PermissionDenied, false},
		{"Internal (terminal)", codes.Internal, false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Wrap as the VStream pump does (cdc_vstream.go pump:
			// classifyReaderError(fmt.Errorf("mysql/vstream: recv: %w", err))).
			raw := status.Error(c.code, "connector reset by peer")
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", raw)

			got := classifyReaderError(wrapped)
			var re ir.RetriableError
			gotRetriable := errors.As(got, &re)
			if gotRetriable != c.retriable {
				t.Errorf("classifyReaderError(grpc %s) retriable=%v, want %v", c.code, gotRetriable, c.retriable)
			}
			// The original status error must remain reachable via the
			// chain so downstream errors.Is/As against the gRPC status
			// still works from the consumer side.
			if st, ok := status.FromError(got); !ok || st.Code() != c.code {
				t.Errorf("classifyReaderError(grpc %s) lost the underlying status (ok=%v code=%v)", c.code, ok, st.Code())
			}
		})
	}
}

// A long-lived VStream that drops mid-flight surfaces the TRANSPORT-level
// abnormal close as codes.Internal — which is deliberately NOT in the retriable
// code set (a genuine vtgate Internal fault must stay terminal). Ground truth
// (2026-07-22): a multi-day soak against real PlanetScale died with
// `code = Internal desc = server closed the stream without sending trailers`
// after ~17h of healthy streaming; it fell through TERMINAL, so the ADR-0038
// retry loop never saw a retriable shape and the sync exited instead of
// reopening from its persisted position.
//
// This pins the discriminator: the grpc-go-generated transport wordings are
// retriable, while a vtgate-authored Internal message stays terminal. A grpc-go
// rewording fails this pin rather than silently reverting to a fatal exit on a
// routine drop.
func TestClassifyReaderError_GRPCAbnormalStreamClose(t *testing.T) {
	cases := []struct {
		name      string
		code      codes.Code
		msg       string
		retriable bool
	}{
		{"Internal + server-closed-without-trailers (observed)", codes.Internal, "server closed the stream without sending trailers", true},
		{"Internal + unexpected EOF", codes.Internal, "unexpected EOF", true},
		{"Internal + RST_STREAM", codes.Internal, "stream terminated by RST_STREAM with error code: INTERNAL_ERROR", true},
		{"Internal + genuine vtgate fault stays TERMINAL", codes.Internal, "vttablet: rpc error: internal server error executing query", false},
		{"non-Internal code is not matched by this helper", codes.InvalidArgument, "server closed the stream without sending trailers", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Wrap as the VStream pump does (cdc_vstream.go:1226).
			raw := status.Error(c.code, c.msg)
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", raw)

			got := classifyReaderError(wrapped)
			var re ir.RetriableError
			gotRetriable := errors.As(got, &re)
			if gotRetriable != c.retriable {
				t.Errorf("classifyReaderError(grpc %s %q) retriable=%v, want %v", c.code, c.msg, gotRetriable, c.retriable)
			}
			// The underlying status must stay reachable for diagnostics.
			if st, ok := status.FromError(got); !ok || st.Code() != c.code {
				t.Errorf("classifyReaderError lost the underlying status (ok=%v code=%v)", ok, st.Code())
			}
		})
	}
}

// TestClassifyReaderError_ReshardPrimaryUnroutable pins the roadmap item 72(b)
// carve-out: when the Streamer FOLLOWS a 1->2 reshard, reopenAfterReshard's
// first Recv can race the documented post-SwitchTraffic window in which the
// resharded shard's PRIMARY is not yet routable — vtgate answers codes.NotFound
// with `tablet uid:N is either down or nonexistent`. That specific shape is
// TRANSIENT (a reconnect lands on the now-serving primary) and must be
// retriable, while codes.NotFound stays TERMINAL wholesale for every other
// flavour.
//
// NARROWNESS is the load-bearing property, so it is pinned explicitly: a bogus
// keyspace, a dropped shard, and a real missing table are all codes.NotFound and
// MUST stay terminal. If the carve-out were broadened to all-NotFound (drop the
// wording check in isReshardPrimaryUnroutableError), the three terminal cases
// below flip to retriable and this test reddens — that is the mutation check.
func TestClassifyReaderError_ReshardPrimaryUnroutable(t *testing.T) {
	cases := []struct {
		name      string
		code      codes.Code
		msg       string
		retriable bool
	}{
		// The transient reshard-reopen window (ground truth: extended-suites
		// runs 32633905540 / 32791797825).
		{
			"NotFound + tablet-unroutable (post-SwitchTraffic window)",
			codes.NotFound,
			`error starting stream from shard GTID shard:"80-" gtid:"MySQL56/...:1-84": failed to get tablet connection to zone1-0000000201: target: commerce.80-.primary: tablet uid:201 is either down or nonexistent`,
			true,
		},
		// Every OTHER NotFound stays terminal — the operator's target is wrong
		// and retrying would mask it. These are the narrowness pins.
		{"NotFound + bogus keyspace stays TERMINAL", codes.NotFound, "keyspace commerce not found in vschema", false},
		{"NotFound + dropped shard stays TERMINAL", codes.NotFound, `shard commerce/40-80 not found`, false},
		{"NotFound + missing table stays TERMINAL", codes.NotFound, `table "ledger" not found in schema`, false},
		{"NotFound + generic connection text stays TERMINAL", codes.NotFound, "connector reset by peer", false},
		// The wording alone under a NON-NotFound code is not matched by THIS
		// helper (an Unavailable carrying the same text is retriable anyway via
		// isRetriableGRPCCode, so a non-NotFound + wording case is asserted
		// against the FailedPrecondition terminal code to keep the pin clean).
		{"FailedPrecondition + tablet-unroutable text stays TERMINAL", codes.FailedPrecondition, "tablet uid:201 is either down or nonexistent", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Wrap exactly as reopenAfterReshard's Recv does
			// (cdc_vstream_snapshot.go: "mysql/vstream: snapshot: cdc recv: %w").
			raw := status.Error(c.code, c.msg)
			wrapped := fmt.Errorf("mysql/vstream: snapshot: cdc recv: %w", raw)

			got := classifyReaderError(wrapped)
			var re ir.RetriableError
			gotRetriable := errors.As(got, &re)
			if gotRetriable != c.retriable {
				t.Errorf("classifyReaderError(grpc %s %q) retriable=%v, want %v", c.code, c.msg, gotRetriable, c.retriable)
			}
			// The underlying status must stay reachable for diagnostics.
			if st, ok := status.FromError(got); !ok || st.Code() != c.code {
				t.Errorf("classifyReaderError lost the underlying status (ok=%v code=%v)", ok, st.Code())
			}
		})
	}
}

// TestIsReshardPrimaryWindowError pins the routing marker (item 72(b)): the
// reshard-follow reopen reads [isReshardPrimaryWindowError] to keep the recovery
// on the PRIMARY-pinned reshard tail ([reopenReshardWindow]) instead of escaping
// to the REPLICA-defaulting warm-resume. So the classified post-SwitchTraffic
// window error MUST carry the marker, and no OTHER retriable shape may — a
// generic GOAWAY / Unavailable blip must settle → warm-resume as before, not be
// mistaken for the reshard window.
func TestIsReshardPrimaryWindowError(t *testing.T) {
	windowRaw := status.Error(codes.NotFound,
		`failed to get tablet connection to zone1-0000000201: target: commerce.80-.primary: tablet uid:201 is either down or nonexistent`)
	window := classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: cdc recv: %w", windowRaw))
	if !isReshardPrimaryWindowError(window) {
		t.Errorf("classified post-SwitchTraffic window error is not recognized by isReshardPrimaryWindowError; the reshard-window recovery would never engage")
	}

	// A different retriable shape (an Unavailable transient) is retriable but is
	// NOT the reshard window — it must settle to the generic warm-resume.
	otherRaw := status.Error(codes.Unavailable, "connection reset by peer")
	other := classifyReaderError(fmt.Errorf("mysql/vstream: snapshot: cdc recv: %w", otherRaw))
	var re ir.RetriableError
	if !errors.As(other, &re) {
		t.Fatalf("precondition: Unavailable should classify retriable")
	}
	if isReshardPrimaryWindowError(other) {
		t.Errorf("a generic Unavailable transient was mis-tagged as the reshard primary-routable window")
	}

	// A plain (non-retriable, non-window) error carries no marker.
	if isReshardPrimaryWindowError(errors.New("some terminal error")) {
		t.Errorf("a plain error was mis-tagged as the reshard primary-routable window")
	}
}

// A VStream teardown on an operator `sync stop` (or Ctrl-C / outer-ctx cancel)
// surfaces from Recv as a gRPC Canceled / DeadlineExceeded status. The reader
// classifier normalizes those to the standard context sentinels so the
// engine-neutral streamer's errors.Is(context.Canceled) ctx-termination check
// recognizes the clean stop and completes the `sync stop --wait` drain
// handshake — rather than treating the raw gRPC status as terminal, which left
// stop_requested_at set and produced a FALSE drain timeout. NOT retriable (a
// cancel is intentional, not transient); the original status stays reachable.
func TestClassifyReaderError_CancellationNormalized(t *testing.T) {
	cases := []struct {
		name     string
		code     codes.Code
		sentinel error
	}{
		{"Canceled -> context.Canceled", codes.Canceled, context.Canceled},
		{"DeadlineExceeded -> context.DeadlineExceeded", codes.DeadlineExceeded, context.DeadlineExceeded},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			raw := status.Error(c.code, "context canceled")
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", raw)

			got := classifyReaderError(wrapped)
			if !errors.Is(got, c.sentinel) {
				t.Errorf("classifyReaderError(grpc %s): errors.Is(%v)=false; the streamer won't recognize the clean stop", c.code, c.sentinel)
			}
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("classifyReaderError(grpc %s) was classified retriable; a cancel is intentional, not a transient", c.code)
			}
			// The underlying gRPC status stays on the chain for diagnostics.
			if st, ok := status.FromError(got); !ok || st.Code() != c.code {
				t.Errorf("classifyReaderError(grpc %s) lost the underlying status (ok=%v)", c.code, ok)
			}
		})
	}
}

// TestClassifyReaderError_GracefulGoAway pins roadmap item 79: a server-side
// HTTP/2 GOAWAY carrying ErrCode=NO_ERROR is a GRACEFUL DRAIN — "reconnect" —
// and must be retriable, even though grpc-go delivers it as
// codes.InvalidArgument, which this classifier otherwise (correctly) treats as
// terminal.
//
// The observed failure (soak231, 2026-07-24, v0.100.0): a routine PlanetScale
// edge-pod rotation exited the stream, and an unattended sync stayed down.
// Zero-loss — the position was intact — but continuous replication that stops
// on routine platform maintenance is not continuous.
//
// Pin-the-class, both directions: the graceful code retries; every
// error-carrying GOAWAY stays terminal, INCLUDING one whose debug text says
// "graceful_stop" (the debug field is peer-chosen and must not be
// load-bearing — only the standards-defined ErrCode decides).
func TestClassifyReaderError_GracefulGoAway(t *testing.T) {
	// The verbatim production text, copied from the soak log.
	const productionDesc = `protocol error: incomplete envelope: ` +
		`http2: server sent GOAWAY and closed the connection; ` +
		`LastStreamID=1, ErrCode=NO_ERROR, debug="graceful_stop"`

	cases := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			"verbatim production shape, as a gRPC InvalidArgument status",
			status.Error(codes.InvalidArgument, productionDesc),
			true,
		},
		{
			"same shape as a bare (non-status) error",
			errors.New(productionDesc),
			true,
		},
		{
			"PROTOCOL_ERROR GOAWAY stays terminal",
			status.Error(codes.InvalidArgument,
				`http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=PROTOCOL_ERROR`),
			false,
		},
		{
			"ENHANCE_YOUR_CALM GOAWAY stays terminal (we are being throttled off)",
			status.Error(codes.InvalidArgument,
				`http2: server sent GOAWAY; ErrCode=ENHANCE_YOUR_CALM, debug="too_many_pings"`),
			false,
		},
		{
			"debug=graceful_stop WITHOUT a no-error code stays terminal",
			status.Error(codes.InvalidArgument,
				`http2: server sent GOAWAY; ErrCode=PROTOCOL_ERROR, debug="graceful_stop"`),
			false,
		},
		{
			"a genuine InvalidArgument (no GOAWAY) still stays terminal",
			status.Error(codes.InvalidArgument, "vstream: malformed vgtid"),
			false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", c.err)
			got := classifyReaderError(wrapped)

			var re ir.RetriableError
			if gotRetriable := errors.As(got, &re); gotRetriable != c.retriable {
				t.Errorf("retriable=%v, want %v (err: %v)", gotRetriable, c.retriable, got)
			}
			if !errors.Is(got, c.err) {
				t.Error("classifyReaderError lost the underlying error from the chain")
			}
			// A graceful drain must NOT be mistaken for an invalid position:
			// the stream reconnects from the SAME position, it does not
			// re-snapshot. (Routing it to ErrPositionInvalid would turn a
			// free reconnect into a full cold-start re-copy.)
			if errors.Is(got, ir.ErrPositionInvalid) {
				t.Error("classified as ErrPositionInvalid; a graceful drain must resume from the same position, never force a re-snapshot")
			}
		})
	}
}

// TestClassifyReaderError_MissingFieldEvent pins roadmap item 81: a ROW event
// arriving before its table's FIELD event must be RETRIABLE, not terminal.
//
// The mechanism (verified by code-reading against a live production kill):
// dispatchDDL invalidates the field cache with a BLANKET clear across every
// (shard, table), but vtgate only re-emits a FIELD event for the table whose
// shape actually changed. So a DDL on one table leaves every OTHER table in
// the keyspace with an empty cache entry, and the next ROW event on a
// long-established table trips the loud floor — which is why the observed
// fatal named a different table than the DDL touched.
//
// Retrying is safe precisely because nothing decodes with guessed metadata: a
// reconnect re-opens at the last position, where VStream emits a FIELD event
// ahead of the first ROW event per table, so the reader re-learns the CURRENT
// shape. The near-miss cases below keep that carve-out from widening into
// "any row-event complaint is transient".
func TestClassifyReaderError_MissingFieldEvent(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			// The verbatim production shape that killed the soak231 stream.
			"verbatim: dispatchRow floor",
			errors.New(`mysql/vstream: row event for "-/soak-mysql231.soak_events" without preceding FIELD event`),
			true,
		},
		{
			// The concurrent-COPY twin raises the same class from a different
			// call site; it must classify identically.
			"concurrent COPY twin",
			errors.New(`mysql/vstream: snapshot: concurrent COPY: row event for "orders" without preceding FIELD event`),
			true,
		},
		{
			// Near-miss: "field event" alone must NOT make an unrelated error
			// retriable.
			"unrelated mention of a field event stays terminal",
			errors.New("mysql/vstream: malformed field event payload: bad wire type"),
			false,
		},
		{
			// Near-miss: a generic row-event complaint is not this shape.
			"generic row-event error stays terminal",
			errors.New("mysql/vstream: row event decode failed: unsupported column type"),
			false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			wrapped := fmt.Errorf("mysql/vstream: recv: %w", c.err)
			got := classifyReaderError(wrapped)
			var re ir.RetriableError
			if gotRetriable := errors.As(got, &re); gotRetriable != c.retriable {
				t.Errorf("retriable=%v, want %v (got %v)", gotRetriable, c.retriable, got)
			}
			if !errors.Is(got, c.err) {
				t.Error("classifyReaderError lost the underlying error from the chain")
			}
			// A DDL-boundary cache miss must resume from the SAME position —
			// routing it to ErrPositionInvalid would turn a free reconnect
			// into a full cold-start re-copy.
			if errors.Is(got, ir.ErrPositionInvalid) {
				t.Error("classified as ErrPositionInvalid; a FIELD-cache miss must resume from the same position, never force a re-snapshot")
			}
		})
	}
}
