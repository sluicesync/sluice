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
