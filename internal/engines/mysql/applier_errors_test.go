// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// TestIsMySQLDeadlock pins the predicate the shard-lease acquire uses to
// retry on InnoDB deadlock (1213) — including the wrapped form it sees
// from tryAcquireShardLeaseOnce's "lease acquire: insert: %w".
func TestIsMySQLDeadlock(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"deadlock 1213", &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock"}, true},
		{"wrapped 1213", fmt.Errorf("mysql: lease acquire: insert: %w", &gomysql.MySQLError{Number: 1213}), true},
		{"dup key 1062", &gomysql.MySQLError{Number: 1062}, false},
		{"plain error", errors.New("nope"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isMySQLDeadlock(tc.err); got != tc.want {
			t.Errorf("%s: isMySQLDeadlock = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestSchemaWriter_IsTransientError pins the ADR-0114 DDL-phase retry
// verdict the orchestrator reads via [ir.TransientClassifier]: a
// PlanetScale reparent / storage-grow shape (vttablet "not serving",
// disk-full, read-only window) must classify transient so a grow landing
// on the index/constraint phase retries, while a real DDL fault (unknown
// column, duplicate key) must NOT — a broken DDL still fails loudly.
// Delegates to classifyApplierError, guarding against a second classifier.
func TestSchemaWriter_IsTransientError(t *testing.T) {
	w := &SchemaWriter{}
	transient := []error{
		&gomysql.MySQLError{Number: 1105, Message: "target: ks.0.primary: vttablet: rpc error: code = Unavailable desc = primary is not serving"},
		// The UN-framed vtgate cluster event (no `vttablet` tag) — the
		// 2026-07-28 PS-160 shape. The DDL phase rides the same reparent the
		// copy phase does; see TestClassifyApplierError_UnframedVtgateReparent.
		&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: primary is not serving, there may be a reparent operation in progress"},
		&gomysql.MySQLError{Number: 1021, Message: "No space left on device"},
		&gomysql.MySQLError{Number: 1290, Message: "The MySQL server is running with the --read-only option so it cannot execute this statement"},
	}
	for _, e := range transient {
		if !w.IsTransientError(e) {
			t.Errorf("IsTransientError(%v) = false; want true (a reparent/grow transient must retry)", e)
		}
	}
	// NOTE: 1054/1146 are classified retriable schema-drift (self-heals when
	// the operator adds the missing column/table), so they are deliberately
	// NOT in this terminal set — assert only the genuinely-terminal shapes.
	terminal := []error{
		&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'PRIMARY'"},
		errors.New("some random non-transient failure"),
	}
	for _, e := range terminal {
		if w.IsTransientError(e) {
			t.Errorf("IsTransientError(%v) = true; want false (a real DDL fault must fail loudly)", e)
		}
	}
	if w.IsTransientError(nil) {
		t.Error("IsTransientError(nil) = true; want false")
	}
}

// TestClassifyApplierError_NilInNilOut is the boring boundary case
// the pipeline relies on: classifier must pass nil through unchanged
// so wrapping every applier return site doesn't accidentally turn a
// success into a typed-error.
func TestClassifyApplierError_NilInNilOut(t *testing.T) {
	if got := classifyApplierError(nil); got != nil {
		t.Errorf("classifyApplierError(nil) = %v; want nil", got)
	}
}

// TestClassifyApplierError_NonRetriableUnchanged covers the
// default-deny invariant from ADR-0038. Errors that don't match a
// known transient shape return verbatim — the pipeline's retry loop
// treats those as terminal (errors.As against ir.RetriableError will
// fail).
func TestClassifyApplierError_NonRetriableUnchanged(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("some random failure")},
		{"wrapped error", fmt.Errorf("wrapping: %w", errors.New("inner"))},
		// Bug 200 negative pin: a typo'd endpoint's dial error stays
		// terminal — the dial leg matches transient shapes, never dialing
		// per se.
		{"dial to a typo'd host (no such host) stays terminal", errors.New("dial tcp: lookup db.exmple.com: no such host")},
		{"duplicate key (explicit non-retriable per ADR-0038)", &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '1179' for key 'events.PRIMARY'"}},
		{"foreign key violation", &gomysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row"}},
		{"syntax error", &gomysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}},
		// 1290 (ER_OPTION_PREVENTS_STATEMENT) is GENERIC — only the
		// read-only variant is the grow/reparent transient. A 1290 for any
		// OTHER server option must stay TERMINAL (no over-match), exactly
		// like the v0.99.94 "Canceled without TerminateAll" guard.
		{"1290 non-read-only option stays terminal (no over-match)", &gomysql.MySQLError{Number: 1290, Message: "The MySQL server is running with the --skip-grant-tables option so it cannot execute this statement"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			// Identity comparison is deliberate here: the
			// classifier MUST return the input value unchanged
			// (not a semantically-equivalent wrapper) so the
			// pipeline's errors.As(... &RetriableError{}) gate
			// fails for non-retriable inputs. errors.Is would
			// be wrong — it'd pass even on a hypothetical
			// future "non-retriable wrapper" that still chained
			// the original.
			//nolint:errorlint // see comment above — identity not equivalence
			if got != c.err {
				t.Errorf("classifyApplierError should return non-retriable errors verbatim; got wrapped %T", got)
			}
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("non-retriable error matched ir.RetriableError via errors.As — this is the bug ADR-0038's default-deny is meant to prevent")
			}
		})
	}
}

// TestClassifyApplierError_TerminalCodeShield pins the terminal-code
// shield (audit 2026-07-23 D0-3, HIGH silent-loss class): when the error
// chain carries a structured *gomysql.MySQLError, classification is
// decided by its CODE alone — the transport-text fallback legs must be
// UNREACHABLE, because a server error's message routinely echoes row
// data, key values, and (via the flush wrapper's `flush table %q` frame)
// table names, any of which can carry transient wording. Pre-shield,
// `case 1062:`'s empty body fell OUT of the switch into the text legs, so
// a duplicate-key error whose echoed value or table name contained
// "reparent" classified RETRIABLE — and the ADR-0108
// tolerate-1062-on-retry wart then swallowed the retry's 1062 as "rows
// already landed" even though BOTH atomic INSERTs rolled back: the whole
// batch silently absent at exit 0.
//
// Structure: the audit's OBSERVED misclassification cells first (live
// shapes), then the G-3 cross-product — every structurally-terminal code
// × every transient substring from the text legs (reparent set,
// connection set, disk-full set). All must return VERBATIM (identity),
// exactly like TestClassifyApplierError_NonRetriableUnchanged.
//
// The documented structured-code+message AND-gate exceptions (1105 +
// vttablet framing, 1290 + read-only wording, Error 3 / 1021 / 1114
// disk-full) stay retriable — pinned in
// TestClassifyApplierError_RetriableShapes; this test asserts the UNSAFE
// shape only: bare text matching reachable with a structured code present.
func TestClassifyApplierError_TerminalCodeShield(t *testing.T) {
	observed := []struct {
		name string
		err  error
	}{
		// The audit's five OBSERVED cells (throwaway in-package test,
		// 2026-07-23), re-pinned here permanently.
		{
			"1062 echoing a row value containing 'reparent'",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry 'planned-reparent-2026-07' for key 'jobs.name'"},
		},
		{
			"1062 on a table named reparent_history (key echo)",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '42' for key 'reparent_history.PRIMARY'"},
		},
		{
			"1062 echoing a row value containing 'connection refused'",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry 'connection refused by upstream' for key 'log_lines.msg'"},
		},
		{
			"1062 echoing a row value containing 'disk full'",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry 'disk full on node 3' for key 'incidents.title'"},
		},
		{
			"3819 CHECK violation echoing 'not serving'",
			&gomysql.MySQLError{Number: 3819, Message: "Check constraint 'status_chk' is violated: value 'not serving'"},
		},
		// The flush wrapper injects the TABLE NAME around the driver error
		// (row_writer.go `flush table %q`); the wrapped chain still carries
		// the structured *MySQLError, so the shield must decide by code
		// even though the wrapper text matches the reparent substring set.
		{
			"1062 wrapped by the flush frame naming table reparent_history",
			fmt.Errorf("mysql: flush table %q: %w", "reparent_history",
				&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '7' for key 'PRIMARY'"}),
		},
	}

	// G-3 cross-product: structurally-terminal codes × every transient
	// substring the text legs match. The substring lists are duplicated
	// from production deliberately (a pin that reads the value it guards
	// cannot detect the value changing).
	terminalCodes := []struct {
		number uint16
		label  string
	}{
		{1062, "duplicate key"},
		{3819, "check violation"},
		{1452, "fk violation"},
		{1064, "syntax error"},
		{1048, "column cannot be null"},
		// 1105 is INCLUDED deliberately, even though it is the one code with
		// message-dependent semantics: its AND-gates are the vttablet framing
		// and the FULL canonical vtgate cluster-event sentences
		// ([vtgateTransientSubstrings]), never the loose tokens below. A
		// bare "reparent" / "not serving" in an echoed statement or row value
		// must still be terminal — this row is what fails if someone widens
		// the 1105 arm to scan reparentRetriableSubstrings, which would arm
		// the cold-copy tolerate-1062-on-retry wart off a false transient.
		{1105, "vtgate generic (narrowness of the vtgate-sentence AND-gate)"},
	}
	transientSubstrings := []string{
		// reparentRetriableSubstrings
		"not serving", "reparent",
		// connection-shape leg
		"connection reset by peer", "connection refused", "broken pipe",
		"i/o timeout", "connectex:", "actively refused", "connection timed out",
		// isDiskFullSignal text set
		"no space left on device", "errno: 28", "disk full",
		"waiting for someone to free some space", "the table is full", "is full (errno",
	}

	cases := observed
	for _, code := range terminalCodes {
		for _, sub := range transientSubstrings {
			cases = append(cases, struct {
				name string
				err  error
			}{
				fmt.Sprintf("%d (%s) echoing %q", code.number, code.label, sub),
				&gomysql.MySQLError{Number: code.number, Message: fmt.Sprintf("server error echoing stored text: '%s'", sub)},
			})
		}
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Fatalf("structurally-terminal MySQL error classified RETRIABLE via text over-match (D0-3): %v", c.err)
			}
			//nolint:errorlint // identity not equivalence — terminal errors return verbatim
			if got != c.err {
				t.Errorf("terminal error not returned verbatim; got %T", got)
			}
		})
	}

	// The AND-gate exceptions must SURVIVE the shield: a structured code
	// whose documented message gate matches stays retriable (regression
	// guard so the shield isn't over-tightened to code-only-no-exceptions).
	exceptions := []struct {
		name string
		err  error
	}{
		{"1105 + vttablet framing (ADR-0038 pin-down 4)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = tablet not serving"}},
		{"1290 + read-only wording (PS-320-v10)", &gomysql.MySQLError{Number: 1290, Message: "The MySQL server is running with the --read-only option so it cannot execute this statement"}},
		{"Error 3 + ENOSPC wording (PS-320-v4)", &gomysql.MySQLError{Number: 3, Message: "Error writing file '/vt/tmp/ML' (OS errno 28 - No space left on device)"}},
		{"1021 ER_DISK_FULL", &gomysql.MySQLError{Number: 1021, Message: "Disk full (/tmp); waiting for someone to free some space..."}},
		{"1114 ER_RECORD_FILE_FULL", &gomysql.MySQLError{Number: 1114, Message: "The table '_tally' is full (errno: 28 - No space left on device)"}},
	}
	for _, c := range exceptions {
		c := c
		t.Run("exception/"+c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Errorf("documented AND-gate exception must stay retriable; got %T (%v)", got, got)
			}
		})
	}
}

// TestClassifyApplierError_RetriableShapes covers each documented
// transient shape from the ADR-0038 classifier table. Each must
// produce a value that (a) satisfies ir.RetriableError, (b) reports
// Retriable()==true, (c) preserves the original error via Unwrap.
func TestClassifyApplierError_RetriableShapes(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"InnoDB deadlock (Error 1213)", &gomysql.MySQLError{Number: 1213, Message: "Deadlock found when trying to get lock; try restarting transaction"}},
		{"InnoDB lock-wait-timeout (Error 1205, PS-320-v5 storage-grow contention)", &gomysql.MySQLError{Number: 1205, Message: "target: lst-mysql-d-ps320-v5.-.primary: vttablet: rpc error: code = DeadlineExceeded desc = Lock wait timeout exceeded; try restarting transaction"}},
		{"Vitess tx-killer Aborted (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Aborted desc = transaction 1234: in use: for tx killer rollback"}},
		{"Vitess Unknown (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unknown desc = caller id churn"}},
		{"Vitess Unavailable (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Unavailable desc = tablet not serving"}},
		{"Vitess ResourceExhausted (Error 1105)", &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = ResourceExhausted desc = throttler engaged"}},
		{"Vitess query-killer Canceled/TerminateAll (Error 1105, PS-320-v3 storage-grow finding)", &gomysql.MySQLError{Number: 1105, Message: "target: lst-mysql-d-ps320-v3.-.primary: vttablet: rpc error: code = Canceled desc = QueryList.TerminateAll(), elapsed time: 1m1.46075474s, killing connection ID 167 (CallerID: bnqr12v83ivogvozijwa)"}},
		{"target out of disk Error 3 errno-28 (PS-320-v4 storage-grow root face)", &gomysql.MySQLError{Number: 3, Message: "target: lst-mysql-d-ps320-v4.-.primary: vttablet: rpc error: code = Unknown desc = Error writing file '/vt/vtdataroot/vt_2760286790/tmp/MLfd=122' (OS errno 28 - No space left on device) (errno 3) (sqlstate HY000)"}},
		{"target out of disk ER_DISK_FULL 1021", &gomysql.MySQLError{Number: 1021, Message: "Disk full (/tmp); waiting for someone to free some space..."}},
		{"target table full ER_RECORD_FILE_FULL 1114 (PS-320-v6 storage-grow root variant)", &gomysql.MySQLError{Number: 1114, Message: "target: lst-mysql-d-ps320-v6.-.primary: vttablet: rpc error: code = ResourceExhausted desc = The table '_tally' is full (errno: 28 - No space left on device)"}},
		{"target transiently read-only ER_OPTION_PREVENTS_STATEMENT 1290 (PS-320-v10 grow/reparent face, the ADR-0110 live finding)", &gomysql.MySQLError{Number: 1290, Message: "target: lst-mysql-d-ps320-v10.-.primary: vttablet: rpc error: code = Code(17) desc = The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000) (CallerID: 0stqntpljpw3ts7gxjxr)"}},
		{"schema drift: unknown column 1054 (Bug F8)", &gomysql.MySQLError{Number: 1054, Message: "Unknown column 'soak_extra' in 'field list'"}},
		{"schema drift: no such table 1146 (Bug F8)", &gomysql.MySQLError{Number: 1146, Message: "Table 'soak.new_table' doesn't exist"}},
		{"driver.ErrBadConn", driver.ErrBadConn},
		{"io.EOF", io.EOF},
		{"gomysql.ErrInvalidConn (GitHub #21)", gomysql.ErrInvalidConn},
		{"wrapped gomysql.ErrInvalidConn (GitHub #21)", fmt.Errorf("mysql: applier: insert: %w", gomysql.ErrInvalidConn)},
		{"context.DeadlineExceeded (GitHub #23 per-exec timeout)", context.DeadlineExceeded},
		{"wrapped context.DeadlineExceeded (GitHub #23)", fmt.Errorf("mysql: applier: insert into x: %w", context.DeadlineExceeded)},
		{"wrapped driver.ErrBadConn", fmt.Errorf("query: %w", driver.ErrBadConn)},
		{"connection reset by peer", errors.New("write tcp: connection reset by peer")},
		{"connection refused", errors.New("dial tcp: connection refused")},
		{"broken pipe", errors.New("write tcp: broken pipe")},
		{"i/o timeout", errors.New("read tcp: i/o timeout")},
		// Bug 200: the Windows winsock dial wordings on the APPLY path —
		// a target restart's refused window surfaced at begin-tx and exited
		// with zero retries because only the POSIX wording was matched.
		{"Windows refused dial at begin tx (Bug 200)", errors.New("mysql: applier: pkForRedact: begin tx: dial tcp 127.0.0.1:3311: connectex: No connection could be made because the target machine actively refused it")},
		{"actively refused wording alone (Bug 200)", errors.New("dial: the target machine actively refused it")},
		{"dial connection timed out (Bug 200)", errors.New("dial tcp 10.0.0.9:3306: connect: connection timed out")},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if !errors.As(got, &re) {
				t.Fatalf("classifyApplierError did not produce ir.RetriableError; got %T (%v)", got, got)
			}
			if !re.Retriable() {
				t.Errorf("classified error's Retriable() = false; want true")
			}
			if !errors.Is(got, c.err) {
				t.Errorf("Unwrap chain broken: errors.Is(classified, original) = false")
			}
		})
	}
}

// TestClassifyApplierError_ConnectionLostErrno2013 pins the connection-lost
// family fix (bug175-repro live finding, 2026-07-02): a dropped tablet
// connection that vtgate surfaces as a MySQL ERR packet carrying errno 2013
// (CR_SERVER_LOST) / 2006 (CR_SERVER_GONE_ERROR) must classify RETRIABLE so
// the cold-copy reparent-retry (ADR-0108) re-acquires a fresh conn and rides
// the storage-grow reparent instead of aborting loudly on the first drop.
//
// This is the shape the pre-fix classifier MISSED: `desc = EOF` is text (not
// the io.EOF sentinel), the Number is 2013 (not 1105, so the vttablet
// gRPC-code branch never runs), and the reparent text fallback does not match.
//
// The companion assertions guard that the fix is keyed on the NUMBER and does
// NOT disturb the deliberate bare-`code = Canceled` client-cancel exclusion
// (v0.99.94): a client-side cancel — context.Canceled, or a 1105 message
// `code = Canceled desc = context canceled` — must stay TERMINAL. This is the
// "prove the pin catches the regression": the retriable shape flips to
// retriable, the still-terminal shapes stay terminal.
func TestClassifyApplierError_ConnectionLostErrno2013(t *testing.T) {
	retriable := []struct {
		name string
		err  error
	}{
		{
			name: "errno 2013 CR_SERVER_LOST — the live bug175-repro shape (code = Canceled desc = EOF)",
			err:  &gomysql.MySQLError{Number: 2013, Message: "target: bug175-repro.-.primary: vttablet: rpc error: code = Canceled desc = EOF (errno 2013) (sqlstate HY000) (CallerID: bnqr12v83ivogvozijwa): Sql: \"insert into events(...) values (...)\""},
		},
		{
			name: "errno 2013 wrapped by the flush closure",
			err:  fmt.Errorf("mysql: cold-copy flush: %w", &gomysql.MySQLError{Number: 2013, Message: "vttablet: rpc error: code = Canceled desc = EOF"}),
		},
		{
			name: "errno 2006 CR_SERVER_GONE_ERROR (the connection-lost sibling)",
			err:  &gomysql.MySQLError{Number: 2006, Message: "MySQL server has gone away"},
		},
	}
	for _, c := range retriable {
		c := c
		t.Run("retriable/"+c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Fatalf("errno-2013/2006 connection-lost shape must classify retriable for the ADR-0108 reparent-retry; got %T (%v)", got, got)
			}
			if !errors.Is(got, c.err) {
				t.Errorf("Unwrap chain broken: errors.Is(classified, original) = false")
			}
			// Connection-lost is a same-size retry (re-acquire a fresh conn),
			// NOT an oversized-tx signal — must not force an AIMD shrink.
			var tk ir.TransactionKilledError
			if errors.As(got, &tk) && tk.TransactionKilled() {
				t.Errorf("connection-lost wrongly flagged TransactionKilled(); it is a transport drop, not a tx-killer")
			}
		})
	}

	// STILL TERMINAL — the bare client-cancel exclusion (v0.99.94) is untouched
	// by the Number-2013 fix. Prove the pin fails to over-retry these.
	terminal := []struct {
		name string
		err  error
	}{
		{
			name: "context.Canceled client-cancel (clean shutdown) stays terminal",
			err:  context.Canceled,
		},
		{
			name: "1105 bare code = Canceled desc = context canceled stays terminal",
			err:  &gomysql.MySQLError{Number: 1105, Message: "vttablet: rpc error: code = Canceled desc = context canceled"},
		},
	}
	for _, c := range terminal {
		c := c
		t.Run("terminal/"+c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("%s wrongly classified retriable — the errno-2013 fix must not disturb the client-cancel exclusion", c.name)
			}
		})
	}
}

// TestClassifyApplierError_VitessNonTransientCodesNotRetriable covers
// the discriminator inside the Error-1105 branch: only Aborted /
// Unavailable / ResourceExhausted are transients. Other gRPC codes
// (InvalidArgument, FailedPrecondition, NotFound) represent terminal
// semantic errors and must NOT be retried — retrying would mask real
// bugs.
func TestClassifyApplierError_VitessNonTransientCodesNotRetriable(t *testing.T) {
	cases := []struct {
		name string
		msg  string
	}{
		{"InvalidArgument", "vttablet: rpc error: code = InvalidArgument desc = column 'foo' not in schema"},
		{"FailedPrecondition", "vttablet: rpc error: code = FailedPrecondition desc = primary readonly"},
		{"NotFound", "vttablet: rpc error: code = NotFound desc = keyspace 'unknown' not found"},
		{"PermissionDenied", "vttablet: rpc error: code = PermissionDenied desc = user lacks INSERT"},
		// A bare code=Canceled WITHOUT the server-side QueryList.TerminateAll
		// reason is a CLIENT-side cancel (clean shutdown) and MUST stay
		// terminal — only the specific server query-killer reason is retriable
		// (v0.99.94: do not blanket-retry code=Canceled).
		{"Canceled client-cancel (no TerminateAll) stays terminal", "vttablet: rpc error: code = Canceled desc = context canceled"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := &gomysql.MySQLError{Number: 1105, Message: c.msg}
			got := classifyApplierError(err)
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("Vitess non-transient %s wrongly classified as retriable; would mask real bugs", c.name)
			}
		})
	}
}

// TestClassifyApplierError_Error1105WithoutVttablet covers the bare
// "Error 1105" shape that some non-Vitess MySQL builds emit for
// HY000-generic errors. Only Vitess-tagged messages should be
// retriable — a generic HY000 without "vttablet" stays terminal.
func TestClassifyApplierError_Error1105WithoutVttablet(t *testing.T) {
	err := &gomysql.MySQLError{Number: 1105, Message: "Unknown error condition during apply"}
	got := classifyApplierError(err)
	var re ir.RetriableError
	if errors.As(got, &re) {
		t.Errorf("Error 1105 without vttablet message wrongly classified as retriable")
	}
}

// TestClassifyApplierError_TxKillerSetsTransactionKilled pins the
// v0.99.69 fix: a Vitess tx-killer abort (Error 1105 with the "tx
// killer" reason fragment) must classify as a retriable error that
// ALSO satisfies ir.TransactionKilledError with TransactionKilled()
// ==true — the signal the AIMD controller reads to shrink immediately.
// The other retriable 1105 shapes (Aborted-without-killer, Unknown,
// Unavailable, ResourceExhausted) stay retriable but report
// TransactionKilled()==false so a same-size retry rides them out.
func TestClassifyApplierError_TxKillerSetsTransactionKilled(t *testing.T) {
	cases := []struct {
		name       string
		msg        string
		wantKilled bool
	}{
		{
			name:       "tx-killer Aborted (the live v0.99.69 shape)",
			msg:        "target: lst-mysql-b.-.primary: vttablet: rpc error: code = Aborted desc = transaction 173: in use: in use: for tx killer rollback",
			wantKilled: true,
		},
		{
			name:       "Aborted without tx-killer (e.g. primary stepping down)",
			msg:        "vttablet: rpc error: code = Aborted desc = primary is stepping down",
			wantKilled: false,
		},
		{
			name:       "Unknown — retriable but not a tx-killer",
			msg:        "vttablet: rpc error: code = Unknown desc = caller id churn",
			wantKilled: false,
		},
		{
			name:       "ResourceExhausted — retriable but not a tx-killer",
			msg:        "vttablet: rpc error: code = ResourceExhausted desc = throttler engaged",
			wantKilled: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(&gomysql.MySQLError{Number: 1105, Message: c.msg})
			var re ir.RetriableError
			if !errors.As(got, &re) {
				t.Fatalf("not classified retriable; got %T", got)
			}
			var tk ir.TransactionKilledError
			if !errors.As(got, &tk) {
				t.Fatalf("classified error does not satisfy ir.TransactionKilledError; got %T", got)
			}
			if tk.TransactionKilled() != c.wantKilled {
				t.Errorf("TransactionKilled() = %v; want %v for %q", tk.TransactionKilled(), c.wantKilled, c.msg)
			}
		})
	}
}

// TestVitessTxKillerSubstrings_PinDown is the change-detector for the
// tx-killer discriminator, in the same spirit as
// TestVitessRetriableSubstrings_PinDown4. If Vitess ever reworded the
// tx-killer reason fragment ("for tx killer rollback"), this fails
// loudly — a maintainer must re-derive the fragment and update both the
// production slice and this pin. Without it, a reworded tx-killer abort
// would silently classify as a generic transient and re-open the
// v0.99.69 die-on-sustained-kill failure mode (re-submitting the same
// too-large batch every retry).
func TestVitessTxKillerSubstrings_PinDown(t *testing.T) {
	want := []string{"tx killer"}
	if len(vitessTxKillerSubstrings) != len(want) {
		t.Fatalf("vitessTxKillerSubstrings = %q; pin expects %q. If Vitess reworded the tx-killer reason, update both.",
			vitessTxKillerSubstrings, want)
	}
	for i, w := range want {
		if vitessTxKillerSubstrings[i] != w {
			t.Errorf("vitessTxKillerSubstrings[%d] = %q; want %q", i, vitessTxKillerSubstrings[i], w)
		}
	}
	// End-to-end: the live shape is a tx-killer; a bare Aborted is not.
	if !isVitessTxKillerMessage("vttablet: rpc error: code = Aborted desc = transaction 1: in use: for tx killer rollback") {
		t.Error("live tx-killer shape not detected by isVitessTxKillerMessage")
	}
	if isVitessTxKillerMessage("vttablet: rpc error: code = Aborted desc = primary stepping down") {
		t.Error("non-killer Aborted wrongly detected as tx-killer")
	}
	if isVitessTxKillerMessage("for tx killer rollback (no discriminator tag)") {
		t.Error("tx-killer fragment without the vttablet discriminator wrongly detected")
	}
}

// TestClassifyVitessMessage covers the leaf helper directly so the
// gRPC-code matching is testable without constructing a full
// MySQLError shell.
func TestClassifyVitessMessage(t *testing.T) {
	cases := []struct {
		msg  string
		want bool
	}{
		{"vttablet: rpc error: code = Aborted desc = ...", true},
		{"vttablet: rpc error: code = Unknown desc = ...", true},
		{"vttablet: rpc error: code = Unavailable desc = ...", true},
		{"vttablet: rpc error: code = ResourceExhausted desc = ...", true},
		{"vttablet: rpc error: code = InvalidArgument desc = ...", false},
		{"vttablet: rpc error: code = NotFound desc = ...", false},
		{"some other error", false},
		{"", false},
		{"code = Aborted desc = ... without the discriminator tag", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.msg, func(t *testing.T) {
			if got := classifyVitessMessage(c.msg); got != c.want {
				t.Errorf("classifyVitessMessage(%q) = %v; want %v", c.msg, got, c.want)
			}
		})
	}
}

// TestVitessRetriableSubstrings_PinDown4 is the MANDATORY test
// required by ADR-0038's Operator-review sign-off, pin-down 4:
//
//	"Vitess Error 1105 substring classification accepted as the
//	 pragmatic choice (Vitess wraps all transients in 1105 (HY000)
//	 with a free-text payload — no structured code exists to match
//	 on). Mandatory mitigation: a unit test that PINS THE EXACT
//	 MATCHED SUBSTRINGS (vttablet / code = Aborted / code = Unknown /
//	 code = Unavailable / code = ResourceExhausted) plus an inline
//	 comment + this ADR ref so a future Vitess wording change is
//	 caught by a failing test, not a silently-non-retried production
//	 error."
//
// This is a CHANGE-DETECTOR by design (it asserts on the literal
// match set, not behaviour). If Vitess ever changes its wire wording
// — e.g. emits "rpc status = ABORTED" instead of "code = Aborted",
// or drops the "vttablet" tag — this test fails LOUDLY. That is the
// intended signal: a maintainer must then re-derive the substring
// set against the new Vitess wording and update both
// vitessRetriableSubstrings and this pin together. Without this
// pin, the same wording drift would silently route a real
// PlanetScale tx-killer transient down the non-retriable path and
// exit the operator's stream — the exact GitHub #13 failure mode
// ADR-0038 exists to close.
func TestVitessRetriableSubstrings_PinDown4(t *testing.T) {
	// (a) The discriminator tag. Pinned as a standalone literal so a
	// rename of the Vitess component tag is caught independently of
	// the gRPC-code substrings.
	const discriminator = "vttablet"

	// (b) The EXACT four gRPC-code substrings ADR-0038 marks
	// retriable. This literal slice is intentionally duplicated from
	// production (vitessRetriableSubstrings) rather than referenced —
	// a pin that reads the value it guards cannot detect the value
	// changing. Order-independent equality is asserted below.
	wantCodeSubstrings := []string{
		"code = Aborted",
		"code = Unknown",
		"code = Unavailable",
		"code = ResourceExhausted",
		"QueryList.TerminateAll",
	}

	// Pin the production set length + membership against the literal
	// expectation. Adding/removing/renaming any production substring
	// without updating this test (and ADR-0038) fails here.
	if len(vitessRetriableSubstrings) != len(wantCodeSubstrings) {
		t.Fatalf("vitessRetriableSubstrings has %d entries %q; ADR-0038 pin-down 4 pins exactly %d %q. "+
			"If Vitess wording changed, update BOTH the production slice and this pin (and ADR-0038).",
			len(vitessRetriableSubstrings), vitessRetriableSubstrings,
			len(wantCodeSubstrings), wantCodeSubstrings)
	}
	got := make(map[string]bool, len(vitessRetriableSubstrings))
	for _, s := range vitessRetriableSubstrings {
		got[s] = true
	}
	for _, want := range wantCodeSubstrings {
		if !got[want] {
			t.Errorf("ADR-0038 pin-down 4: production vitessRetriableSubstrings is missing %q. "+
				"Got %q. A Vitess transient with this code would silently NON-retry.",
				want, vitessRetriableSubstrings)
		}
	}

	// (c) End-to-end: each pinned substring, combined with the
	// discriminator, MUST classify as a retriable Vitess transient
	// through the real classifier — and the discriminator alone (no
	// code) MUST NOT. This catches a regression where the slice is
	// correct but classifyVitessMessage stops consulting it.
	for _, code := range wantCodeSubstrings {
		msg := "target: ks.-.primary: " + discriminator + ": rpc error: " + code + " desc = transient"
		if !classifyVitessMessage(msg) {
			t.Errorf("classifyVitessMessage(%q) = false; ADR-0038 pin-down 4 requires this exact substring to be retriable", msg)
		}
	}
	if classifyVitessMessage(discriminator + ": rpc error: code = InvalidArgument desc = bad SQL") {
		t.Error("a non-pinned gRPC code (InvalidArgument) classified retriable — default-deny per ADR-0038 violated")
	}
	if classifyVitessMessage("rpc error: code = Aborted desc = no discriminator tag") {
		t.Errorf("missing %q discriminator still classified retriable — ADR-0038 pin-down 4 requires the tag", discriminator)
	}
}

// TestClassifyApplierError_ReparentSubstrings pins the ADR-0108 text
// fallback: an un-framed primary-reparent / "not serving" error (one
// WITHOUT the vttablet `code = Unavailable` framing the Vitess branch
// already catches) classifies as retriable, case-insensitively — so both
// the cold-copy reparent-retry (ADR-0108) and the CDC apply retry
// (ADR-0038) ride it out. An unrelated error stays terminal.
func TestClassifyApplierError_ReparentSubstrings(t *testing.T) {
	retriable := []struct {
		name string
		msg  string
	}{
		{"not serving (lower)", "tablet ks/-80 is not serving"},
		{"Not Serving (mixed case)", "ERROR: primary is Not Serving during failover"},
		{"reparent (lower)", "operation interrupted by emergency reparent"},
		{"Reparent (mixed case)", "PlanetScale: Planned Reparent in progress, retry shortly"},
	}
	for _, c := range retriable {
		c := c
		t.Run("retriable/"+c.name, func(t *testing.T) {
			got := classifyApplierError(errors.New(c.msg))
			var re ir.RetriableError
			if !errors.As(got, &re) || !re.Retriable() {
				t.Errorf("ADR-0108: %q should classify retriable; got %T (%v)", c.msg, got, got)
			}
		})
	}

	terminal := []struct {
		name string
		msg  string
	}{
		{"unrelated error stays terminal", "syntax error near 'FROM'"},
		{"serving (no 'not') stays terminal", "tablet is serving traffic normally"},
		{"parent (substring near-miss) stays terminal", "parent table missing for FK"},
	}
	for _, c := range terminal {
		c := c
		t.Run("terminal/"+c.name, func(t *testing.T) {
			got := classifyApplierError(errors.New(c.msg))
			var re ir.RetriableError
			if errors.As(got, &re) {
				t.Errorf("ADR-0108 default-deny: %q must stay terminal; got retriable", c.msg)
			}
		})
	}
}

// TestReparentRetriableSubstrings_PinDown is the change-detector for the
// ADR-0108 reparent-fallback match set, in the same discipline as
// TestVitessRetriableSubstrings_PinDown4. If a future Vitess/PlanetScale
// wording change drops "not serving" / "reparent", this fails LOUDLY so a
// maintainer re-derives the set rather than silently non-retrying a
// production reparent. The literals are duplicated from production (a pin
// that reads the value it guards cannot detect the value changing) and
// MUST be lower-case (the matcher lower-cases the error text first).
func TestReparentRetriableSubstrings_PinDown(t *testing.T) {
	want := []string{"not serving", "reparent"}
	if len(reparentRetriableSubstrings) != len(want) {
		t.Fatalf("reparentRetriableSubstrings = %q; ADR-0108 pins exactly %q. "+
			"If the reparent wording changed, update BOTH the production slice and this pin (and ADR-0108).",
			reparentRetriableSubstrings, want)
	}
	got := make(map[string]bool, len(reparentRetriableSubstrings))
	for _, s := range reparentRetriableSubstrings {
		if s != strings.ToLower(s) {
			t.Errorf("reparentRetriableSubstrings entry %q is not lower-case; the matcher lower-cases the error text, so a mixed-case literal can never match", s)
		}
		got[s] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("ADR-0108: reparentRetriableSubstrings is missing %q (got %q); a reparent with this phrasing would silently NON-retry", w, reparentRetriableSubstrings)
		}
	}
}

// TestClassifyApplierError_UnframedVtgateReparent is the gate for the
// 2026-07-28 live PlanetScale PS-160 finding (122 GB copy, 153M-row table):
// vtgate answers a query for a mid-reparent primary with
//
//	Error 1105 (HY000): target: scaletest-mysql.-.primary: primary is not
//	serving, there may be a reparent operation in progress
//
// The sentence is vtgate's OWN (vitess.io/vitess `go/vt/vtgate/buffer`
// ClusterEventReparentInProgress, raised at tabletgateway.go), so it carries
// no `vttablet` tag — [classifyVitessMessage] requires that tag by design and
// cannot match. Pre-fix the error fell straight to the terminal-code shield
// and ABORTED the copy after 38s with neither ADR-0108 bound reached (30m
// wall-clock, 100000 attempts), while the vttablet-framed sibling error in
// the SAME reparent window was ridden out. The ADR-0108 "belt-and-suspenders"
// text legs that claimed to cover this were unreachable for every structured
// *MySQLError from the moment the shield landed — a written invariant nobody
// checked.
//
// Table-driven over the three shapes the real reparent window produced, plus
// the terminal codes that must NOT move, plus the OVER-match negatives that
// keep the shield load-bearing: an echoed statement or identifier carrying
// the loose "reparent" / "not serving" tokens under a structured code must
// stay terminal (a false-positive transient arms the cold-copy
// tolerate-1062-on-retry wart — the D0-3 silent-batch-skip chain).
func TestClassifyApplierError_UnframedVtgateReparent(t *testing.T) {
	// The verbatim wire string from the live run, kept as one constant so
	// the pin cannot drift from what was observed.
	const unframedReparent = "target: scaletest-mysql.-.primary: primary is not serving, " +
		"there may be a reparent operation in progress"

	cases := []struct {
		name      string
		err       error
		retriable bool
	}{
		// --- the three errors the live PS-160 reparent window produced ---
		{
			"un-framed vtgate reparent (THE defect: aborted the 122 GB copy)",
			&gomysql.MySQLError{Number: 1105, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: unframedReparent},
			true,
		},
		{
			"vttablet-framed QueryList.TerminateAll in the same window (was already retried)",
			&gomysql.MySQLError{Number: 1105, Message: "target: scaletest-mysql.-.primary: vttablet: rpc error: " +
				"code = Canceled desc = QueryList.TerminateAll(), elapsed time: 1m2.1s, killing connection ID 204"},
			true,
		},
		{
			"1205 lock-wait-timeout in the same window (was already retried, code-only)",
			&gomysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"},
			true,
		},
		// --- run 2, clean PS-160: rode 18 transients, then died on THIS ---
		// Raised at tabletgateway.go:400 as Code_UNAVAILABLE, NOT
		// Code_CLUSTER_EVENT — which is exactly why it is absent from
		// buffer.ClusterEvents and why a set derived only from that list
		// missed it. Verbatim from the field capture.
		{
			"un-framed vtgate inconsistent-state (the SECOND field abort, ~7.1M rows in)",
			&gomysql.MySQLError{Number: 1105, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: "target: scaletest-my3.-.primary: " +
				"inconsistent state detected, primary is serving but initially found no available tablet"},
			true,
		},
		{
			// Upstream's own tabletgateway_flaky_test.go:349 pins this
			// sentence and the one above TOGETHER — "depending on whether the
			// health check ticks before or after the buffering code, we might
			// get different errors". Two faces of ONE race; matching one
			// without the other would be arbitrary.
			"un-framed vtgate no-healthy-tablet (the other face of the same race)",
			&gomysql.MySQLError{Number: 1105, Message: `target: ks.-80.primary: ` +
				`no healthy tablet available for 'keyspace:"ks" shard:"-80" tablet_type:PRIMARY'`},
			true,
		},
		// --- the sibling vtgate cluster event, same class, same code path ---
		{
			"un-framed vtgate resharding cutover (ClusterEventReshardingInProgress)",
			&gomysql.MySQLError{Number: 1105, Message: "target: scaletest-mysql.-.primary: current keyspace is being resharded"},
			true,
		},
		{
			"un-framed vtgate no-connection-for-tablet (VT14003)",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: no connection for tablet alias:{cell:\"zone1\" uid:101}"},
			true,
		},
		{
			// The buffer sentinels arrive inside the WaitForFailoverEnd wrap
			// (vterrors.Wrapf → `msg + ": " + cause`), so the sentinel text is
			// present in the final message. Shape reproduced here.
			"vtgate buffer full during a failover (buffer.go:48, via the WaitForFailoverEnd wrap)",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: failed to automatically buffer and retry " +
				"failed request during failover. original err (type=*vterrors.fundamental): <nil>: primary buffer is full"},
			true,
		},
		{
			"vtgate buffer eviction during a failover (buffer.go:49)",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: failed to automatically buffer and retry " +
				"failed request during failover. original err (type=*vterrors.fundamental): <nil>: buffer full: request evicted for newer request"},
			true,
		},
		{
			"vtgate destination shard missing after resharding (buffer.go:47)",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: destination shard is missing after a resharding operation"},
			true,
		},
		// --- REJECTED vtgate raises: availability-adjacent but must NOT retry ---
		{
			// buffer.go:50. Cancel-flavored — a CLIENT-side shutdown must stay
			// terminal, the same reason a bare `code = Canceled` is absent
			// from vitessRetriableSubstrings (v0.99.94). Note the surrounding
			// failover wrap does NOT rescue it: the wrap is deliberately not
			// in the match set precisely so this exclusion holds.
			"vtgate 'context was canceled before failover finished' stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: failed to automatically buffer and retry " +
				"failed request during failover. original err (type=*vterrors.fundamental): <nil>: context was canceled before failover finished"},
			false,
		},
		{
			// tabletgateway.go:349 — vtgate configuration, never self-heals.
			"vtgate disallowed tablet type (FAILED_PRECONDITION) stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.replica: requested tablet type REPLICA is not part of " +
				"the allowed tablet types for this vtgate: [PRIMARY]"},
			false,
		},
		{
			// tabletgateway.go:414 VT14002 — availability, but three generic
			// English words a migrated log row can carry, so it fails the
			// echo-safety rule this set is built on.
			"vtgate bare 'no available connection' (VT14002) stays terminal — echo-unsafe",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: no available connection"},
			false,
		},
		{
			// buffer.go:74 ClusterEventMoveTables — routing-rule denial.
			"vtgate MoveTables 'disallowed due to rule' stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: disallowed due to rule: enforce denied tables"},
			false,
		},
		// --- both consumers of the classifier see the wrapped form ---
		{
			"cold-copy flush frame (ADR-0108) around the un-framed reparent",
			fmt.Errorf("mysql: insert into %q (%d rows): %w", "events", 1000,
				&gomysql.MySQLError{Number: 1105, Message: unframedReparent}),
			true,
		},
		{
			"CDC apply frame (ADR-0038) around the un-framed reparent",
			fmt.Errorf("mysql: applier: begin tx: %w",
				&gomysql.MySQLError{Number: 1105, Message: unframedReparent}),
			true,
		},
		// --- the terminal codes must not move (shield still load-bearing) ---
		{
			"1062 duplicate key stays terminal",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '1179' for key 'events.PRIMARY'"},
			false,
		},
		{
			"1062 whose ECHOED ROW VALUE is the verbatim reparent sentence stays terminal",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '" + unframedReparent + "' for key 'incidents.title'"},
			false,
		},
		{
			"1062 whose ECHOED ROW VALUE is the verbatim inconsistent-state sentence stays terminal",
			&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry 'inconsistent state detected, primary is serving " +
				"but initially found no available tablet' for key 'incidents.title'"},
			false,
		},
		{
			"1064 syntax error stays terminal",
			&gomysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"},
			false,
		},
		{
			"1452 FK violation stays terminal",
			&gomysql.MySQLError{Number: 1452, Message: "Cannot add or update a child row"},
			false,
		},
		// --- narrowness: the loose tokens must NOT flip a structured 1105 ---
		{
			"1105 whose Sql echo names a table containing 'reparent' stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: " +
				"code = InvalidArgument desc = column x not found (CallerID: abc): " +
				`Sql: "insert into reparent_history(a) values (1)"`},
			false,
		},
		{
			"1105 echoing a stored row value 'not serving' stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: "internal error handling value 'not serving'"},
			false,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			gotRetriable := errors.As(got, &re) && re.Retriable()
			if gotRetriable != c.retriable {
				t.Fatalf("retriable=%v, want %v (classified %T: %v)", gotRetriable, c.retriable, got, got)
			}
			if !errors.Is(got, c.err) {
				t.Errorf("Unwrap chain broken: errors.Is(classified, original) = false")
			}
		})
	}

	// A reparent is a FAILOVER, not an oversized transaction: the same batch
	// re-applied against the new primary succeeds, so it must retry AT SIZE.
	// Flagging it TransactionKilled would force a needless AIMD shrink
	// (ADR-0052) on every reparent.
	got := classifyApplierError(&gomysql.MySQLError{Number: 1105, Message: unframedReparent})
	var tk ir.TransactionKilledError
	if errors.As(got, &tk) && tk.TransactionKilled() {
		t.Error("un-framed reparent reported TransactionKilled(); a failover must retry at size, not force an AIMD shrink")
	}
}

// TestVtgateTransientSubstrings_PinDown is the change-detector for the
// vtgate availability match set, in the same discipline as
// TestReparentRetriableSubstrings_PinDown. The literals are duplicated from
// production (a pin that reads the value it guards cannot detect the value
// changing) and MUST be lower-case (the matcher lower-cases first).
//
// Ground truth for the wording (vitess.io/vitess@v0.24.2), BOTH sources —
// deriving from the first alone is what shipped an incomplete set on
// 2026-07-28 and cost a second 122 GB run:
//
//   - `go/vt/vtgate/buffer/buffer.go:72-73` — the buffer.ClusterEvents
//     constants (Code_CLUSTER_EVENT).
//   - `go/vt/vtgate/tabletgateway.go:400,406,422` + `buffer.go:47-49` — the
//     Code_UNAVAILABLE raises in the SAME withRetry function, which upstream
//     does NOT list in buffer.ClusterEvents.
//
// If a Vitess upgrade rewords a sentence, this fails LOUDLY rather than
// silently non-retrying a production failover — exactly the failure mode
// this whole test file exists to prevent.
func TestVtgateTransientSubstrings_PinDown(t *testing.T) {
	want := []string{
		"primary is not serving",
		"reparent operation in progress",
		"current keyspace is being resharded",
		"inconsistent state detected, primary is serving",
		"no healthy tablet available for",
		"no connection for tablet",
		"primary buffer is full",
		"buffer full: request evicted for newer request",
		"destination shard is missing after a resharding operation",
		// FIELD-DERIVED (2026-08-04), not read off an upstream constant —
		// see the production slice for the provenance and the echo-safety
		// argument. vtgate's own transport-loss framing, the sibling of
		// the Number-2013 branch one wire framing over.
		"vtgate connection error",
	}
	if len(vtgateTransientSubstrings) != len(want) {
		t.Fatalf("vtgateTransientSubstrings = %q; pinned set is exactly %q. "+
			"If vtgate's wording changed, update BOTH the production slice and this pin.",
			vtgateTransientSubstrings, want)
	}
	got := make(map[string]bool, len(vtgateTransientSubstrings))
	for _, s := range vtgateTransientSubstrings {
		if s != strings.ToLower(s) {
			t.Errorf("vtgateTransientSubstrings entry %q is not lower-case; the matcher lower-cases the error text, so a mixed-case literal can never match", s)
		}
		got[s] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("vtgateTransientSubstrings is missing %q (got %q); a vtgate availability error with this phrasing would silently NON-retry", w, vtgateTransientSubstrings)
		}
	}

	// End-to-end through the real classifier, under the code vtgate actually
	// uses — catches the regression where the slice is right but nothing
	// consults it (the shape the original defect had).
	for _, canonical := range []string{
		"primary is not serving, there may be a reparent operation in progress",
		"current keyspace is being resharded",
		"inconsistent state detected, primary is serving but initially found no available tablet",
		`no healthy tablet available for 'keyspace:"ks" shard:"-80" tablet_type:PRIMARY'`,
	} {
		msg := "target: ks.-.primary: " + canonical
		out := classifyApplierError(&gomysql.MySQLError{Number: 1105, Message: msg})
		var re ir.RetriableError
		if !errors.As(out, &re) || !re.Retriable() {
			t.Errorf("Error 1105 carrying the canonical vtgate sentence %q must classify retriable; got %T", canonical, out)
		}
		// Case-insensitivity: vtgate/vttablet wording varies in case across
		// versions, and the matcher promises to absorb that.
		out = classifyApplierError(&gomysql.MySQLError{Number: 1105, Message: strings.ToUpper(msg)})
		if !errors.As(out, &re) || !re.Retriable() {
			t.Errorf("upper-cased %q must still classify retriable (the matcher lower-cases)", canonical)
		}
	}
}

// TestClassifyApplierError_BulkCopyReadDrop pins ADR-0109's reader-side
// classification contract: the RowReader.stream path wraps a mid-table
// connection-drop as `mysql: rows iteration: %w` and routes it through
// classifyApplierError so the sticky Err() carries an ir.RetriableError
// for the source-read reconnect-and-resume retry. A NON-connection
// iteration error (a decode/value fault) must stay terminal. This pins
// the EXACT shapes the reader produces — the wire path differs by the
// underlying driver error even though sluice's wrap is identical, so each
// connection-drop family is exercised, not one representative.
func TestClassifyApplierError_BulkCopyReadDrop(t *testing.T) {
	retriable := []error{
		fmt.Errorf("mysql: rows iteration: %w", gomysql.ErrInvalidConn),
		fmt.Errorf("mysql: rows iteration: %w", driver.ErrBadConn),
		fmt.Errorf("mysql: rows iteration: %w", io.EOF),
		fmt.Errorf("mysql: rows iteration: %w", errors.New("read tcp 10.0.0.1:3306: connection reset by peer")),
		fmt.Errorf("mysql: rows iteration: %w", errors.New("write tcp: broken pipe")),
		fmt.Errorf("mysql: rows iteration: %w", errors.New("dial tcp: i/o timeout")),
	}
	for _, in := range retriable {
		out := classifyApplierError(in)
		var re ir.RetriableError
		if !errors.As(out, &re) || !re.Retriable() {
			t.Errorf("connection-drop iteration error %q must classify retriable for the ADR-0109 source-read retry; got %T", in, out)
		}
	}

	// A non-connection iteration error (a real value fault that surfaced
	// during iteration) must NOT be retriable — the copy stays terminal.
	terminal := fmt.Errorf("mysql: rows iteration: %w", errors.New("invalid utf8 sequence in column data"))
	if re := classifyApplierError(terminal); func() bool { var r ir.RetriableError; return errors.As(re, &r) }() {
		t.Errorf("a non-connection iteration error must stay TERMINAL; got retriable for %q", terminal)
	}
}

// TestClassifyApplierError_GracefulGoAwayIn1105 pins the APPLY-path sibling of
// roadmap item 79. Vitess wraps the vtgate->vttablet gRPC status inside a
// MySQL Error 1105 message, so the same graceful drain that killed the VStream
// reader reaches the applier as an InvalidArgument-carrying 1105.
// InvalidArgument is deliberately absent from vitessRetriableSubstrings — a
// genuinely malformed statement must stay terminal — so before this the drain
// failed the batch instead of retrying it.
//
// Pin-the-class both ways, including the shield interaction: the retriable
// verdict must come from the structured 1105 code AND its message, never from
// a bare text scan over some other code.
func TestClassifyApplierError_GracefulGoAwayIn1105(t *testing.T) {
	const goawayDesc = `target: ks.-.primary: vttablet: rpc error: ` +
		`code = InvalidArgument desc = protocol error: incomplete envelope: ` +
		`http2: server sent GOAWAY and closed the connection; ` +
		`LastStreamID=1, ErrCode=NO_ERROR, debug="graceful_stop"`

	cases := []struct {
		name      string
		err       error
		retriable bool
	}{
		{
			"1105 wrapping a graceful GOAWAY is retriable",
			&gomysql.MySQLError{Number: 1105, Message: goawayDesc},
			true,
		},
		{
			"1105 wrapping a PROTOCOL_ERROR GOAWAY stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: `target: ks.-.primary: vttablet: rpc error: ` +
				`code = InvalidArgument desc = http2: server sent GOAWAY; ErrCode=PROTOCOL_ERROR`},
			false,
		},
		{
			// The GOAWAY leg lives behind the "vttablet" gate, exactly like
			// every other 1105 shape: a 1105 that is not vttablet-framed must
			// not be swept in.
			"1105 without vttablet framing stays terminal",
			&gomysql.MySQLError{Number: 1105, Message: `http2: server sent GOAWAY; ErrCode=NO_ERROR`},
			false,
		},
		{
			// TERMINAL-CODE SHIELD: a duplicate-key error whose message
			// happens to echo GOAWAY text (a row value, a table name) must
			// STAY terminal. 1062's semantics are code-only; the message is
			// never consulted. This is the audit D0-3 hazard — a terminal
			// code flipped retriable by text — and the reason the GOAWAY
			// check went inside the 1105 branch rather than beside the
			// transport-text legs.
			"1062 whose message echoes GOAWAY/NO_ERROR stays terminal",
			&gomysql.MySQLError{Number: 1062, Message: `Duplicate entry 'GOAWAY ErrCode=NO_ERROR' for key 'notes.PRIMARY'`},
			false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := classifyApplierError(c.err)
			var re ir.RetriableError
			if gotRetriable := errors.As(got, &re); gotRetriable != c.retriable {
				t.Errorf("retriable=%v, want %v (got %v)", gotRetriable, c.retriable, got)
			}
		})
	}
}
