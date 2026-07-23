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
