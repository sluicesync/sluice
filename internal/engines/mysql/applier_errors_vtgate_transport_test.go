// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two framings of ONE condition must get one verdict.
//
// A vtgate→tablet connection that dies mid-statement surfaces two ways
// depending on which layer reports it:
//
//	vttablet framing  target: k.-.primary: vttablet: rpc error:
//	                  code = Unavailable desc = connection reset by peer
//	vtgate framing    internal: vtgate connection error
//	                  (read: connection reset by peer)
//
// Both arrive as MySQL Error 1105 (HY000). sluice retried the first and
// treated the second as terminal, so the same underlying loss was survivable
// or fatal depending on wording — and the fatal one is what an operator hits
// on a cold copy. Field-reported 2026-08-04: a MariaDB→PlanetScale
// `sync` cold start failed 5 times out of 5, always on the widest table,
// always 90–130s in, at a varying chunk index (so: not one bad row).
//
// The miss is the sibling of the Number-2013 branch in classifyApplierError,
// whose own comment already named the shape — the transport cause is TEXT
// inside a *MySQLError message, so errors.Is(err, io.EOF) cannot see it, and
// under Number 1105 only the substring set is consulted. That branch was
// added for one framing and the sweep stopped there.
//
// One classifier serves BOTH consumers — flushWithReparentRetry (the
// cold-copy bulk path, ADR-0108) and the CDC apply retry loop both route
// through classifyApplierError — so this is a single-point fix reaching both,
// which TestVtgateTransportLoss_ReachesBothRetryConsumers asserts rather than
// assumes.

package mysql

import (
	"errors"
	"os"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// readPackageFile reads a source file from this package for the
// call-site-sharing assertion below.
func readPackageFile(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

func hy000(number uint16, msg string) error {
	return &gomysql.MySQLError{Number: number, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}, Message: msg}
}

func classifiedRetriable(err error) bool {
	var re ir.RetriableError
	return errors.As(classifyApplierError(err), &re) && re.Retriable()
}

func TestVtgateTransportLoss_BothFramingsAgree(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want bool
	}{
		{
			// The framing that already worked, kept as the reference: if
			// this ever stops being retriable the two are still equal, but
			// for the wrong reason, so it is asserted rather than assumed.
			name: "vttablet gRPC framing",
			msg:  "target: k.-.primary: vttablet: rpc error: code = Unavailable desc = connection reset by peer",
			want: true,
		},
		{
			name: "vtgate framing, connection reset",
			msg:  "internal: vtgate connection error (read: connection reset by peer)",
			want: true,
		},
		{
			// The same framing with a different transport tail. The anchor
			// is the vtgate framing, so the tail is free to vary — which is
			// the point of anchoring there rather than on the tail.
			name: "vtgate framing, unexpected EOF",
			msg:  "internal: vtgate connection error (read: unexpected EOF)",
			want: true,
		},
		{
			// ECHO-SAFETY. A 1105 message can carry an echoed statement, and
			// a false positive here is not merely a wasted retry: it arms
			// the cold-copy tolerate-1062-on-retry wart on the next attempt.
			// So the generic transport tail on its own must NOT match — only
			// vtgate's own product noun does.
			name: "echoed statement carrying the transport tail alone",
			msg:  `Sql: "insert into t (note) values ('connection reset by peer')"`,
			want: false,
		},
		{
			// The deliberate client-cancel exclusion stays excluded: a
			// client-side shutdown must remain terminal.
			name: "client cancel stays terminal",
			msg:  "code = Canceled desc = context canceled",
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifiedRetriable(hy000(1105, tc.msg)); got != tc.want {
				t.Fatalf("retriable = %v; want %v for 1105 %q\n\n"+
					"Two framings of one transport loss must get one verdict — the operator does not "+
					"choose which layer reports it.", got, tc.want, tc.msg)
			}
		})
	}
}

// TestVtgateTransportLoss_ReachesBothRetryConsumers is the sibling-sweep
// assertion. The defect this closes was reported against the cold-copy bulk
// path, but the CDC apply loop consults the same classifier, so a fix at the
// classifier reaches both — and a future refactor that gave either consumer
// its own copy of the policy would silently re-open half of it.
//
// It proves the sharing by SOURCE, because the two consumers cannot both be
// driven from a unit test without a live target: both call sites must name
// classifyApplierError.
func TestVtgateTransportLoss_ReachesBothRetryConsumers(t *testing.T) {
	for _, f := range []string{"row_writer_reparent_retry.go", "change_applier.go"} {
		src := readPackageFile(t, f)
		if !strings.Contains(src, "classifyApplierError") {
			t.Errorf("%s no longer routes through classifyApplierError.\n\n"+
				"Both retry consumers must share ONE transient policy. If this file grew its own, the "+
				"vtgate/vttablet framing agreement pinned above holds for only one of them, and which "+
				"one is invisible from here.", f)
		}
	}
}

// TestVitessTransportLoss_AndGateCoversUnknownWordings pins the half that
// exists because the field-reported literal is UNCONFIRMED: it appears
// nowhere in upstream Vitess v0.24.2, and the report's text reads as two
// messages merged, so sluice cannot rely on matching that sentence exactly.
//
// A message naming a Vitess component AND carrying a transport-loss phrase is
// a dropped connection whatever the surrounding prose. Requiring BOTH is what
// keeps an echoed statement from arming a false transient.
func TestVitessTransportLoss_AndGateCoversUnknownWordings(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want bool
	}{
		{
			// Wordings sluice has never seen, which is the point.
			name: "unknown vtgate prose + transport phrase",
			msg:  "internal: vtgate could not reach the tablet: read tcp 10.0.0.1:3306: connection reset by peer",
			want: true,
		},
		{
			name: "vttablet prose + unexpected EOF",
			msg:  "internal: vttablet stream ended: unexpected EOF",
			want: true,
		},
		{
			name: "vtgate + broken pipe",
			msg:  "internal: vtgate: write tcp: broken pipe",
			want: true,
		},
		{
			// NEITHER half alone may fire. A transport phrase with no Vitess
			// noun is exactly what an echoed statement looks like.
			name: "transport phrase with no Vitess noun",
			msg:  `Sql: "insert into logs (msg) values ('connection reset by peer')"`,
			want: false,
		},
		{
			// A Vitess noun with no transport phrase must fall through to the
			// literal availability set, which decides on its own terms.
			name: "Vitess noun with no transport phrase",
			msg:  "internal: vtgate rejected the query for an unrelated reason",
			want: false,
		},
		{
			// A bare "EOF" is deliberately NOT a token: three characters that
			// appear in ordinary prose and in migrated data.
			name: "bare EOF is not enough",
			msg:  "internal: vtgate says EOF",
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifiedRetriable(hy000(1105, tc.msg)); got != tc.want {
				t.Fatalf("retriable = %v; want %v for 1105 %q", got, tc.want, tc.msg)
			}
		})
	}
}
