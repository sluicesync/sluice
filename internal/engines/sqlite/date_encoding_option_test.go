// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestWithDateEncoding_Precedence pins the task-2.5 per-instance --sqlite-date-
// encoding option (replacing SetDefaultDateEncoding): the builder validates the
// value onto the engine, and foldDateEncoding resolves the per-source DSN param >
// engine default > ISO. This is the sqlite twin of the mysql --zero-date fold.
func TestWithDateEncoding_Precedence(t *testing.T) {
	// The builder maps each accepted value; a bad one refuses loudly.
	cases := []struct {
		in   string
		want dateEncoding
	}{
		{"", dateEncodingISO},
		{"iso", dateEncodingISO},
		{"unixepoch", dateEncodingUnixEpoch},
		{"unixmillis", dateEncodingUnixMillis},
		{"julian", dateEncodingJulian},
	}
	for _, c := range cases {
		e, err := Engine{}.WithDateEncoding(c.in)
		if err != nil {
			t.Fatalf("WithDateEncoding(%q): %v", c.in, err)
		}
		if got := e.(Engine).dateEncoding; got != c.want {
			t.Errorf("WithDateEncoding(%q): engine dateEncoding = %v; want %v", c.in, got, c.want)
		}
	}
	if _, err := (Engine{}).WithDateEncoding("bogus"); err == nil {
		t.Error("WithDateEncoding(\"bogus\") err = nil; want a loud refusal")
	}

	// foldDateEncoding precedence: DSN param wins; unset DSN falls back to the
	// engine default; both unset → inherit (which resolveDateEncoding → ISO).
	if got := foldDateEncoding(dateEncodingUnixEpoch, dateEncodingJulian); got != dateEncodingUnixEpoch {
		t.Errorf("DSN unixepoch over engine julian: got %v; want unixepoch", got)
	}
	if got := foldDateEncoding(dateEncodingInherit, dateEncodingJulian); got != dateEncodingJulian {
		t.Errorf("unset DSN falls back to engine julian: got %v; want julian", got)
	}
	if got := foldDateEncoding(dateEncodingInherit, dateEncodingInherit); got != dateEncodingInherit {
		t.Errorf("both unset stays inherit: got %v; want inherit", got)
	}
	if got := resolveDateEncoding(dateEncodingInherit); got != dateEncodingISO {
		t.Errorf("resolveDateEncoding(inherit) = %v; want ISO (the decode-time default)", got)
	}

	// d1Engine carries the same per-instance option (applied by the CLI via the
	// same structural interface). It validates and refuses loudly too.
	d1, ok := NewD1Engine().(interface {
		WithDateEncoding(string) (ir.Engine, error)
	})
	if !ok {
		t.Fatal("d1 engine must implement WithDateEncoding for the CLI to apply --sqlite-date-encoding")
	}
	if _, err := d1.WithDateEncoding("unixmillis"); err != nil {
		t.Errorf("d1 WithDateEncoding(unixmillis): %v", err)
	}
	if _, err := d1.WithDateEncoding("bogus"); err == nil {
		t.Error("d1 WithDateEncoding(\"bogus\") err = nil; want a loud refusal")
	}
}
