package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestConfirmDestructiveAccepts(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantBool bool
	}{
		{"empty refuses", "\n", false},
		{"n refuses", "n\n", false},
		{"no refuses", "no\n", false},
		{"y accepts", "y\n", true},
		{"Y accepts", "Y\n", true},
		{"yes accepts", "yes\n", true},
		{"YES accepts", "YES\n", true},
		{"  y   with whitespace accepts", "  y  \n", true},
		{"random word refuses", "maybe\n", false},
		{"empty stream refuses", "", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			in := strings.NewReader(c.input)
			got, err := confirmDestructive(in, out, "Are you sure? ")
			if err != nil {
				t.Fatalf("confirmDestructive: %v", err)
			}
			if got != c.wantBool {
				t.Errorf("got %v; want %v", got, c.wantBool)
			}
			if !strings.Contains(out.String(), "Are you sure?") {
				t.Errorf("prompt not written to out: %q", out.String())
			}
		})
	}
}

func TestConfirmTypedDestructiveAccepts(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
		want     bool
	}{
		{"empty refuses", "\n", "reset", false},
		{"y refuses", "y\n", "reset", false},
		{"yes refuses", "yes\n", "reset", false},
		{"reset accepts", "reset\n", "reset", true},
		{"trim whitespace", "  reset  \n", "reset", true},
		{"case-sensitive: RESET refuses", "RESET\n", "reset", false},
		{"close-but-typo refuses", "rest\n", "reset", false},
		{"empty stream refuses", "", "reset", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			out := &bytes.Buffer{}
			in := strings.NewReader(c.input)
			got, err := confirmTypedDestructive(in, out, "Type 'reset' to confirm: ", c.expected)
			if err != nil {
				t.Fatalf("confirmTypedDestructive: %v", err)
			}
			if got != c.want {
				t.Errorf("got %v; want %v", got, c.want)
			}
			if !strings.Contains(out.String(), "Type 'reset' to confirm") {
				t.Errorf("prompt not written to out: %q", out.String())
			}
		})
	}
}

// TestSlotDropRefusesWithoutYes pins the non-interactive contract: an
// agent running `slot drop` without --yes must get a loud, coded
// refusal (exit 3) — never a prompt that reads EOF on a non-TTY and
// silently no-ops. The refusal fires before any source connection, so
// the test needs no database.
func TestSlotDropRefusesWithoutYes(t *testing.T) {
	cmd := &SlotDropCmd{
		SourceDriver: "postgres",
		Source:       "postgres://localhost/db",
		Name:         "myslot",
	}
	err := cmd.Run(nil)
	if err == nil {
		t.Fatal("slot drop without --yes must refuse, got nil error")
	}

	ce, ok := sluicecode.FromError(err)
	if !ok {
		t.Fatalf("want a CodedError, got %T: %v", err, err)
	}
	if ce.Code != sluicecode.CodeConfirmationRequired {
		t.Errorf("code: got %q, want %q", ce.Code, sluicecode.CodeConfirmationRequired)
	}
	if ce.ExitCode() != sluicecode.ExitRefusal {
		t.Errorf("exit code: got %d, want %d (ExitRefusal)", ce.ExitCode(), sluicecode.ExitRefusal)
	}
	if !strings.Contains(err.Error(), "myslot") {
		t.Errorf("refusal must name the slot; got %q", err.Error())
	}
}

// TestSlotDropProceedsWithYes pins that --yes clears the confirmation
// gate: Run gets past the refusal and reaches the source-connection
// path (which then fails on an unknown driver here — proving the gate
// was skipped, not that a drop succeeded).
func TestSlotDropProceedsWithYes(t *testing.T) {
	cmd := &SlotDropCmd{
		SourceDriver: "not-a-real-engine",
		Source:       "dsn",
		Name:         "myslot",
		Yes:          true,
	}
	err := cmd.Run(nil)
	if err == nil {
		t.Fatal("expected an engine-resolution error past the confirmation gate")
	}
	if ce, ok := sluicecode.FromError(err); ok && ce.Code == sluicecode.CodeConfirmationRequired {
		t.Fatalf("--yes must skip the confirmation refusal; got %v", err)
	}
}

func TestIsSlotNotFoundErr(t *testing.T) {
	if isSlotNotFoundErr(nil) {
		t.Error("nil error should not be slot-not-found")
	}
	if !isSlotNotFoundErr(fmt.Errorf("postgres: slot not found: %q", "x")) {
		t.Error("wrapped slot-not-found should match")
	}
	if isSlotNotFoundErr(errors.New("permission denied")) {
		t.Error("unrelated error should not match")
	}
}

func TestWALStatusOrDashRenders(t *testing.T) {
	if got := walStatusOrDash(""); got != "-" {
		t.Errorf("empty got %q; want -", got)
	}
	if got := walStatusOrDash("reserved"); got != "reserved" {
		t.Errorf("reserved got %q; want reserved", got)
	}
}

func TestLSNOrDashRenders(t *testing.T) {
	if got := lsnOrDash(""); got != "-" {
		t.Errorf("empty got %q; want -", got)
	}
	if got := lsnOrDash("0/16B7350"); got != "0/16B7350" {
		t.Errorf("lsn got %q; want passthrough", got)
	}
}
