package main

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
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
