package main

import (
	"errors"
	"fmt"
	"testing"
)

// TestIsStreamNotFoundErr mirrors TestIsSlotNotFoundErr: the helper
// substring-matches a wrapped engine sentinel so the CLI can branch
// to a friendly "no stream X on target" message instead of bleeding
// engine-specific error text to the operator.
func TestIsStreamNotFoundErr(t *testing.T) {
	if isStreamNotFoundErr(nil) {
		t.Error("nil error should not be stream-not-found")
	}
	if !isStreamNotFoundErr(fmt.Errorf("postgres: stream not found: %q", "x")) {
		t.Error("wrapped postgres stream-not-found should match")
	}
	if !isStreamNotFoundErr(fmt.Errorf("mysql: stream not found: %q", "x")) {
		t.Error("wrapped mysql stream-not-found should match")
	}
	if isStreamNotFoundErr(errors.New("permission denied")) {
		t.Error("unrelated error should not match")
	}
}
