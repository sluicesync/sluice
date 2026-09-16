// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package logcapture

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// The guard's own pins. This type is about to have ~56 callers, so a guard
// that did not work would be WORSE than the unguarded buffer it replaces:
// every one of those sites would carry a false assurance.
//
// These run under `-race` in CI's Test job like any other unit test, which is
// where the concurrency assertions below actually earn their keep — without
// the detector they mostly prove the code does not panic.

// TestBuffer_ConcurrentWritesAndReads is the defect itself, in miniature: a
// writer still running while a reader reads. Unguarded, this is the data race
// CI caught on run 35043003665.
func TestBuffer_ConcurrentWritesAndReads(t *testing.T) {
	var b Buffer
	var wg sync.WaitGroup

	// Writers stand in for the stray goroutine a test does not own — a
	// syncer that keeps logging after Close, a pump, a blocked CDC open.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if _, err := b.Write([]byte("line\n")); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}()
	}
	// Readers stand in for the test's own assertions.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				_ = b.String()
				_ = b.Bytes()
				_ = b.Len()
			}
		}()
	}
	wg.Wait()

	if got, want := b.Len(), 4*200*len("line\n"); got != want {
		t.Errorf("Len after all writers finished = %d; want %d — writes were lost under contention", got, want)
	}
}

// TestBuffer_BytesReturnsACopy pins the one behaviour a later reader is most
// likely to "simplify" into a race.
//
// Returning the buffer's own backing array would move the hazard one layer
// down: the caller reads it after the lock is released, while a concurrent
// write reallocates or overwrites underneath them. Two of the three private
// copies this package replaced were safe here only by accident, because they
// exposed String (which copies) and never Bytes.
func TestBuffer_BytesReturnsACopy(t *testing.T) {
	var b Buffer
	if _, err := b.Write([]byte("original")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := b.Bytes()
	got[0] = 'X' // a caller mutating what it was handed

	if after := b.String(); after != "original" {
		t.Errorf("mutating the slice from Bytes() changed the buffer: %q — Bytes must return a COPY, "+
			"not b.buf.Bytes(); see the package doc", after)
	}

	// And the copy must not alias a LATER write either.
	before := b.Bytes()
	if _, err := b.Write([]byte("-more")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !bytes.Equal(before, []byte("original")) {
		t.Errorf("a slice returned by Bytes() changed after a subsequent Write: %q", before)
	}
}

// TestBuffer_AccessorsMatchBytesBuffer keeps the drop-in claim honest: the
// sweep swaps this type in for a bytes.Buffer at ~56 sites WITHOUT reshaping
// the assertions around it, so the accessors must behave the same.
func TestBuffer_AccessorsMatchBytesBuffer(t *testing.T) {
	var guarded Buffer
	var plain bytes.Buffer

	for _, s := range []string{"alpha\n", "beta\n", strings.Repeat("x", 300)} {
		if _, err := guarded.Write([]byte(s)); err != nil {
			t.Fatalf("Write: %v", err)
		}
		plain.WriteString(s)
	}
	if guarded.String() != plain.String() {
		t.Errorf("String diverges from bytes.Buffer")
	}
	if !bytes.Equal(guarded.Bytes(), plain.Bytes()) {
		t.Errorf("Bytes diverges from bytes.Buffer")
	}
	if guarded.Len() != plain.Len() {
		t.Errorf("Len = %d; bytes.Buffer = %d", guarded.Len(), plain.Len())
	}

	guarded.Reset()
	plain.Reset()
	if guarded.Len() != 0 || guarded.String() != "" || plain.Len() != 0 {
		t.Errorf("Reset left %d bytes (%q)", guarded.Len(), guarded.String())
	}

	// Usable again after Reset — the two-phase capture some sites do.
	if _, err := guarded.Write([]byte("second phase")); err != nil {
		t.Fatalf("Write after Reset: %v", err)
	}
	if guarded.String() != "second phase" {
		t.Errorf("after Reset + Write = %q; want %q", guarded.String(), "second phase")
	}
}

// TestBuffer_ZeroValueIsReady pins the doc's claim, since every call site
// relies on it: `var b Buffer` or `&Buffer{}` with no constructor.
func TestBuffer_ZeroValueIsReady(t *testing.T) {
	b := &Buffer{}
	if b.Len() != 0 || b.String() != "" || len(b.Bytes()) != 0 {
		t.Fatalf("zero value not empty: len=%d str=%q", b.Len(), b.String())
	}
	if _, err := b.Write([]byte("ok")); err != nil {
		t.Fatalf("Write on zero value: %v", err)
	}
	if b.String() != "ok" {
		t.Errorf("zero value after Write = %q", b.String())
	}
}
