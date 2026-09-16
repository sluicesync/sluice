// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package logcapture provides the one goroutine-safe sink a test uses when it
// captures slog output.
//
// # Why this exists
//
// `slog.SetDefault` is PROCESS-WIDE. A test that points the default logger at
// a plain `bytes.Buffer` and then reads it races every goroutine still logging
// anywhere in the test binary — including goroutines the test does not own and
// cannot join.
//
// That is not hypothetical. CI's `-race` shard caught it on run 35043003665:
// a helper in the MySQL engine read `buf.String()` while a go-mysql
// `BinlogSyncer` started by an EARLIER subtest was still writing INFO lines
// through `handleEventAndACK`. The syncer keeps logging after `Close()`
// returns, so there was nothing to wait for. Guarding the buffer was the only
// fix available, and it holds for any stray logger rather than just the one
// that run happened to expose (audit A0915-LOGCAPTURE-1).
//
// The helper's own comment had asserted "the callers are not parallel" — true,
// and beside the point. The racing writer was never a caller.
//
// # Three real mechanisms, not one abstract rule
//
// Before this package, three separate copies of this type existed, each
// written for a different concrete hazard, and between them they are the
// argument for the guard:
//
//   - a `BinlogSyncer` from an earlier subtest, still emitting after Close;
//   - a CDC open that runs on its own goroutine and blocks at slot creation by
//     design, so its WARN write races the test's poll;
//   - a JSON handler written from the streamer's pump, the orchestrator's main
//     goroutine and the test goroutine at once.
//
// # Bytes copies, and that is load-bearing
//
// [Buffer.Bytes] returns a COPY. Handing out the underlying slice would move
// the race one layer down: the caller reads it after the lock is released
// while a concurrent write reallocates or overwrites underneath them. The
// string accessors are safe for the same reason by accident — converting to a
// string copies — so the copy in Bytes is the only place this has to be
// deliberate. Do not "simplify" it to `return b.buf.Bytes()`.
//
// # What this does NOT do
//
// It does not make capturing correct on its own. A test that installs the
// default logger and never restores it still leaks that handler into every
// later test in the binary; restore with `defer` or `t.Cleanup` as before.
// And it cannot order a stray goroutine's writes against the read — it only
// guarantees the read is not torn. A capture that races a still-running
// producer may legitimately see a partial transcript; assert on presence of
// what must be there, not on absence of what must not.
package logcapture

import (
	"bytes"
	"sync"
)

// Buffer is a mutex-guarded [bytes.Buffer], safe for a logger writing from one
// goroutine while a test reads from another. The zero value is ready to use.
//
// It carries the accessor set the capture sites in this repo actually call —
// String, Bytes, Len and Reset — so it is a drop-in for the `bytes.Buffer` it
// replaces without reshaping the assertions around it.
type Buffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements [io.Writer]. This is the half a slog handler calls, from
// whichever goroutine happens to be logging.
func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String returns the captured output. Safe to call while a logger is still
// writing; the conversion copies, so the result cannot be mutated afterwards.
func (b *Buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Bytes returns a COPY of the captured output.
//
// The copy is deliberate: returning the buffer's own backing array would let
// the caller read it after the lock is released, racing any concurrent write
// that reallocates or overwrites it. See the package doc.
func (b *Buffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, b.buf.Len())
	copy(out, b.buf.Bytes())
	return out
}

// Len returns the number of bytes captured so far.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// Reset discards the captured output, so one test can assert on a first phase
// and then on a second without a fresh capture.
func (b *Buffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}
