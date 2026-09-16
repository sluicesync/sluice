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
// # Nine copies, and every one of them recorded a different real incident
//
// Before this package there were NINE hand-rolled copies of this type under
// four names — `lockedBuffer` (×3), `syncBuffer`, `syncLogBuffer` (×2),
// `syncBuf` and `safeBuffer` (×2). They had drifted apart in API: some
// exposed String only, some added Bytes, one added a WriteString with a
// deliberately non-standard signature, and only two of the nine made Bytes
// return a copy.
//
// Their doc comments are preserved here because each was written from a
// measured failure, and together they are the argument for the guard:
//
//   - a `BinlogSyncer` from an earlier subtest, still emitting after Close
//     (CI run 35043003665);
//   - a CDC open that runs on its own goroutine and blocks at slot creation by
//     design, so its WARN write races the test's poll;
//   - a JSON handler written from the streamer's pump, the orchestrator's main
//     goroutine and the test goroutine at once;
//   - the streamer, the CDC pump and go-mysql's binlogsyncer all logging while
//     the test read `buf.String()` — CI run 26134035839, latent since the
//     helper landed and exposed only when a longer-running pin arrived;
//   - a heartbeat goroutine logging on a ticker while the test polled
//     (surfaced by v0.48.0 CI, invisible locally because a CGO_ENABLED=0
//     Windows build silently disables `-race`);
//   - a pgtrigger pump writing WARN lines while the test read them;
//   - the grow-gate owner goroutine in migcore;
//   - a slot-health probe loop writing from its own goroutine.
//
// One of the nine was guarded pre-emptively, by an author who wrote that these
// tests do not log from goroutines "but the buffer is guarded anyway so a
// future one can". That is the right instinct and the reason this type is now
// the default rather than a remedy applied after a `-race` failure.
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
// String, Bytes, Len, Reset and WriteString — so it is a drop-in for the
// `bytes.Buffer` it replaces without reshaping the assertions around it.
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

// WriteString appends a string, mirroring [bytes.Buffer.WriteString].
//
// The signature is deliberately the standard one, and that choice is NOT free.
// One of the private copies this package replaced declared
// `WriteString(string)` with no return value, and its doc explained exactly
// why: errcheck carries a default exclusion for `bytes.Buffer.WriteString` BY
// TYPE, which a wrapper does not inherit, so every call site discarding the
// result must spell `_, _ =`. That prediction was right — golangci-lint
// flagged the one such call site in the tree as soon as this landed.
//
// The standard shape is kept anyway: a type whose entire claim is "drop-in for
// bytes.Buffer" should not quietly diverge from bytes.Buffer's own signature,
// and one `_, _ =` is a smaller cost than a method that looks like the
// standard library's and behaves differently.
func (b *Buffer) WriteString(s string) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.WriteString(s)
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
