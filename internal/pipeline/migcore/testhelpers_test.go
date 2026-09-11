// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"
)

// safeBuffer is a mutex-guarded bytes.Buffer so a slog handler writing
// from the grow-gate owner goroutine never races the test reading it.
// A local copy of pipeline-root's identical helper — a private test
// helper does not cross a package boundary (the blobcodec-carve convention).
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *safeBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Bytes()
}

func (s *safeBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// WriteString exists because a test appends its own marker into the same
// buffer the slog handler writes to. Going through the lock matters as much
// here as on the handler's path: the point of this type is that there is ONE
// serialized writer to the underlying buffer, and a test-side append that
// bypassed the mutex would reintroduce exactly the race the type removes.
//
// It returns nothing on purpose. bytes.Buffer's WriteString returns
// `(int, error)` that is documented never to fail, and errcheck carries a
// default exclusion for it BY TYPE — which a wrapper does not inherit, so the
// (int, error) shape would make every call site write `_, _ =` for a value
// that cannot be anything else. Dropping the returns says the same thing
// without the noise.
func (s *safeBuffer) WriteString(str string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.WriteString(str)
}

// captureSlog redirects the default slog logger to a mutex-guarded buffer
// for the duration of the test, restoring it on cleanup. Returns the
// buffer so a test can assert on emitted log lines.
func captureSlog(t *testing.T) *safeBuffer {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	buf := &safeBuffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf
}
