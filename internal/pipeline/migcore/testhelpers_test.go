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
