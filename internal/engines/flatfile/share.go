// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package flatfile

import (
	"context"
	"os"
	"sync"
)

// stageShare is the stage-once handle (audit 2026-07-15 MED-P2): one
// staged temp SQLite copy per source path, shared by the schema and row
// readers instead of each open staging its own. Before it, a migrate
// staged the whole flat file TWICE and held both copies for the run —
// the orchestrator keeps the schema reader open run-long, so a 20 GB
// CSV cost ~40 GB of staging writes and ~40 GB of peak temp space.
//
// Ownership follows the ADR-0130 tempPath rules, refcounted: acquire
// bumps the count, each reader's Close releases exactly once, and the
// LAST release removes the file (so the row reader closing first never
// yanks the staged copy out from under the still-open schema reader).
// A later acquire after the count hits zero re-stages — the same
// re-stage-per-use semantics a fresh open always had.
type stageShare struct {
	mu    sync.Mutex
	files map[string]*sharedStagedFile // keyed by source path
}

// sharedStagedFile is one staged copy and its reader refcount.
type sharedStagedFile struct {
	path string
	refs int
}

// acquireStaged returns a staged copy of the flat file at dsn plus the
// release the reader must call on Close. Without a share (the registry's
// zero-value engine — WithFlatFileOptions, the CLI path, always installs
// one) it falls back to a fresh staged copy per open with a nil release:
// the sqlite staged reader then owns and removes it itself, exactly the
// pre-share behavior.
func (e Engine) acquireStaged(ctx context.Context, dsn string) (staged string, release func() error, err error) {
	if e.share == nil {
		staged, err = e.stage(ctx, dsn)
		return staged, nil, err
	}
	return e.share.acquire(ctx, e, dsn)
}

// acquire stages dsn on first use and hands out refcounted releases.
// The lock is held across the staging read: opens are sequential in
// practice (the orchestrator opens the schema reader, then the row
// reader), and holding it keeps a concurrent open from staging a
// duplicate copy of the same file.
func (s *stageShare) acquire(ctx context.Context, e Engine, dsn string) (staged string, release func() error, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[dsn]
	if f == nil {
		staged, err := e.stage(ctx, dsn)
		if err != nil {
			return "", nil, err
		}
		f = &sharedStagedFile{path: staged}
		if s.files == nil {
			s.files = map[string]*sharedStagedFile{}
		}
		s.files[dsn] = f
	}
	f.refs++
	return f.path, func() error { return s.release(dsn, f) }, nil
}

// release drops one reader's reference; the last one out removes the
// staged file. Each reader calls its release at most once (the sqlite
// readers' Close guards guarantee it).
func (s *stageShare) release(dsn string, f *sharedStagedFile) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f.refs--
	if f.refs > 0 {
		return nil
	}
	delete(s.files, dsn)
	return os.Remove(f.path)
}
