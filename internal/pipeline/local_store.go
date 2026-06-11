// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Local-filesystem implementation of [irbackup.BackupStore].
//
// This is the Phase 1 reference backend. Pure stdlib (`os` +
// `path/filepath` + `io.fs`); zero external dependencies. Phase 2 cloud
// backends (S3, GCS, Azure) implement the same interface so the
// orchestrator code in `internal/pipeline/backup.go` and
// `internal/pipeline/restore.go` doesn't change when those land.
//
// Path semantics:
//
//   - The store is rooted at a single directory operators name via
//     `--output-dir` / `--from-dir`.
//   - Paths passed to Put / Get / List / Delete are forward-slash-
//     separated and relative to that root. The store is responsible
//     for translating to OS-native conventions (Windows backslashes).
//   - Paths SHALL NOT contain `..` segments — the store rejects them
//     with a clear error to prevent a malicious / corrupted manifest
//     from writing outside the named directory.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// LocalStore is the local-filesystem implementation of
// [irbackup.BackupStore]. Construct with [NewLocalStore].
//
// Concurrent Put / Get on the same path is unsafe (the underlying
// `os.Create` truncates); Phase 1 backup orchestrator is sequential
// per table so this isn't a concern. Phase 2 / parallel backup will
// need to coordinate at a higher layer.
type LocalStore struct {
	root string
}

// NewLocalStore creates a [LocalStore] rooted at root. The directory
// is created if it doesn't exist (via `os.MkdirAll`); existing
// content is preserved (Put overwrites individual files but doesn't
// clean up siblings).
//
// Directories are 0700 and files 0600: backup chunks contain full row
// data and `--encrypt` is opt-in, so a world-readable backup dir would
// hand any local user the whole dataset. Owner-only is the safe
// default; operators who need group/other access can widen it on the
// directory themselves. (No effect on Windows, where os perm bits are
// approximated.)
func NewLocalStore(root string) (*LocalStore, error) {
	if root == "" {
		return nil, errors.New("local store: root directory is empty")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("local store: create root %q: %w", root, err)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("local store: resolve root %q: %w", root, err)
	}
	return &LocalStore{root: abs}, nil
}

// Root returns the absolute path of the store's root directory.
// Useful for log lines and tests.
func (s *LocalStore) Root() string { return s.root }

// Put implements [irbackup.BackupStore.Put]. The path is created relative
// to the store root; intermediate directories are created as needed.
// Existing content at the path is overwritten.
//
// Implementation note: writes go to a `.tmp` sibling first and are
// renamed in to avoid leaving partial files on disk if the process
// dies mid-write. Same atomic-write trick the schema-preview /
// verify output paths use.
func (s *LocalStore) Put(ctx context.Context, path string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	abs, err := s.absPath(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return fmt.Errorf("local store: mkdir for %q: %w", path, err)
	}
	tmp := abs + ".tmp"
	// 0600, not os.Create's 0644 — chunk contents are row data; see
	// the NewLocalStore doc comment.
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("local store: create %q: %w", tmp, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("local store: write %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("local store: sync %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("local store: close %q: %w", path, err)
	}
	if err := os.Rename(tmp, abs); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("local store: rename %q: %w", path, err)
	}
	return nil
}

// Get implements [irbackup.BackupStore.Get].
func (s *LocalStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	abs, err := s.absPath(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil, fmt.Errorf("local store: open %q: %w", path, err)
	}
	return f, nil
}

// List implements [irbackup.BackupStore.List]. Walks the directory rooted
// at the store's root and returns every regular file whose path
// (forward-slash separated, relative to root) starts with prefix.
//
// Order is filesystem-dependent (filepath.Walk visits in lexical
// order, which is good enough for "find every chunk under prefix"
// queries; callers that need a stable order sort).
func (s *LocalStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []string
	walkErr := filepath.WalkDir(s.root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		// Normalise to forward-slash regardless of host OS.
		rel = filepath.ToSlash(rel)
		if prefix == "" || strings.HasPrefix(rel, prefix) {
			out = append(out, rel)
		}
		return nil
	})
	if walkErr != nil {
		// A missing root directory is "no entries", not an error —
		// matches the contract cloud stores will follow.
		if errors.Is(walkErr, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("local store: list %q: %w", prefix, walkErr)
	}
	return out, nil
}

// Exists implements [irbackup.BackupStore.Exists]. Reports whether a regular
// file is present at path within the store root. Used by the resumable
// backup writer to skip re-uploading already-completed chunks.
func (s *LocalStore) Exists(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	abs, err := s.absPath(path)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("local store: stat %q: %w", path, err)
	}
	if info.IsDir() {
		return false, nil
	}
	return true, nil
}

// Delete implements [irbackup.BackupStore.Delete]. Idempotent — a missing
// path returns nil rather than an error.
func (s *LocalStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	abs, err := s.absPath(path)
	if err != nil {
		return err
	}
	if err := os.Remove(abs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("local store: delete %q: %w", path, err)
	}
	return nil
}

// absPath resolves a forward-slash relative path against the store's
// root and rejects path-traversal attempts. The resulting absolute
// path is guaranteed to live under the root (by string-prefix check
// after Clean) so a malicious manifest can't write to /etc/passwd
// via a `../../etc/passwd` chunk reference.
func (s *LocalStore) absPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("local store: empty path")
	}
	// Reject explicit `..` segments before any cleaning so the error
	// message is operator-actionable (Clean would silently absorb
	// them in some edge cases).
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", fmt.Errorf("local store: path traversal not allowed: %q", path)
		}
	}
	native := filepath.FromSlash(path)
	abs := filepath.Join(s.root, native)
	// Defence-in-depth: re-check the joined result is still rooted at
	// s.root in case a clever input slipped past the segment scan.
	rel, err := filepath.Rel(s.root, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("local store: path escapes root: %q", path)
	}
	return abs, nil
}

// Compile-time check that LocalStore satisfies irbackup.BackupStore.
var _ irbackup.BackupStore = (*LocalStore)(nil)
