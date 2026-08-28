// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Local-filesystem implementation of [irbackup.Store].
//
// This is the Phase 1 reference backend. Pure stdlib (`os` +
// `path/filepath` + `io.fs`); zero external dependencies. Phase 2 cloud
// backends (S3, GCS, Azure) implement the same interface so the
// orchestrator code in `internal/pipeline/backup/backup.go` and
// `internal/pipeline/backup/restore.go` doesn't change when those land.
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
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// LocalStore is the local-filesystem implementation of
// [irbackup.Store]. Construct with [NewLocalStore].
//
// Concurrent Puts to the same path are safe: each writes a
// uniquely-named temp file and renames it in, so the last COMPLETE
// rename wins (see Put's implementation note — the fixed-name temp
// this doc once warned about was the PUT-TMP-RACE torn-file bug,
// fixed 2026-08-26). A Get racing a Put observes either the old or
// the new complete bytes, never a blend.
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

// Put implements [irbackup.Store.Put]. The path is created relative
// to the store root; intermediate directories are created as needed.
// Existing content at the path is overwritten.
//
// Implementation note: writes go to a UNIQUELY-NAMED `.tmp.*` sibling
// first and are renamed in, so a process dying mid-write leaves no
// partial file at the final path. The tmp name is unique PER CALL
// (os.CreateTemp), not the fixed `abs + ".tmp"` it once was: with a
// shared tmp name, two concurrent Puts to the same key opened the SAME
// temp file with independent fd offsets — one writer's O_TRUNC plus the
// other's later chunks landing at its stale offset produced a TORN file
// (complete JSON + trailing garbage) that the rename then installed.
// Observed live as `decode "lineage.json": invalid character '}' after
// top-level value` when two simultaneous incremental extenders raced
// through the ADR-0160 claim→Put residual window (CI run 33000462974,
// caught by TestIncrementalBackup_SimultaneousExtenders_NeverForkTheChain).
// With unique tmp names each rename atomically installs one writer's
// COMPLETE bytes — last rename wins, never a blend.
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
	// A crash between CreateTemp and the rename orphans the unique temp
	// file forever (nothing else ever names it). Sweep STALE same-key
	// temps opportunistically on the next write of that key — bounded to
	// this key's siblings, best-effort (a sweep failure never fails the
	// write), and age-guarded so a live concurrent writer's fresh temp is
	// never removed.
	s.sweepStaleTemps(abs)
	// os.CreateTemp opens O_EXCL with 0600 perms — unique per call (the
	// concurrency requirement above) and never world-readable (chunk
	// contents are row data; see the NewLocalStore doc comment).
	f, err := os.CreateTemp(filepath.Dir(abs), filepath.Base(abs)+".tmp.*")
	if err != nil {
		return fmt.Errorf("local store: create temp for %q: %w", path, err)
	}
	tmp := f.Name()
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

// Append implements [irbackup.Appender] — the optional store
// capability the ADR-0086 progress-sidecar checkpoints ride on. The
// payload (one whole JSONL line per call, by the Appender contract) is
// appended via O_APPEND in a single write and fsynced, so each
// checkpoint is durable and O(1); a crash mid-call tears at most the
// final line, which [irbackup.ReplayProgress] tolerates by design.
//
// Deliberately NOT tmp+rename like Put: append-then-rename would
// re-copy the whole file per call — the exact O(N²) shape the sidecar
// exists to remove.
func (s *LocalStore) Append(ctx context.Context, path string, r io.Reader) error {
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
	// 0600 like Put — see the NewLocalStore doc comment.
	f, err := os.OpenFile(abs, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("local store: open for append %q: %w", path, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("local store: append %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("local store: sync %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("local store: close %q: %w", path, err)
	}
	return nil
}

// PutIfAbsent implements [irbackup.ConditionalPutter] — the optional
// create-only conditional write the ADR-0160 chain concurrent-writer
// guard rides on. O_EXCL makes the create itself the atomic claim: of
// any number of concurrent callers for one path, exactly one open
// succeeds and the rest fail with [irbackup.ErrPathExists].
//
// Deliberately NOT tmp+rename like Put: the exclusive create IS the
// arbitration, and renaming over the path would clobber a concurrent
// winner. A write failure after a successful create removes the file
// (best-effort) so a half-written claim doesn't squat on the slot.
func (s *LocalStore) PutIfAbsent(ctx context.Context, path string, r io.Reader) error {
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
	// Same crash-orphan sweep as Put: PutIfAbsent writes no temp file
	// itself, but a crashed PUT of the same key may have left one, and
	// this is the next write of that key.
	s.sweepStaleTemps(abs)
	// 0600 like Put — see the NewLocalStore doc comment.
	f, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("local store: %q: %w", path, irbackup.ErrPathExists)
		}
		return fmt.Errorf("local store: create-if-absent %q: %w", path, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(abs)
		return fmt.Errorf("local store: write %q: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(abs)
		return fmt.Errorf("local store: sync %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(abs)
		return fmt.Errorf("local store: close %q: %w", path, err)
	}
	return nil
}

// Get implements [irbackup.Store.Get].
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

// List implements [irbackup.Store.List]. Walks the directory rooted
// at the store's root and returns every regular file whose path
// (forward-slash separated, relative to root) starts with prefix.
//
// In-flight and crash-orphaned Put temp files ([isPutTempName]) are
// filtered out: they are never legitimate objects — the naming is
// Put's own os.CreateTemp pattern — and before this filter a crash
// mid-Put left a phantom entry in every subsequent listing forever
// (surfacing in verify sweeps and consumer walks). The files
// themselves are reclaimed by [LocalStore.sweepStaleTemps] on the next
// same-key write.
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
		if isPutTempName(d.Name()) {
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

// Exists implements [irbackup.Store.Exists]. Reports whether a regular
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

// Delete implements [irbackup.Store.Delete]. Idempotent — a missing
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

// localStoreTempSweepAge is how old a same-key `.tmp.*` sibling must be
// before [LocalStore.sweepStaleTemps] reclaims it. Any live writer's temp
// is seconds old (Put streams one object and renames); an hour-old temp is
// a crash orphan. Generous on purpose — the cost of waiting is a little
// disk, the cost of guessing low is deleting a slow concurrent writer's
// in-flight bytes out from under it (that writer's rename would then fail
// loudly, not corrupt — but there is no reason to cause it).
const localStoreTempSweepAge = time.Hour

// isPutTempName reports whether a file NAME (no directory) matches Put's
// os.CreateTemp pattern — `<base>.tmp.<digits>` — and is therefore an
// in-flight or crash-orphaned temp file, never a legitimate object. Tight
// to CreateTemp's own suffix shape (a non-empty all-digit tail) so an
// operator file that merely contains ".tmp." elsewhere is not swallowed.
func isPutTempName(name string) bool {
	i := strings.LastIndex(name, ".tmp.")
	if i < 0 {
		return false
	}
	tail := name[i+len(".tmp."):]
	if tail == "" {
		return false
	}
	for _, c := range tail {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// sweepStaleTemps best-effort removes crash-orphaned Put temp files for
// ONE key: `<abs>.tmp.<digits>` siblings older than
// [localStoreTempSweepAge]. Called by Put and PutIfAbsent before writing
// that key — the only moment such orphans are cheaply discoverable without
// a store-wide walk. Errors are deliberately swallowed: reclaiming an
// orphan must never fail or slow the write that triggered it, and an
// orphan that survives a failed sweep is retried on the key's next write.
func (s *LocalStore) sweepStaleTemps(abs string) {
	matches, err := filepath.Glob(abs + ".tmp.*")
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-localStoreTempSweepAge)
	for _, m := range matches {
		if !isPutTempName(filepath.Base(m)) {
			continue
		}
		info, err := os.Stat(m)
		if err != nil || info.IsDir() || info.ModTime().After(cutoff) {
			continue // fresh (a live writer's), vanished, or not a file
		}
		_ = os.Remove(m)
	}
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

// Compile-time checks that LocalStore satisfies irbackup.Store and the
// optional capabilities: append (progress sidecar) and conditional
// create (chain concurrent-writer guard).
var (
	_ irbackup.Store             = (*LocalStore)(nil)
	_ irbackup.Appender          = (*LocalStore)(nil)
	_ irbackup.ConditionalPutter = (*LocalStore)(nil)
)
