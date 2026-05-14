// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"io"
	"path"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// prefixedStore wraps an [ir.BackupStore] with a path prefix that's
// transparently prepended to every operation. Used by the rotate-at
// rotation (GitHub #20 chunk 14b) so a new chain landing in
// `<output-dir>/rotated-<unix-ms>/` can reuse the existing
// [Backup.Run] machinery against the parent store without anyone
// downstream having to know about the rotation subdirectory.
//
// The wrapper handles:
//
//   - `Put` / `Get` / `Exists` / `Delete` against `<prefix>/<path>`
//   - `List(p)` against `<prefix>/<p>` with the prefix stripped on
//     the way out so callers see relative paths just like a bare
//     [BackupStore] would return.
//
// Construction is via [newPrefixedStore]; the prefix is normalised
// (no leading/trailing slashes) and an empty prefix degenerates to
// the wrapped store unchanged.
type prefixedStore struct {
	inner  ir.BackupStore
	prefix string // canonical form: no leading/trailing "/", may be empty
}

// newPrefixedStore wraps inner with a path prefix. An empty / "/" /
// "." prefix returns the inner store directly (no wrapping cost
// when the rotation is at chain root).
func newPrefixedStore(inner ir.BackupStore, prefix string) ir.BackupStore {
	canon := strings.Trim(path.Clean(prefix), "/")
	if canon == "" || canon == "." {
		return inner
	}
	return &prefixedStore{inner: inner, prefix: canon}
}

// fullPath joins the wrapper's prefix with the caller's path.
// Always returns a forward-slash-separated string per the
// [ir.BackupStore] path convention.
func (s *prefixedStore) fullPath(p string) string {
	if p == "" {
		return s.prefix
	}
	return s.prefix + "/" + p
}

func (s *prefixedStore) Put(ctx context.Context, p string, r io.Reader) error {
	return s.inner.Put(ctx, s.fullPath(p), r)
}

func (s *prefixedStore) Get(ctx context.Context, p string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, s.fullPath(p))
}

func (s *prefixedStore) List(ctx context.Context, prefix string) ([]string, error) {
	listPrefix := s.fullPath(prefix)
	raw, err := s.inner.List(ctx, listPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(raw))
	stripLen := len(s.prefix) + 1 // +1 for the separator slash
	for _, p := range raw {
		if !strings.HasPrefix(p, s.prefix+"/") {
			// Shouldn't happen — inner.List(<prefix>/X) only
			// returns paths starting with <prefix>/. Defence-in-
			// depth: pass through unchanged rather than slice an
			// out-of-bounds index.
			out = append(out, p)
			continue
		}
		out = append(out, p[stripLen:])
	}
	return out, nil
}

func (s *prefixedStore) Delete(ctx context.Context, p string) error {
	return s.inner.Delete(ctx, s.fullPath(p))
}

func (s *prefixedStore) Exists(ctx context.Context, p string) (bool, error) {
	return s.inner.Exists(ctx, s.fullPath(p))
}
