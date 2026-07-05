// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"io"
	"strings"
	"testing"
)

// TestPrefixedStore_RoundTrip pins the wrapper's transparency: writes
// + reads + lists against a prefixed store land at the prefixed path
// in the wrapped store, and the wrapper strips the prefix back off
// on List() so callers see relative paths.
func TestPrefixedStore_RoundTrip(t *testing.T) {
	inner := newMemStore()
	wrapped := NewPrefixedStore(inner, "rotated-1")

	if err := wrapped.Put(context.Background(), "manifest.json", strings.NewReader("hello")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Inner store should see the prefixed path.
	if exists, _ := inner.Exists(context.Background(), "rotated-1/manifest.json"); !exists {
		t.Errorf("inner store missing the prefixed path")
	}
	// Wrapper-side Get sees the relative path.
	rc, err := wrapped.Get(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	body, _ := io.ReadAll(rc)
	if string(body) != "hello" {
		t.Errorf("Get returned %q; want %q", body, "hello")
	}

	// List with empty-prefix returns relative paths.
	paths, err := wrapped.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(paths) != 1 || paths[0] != "manifest.json" {
		t.Errorf("List returned %v; want [manifest.json]", paths)
	}
}

// TestPrefixedStore_EmptyPrefixDegenerates pins the no-op path: an
// empty / "/" / "." prefix returns the inner store directly with no
// wrapping cost. Lets construction be unconditional ("we always wrap
// with prefix", and an empty prefix is the chain-root case).
func TestPrefixedStore_EmptyPrefixDegenerates(t *testing.T) {
	inner := newMemStore()
	cases := []string{"", "/", ".", "//", "./"}
	for _, p := range cases {
		got := NewPrefixedStore(inner, p)
		//nolint:errorlint // identity comparison is the assertion
		if got != inner {
			t.Errorf("NewPrefixedStore(_, %q) returned a wrapper; want inner unchanged", p)
		}
	}
}

// TestPrefixedStore_NestedList covers the List output transformation
// when the inner store returns paths that include sub-prefixes.
func TestPrefixedStore_NestedList(t *testing.T) {
	inner := newMemStore()
	wrapped := NewPrefixedStore(inner, "rotated-1")
	for _, p := range []string{"manifests/incr-1.json", "manifests/incr-2.json", "chunks/users/0.gz"} {
		_ = wrapped.Put(context.Background(), p, strings.NewReader("data"))
	}
	out, err := wrapped.List(context.Background(), "manifests/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("List(manifests/) returned %d paths; want 2", len(out))
	}
	for _, p := range out {
		if !strings.HasPrefix(p, "manifests/") {
			t.Errorf("listed path %q missing manifests/ prefix (should NOT include rotated-1/)", p)
		}
		if strings.Contains(p, "rotated-1/") {
			t.Errorf("listed path %q leaks the wrapper prefix", p)
		}
	}
}
