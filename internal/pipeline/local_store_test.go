// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestLocalStore_PutGet(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	want := []byte("hello, sluice backup")
	if err := s.Put(context.Background(), "manifest.json", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	rc, err := s.Get(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q; want %q", got, want)
	}

	// Verify atomic write: there's no leftover .tmp.
	if _, err := os.Stat(filepath.Join(dir, "manifest.json.tmp")); !os.IsNotExist(err) {
		t.Errorf("leftover .tmp file: err=%v", err)
	}
}

// TestLocalStore_OwnerOnlyPermissions pins the 0600/0700 contract:
// backup chunks contain full row data and --encrypt is opt-in, so a
// world-readable backup dir hands any local user the dataset. Skipped
// on Windows, where Go approximates Unix permission bits.
func TestLocalStore_OwnerOnlyPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are approximated on Windows")
	}
	dir := t.TempDir()
	s, err := NewLocalStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := s.Put(context.Background(), "chunks/users/users-0.jsonl.gz", bytes.NewReader([]byte("row data"))); err != nil {
		t.Fatalf("Put: %v", err)
	}

	wantPerms := map[string]os.FileMode{
		filepath.Join(dir, "store"):                                        0o700,
		filepath.Join(dir, "store", "chunks", "users"):                     0o700,
		filepath.Join(dir, "store", "chunks", "users", "users-0.jsonl.gz"): 0o600,
	}
	for path, want := range wantPerms {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s: perm = %o; want %o", path, got, want)
		}
	}
}

func TestLocalStore_PutNestedDirectories(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	if err := s.Put(context.Background(), "chunks/users/users-0.jsonl.gz", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("Put nested: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "chunks", "users", "users-0.jsonl.gz"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "data" {
		t.Errorf("got %q", got)
	}
}

func TestLocalStore_List(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	files := []string{
		"manifest.json",
		"chunks/users/users-0.jsonl.gz",
		"chunks/users/users-1.jsonl.gz",
		"chunks/orders/orders-0.jsonl.gz",
	}
	for _, f := range files {
		if err := s.Put(context.Background(), f, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %s: %v", f, err)
		}
	}

	got, err := s.List(context.Background(), "chunks/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)
	want := []string{
		"chunks/orders/orders-0.jsonl.gz",
		"chunks/users/users-0.jsonl.gz",
		"chunks/users/users-1.jsonl.gz",
	}
	if !equalStrSlices(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}

	// Empty prefix returns everything.
	all, _ := s.List(context.Background(), "")
	if len(all) != len(files) {
		t.Errorf("List(\"\") returned %d; want %d", len(all), len(files))
	}
}

func TestLocalStore_DeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	_ = s.Put(context.Background(), "x", bytes.NewReader([]byte("y")))
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Second delete is a no-op.
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Errorf("second Delete: %v; want nil (idempotent)", err)
	}
}

func TestLocalStore_RejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	cases := []string{
		"../escape",
		"safe/../escape",
		"../../../etc/passwd",
	}
	for _, p := range cases {
		err := s.Put(context.Background(), p, bytes.NewReader([]byte("evil")))
		if err == nil {
			t.Errorf("Put(%q) succeeded; expected path-traversal rejection", p)
			continue
		}
		if !strings.Contains(err.Error(), "path traversal") && !strings.Contains(err.Error(), "escapes root") {
			t.Errorf("Put(%q) error = %v; want path-traversal message", p, err)
		}
	}
}

func TestLocalStore_Exists(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)

	// Absent path.
	exists, err := s.Exists(context.Background(), "absent.txt")
	if err != nil {
		t.Fatalf("Exists(absent): %v", err)
	}
	if exists {
		t.Errorf("Exists(absent) = true; want false")
	}

	// After Put.
	if err := s.Put(context.Background(), "present.txt", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, err = s.Exists(context.Background(), "present.txt")
	if err != nil {
		t.Fatalf("Exists(present): %v", err)
	}
	if !exists {
		t.Errorf("Exists(present) = false; want true")
	}

	// A directory is not a "blob" — Exists returns false.
	if err := os.MkdirAll(filepath.Join(dir, "somedir"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	exists, err = s.Exists(context.Background(), "somedir")
	if err != nil {
		t.Fatalf("Exists(dir): %v", err)
	}
	if exists {
		t.Errorf("Exists(dir) = true; want false (directories are not blobs)")
	}
}

func TestLocalStore_GetMissing(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	if _, err := s.Get(context.Background(), "nope"); err == nil {
		t.Errorf("Get(missing) returned nil err; want a clear error")
	}
}

func TestLocalStore_NewWithEmptyRoot(t *testing.T) {
	if _, err := NewLocalStore(""); err == nil {
		t.Errorf("NewLocalStore(\"\") = nil; want error")
	}
}

func TestLocalStore_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.Put(ctx, "x", bytes.NewReader([]byte("y")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Put with cancelled ctx: err = %v; want context.Canceled", err)
	}
}

func equalStrSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
