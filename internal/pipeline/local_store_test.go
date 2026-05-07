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
