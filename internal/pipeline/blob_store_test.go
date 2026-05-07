// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// fileBlobURL builds a file:// URL gocloud's fileblob driver can open.
// On Windows the absolute path begins with `C:\...` which has to be
// translated to `/C:/...` for a valid URL host-or-path pair.
func fileBlobURL(t *testing.T, dir string) string {
	t.Helper()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	abs = filepath.ToSlash(abs)
	if runtime.GOOS == "windows" {
		// `C:\Users\...` → `file:///C:/Users/...`
		return "file:///" + abs
	}
	return "file://" + abs
}

func TestBlobStore_PutGetRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	want := []byte("hello, sluice phase 2")
	if err := store.Put(context.Background(), "manifest.json", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := store.Get(context.Background(), "manifest.json")
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
}

func TestBlobStore_ExistsAndDelete(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	defer func() { _ = store.Close() }()

	exists, err := store.Exists(context.Background(), "absent.txt")
	if err != nil {
		t.Fatalf("Exists(absent): %v", err)
	}
	if exists {
		t.Errorf("Exists(absent) = true; want false")
	}

	if err := store.Put(context.Background(), "present.txt", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, err = store.Exists(context.Background(), "present.txt")
	if err != nil {
		t.Fatalf("Exists(present): %v", err)
	}
	if !exists {
		t.Errorf("Exists(present) = false; want true")
	}

	if err := store.Delete(context.Background(), "present.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Idempotent.
	if err := store.Delete(context.Background(), "present.txt"); err != nil {
		t.Errorf("Delete (idempotent): %v", err)
	}
}

func TestBlobStore_List(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	defer func() { _ = store.Close() }()

	files := []string{
		"manifest.json",
		"chunks/users/users-0.jsonl.gz",
		"chunks/users/users-1.jsonl.gz",
		"chunks/orders/orders-0.jsonl.gz",
	}
	for _, f := range files {
		if err := store.Put(context.Background(), f, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %s: %v", f, err)
		}
	}

	got, err := store.List(context.Background(), "chunks/")
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
}

func TestBlobStore_RejectsPathTraversal(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	defer func() { _ = store.Close() }()

	for _, p := range []string{"../escape", "safe/../escape", "../../../etc/passwd"} {
		err := store.Put(context.Background(), p, bytes.NewReader([]byte("evil")))
		if err == nil {
			t.Errorf("Put(%q) succeeded; want path-traversal rejection", p)
			continue
		}
		if !strings.Contains(err.Error(), "path traversal") {
			t.Errorf("Put(%q) err = %v; want path-traversal message", p, err)
		}
	}
}

func TestBlobStore_GetMissingReturnsClearError(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	defer func() { _ = store.Close() }()

	_, err := store.Get(context.Background(), "absent")
	if err == nil {
		t.Fatal("Get(absent) returned nil err; want a clear error")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Get(absent) err = %v; want 'not found' wording", err)
	}
}

func TestBlobStore_OpenRejectsEmptyURL(t *testing.T) {
	if _, err := OpenBlobStore(context.Background(), "", BlobStoreOptions{}); err == nil {
		t.Error("OpenBlobStore(\"\") = nil; want error")
	}
}

func TestBlobStore_S3OptsOnNonS3URLIsAnError(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		opts BlobStoreOptions
	}{
		{"endpoint", BlobStoreOptions{Endpoint: "http://localhost:9000"}},
		{"region", BlobStoreOptions{Region: "us-east-1"}},
		{"path style", BlobStoreOptions{PathStyle: true}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := OpenBlobStore(context.Background(), fileBlobURL(t, dir), c.opts)
			if err == nil {
				t.Errorf("expected error for non-s3 URL with %s opt; got nil", c.name)
				return
			}
			if !strings.Contains(err.Error(), "s3://") {
				t.Errorf("err = %v; want mention of s3:// requirement", err)
			}
		})
	}
}

func TestBlobStore_AnnotateURL_S3Params(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		opts    BlobStoreOptions
		want    []string
		notWant []string
	}{
		{
			name: "endpoint sets hostname_immutable too",
			in:   "s3://bucket/prefix",
			opts: BlobStoreOptions{Endpoint: "http://minio:9000"},
			want: []string{"endpoint=http%3A%2F%2Fminio%3A9000", "hostname_immutable=true"},
		},
		{
			name: "all three opts",
			in:   "s3://bucket/prefix",
			opts: BlobStoreOptions{Endpoint: "http://minio:9000", Region: "us-east-1", PathStyle: true},
			want: []string{"endpoint=", "region=us-east-1", "use_path_style=true"},
		},
		{
			name:    "no opts leaves URL alone",
			in:      "s3://bucket/prefix",
			opts:    BlobStoreOptions{},
			want:    []string{"s3://bucket/prefix"},
			notWant: []string{"endpoint", "region", "use_path_style"},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := annotateBlobURL(c.in, c.opts)
			if err != nil {
				t.Fatalf("annotateBlobURL: %v", err)
			}
			for _, frag := range c.want {
				if !strings.Contains(got, frag) {
					t.Errorf("got %q; want substring %q", got, frag)
				}
			}
			for _, frag := range c.notWant {
				if strings.Contains(got, frag) {
					t.Errorf("got %q; should not contain %q", got, frag)
				}
			}
		})
	}
}

func TestBlobStore_GCSAndAzureURLsAccepted(t *testing.T) {
	// Smoke test: confirm gocloud's URL-scheme registry routes gs://
	// and azblob:// to the side-effect-imported drivers. Without
	// credentials available, OpenBucket fails — we just want to
	// confirm the failure mode is NOT "no driver registered for
	// scheme", which would surface a clearly different error string.
	for _, urlStr := range []string{"gs://bucket/prefix", "azblob://container/prefix"} {
		urlStr := urlStr
		t.Run(urlStr, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // cancel up-front so any cred discovery doesn't hang
			_, err := OpenBlobStore(ctx, urlStr, BlobStoreOptions{})
			if err == nil {
				// Some CI environments DO have ambient creds; that's
				// fine — the URL was accepted, which is what we test.
				return
			}
			if strings.Contains(err.Error(), "no driver registered") ||
				strings.Contains(err.Error(), "no scheme matched") {
				t.Errorf("scheme handler missing for %q: %v", urlStr, err)
			}
		})
	}
}

func TestBlobStore_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenBlobStore(context.Background(), fileBlobURL(t, dir), BlobStoreOptions{})
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.Put(ctx, "x", bytes.NewReader([]byte("y")))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Put with cancelled ctx: err = %v; want context.Canceled", err)
	}
}
