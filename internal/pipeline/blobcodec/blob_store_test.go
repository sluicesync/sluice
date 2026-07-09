// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"gocloud.dev/blob"

	// Memory blob driver for prefix tests — no creds, no network.
	_ "gocloud.dev/blob/memblob"
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

// TestBlobStore_AnnotateURL_FileDirMode pins the file:// permission
// posture: the fileblob dir_file_mode option defaults to owner-only
// 0700 (decimal 448 — fileblob parses base-10), an operator-set value
// is preserved, and non-file schemes are untouched.
func TestBlobStore_AnnotateURL_FileDirMode(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"file URL gains 0700 dirs", "file:///backups/x", "dir_file_mode=448"},
		{"operator-set mode preserved", "file:///backups/x?dir_file_mode=511", "dir_file_mode=511"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := annotateBlobURL(c.in, BlobStoreOptions{})
			if err != nil {
				t.Fatalf("annotateBlobURL: %v", err)
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("got %q; want substring %q", got, c.want)
			}
			if c.want == "dir_file_mode=511" && strings.Contains(got, "448") {
				t.Errorf("got %q; operator-set dir_file_mode was overridden", got)
			}
		})
	}
	t.Run("s3 URL untouched", func(t *testing.T) {
		got, err := annotateBlobURL("s3://bucket/prefix", BlobStoreOptions{})
		if err != nil {
			t.Fatalf("annotateBlobURL: %v", err)
		}
		if strings.Contains(got, "dir_file_mode") {
			t.Errorf("got %q; dir_file_mode must not leak onto non-file schemes", got)
		}
	})
}

// TestBlobStore_FileURLWarnsWorldReadable pins the audit posture WARN:
// opening a file:// backup destination (the gocloud fileblob route,
// 0666-minus-umask files) must warn in favour of --output-dir's
// hardened LocalStore; non-file schemes must stay silent.
func TestBlobStore_FileURLWarnsWorldReadable(t *testing.T) {
	capture := func(t *testing.T) *bytes.Buffer {
		t.Helper()
		buf := &bytes.Buffer{}
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
		t.Cleanup(func() { slog.SetDefault(prev) })
		return buf
	}

	t.Run("file URL warns", func(t *testing.T) {
		buf := capture(t)
		store, err := OpenBlobStore(context.Background(), fileBlobURL(t, t.TempDir()), BlobStoreOptions{})
		if err != nil {
			t.Fatalf("OpenBlobStore: %v", err)
		}
		defer func() { _ = store.Close() }()
		out := buf.String()
		if !strings.Contains(out, "world-readable") || !strings.Contains(out, "--output-dir") {
			t.Errorf("file:// open did not warn about the fileblob permission posture; log output: %q", out)
		}
	})
	t.Run("non-file URL is silent", func(t *testing.T) {
		buf := capture(t)
		store, err := OpenBlobStore(context.Background(), "mem://bucket", BlobStoreOptions{})
		if err != nil {
			t.Fatalf("OpenBlobStore(mem://): %v", err)
		}
		defer func() { _ = store.Close() }()
		if out := buf.String(); strings.Contains(out, "world-readable") {
			t.Errorf("non-file scheme triggered the fileblob WARN: %q", out)
		}
	})
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

// TestBlobStore_ExtractPrefix pins the URL → prefix mapping. The path
// component after the bucket name is what gocloud's URL parser drops
// silently (Bug 33); BlobStore now keeps it for prefixing keys.
func TestBlobStore_ExtractPrefix(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"s3 with prefix", "s3://bucket/foo", "foo"},
		{"s3 multi-segment prefix", "s3://bucket/foo/bar", "foo/bar"},
		{"s3 with trailing slash", "s3://bucket/foo/bar/", "foo/bar"},
		{"s3 no prefix", "s3://bucket", ""},
		{"s3 no prefix with slash", "s3://bucket/", ""},
		{"gs with prefix", "gs://bucket/foo/bar", "foo/bar"},
		{"azblob with prefix", "azblob://container/foo", "foo"},
		{"file no prefix (path is bucket)", "file:///tmp/dir", ""},
		{"file with sub-path (still no prefix)", "file:///tmp/dir/sub", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := extractBlobPrefix(c.in)
			if err != nil {
				t.Fatalf("extractBlobPrefix(%q): %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("extractBlobPrefix(%q) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

// TestBlobStore_PrefixRoundTrip verifies Bug 33's fix end-to-end via
// the memblob driver: opening a BlobStore at `mem://anything/prefix/inner`
// causes Put/Get/List/Exists/Delete to operate on prefix-joined keys
// against the underlying bucket. Keys returned by List are relative to
// the configured prefix (matching LocalStore's contract).
func TestBlobStore_PrefixRoundTrip(t *testing.T) {
	store, err := OpenBlobStore(context.Background(), "mem://ignored/foo/bar", BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	if store.prefix != "foo/bar" {
		t.Fatalf("store.prefix = %q; want %q", store.prefix, "foo/bar")
	}

	want := []byte("body")
	if err := store.Put(context.Background(), "manifest.json", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The underlying bucket should see the prefixed key.
	exists, err := store.bucket.Exists(context.Background(), "foo/bar/manifest.json")
	if err != nil {
		t.Fatalf("underlying bucket Exists: %v", err)
	}
	if !exists {
		t.Errorf("underlying bucket key foo/bar/manifest.json missing; prefix not applied on Put")
	}
	// And the un-prefixed path should NOT exist (Bug 33 regression).
	exists, err = store.bucket.Exists(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("underlying bucket Exists (root): %v", err)
	}
	if exists {
		t.Errorf("underlying bucket key manifest.json present at root; prefix should have been applied")
	}

	// BlobStore.Get sees the prefix-relative path.
	rc, err := store.Get(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("got %q; want %q", got, want)
	}

	// BlobStore.Exists sees the prefix-relative path.
	yes, err := store.Exists(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !yes {
		t.Errorf("Exists(manifest.json) = false; want true")
	}

	// Populate a few sub-paths, then List → results are relative.
	for _, k := range []string{"chunks/a.bin", "chunks/b.bin", "manifest.json"} {
		if err := store.Put(context.Background(), k, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}
	keys, err := store.List(context.Background(), "chunks/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(keys)
	wantKeys := []string{"chunks/a.bin", "chunks/b.bin"}
	if !equalStrSlices(keys, wantKeys) {
		t.Errorf("List = %v; want %v (prefix-stripped)", keys, wantKeys)
	}

	// Delete uses the prefix; the underlying object should disappear.
	if err := store.Delete(context.Background(), "chunks/a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, err = store.bucket.Exists(context.Background(), "foo/bar/chunks/a.bin")
	if err != nil {
		t.Fatalf("underlying bucket Exists post-Delete: %v", err)
	}
	if exists {
		t.Errorf("Delete didn't remove prefixed key")
	}
}

// TestBlobStore_PrefixEmpty pins that the no-prefix URL shape behaves
// identically to v0.16.0 — keys land at bucket root.
func TestBlobStore_PrefixEmpty(t *testing.T) {
	store, err := OpenBlobStore(context.Background(), "mem://ignored", BlobStoreOptions{})
	if err != nil {
		t.Fatalf("OpenBlobStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if store.prefix != "" {
		t.Fatalf("store.prefix = %q; want empty", store.prefix)
	}
	if err := store.Put(context.Background(), "key", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, err := store.bucket.Exists(context.Background(), "key")
	if err != nil {
		t.Fatalf("underlying Exists: %v", err)
	}
	if !exists {
		t.Errorf("Put with empty prefix didn't land at root")
	}
}

// TestBlobStore_MemSchemeAccepted is a smoke test: confirms the memblob
// scheme is registered (so the prefix tests above can reach the bucket).
func TestBlobStore_MemSchemeAccepted(t *testing.T) {
	// Direct probe via gocloud — the side-effect import lives in this
	// test file, so this will fail loudly if it ever gets removed.
	b, err := blob.OpenBucket(context.Background(), "mem://")
	if err != nil {
		t.Fatalf("memblob driver missing: %v", err)
	}
	_ = b.Close()
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
