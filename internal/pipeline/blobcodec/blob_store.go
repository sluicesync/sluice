// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package blobcodec

// Cloud-blob implementation of [irbackup.Store] over `gocloud.dev/blob`.
//
// Phase 2 of the logical-backup feature (`docs/dev/design/logical-backups.md`
// + `docs/dev/design/logical-backups-phase-2.md`). Mirrors [LocalStore]'s
// shape so the orchestrator code in `backup.go` / `restore.go` doesn't
// know which backend is underneath; only the URL scheme passed to
// [OpenBlobStore] changes between local-FS, S3, GCS, and Azure.
//
// URL schemes:
//
//   - s3://bucket/prefix?endpoint=...&region=...&use_path_style=true
//   - gs://bucket/prefix
//   - azblob://container/prefix
//   - file:///absolute/path  (parity with LocalStore; gocloud's fileblob)
//
// Path semantics match [LocalStore]: forward-slash separated, relative to
// the bucket+prefix configured in the URL, and `..` segments are rejected
// before any provider call as defence-in-depth.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	// Side-effect imports register the URL-scheme handlers on
	// blob.DefaultURLMux. Each driver pulls its own cloud SDK; the
	// transitive cost of all four is ~3-5 MB of binary growth, which
	// is the documented Phase 2 tradeoff.
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// BlobStore is the cloud-backend implementation of [irbackup.Store].
// Construct with [OpenBlobStore].
type BlobStore struct {
	bucket *blob.Bucket
	url    string

	// prefix is the path component the URL carried after the bucket
	// name (e.g. `backup-v0160` for `s3://bucket/backup-v0160`). It is
	// prepended to every key passed to bucket and stripped from List
	// results so callers see paths relative to the configured prefix.
	//
	// Bug 33 (v0.16.0): gocloud.dev/blob.OpenBucket consumes only the
	// bucket name from the URL; the path is silently dropped, so
	// previously every key landed at bucket root regardless of what
	// the operator wrote. Tracking it here and prefixing on every
	// operation restores the "many backups in one bucket" workflow.
	prefix string
}

// BlobStoreOptions tunes the URL [OpenBlobStore] passes to gocloud.
// Zero value is valid — all fields are optional and only meaningful
// when the URL scheme is `s3://` (the path-style + endpoint + region
// parameters are S3-specific). [OpenBlobStore] returns a clear error
// if any of these are set with a non-S3 scheme.
type BlobStoreOptions struct {
	// Endpoint overrides the AWS-default S3 endpoint. Used to point
	// at MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris, or
	// Archil's read-only S3 surface. Set as the `endpoint` query-string
	// param on the s3blob URL.
	Endpoint string

	// Region overrides the AWS-default region. Required by some
	// S3-compatible providers (Archil uses provider-specific codes
	// like `aws-us-east-1`). Set as the `region` query-string param.
	Region string

	// PathStyle forces path-style addressing (bucket in path, not
	// hostname). Required by Archil and many MinIO setups. Set as
	// `use_path_style=true` on the URL.
	PathStyle bool
}

// OpenBlobStore opens a [BlobStore] backed by gocloud.dev/blob. The
// URL determines the backend driver (s3, gs, azblob, file). The opts
// are S3-specific and ignored for other schemes; passing non-zero opts
// with a non-S3 URL is an operator-actionable error.
func OpenBlobStore(ctx context.Context, urlStr string, opts BlobStoreOptions) (*BlobStore, error) {
	if urlStr == "" {
		return nil, errors.New("blob store: URL is empty")
	}
	full, err := annotateBlobURL(urlStr, opts)
	if err != nil {
		return nil, err
	}
	if isFileBlobURL(full) {
		// The hardened owner-only store is LocalStore (0600 files /
		// 0700 dirs — see NewLocalStore); fileblob writes files at
		// 0666-minus-umask and offers no per-file mode option, so a
		// file:// destination is world-readable on typical umasks.
		// annotateBlobURL defaults the DIRECTORY mode to 0700, but the
		// files themselves stay loose — warn rather than silently
		// diverging from the documented fileblob behaviour.
		slog.WarnContext(ctx, "blob store: file:// URLs use gocloud's fileblob backend, whose files are created world-readable by default (0666 minus umask); prefer --output-dir for the hardened owner-only (0600 files / 0700 dirs) local store",
			slog.String("url", redactBlobURL(full)))
	}
	bucket, err := blob.OpenBucket(ctx, full)
	if err != nil {
		return nil, fmt.Errorf("blob store: open bucket %q: %w", redactBlobURL(full), err)
	}
	prefix, err := extractBlobPrefix(full)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	return &BlobStore{bucket: bucket, url: full, prefix: prefix}, nil
}

// extractBlobPrefix pulls the path-after-bucket component out of the
// (annotated) URL. gocloud's [blob.OpenBucket] only consumes the
// bucket / container name from the URL — for `s3://bucket/foo/bar` the
// driver sees `bucket` and `foo/bar` is silently dropped. We keep the
// dropped piece here so all object keys can be prefixed with it,
// restoring "many backups in one bucket" workflows.
//
// Returns the path with leading and trailing slashes trimmed; empty
// when the URL had no path component.
func extractBlobPrefix(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("blob store: parse URL %q for prefix: %w", urlStr, err)
	}
	// fileblob is the lone exception: the URL's path *is* the bucket
	// (a local directory). gocloud treats the whole thing as the
	// bucket root, so we must not double-prefix.
	if u.Scheme == "file" {
		return "", nil
	}
	p := strings.TrimPrefix(u.Path, "/")
	p = strings.TrimSuffix(p, "/")
	return p, nil
}

// joinBlobKey prepends the BlobStore's prefix to key and returns the
// underlying-bucket path. The empty-prefix case passes key through
// unchanged so the no-prefix URL shape behaves identically to v0.16.0.
func (s *BlobStore) joinBlobKey(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

// stripBlobPrefix removes the BlobStore's prefix from key. Used on
// List output so callers see paths relative to the configured prefix
// (matching [LocalStore]'s contract). Keys that don't start with the
// prefix are returned verbatim — defensive only, the bucket should
// never surface such keys when listing under the prefix.
func (s *BlobStore) stripBlobPrefix(key string) string {
	if s.prefix == "" {
		return key
	}
	pfx := s.prefix + "/"
	if strings.HasPrefix(key, pfx) {
		return strings.TrimPrefix(key, pfx)
	}
	return key
}

// URL returns the (annotated) URL the store was opened with. Useful
// for log lines and tests; credentials are never embedded — gocloud's
// drivers source them from the environment.
func (s *BlobStore) URL() string { return redactBlobURL(s.url) }

// Close releases the underlying bucket handle. Idempotent.
func (s *BlobStore) Close() error {
	if s == nil || s.bucket == nil {
		return nil
	}
	return s.bucket.Close()
}

// Put implements [irbackup.Store.Put]. Streams r to the named key
// within the bucket. gocloud's s3blob driver negotiates multipart
// upload automatically based on stream size; no extra coordination
// is needed.
func (s *BlobStore) Put(ctx context.Context, path string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return err
	}
	key = s.joinBlobKey(key)
	w, err := s.bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return fmt.Errorf("blob store: open writer for %q: %w", path, err)
	}
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return fmt.Errorf("blob store: write %q: %w", path, err)
	}
	if err := w.Close(); err != nil {
		return wrapBlobErr("close writer", path, err)
	}
	return nil
}

// Get implements [irbackup.Store.Get]. Returns a streaming reader for
// the contents of path; caller closes.
func (s *BlobStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return nil, err
	}
	key = s.joinBlobKey(key)
	rc, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, wrapBlobErr("open reader", path, err)
	}
	return rc, nil
}

// List implements [irbackup.Store.List]. Returns every key whose name
// starts with prefix, in unspecified order. Paths in the result are
// relative to the BlobStore's configured prefix — matching
// [LocalStore]'s contract.
func (s *BlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if prefix != "" {
		// Defence-in-depth: catch `..` even on List paths (an attacker
		// who could plant a manifest reference could otherwise probe
		// outside the prefix).
		if _, err := sanitiseBlobKey(prefix); err != nil {
			return nil, err
		}
	}
	listPrefix := s.joinBlobKey(prefix)
	iter := s.bucket.List(&blob.ListOptions{Prefix: listPrefix})
	var out []string
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("blob store: list %q: %w", prefix, err)
		}
		if obj.IsDir {
			continue
		}
		out = append(out, s.stripBlobPrefix(obj.Key))
	}
	return out, nil
}

// Delete implements [irbackup.Store.Delete]. Idempotent: a missing key
// returns nil to match [LocalStore]'s contract.
func (s *BlobStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return err
	}
	key = s.joinBlobKey(key)
	if err := s.bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return wrapBlobErr("delete", path, err)
	}
	return nil
}

// Exists implements [irbackup.Store.Exists]. Returns true iff a blob
// exists at path. Used by the resumable backup writer to skip already-
// uploaded chunks. NotFound is the false-without-error case; any other
// gcerrors code surfaces as a wrapped error.
func (s *BlobStore) Exists(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return false, err
	}
	key = s.joinBlobKey(key)
	exists, err := s.bucket.Exists(ctx, key)
	if err != nil {
		return false, wrapBlobErr("exists", path, err)
	}
	return exists, nil
}

// sanitiseBlobKey enforces the same path-traversal rejection LocalStore
// does, so a malicious manifest can't escape the bucket prefix via
// `../../other-prefix`. gocloud's drivers also sanitise, but the
// no-silent-corruption tenet wants belt-and-braces here.
func sanitiseBlobKey(path string) (string, error) {
	if path == "" {
		return "", errors.New("blob store: empty path")
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." {
			return "", fmt.Errorf("blob store: path traversal not allowed: %q", path)
		}
	}
	// gocloud keys never start with `/`; trim defensively so callers
	// that hand-craft paths don't end up with two leading slashes.
	return strings.TrimPrefix(path, "/"), nil
}

// annotateBlobURL adds the operator-supplied opts to the URL as
// query-string params, but only when the scheme is `s3://`. Non-S3
// schemes with non-zero opts return an operator-actionable error so
// the operator notices the flags don't apply (rather than failing
// silently when MinIO-style endpoints are passed to GCS).
func annotateBlobURL(urlStr string, opts BlobStoreOptions) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("blob store: parse URL %q: %w", urlStr, err)
	}
	if u.Scheme == "" {
		return "", fmt.Errorf("blob store: URL %q has no scheme (expected s3://, gs://, azblob://, or file://)", urlStr)
	}
	hasS3Opts := opts.Endpoint != "" || opts.Region != "" || opts.PathStyle
	if hasS3Opts && u.Scheme != "s3" {
		return "", fmt.Errorf("blob store: --backup-endpoint / --backup-region / --backup-path-style are only meaningful for s3:// URLs; URL scheme is %q", u.Scheme)
	}
	if u.Scheme == "file" {
		// fileblob's default directory mode is 0777 (world-traversable).
		// Default the driver's dir_file_mode option to owner-only 0700 —
		// the value is DECIMAL 448 because fileblob parses it base-10 —
		// unless the operator set their own. The per-FILE mode has no
		// fileblob option (files land 0666 minus umask), which is why
		// OpenBlobStore warns in favour of --output-dir's LocalStore.
		q := u.Query()
		if q.Get("dir_file_mode") == "" {
			q.Set("dir_file_mode", "448") // 0o700
			u.RawQuery = q.Encode()
		}
		return u.String(), nil
	}
	if !hasS3Opts {
		return urlStr, nil
	}
	q := u.Query()
	if opts.Endpoint != "" {
		q.Set("endpoint", opts.Endpoint)
		// hostname_immutable is essentially required when an endpoint
		// is set: without it, the AWS SDK may rewrite the host (e.g.
		// `bucket.endpoint`) and break MinIO / Archil. Set it as a
		// matter of course; operators who want the original behavior
		// can pass `--backup-endpoint URL?hostname_immutable=false`
		// in a future iteration if it ever matters.
		if q.Get("hostname_immutable") == "" {
			q.Set("hostname_immutable", "true")
		}
	}
	if opts.Region != "" {
		q.Set("region", opts.Region)
	}
	if opts.PathStyle {
		q.Set("use_path_style", "true")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// isFileBlobURL reports whether the (annotated) URL routes to gocloud's
// fileblob driver — the one backend whose on-disk permission posture
// diverges from LocalStore's hardened 0600/0700 (see the WARN in
// [OpenBlobStore]). A parse failure reports false; OpenBucket will
// surface the real error.
func isFileBlobURL(urlStr string) bool {
	u, err := url.Parse(urlStr)
	return err == nil && u.Scheme == "file"
}

// redactBlobURL strips the query string from a URL for logging. None
// of the gocloud drivers we register accept secrets via query string
// today (creds come from the environment), but the principle of least
// information in log lines is cheap.
func redactBlobURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	u.RawQuery = ""
	return u.String()
}

// wrapBlobErr maps a gocloud error to an operator-actionable wrapped
// error. NotFound surfaces as a clear sentinel-shaped string the CLI
// can match on; PermissionDenied gets a hint about credentials and
// scope; everything else is wrapped verbatim.
func wrapBlobErr(op, path string, err error) error {
	switch gcerrors.Code(err) {
	case gcerrors.NotFound:
		return fmt.Errorf("blob store: %s %q: not found: %w", op, path, err)
	case gcerrors.PermissionDenied:
		return fmt.Errorf("blob store: %s %q: permission denied (check credentials and bucket policy): %w", op, path, err)
	default:
		return fmt.Errorf("blob store: %s %q: %w", op, path, err)
	}
}

// Compile-time check that BlobStore satisfies irbackup.Store.
var _ irbackup.Store = (*BlobStore)(nil)
