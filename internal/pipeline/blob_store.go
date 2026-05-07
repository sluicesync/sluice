// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// Cloud-blob implementation of [ir.BackupStore] over `gocloud.dev/blob`.
//
// Phase 2 of the logical-backup feature (`docs/dev/design-logical-backups.md`
// + `docs/dev/design-logical-backups-phase-2.md`). Mirrors [LocalStore]'s
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

	"github.com/orware/sluice/internal/ir"
)

// BlobStore is the cloud-backend implementation of [ir.BackupStore].
// Construct with [OpenBlobStore].
type BlobStore struct {
	bucket *blob.Bucket
	url    string
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
	bucket, err := blob.OpenBucket(ctx, full)
	if err != nil {
		return nil, fmt.Errorf("blob store: open bucket %q: %w", redactBlobURL(full), err)
	}
	return &BlobStore{bucket: bucket, url: full}, nil
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

// Put implements [ir.BackupStore.Put]. Streams r to the named key
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

// Get implements [ir.BackupStore.Get]. Returns a streaming reader for
// the contents of path; caller closes.
func (s *BlobStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return nil, err
	}
	rc, err := s.bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, wrapBlobErr("open reader", path, err)
	}
	return rc, nil
}

// List implements [ir.BackupStore.List]. Returns every key whose name
// starts with prefix, in unspecified order.
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
	iter := s.bucket.List(&blob.ListOptions{Prefix: prefix})
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
		out = append(out, obj.Key)
	}
	return out, nil
}

// Delete implements [ir.BackupStore.Delete]. Idempotent: a missing key
// returns nil to match [LocalStore]'s contract.
func (s *BlobStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := sanitiseBlobKey(path)
	if err != nil {
		return err
	}
	if err := s.bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil
		}
		return wrapBlobErr("delete", path, err)
	}
	return nil
}

// Exists implements [ir.BackupStore.Exists]. Returns true iff a blob
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

// Compile-time check that BlobStore satisfies ir.BackupStore.
var _ ir.BackupStore = (*BlobStore)(nil)
