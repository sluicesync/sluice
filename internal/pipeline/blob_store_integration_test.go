//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase-2 cloud-backend integration tests. Spins up a MinIO container
// (S3-compatible), creates a bucket, and roundtrips Put/Get/Exists/
// Delete/List against [blobcodec.BlobStore] over the S3 path. Also runs a full
// PG-source backup → cross-engine MySQL restore through the blob
// store, validating the cross-engine + cloud combination called out
// as a Phase-2 done criterion.

package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	// Both engines registered for the cross-engine test.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// minioCreds are the default credentials baked into the MinIO image
// when MINIO_ROOT_USER / MINIO_ROOT_PASSWORD env vars are passed in.
const (
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioRegion    = "us-east-1"
)

// startMinIO boots a minio/minio container with default credentials,
// creates a single bucket the test can write into, and returns the
// endpoint URL + bucket name + cleanup callback.
func startMinIO(t *testing.T) (endpoint, bucket string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":     minioAccessKey,
			"MINIO_ROOT_PASSWORD": minioSecretKey,
		},
		Cmd: []string{"server", "/data"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("9000/tcp"),
			wait.ForLog("API:").WithStartupTimeout(60*time.Second),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}

	terminate := func() {
		shutdown, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		_ = container.Terminate(shutdown)
	}

	host, err := container.Host(ctx)
	if err != nil {
		terminate()
		t.Fatalf("container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		terminate()
		t.Fatalf("mapped port: %v", err)
	}
	endpoint = fmt.Sprintf("http://%s:%s", host, port.Port())
	bucket = "sluice-test-" + randomSuffix()

	if err := createMinIOBucket(ctx, endpoint, bucket); err != nil {
		terminate()
		t.Fatalf("create bucket: %v", err)
	}
	return endpoint, bucket, terminate
}

// randomSuffix produces a short suffix so concurrent test runs (or
// repeated runs against a persistent MinIO) don't collide on bucket
// names. time.Now-based; collision odds are vanishingly small in the
// integration-test cadence.
func randomSuffix() string {
	return fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// createMinIOBucket builds an AWS SDK v2 client pointed at the MinIO
// endpoint and issues a CreateBucket. Path-style addressing is forced
// (MinIO's default behavior); credentials are the well-known root
// user from the container env.
func createMinIOBucket(ctx context.Context, endpoint, name string) error {
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(minioRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioAccessKey, minioSecretKey, "")),
	)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
	if err != nil {
		return fmt.Errorf("CreateBucket: %w", err)
	}
	return nil
}

// minioBlobStore wraps OpenBlobStore with the right S3 options for the
// test MinIO endpoint and pre-populates the AWS env vars the SDK
// resolves for credentials. Returns the store + a cleanup that
// restores any prior env values.
func minioBlobStore(t *testing.T, endpoint, bucket, prefix string) (*blobcodec.BlobStore, func()) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)
	t.Setenv("AWS_REGION", minioRegion)
	url := fmt.Sprintf("s3://%s/%s", bucket, strings.TrimPrefix(prefix, "/"))
	store, err := blobcodec.OpenBlobStore(context.Background(), url, blobcodec.BlobStoreOptions{
		Endpoint:  endpoint,
		Region:    minioRegion,
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("OpenBlobStore: %v", err)
	}
	cleanup := func() { _ = store.Close() }
	return store, cleanup
}

func TestBlobStore_MinIO_RoundTrip(t *testing.T) {
	endpoint, bucket, cleanup := startMinIO(t)
	defer cleanup()
	// `phase2/` is the URL prefix; v0.16.1 onwards routes object keys
	// under that prefix in the bucket. Caller-side paths are RELATIVE
	// to the prefix (matches the LocalStore contract).
	store, storeCleanup := minioBlobStore(t, endpoint, bucket, "phase2/")
	defer storeCleanup()

	// Put → Exists → Get → checksum-matches.
	want := []byte("phase 2 cloud roundtrip data")
	if err := store.Put(context.Background(), "manifest.json", bytes.NewReader(want)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, err := store.Exists(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Errorf("Exists after Put = false; want true")
	}
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

	// Verify the underlying object actually lives at <bucket>/phase2/manifest.json
	// and not at the bucket root (Bug 33 regression check). Direct AWS
	// SDK call so we're confirming the wire shape, not just that the
	// BlobStore round-trips through itself.
	if err := assertS3KeyExists(t, endpoint, bucket, "phase2/manifest.json"); err != nil {
		t.Errorf("Bug 33 regression: %v", err)
	}
	if err := assertS3KeyAbsent(t, endpoint, bucket, "manifest.json"); err != nil {
		t.Errorf("Bug 33 regression: bucket-root manifest.json should not exist: %v", err)
	}

	// List against a prefix; keys are returned relative to the URL
	// prefix.
	for _, key := range []string{"chunks/users/0.bin", "chunks/users/1.bin", "chunks/orders/0.bin"} {
		if err := store.Put(context.Background(), key, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
	}
	keys, err := store.List(context.Background(), "chunks/users/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(keys)
	wantKeys := []string{"chunks/users/0.bin", "chunks/users/1.bin"}
	if !equalStrSlices(keys, wantKeys) {
		t.Errorf("List = %v; want %v", keys, wantKeys)
	}

	// Delete then Exists returns false.
	if err := store.Delete(context.Background(), "manifest.json"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, err = store.Exists(context.Background(), "manifest.json")
	if err != nil {
		t.Fatalf("Exists after Delete: %v", err)
	}
	if exists {
		t.Errorf("Exists after Delete = true; want false")
	}

	// Idempotent Delete.
	if err := store.Delete(context.Background(), "manifest.json"); err != nil {
		t.Errorf("idempotent Delete: %v", err)
	}
}

// TestBlobStore_MinIO_ConditionalPutChainGuard pins the ADR-0161 chain
// concurrent-writer guard's S3 leg against a REAL S3-compatible server:
// gocloud's WriterOptions.IfNotExist becomes an `If-None-Match: *`
// conditional PUT on the wire, and MinIO (like AWS S3 since 2024)
// enforces it server-side. Two layers:
//
//  1. the raw PutIfAbsent contract (exactly one winner, loser gets
//     irbackup.ErrPathExists, winner's content intact) — proving the
//     server ENFORCES the precondition rather than ignoring the header
//     (an ignoring server would make the second create succeed and
//     fail this test loudly);
//  2. the full guard through the lineage layer: two interleaved
//     catalog writers on the S3 store — first wins, second refuses
//     with the coded SLUICE-E-BACKUP-CHAIN-CONFLICT.
func TestBlobStore_MinIO_ConditionalPutChainGuard(t *testing.T) {
	endpoint, bucket, cleanup := startMinIO(t)
	defer cleanup()
	store, storeCleanup := minioBlobStore(t, endpoint, bucket, "guard/")
	defer storeCleanup()
	ctx := context.Background()

	// Layer 1: the raw conditional-PUT contract on the wire.
	first := []byte(`{"claimed_at":"first"}`)
	if err := store.PutIfAbsent(ctx, "lineage.gen/g-00000000000000000001", bytes.NewReader(first)); err != nil {
		t.Fatalf("first PutIfAbsent: %v", err)
	}
	err := store.PutIfAbsent(ctx, "lineage.gen/g-00000000000000000001", bytes.NewReader([]byte(`{"claimed_at":"second"}`)))
	if err == nil {
		t.Fatal("second PutIfAbsent = nil; the server did not enforce If-None-Match — the chain guard would be inert on this backend")
	}
	if !errors.Is(err, irbackup.ErrPathExists) {
		t.Fatalf("second PutIfAbsent = %v; want an error wrapping irbackup.ErrPathExists (the 412 precondition mapping)", err)
	}
	rc, err := store.Get(ctx, "lineage.gen/g-00000000000000000001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, first) {
		t.Errorf("marker content = %q; want the first writer's %q", got, first)
	}

	// Layer 2: the guard end-to-end through the lineage catalog RMW.
	seed := &lineage.Catalog{
		SourceEngine: "postgres",
		Segments: []lineage.Segment{{
			SegmentID:        "seg0",
			FullManifestPath: lineage.ManifestFileName,
		}},
	}
	if err := lineage.WriteLineageCatalog(ctx, store, seed); err != nil {
		t.Fatalf("seed catalog write: %v", err)
	}
	a, okA, err := lineage.LoadLineageCatalogForUpdate(ctx, store)
	if err != nil || !okA {
		t.Fatalf("writer A load: ok=%v err=%v", okA, err)
	}
	b, okB, err := lineage.LoadLineageCatalogForUpdate(ctx, store)
	if err != nil || !okB {
		t.Fatalf("writer B load: ok=%v err=%v", okB, err)
	}
	a.Segments[0].Incrementals = []string{"manifests/incr-a.json"}
	if err := lineage.WriteLineageCatalog(ctx, store, a); err != nil {
		t.Fatalf("writer A write: %v", err)
	}
	b.Segments[0].Incrementals = []string{"manifests/incr-b.json"}
	err = lineage.WriteLineageCatalog(ctx, store, b)
	if err == nil {
		t.Fatal("writer B write = nil; want the concurrent-writer refusal")
	}
	ce, coded := sluicecode.FromError(err)
	if !coded || ce.Code != sluicecode.CodeBackupChainConflict {
		t.Fatalf("writer B err = %v; want code %s", err, sluicecode.CodeBackupChainConflict)
	}
	final, okF, err := lineage.LoadLineageCatalog(ctx, store)
	if err != nil || !okF {
		t.Fatalf("reload: ok=%v err=%v", okF, err)
	}
	if final.Segments[0].Incrementals[0] != "manifests/incr-a.json" {
		t.Errorf("catalog after conflict = %v; want writer A's update intact", final.Segments[0].Incrementals)
	}
}

// assertS3KeyExists builds a fresh AWS SDK client against the MinIO
// endpoint and HEADs the key directly, bypassing the BlobStore. Used to
// verify that an object the BlobStore wrote actually landed at the
// expected bucket-prefix path (Bug 33 regression check).
func assertS3KeyExists(t *testing.T, endpoint, bucket, key string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(minioRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioAccessKey, minioSecretKey, "")),
	)
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(endpoint)
	})
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("HeadObject %s/%s: %w", bucket, key, err)
	}
	return nil
}

// assertS3KeyAbsent is the inverse: returns an error iff a key exists.
func assertS3KeyAbsent(t *testing.T, endpoint, bucket, key string) error {
	t.Helper()
	if err := assertS3KeyExists(t, endpoint, bucket, key); err == nil {
		return fmt.Errorf("key %s/%s exists; expected absent", bucket, key)
	}
	return nil
}

// TestBlobStore_MinIO_BackupRestoreRoundTrip is the cross-engine
// done-criterion for Phase 2: a Postgres source is backed up to
// MinIO via BlobStore, then restored into a MySQL target. The
// existing translate.RetargetForEngine + BackupStore-shaped
// restore path should carry the data across without sluice
// caring whether the bytes came from local FS or a cloud bucket.
func TestBlobStore_MinIO_BackupRestoreRoundTrip(t *testing.T) {
	endpoint, bucket, cleanup := startMinIO(t)
	defer cleanup()

	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true
		);
		INSERT INTO users (email, active) VALUES
			('alice@example.com', true),
			('bob@example.com',   false);
	`
	applyPGDDL(t, pgSource, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	// Phase 1: backup PG → MinIO.
	store, storeCleanup := minioBlobStore(t, endpoint, bucket, "users-backup/")
	defer storeCleanup()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer ctxCancel()

	bk := &backup.Backup{
		Source:        pgEng,
		SourceDSN:     pgSource,
		Store:         store,
		ChunkRows:     100,
		SluiceVersion: "test-phase2",
	}
	if err := bk.Run(ctx); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Phase 2: restore MinIO → MySQL.
	rs := &backup.Restore{
		Target:    mysqlEng,
		TargetDSN: mysqlTarget,
		Store:     store,
	}
	if err := rs.Run(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify rows landed.
	rr, err := mysqlEng.OpenRowReader(ctx, mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql row reader: %v", err)
	}
	defer migcore.CloseIf(rr)

	sr, err := mysqlEng.OpenSchemaReader(ctx, mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql schema reader: %v", err)
	}
	defer migcore.CloseIf(sr)

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read mysql schema: %v", err)
	}
	var usersTable *ir.Table
	for _, tbl := range schema.Tables {
		if tbl.Name == "users" {
			usersTable = tbl
			break
		}
	}
	if usersTable == nil {
		t.Fatal("users table not in mysql target after restore")
	}

	rows, err := rr.ReadRows(ctx, usersTable)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	count := 0
	for range rows {
		count++
	}
	if count != 2 {
		t.Errorf("rows in target = %d; want 2", count)
	}
}

// TestBlobStore_MinIO_ResumableBackup validates the Phase 2.1.1
// resumable-writer end-to-end against the MinIO store: backup is
// killed mid-job, restarted, and completes with the same total
// chunk count + content. This exercises the per-table checkpoint
// + per-chunk Exists/SHA-256 skip paths against a real cloud-style
// store rather than the local-FS unit-test fakes.
func TestBlobStore_MinIO_ResumableBackup(t *testing.T) {
	endpoint, bucket, cleanup := startMinIO(t)
	defer cleanup()

	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	const seedDDL = `
		CREATE TABLE t1 (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);
		CREATE TABLE t2 (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);
		INSERT INTO t1 SELECT generate_series(1, 5);
		INSERT INTO t2 SELECT generate_series(1, 5);
	`
	applyPGDDL(t, pgSource, seedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	store, storeCleanup := minioBlobStore(t, endpoint, bucket, "resume/")
	defer storeCleanup()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer ctxCancel()

	// First run: fail on the third Put (after t1 chunk + manifest
	// checkpoint, while uploading t2 chunk 0 — see the unit test for
	// the same pattern).
	failing := &failOnNthPutBlob{BlobStore: store, failOn: 3}
	bk1 := &backup.Backup{
		Source:    pgEng,
		SourceDSN: pgSource,
		Store:     failing,
		ChunkRows: 100,
	}
	if err := bk1.Run(ctx); err == nil {
		t.Fatal("first Run: expected injected failure; got nil")
	}

	// Resume: re-run against the same destination.
	bk2 := &backup.Backup{
		Source:    pgEng,
		SourceDSN: pgSource,
		Store:     store,
		ChunkRows: 100,
	}
	if err := bk2.Run(ctx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}

	final, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if final.PartialState != irbackup.BackupStateComplete {
		t.Errorf("PartialState = %q; want complete", final.PartialState)
	}
	if len(final.Tables) != 2 {
		t.Errorf("Tables = %d; want 2", len(final.Tables))
	}

	// Confirm the bytes are good.
	total, mismatches, err := backup.VerifyBackup(ctx, store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 || total < 2 {
		t.Errorf("VerifyBackup = %d mismatches over %d chunks; want 0 mismatches over ≥2", mismatches, total)
	}
}

// failOnNthPutBlob wraps a [blobcodec.BlobStore] so the Nth Put returns an
// injected failure, simulating a mid-backup crash for the resumable-
// writer test. Identical shape to the unit-test version but wraps the
// blob-backed store.
type failOnNthPutBlob struct {
	*blobcodec.BlobStore

	failOn int
	putN   int
}

func (s *failOnNthPutBlob) Put(ctx context.Context, path string, r io.Reader) error {
	s.putN++
	if s.putN == s.failOn {
		return fmt.Errorf("injected failure on Put #%d", s.putN)
	}
	return s.BlobStore.Put(ctx, path, r)
}

// equalStrSlices reports whether two string slices are element-equal.
// Local copy of the identically-named helper in internal/pipeline/blobcodec
// (private test helpers don't cross package boundaries).
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
