//go:build integration && kmsverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase 6.2 AWS KMS integration tests against a localstack KMS
// container. Runs ONLY under `integration && kmsverify` — the
// localstack image is heavy enough that the main `integration` build
// tag stays focused on real-database scenarios. CI entry point: the
// `kmsverify` leg in .github/workflows/extended-suites.yml (weekly +
// workflow_dispatch), guarded by scripts/check-run-filter-coverage.sh
// (kmsverify axis).
//
// To run locally:
//
//	# Linux / macOS (testcontainers manages the container lifecycle)
//	go test -tags='integration kmsverify' -run '^TestBackup_KMSEncryption_' ./internal/pipeline/
//
//	# Windows + Rancher Desktop: also export
//	#   TESTCONTAINERS_RYUK_DISABLED=true
//	# (see CLAUDE.md for the full PATH override)
//
// What's covered (live, against localstack KMS + real PG):
//
//   1. Round-trip — encrypted full backup + restore against
//      same-engine PG; chunks are ciphertext on disk; the manifest
//      records KEKMode "aws-kms" + the key ARN; data round-trips
//      through a real kms.Encrypt / kms.Decrypt pair.
//   2. Chain extension — encrypted full + encrypted incremental +
//      chain restore; verifies the Bug-43 chain-extension pattern is
//      moot for KMS (unwrap doesn't depend on a chain-recorded
//      Argon2id salt, so the SAME envelope extends the chain with no
//      RebuildForChain hook).
//   3. Wrong-key refusal — chain wrapped under KMS key A, restored
//      with KMS key B → refused with a loud kms-decrypt error; no
//      partial data lands.
//   4. Missing-key refusal — encrypted chain restored without an
//      envelope → refused up-front with an operator-actionable error
//      citing the chain's KEKMode + KEKRef.
//
// Still scaffolded (honest skip, not silently green):
//
//   5. Access-denied translation — needs IAM policy ENFORCEMENT,
//      which localstack community edition does not do (every
//      principal is admin). The translateKMSError AccessDenied branch
//      is pinned at the unit layer (internal/crypto/aws_kms_test.go)
//      against the stubbed SDK error shape instead.

package pipeline

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/postgres"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// localstackImage is pinned (not :latest) so a localstack release
// can't silently change KMS semantics under the weekly leg.
const localstackImage = "localstack/localstack:4.0"

// localstackRegion is arbitrary — localstack accepts any region; the
// value just has to be consistent between key creation and envelope
// construction.
const localstackRegion = "us-east-1"

// startLocalstackKMS boots a localstack container scoped to the KMS
// service and returns its edge endpoint URL plus a cleanup callback.
func startLocalstackKMS(t *testing.T) (endpoint string, cleanup func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        localstackImage,
		ExposedPorts: []string{"4566/tcp"},
		// Scope to KMS only: skips the other service providers'
		// startup cost (the full set roughly doubles boot time).
		Env: map[string]string{"SERVICES": "kms"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("4566/tcp"),
			// localstack prints "Ready." once the edge router accepts
			// service requests; the port alone comes up earlier.
			wait.ForLog("Ready.").WithStartupTimeout(2*time.Minute),
		),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start localstack: %v", err)
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
	port, err := container.MappedPort(ctx, "4566/tcp")
	if err != nil {
		terminate()
		t.Fatalf("mapped port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), terminate
}

// localstackAWSConfig returns an aws.Config pointed at the localstack
// endpoint with the fixed test credentials localstack accepts.
func localstackAWSConfig(endpoint string) aws.Config {
	return aws.Config{
		Region:       localstackRegion,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint: aws.String(endpoint),
	}
}

// createLocalstackKMSKey creates a fresh symmetric KMS key in the
// localstack instance and returns its ARN.
func createLocalstackKMSKey(t *testing.T, endpoint string) string {
	t.Helper()
	client := kms.NewFromConfig(localstackAWSConfig(endpoint))
	out, err := client.CreateKey(context.Background(), &kms.CreateKeyInput{})
	if err != nil {
		t.Fatalf("localstack CreateKey: %v", err)
	}
	if out.KeyMetadata == nil || out.KeyMetadata.Arn == nil {
		t.Fatal("localstack CreateKey returned no key ARN")
	}
	return *out.KeyMetadata.Arn
}

// newLocalstackKMSEnvelope builds a KMSEnvelope against the localstack
// endpoint — the same construction path production uses (including the
// DescribeKey preflight), just with the endpoint + credentials pinned.
func newLocalstackKMSEnvelope(t *testing.T, endpoint, keyARN string) *crypto.KMSEnvelope {
	t.Helper()
	env, err := crypto.NewKMSEnvelope(context.Background(), keyARN,
		crypto.WithKMSConfig(localstackAWSConfig(endpoint)))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	return env
}

// TestBackup_KMSEncryption_RoundTrip — criterion 1: KMS-encrypted full
// backup, ciphertext chunks on disk, KEK metadata on the manifest,
// restore with a freshly-constructed envelope on the same key (the CLI
// restore path's shape: the operator re-supplies --kms-key-arn).
func TestBackup_KMSEncryption_RoundTrip(t *testing.T) {
	endpoint, lsCleanup := startLocalstackKMS(t)
	defer lsCleanup()
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		INSERT INTO users (email) VALUES
			('alice@example.com'),
			('bob@example.com'),
			('carol@example.com');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	keyARN := createLocalstackKMSKey(t, endpoint)
	envBackup := newLocalstackKMSEnvelope(t, endpoint, keyARN)

	if err := (&backup.Backup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		SluiceVersion: "test-kmsverify",
		Encryption: &lineage.BackupEncryption{
			Envelope: envBackup,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   keyARN,
		},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// Manifest must carry the KMS encryption metadata.
	root, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if root.ChainEncryption == nil {
		t.Fatal("ChainEncryption nil after KMS-encrypted backup")
	}
	if root.ChainEncryption.KEKMode != crypto.KEKModeAWSKMS {
		t.Errorf("KEKMode: got %q want %q", root.ChainEncryption.KEKMode, crypto.KEKModeAWSKMS)
	}
	if root.ChainEncryption.KEKRef != keyARN {
		t.Errorf("KEKRef: got %q want %q", root.ChainEncryption.KEKRef, keyARN)
	}
	if len(root.ChainEncryption.WrappedCEK) == 0 {
		t.Error("WrappedCEK empty — the CEK was not KMS-wrapped")
	}
	if root.ChainEncryption.Argon2id != nil {
		t.Error("Argon2id params recorded on a KMS chain (should be nil; KMS handles its own key state)")
	}

	// On-disk chunk bytes must NOT contain any of the plaintext values.
	if len(root.Tables) == 0 || len(root.Tables[0].Chunks) == 0 {
		t.Fatal("no chunks in manifest")
	}
	chunkPath := root.Tables[0].Chunks[0].File
	if chunkContainsPlaintext(t, store, chunkPath, "alice", "bob", "carol", "@example.com") {
		t.Error("encrypted chunk on disk contains plaintext rows; encryption layer broken")
	}

	// Restore with a FRESH envelope on the same key — a real
	// kms.Decrypt against localstack, not the cached backup-side CEK.
	envRestore := newLocalstackKMSEnvelope(t, endpoint, keyARN)
	if err := (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
		Envelope:  envRestore,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	got := pgQueryEmails(t, targetDSN)
	gotMap := map[string]bool{}
	for _, e := range got {
		gotMap[e] = true
	}
	for _, want := range []string{"alice@example.com", "bob@example.com", "carol@example.com"} {
		if !gotMap[want] {
			t.Errorf("target missing email %q (got %v)", want, got)
		}
	}
}

// TestBackup_KMSEncryption_ChainExtension — criterion 2: encrypted
// full + encrypted incremental + chain restore. The Bug-43 hazard
// (chain extension re-deriving the KEK against a fresh Argon2id salt)
// has no KMS analog: unwrap needs only the key ARN, so the same
// envelope extends the chain and no RebuildForChain hook is supplied.
func TestBackup_KMSEncryption_ChainExtension(t *testing.T) {
	endpoint, lsCleanup := startLocalstackKMS(t)
	defer lsCleanup()
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email) VALUES
			('alice@example.com'),
			('bob@example.com');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	keyARN := createLocalstackKMSKey(t, endpoint)
	env := newLocalstackKMSEnvelope(t, endpoint, keyARN)

	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	// 1. KMS-encrypted full backup.
	if err := (&backup.Backup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		SluiceVersion: "test-kmsverify",
		Encryption: &lineage.BackupEncryption{
			Envelope: env,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   keyARN,
		},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (full): %v", err)
	}

	// Stamp EndPosition + recompute BackupID so the incremental can
	// chain off the full (mirrors backup_encryption_chain_extension_
	// integration_test.go).
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "postgres",
		Token:  fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, slotLSN),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full manifest: %v", err)
	}

	// 2. Mutate source to generate CDC events.
	applyDDL(t, sourceDSN, `
		INSERT INTO users (email) VALUES ('carol@example.com');
		INSERT INTO users (email) VALUES ('dave@example.com');
	`)

	// 3. KMS-encrypted incremental with the SAME envelope, no rebuild
	// hook — unwrapping the parent's WrappedCEK is a kms.Decrypt, so
	// nothing chain-recorded feeds envelope construction.
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer incrCancel()
	if err := (&IncrementalBackup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		ParentRef:     full.BackupID,
		Window:        15 * time.Second,
		MaxChanges:    20,
		ChunkChanges:  10,
		SluiceVersion: "test-kmsverify",
		Encryption: &lineage.BackupEncryption{
			Envelope: env,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   keyARN,
		},
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// 4. Chain restore with a fresh envelope on the same key.
	envRestore := newLocalstackKMSEnvelope(t, endpoint, keyARN)
	if err := (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
		Envelope:  envRestore,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	// 5. Target reflects every row from full + incremental.
	got := pgQueryEmails(t, targetDSN)
	gotMap := map[string]bool{}
	for _, e := range got {
		gotMap[e] = true
	}
	for _, want := range []string{"alice@example.com", "bob@example.com", "carol@example.com", "dave@example.com"} {
		if !gotMap[want] {
			t.Errorf("target missing email %q (got %v)", want, got)
		}
	}
}

// TestBackup_KMSEncryption_MissingKey — criterion 4: an encrypted
// chain restored without any envelope refuses up-front with an
// operator-actionable error citing the chain's KEKMode + KEKRef.
func TestBackup_KMSEncryption_MissingKey(t *testing.T) {
	endpoint, lsCleanup := startLocalstackKMS(t)
	defer lsCleanup()
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255));
		INSERT INTO users VALUES (1, 'alice@example.com');
	`)

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	keyARN := createLocalstackKMSKey(t, endpoint)
	env := newLocalstackKMSEnvelope(t, endpoint, keyARN)
	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
		Encryption: &lineage.BackupEncryption{
			Envelope: env,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   keyARN,
		},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// No envelope → must refuse at preflight, naming what to supply.
	err = (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
		// Envelope intentionally nil.
	}).Run(context.Background())
	if err == nil {
		t.Fatal("expected restore to refuse without envelope; got nil")
	}
	if !strings.Contains(err.Error(), crypto.KEKModeAWSKMS) || !strings.Contains(err.Error(), keyARN) {
		t.Errorf("refusal should cite the chain's KEKMode %q + KEKRef %q so the operator knows what to supply; got: %v",
			crypto.KEKModeAWSKMS, keyARN, err)
	}
}

// TestBackup_KMSEncryption_WrongKey — criterion 3: a chain wrapped
// under key A restored with an envelope on key B fails loudly at the
// kms.Decrypt (the envelope passes KeyId as a belt-and-braces guard,
// so KMS refuses the cross-key decrypt); no partial data lands.
func TestBackup_KMSEncryption_WrongKey(t *testing.T) {
	endpoint, lsCleanup := startLocalstackKMS(t)
	defer lsCleanup()
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (id BIGINT PRIMARY KEY, email VARCHAR(255));
		INSERT INTO users VALUES (1, 'alice@example.com');
	`)

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	keyA := createLocalstackKMSKey(t, endpoint)
	keyB := createLocalstackKMSKey(t, endpoint)
	envA := newLocalstackKMSEnvelope(t, endpoint, keyA)

	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
		Encryption: &lineage.BackupEncryption{
			Envelope: envA,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   keyA,
		},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	envB := newLocalstackKMSEnvelope(t, endpoint, keyB)
	err = (&backup.Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
		Envelope:  envB,
	}).Run(context.Background())
	if err == nil {
		t.Fatal("expected restore with the wrong KMS key to fail; got nil")
	}
	if !strings.Contains(err.Error(), "kms decrypt") {
		t.Errorf("wrong-key refusal should surface as a translated kms decrypt error; got: %v", err)
	}

	// No partial data on the target.
	if got := pgQueryEmailsTolerant(t, targetDSN); len(got) != 0 {
		t.Errorf("wrong-key restore left partial data on the target: %v", got)
	}
}

// TestBackup_KMSEncryption_AccessDenied remains scaffolding: IAM
// policy denial requires policy ENFORCEMENT, which localstack
// community edition does not implement (every principal is admin).
// The AccessDenied translation branch is pinned at the unit layer in
// internal/crypto/aws_kms_test.go against the stubbed SDK error shape;
// exercising it live needs real AWS (or localstack Pro) and stays
// operator-run.
func TestBackup_KMSEncryption_AccessDenied(t *testing.T) {
	t.Skip("kmsverify scaffolding — localstack community edition does not enforce IAM; the translateKMSError AccessDenied branch is unit-pinned in internal/crypto/aws_kms_test.go")
}
