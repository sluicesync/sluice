// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Unit-level integration tests for Phase 6.2 AWS KMS envelope
// encryption. Uses a stubbed [crypto.KMSAPI] (no AWS calls, no
// localstack container) to exercise the manifest integration:
//
//   1. ChainEncryption.KEKMode = "aws-kms" (vs Phase 6.1's "passphrase-argon2id").
//   2. ChainEncryption.KEKRef = the operator's key ARN.
//   3. ChainEncryption.Argon2id is omitted (KMS doesn't use it).
//   4. Per-chain CEK caching: a multi-chunk chain restore makes one
//      KMS Decrypt call regardless of chunk count.
//
// Live KMS round-trip + access-denied / wrong-key / disabled
// scenarios live in `backup_kms_localstack_integration_test.go` under
// the `kmsverify` build tag (operator-run, optional). The unit tests
// here run on every CI cycle.

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// stubKMS is a deterministic in-process [crypto.KMSAPI] implementation
// for the pipeline-level tests. Mirrors the test-local fakeKMS in the
// crypto package but lives in the pipeline package's test scope so we
// don't have to export the crypto-side fake. The wire format is
// `kms:<keyID>|<plaintext>` so cross-key UnwrapCEK calls fail loudly.
type stubKMS struct {
	keyID    string
	encrypts int64
	decrypts int64
}

func (s *stubKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	atomic.AddInt64(&s.encrypts, 1)
	blob := append([]byte("kms:"+s.keyID+"|"), in.Plaintext...)
	return &kms.EncryptOutput{CiphertextBlob: blob, KeyId: aws.String(s.keyID)}, nil
}

func (s *stubKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	atomic.AddInt64(&s.decrypts, 1)
	prefix := "kms:" + s.keyID + "|"
	if !bytes.HasPrefix(in.CiphertextBlob, []byte(prefix)) {
		return nil, &kmstypes.IncorrectKeyException{Message: aws.String("ciphertext was not produced by " + s.keyID)}
	}
	return &kms.DecryptOutput{Plaintext: in.CiphertextBlob[len(prefix):], KeyId: aws.String(s.keyID)}, nil
}

func (s *stubKMS) DescribeKey(_ context.Context, _ *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	return &kms.DescribeKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{KeyId: aws.String(s.keyID), Arn: aws.String(s.keyID)}}, nil
}

func (s *stubKMS) Decrypts() int64 { return atomic.LoadInt64(&s.decrypts) }

func newStubKMSEnvelope(t *testing.T, keyID string) (*crypto.KMSEnvelope, *stubKMS) {
	t.Helper()
	stub := &stubKMS{keyID: keyID}
	env, err := crypto.NewKMSEnvelope(context.Background(), keyID, crypto.WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	return env, stub
}

// TestBackup_SetupChainEncryption_KMS pins the manifest integration:
// when a KMS-mode envelope is supplied, [Backup.setupChainEncryption]
// records the right KEKMode + KEKRef, omits Argon2id, and wraps the
// generated CEK via the envelope's KMS client.
func TestBackup_SetupChainEncryption_KMS(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:111111111111:key/setup-test"
	env, stub := newStubKMSEnvelope(t, arn)

	manifest := &irbackup.Manifest{}
	b := &Backup{
		Encryption: &lineage.BackupEncryption{
			Envelope: env,
			Mode:     crypto.EncryptModePerChain,
			KEKRef:   arn,
		},
	}
	cek, err := b.setupChainEncryption(manifest, nil)
	if err != nil {
		t.Fatalf("setupChainEncryption: %v", err)
	}
	if len(cek) != crypto.CEKLen {
		t.Errorf("chain CEK length: got %d want %d", len(cek), crypto.CEKLen)
	}
	if manifest.ChainEncryption == nil {
		t.Fatal("ChainEncryption was not stamped on the manifest")
	}
	enc := manifest.ChainEncryption
	if enc.Algorithm != crypto.AlgorithmAESGCM {
		t.Errorf("Algorithm: got %q want %q", enc.Algorithm, crypto.AlgorithmAESGCM)
	}
	if enc.KEKMode != crypto.KEKModeAWSKMS {
		t.Errorf("KEKMode: got %q want %q", enc.KEKMode, crypto.KEKModeAWSKMS)
	}
	if enc.KEKRef != arn {
		t.Errorf("KEKRef: got %q want %q", enc.KEKRef, arn)
	}
	if enc.Argon2id != nil {
		t.Errorf("Argon2id should be nil for KMS mode; got %+v", enc.Argon2id)
	}
	if len(enc.WrappedCEK) == 0 {
		t.Error("WrappedCEK should be populated in per-chain mode")
	}
	if got := atomic.LoadInt64(&stub.encrypts); got != 1 {
		t.Errorf("expect exactly 1 KMS Encrypt call (chain CEK wrap); got %d", got)
	}
}

// TestRestore_PreflightEncryption_KMS_PerChainCaching pins the
// load-bearing acceptance criterion 4: a 100-chunk chain restore
// makes exactly 1 KMS Decrypt call (per-chain cache).
func TestRestore_PreflightEncryption_KMS_PerChainCaching(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:111111111111:key/cache-test"
	writeEnv, _ := newStubKMSEnvelope(t, arn)

	// Wrap a CEK as a backup-side write would; record it on a
	// synthetic manifest's ChainEncryption.
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	wrapped, err := writeEnv.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	manifest := &irbackup.Manifest{
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			Mode:       crypto.EncryptModePerChain,
			KEKMode:    crypto.KEKModeAWSKMS,
			KEKRef:     arn,
			WrappedCEK: wrapped,
		},
	}

	// Restore-side: build a fresh envelope (matching what the CLI
	// does on restore — DescribeKey at construction is one Decrypt
	// the SDK doesn't run; the production preflight call counter
	// only ticks on actual Encrypt/Decrypt operations).
	readEnv, readStub := newStubKMSEnvelope(t, arn)
	r := &Restore{Envelope: readEnv}
	if err := r.preflightEncryption(manifest); err != nil {
		t.Fatalf("preflightEncryption: %v", err)
	}
	// Simulate 100 chunk reads. preflight has cached r.chainCEK; each
	// chunkCEK call returns the cached value with zero KMS roundtrips.
	chunkInfo := &irbackup.ChunkInfo{
		Encryption: &irbackup.ChunkEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			NonceLen:   crypto.NonceLen,
			AuthTagLen: crypto.AuthTagLen,
			// Empty WrappedCEK = use chain-level CEK (per-chain mode).
		},
	}
	for i := 0; i < 100; i++ {
		got, err := r.chunkCEK(chunkInfo)
		if err != nil {
			t.Fatalf("chunkCEK iter %d: %v", i, err)
		}
		if !bytes.Equal(got, cek) {
			t.Fatalf("chunkCEK iter %d: cek mismatch", i)
		}
	}
	if got := readStub.Decrypts(); got != 1 {
		t.Errorf("acceptance criterion 4: 100 chunk reads must make ≤1 KMS Decrypt call; got %d", got)
	}
}

// TestRestore_PreflightEncryption_KMS_WrongKey pins the wrong-key
// refusal path. A chain wrapped under key A, restored with an
// envelope tied to key B, fails at preflight with a clear error.
func TestRestore_PreflightEncryption_KMS_WrongKey(t *testing.T) {
	const arnA = "arn:aws:kms:us-east-1:1:key/A"
	const arnB = "arn:aws:kms:us-east-1:1:key/B"
	writeEnv, _ := newStubKMSEnvelope(t, arnA)
	cek, _ := crypto.GenerateCEK()
	wrapped, _ := writeEnv.WrapCEK(cek)
	manifest := &irbackup.Manifest{
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm:  crypto.AlgorithmAESGCM,
			Mode:       crypto.EncryptModePerChain,
			KEKMode:    crypto.KEKModeAWSKMS,
			KEKRef:     arnA,
			WrappedCEK: wrapped,
		},
	}
	wrongEnv, _ := newStubKMSEnvelope(t, arnB)
	r := &Restore{Envelope: wrongEnv}
	if err := r.preflightEncryption(manifest); err == nil {
		t.Fatal("expected wrong-key refusal; got nil")
	}
}

// TestRestore_PreflightEncryption_KMS_MissingEnvelope pins the
// missing-key refusal path: an encrypted chain restored without an
// envelope refuses with operator-actionable error citing the chain's
// KEKMode + KEKRef so the operator knows what to supply.
func TestRestore_PreflightEncryption_KMS_MissingEnvelope(t *testing.T) {
	manifest := &irbackup.Manifest{
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm: crypto.AlgorithmAESGCM,
			Mode:      crypto.EncryptModePerChain,
			KEKMode:   crypto.KEKModeAWSKMS,
			KEKRef:    "arn:aws:kms:us-east-1:1:key/x",
		},
	}
	r := &Restore{} // no Envelope
	err := r.preflightEncryption(manifest)
	if err == nil {
		t.Fatal("expected missing-envelope refusal; got nil")
	}
	msg := err.Error()
	if !bytes.Contains([]byte(msg), []byte(crypto.KEKModeAWSKMS)) {
		t.Errorf("error should name the kek_mode; got %q", msg)
	}
	if !bytes.Contains([]byte(msg), []byte("arn:aws:kms:us-east-1:1:key/x")) {
		t.Errorf("error should name the kek_ref; got %q", msg)
	}
}

// TestRestore_PreflightEncryption_KMS_ModeMismatch pins the
// envelope-mode-vs-chain-mode validation: a chain recorded as KMS
// mode but restored with a passphrase envelope fails with a clear
// operator-facing message rather than a cryptic auth-tag mismatch.
func TestRestore_PreflightEncryption_KMS_ModeMismatch(t *testing.T) {
	manifest := &irbackup.Manifest{
		ChainEncryption: &irbackup.ChainEncryption{
			Algorithm: crypto.AlgorithmAESGCM,
			Mode:      crypto.EncryptModePerChain,
			KEKMode:   crypto.KEKModeAWSKMS,
			KEKRef:    "arn:aws:kms:us-east-1:1:key/x",
		},
	}
	// Operator supplied a passphrase envelope by mistake.
	params, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	pe, err := crypto.NewPassphraseEnvelope("oops", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	r := &Restore{Envelope: pe}
	err = r.preflightEncryption(manifest)
	if err == nil {
		t.Fatal("expected mode-mismatch refusal; got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("does not match")) {
		t.Errorf("error should name the mode mismatch; got %v", err)
	}
}
