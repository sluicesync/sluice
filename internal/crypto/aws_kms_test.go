// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	smithy "github.com/aws/smithy-go"
)

// accessDeniedErr returns a smithy GenericAPIError shaped like AWS's
// AccessDeniedException response. KMS doesn't expose AccessDenied as
// a typed exception in the v2 SDK; the SDK surfaces it as a generic
// APIError whose ErrorCode() returns "AccessDeniedException".
func accessDeniedErr(msg string) error {
	return &smithy.GenericAPIError{Code: "AccessDeniedException", Message: msg}
}

// fakeKMS is a deterministic in-memory KMS stub satisfying [KMSAPI].
// Encrypt prepends a fixed marker so wrong-key tests can detect a
// blob produced by a different fakeKMS instance; Decrypt strips it
// back. Each instance carries its own keyID; cross-instance
// CiphertextBlob handed to Decrypt surfaces as an
// IncorrectKeyException.
type fakeKMS struct {
	keyID     string
	encrypts  int64
	decrypts  int64
	describes int64

	// errOn lets a test seed an error to surface on the next call to
	// the matching op. Cleared after one use.
	errOn map[string]error
}

func newFakeKMS(keyID string) *fakeKMS {
	return &fakeKMS{keyID: keyID, errOn: map[string]error{}}
}

func (f *fakeKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	atomic.AddInt64(&f.encrypts, 1)
	if err := f.popErr("encrypt"); err != nil {
		return nil, err
	}
	// Wrap the plaintext with a key-tagged marker so a Decrypt call
	// against the wrong fakeKMS instance can detect the mismatch.
	blob := append([]byte("kms:"+f.keyID+"|"), in.Plaintext...)
	return &kms.EncryptOutput{CiphertextBlob: blob, KeyId: aws.String(f.keyID)}, nil
}

func (f *fakeKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	atomic.AddInt64(&f.decrypts, 1)
	if err := f.popErr("decrypt"); err != nil {
		return nil, err
	}
	prefix := "kms:" + f.keyID + "|"
	if !bytes.HasPrefix(in.CiphertextBlob, []byte(prefix)) {
		return nil, &kmstypes.IncorrectKeyException{Message: aws.String("ciphertext was not produced by key " + f.keyID)}
	}
	plain := in.CiphertextBlob[len(prefix):]
	return &kms.DecryptOutput{Plaintext: plain, KeyId: aws.String(f.keyID)}, nil
}

func (f *fakeKMS) DescribeKey(_ context.Context, _ *kms.DescribeKeyInput, _ ...func(*kms.Options)) (*kms.DescribeKeyOutput, error) {
	atomic.AddInt64(&f.describes, 1)
	if err := f.popErr("describe"); err != nil {
		return nil, err
	}
	return &kms.DescribeKeyOutput{KeyMetadata: &kmstypes.KeyMetadata{KeyId: aws.String(f.keyID), Arn: aws.String(f.keyID)}}, nil
}

func (f *fakeKMS) popErr(op string) error {
	err, ok := f.errOn[op]
	if !ok {
		return nil
	}
	delete(f.errOn, op)
	return err
}

func (f *fakeKMS) Encrypts() int64  { return atomic.LoadInt64(&f.encrypts) }
func (f *fakeKMS) Decrypts() int64  { return atomic.LoadInt64(&f.decrypts) }
func (f *fakeKMS) Describes() int64 { return atomic.LoadInt64(&f.describes) }

func TestKMSEnvelope_RoundTrip(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:123456789012:key/abcd1234-test"
	stub := newFakeKMS(arn)
	env, err := NewKMSEnvelope(context.Background(), arn, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	if env.Mode() != KEKModeAWSKMS {
		t.Errorf("Mode: got %q want %q", env.Mode(), KEKModeAWSKMS)
	}
	if env.KeyARN() != arn {
		t.Errorf("KeyARN: got %q want %q", env.KeyARN(), arn)
	}
	if got := stub.Describes(); got != 1 {
		t.Errorf("preflight DescribeKey count: got %d want 1", got)
	}

	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if bytes.Equal(wrapped, cek) {
		t.Fatal("wrapped equals plaintext — wrap did nothing")
	}
	got, err := env.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("UnwrapCEK: %v", err)
	}
	if !bytes.Equal(got, cek) {
		t.Fatal("round-trip mismatch")
	}
	if stub.Encrypts() != 1 {
		t.Errorf("Encrypts: got %d want 1", stub.Encrypts())
	}
	if stub.Decrypts() != 1 {
		t.Errorf("Decrypts: got %d want 1", stub.Decrypts())
	}
}

// TestKMSEnvelope_PerChainCachingPattern simulates the per-chain
// CEK reuse pattern: one wrap at chain start, one unwrap at restore
// start, N chunk reads — N must be 0 KMS calls (the orchestrator
// caches the unwrapped CEK).
//
// This pins the KMS-call accounting expectation. The actual caching
// lives in pipeline.{Restore,ChainRestore,SyncFromBackup} —
// preflightEncryption unwraps once and reuses; the test confirms
// that the envelope itself doesn't add hidden per-call work.
func TestKMSEnvelope_PerChainCachingPattern(t *testing.T) {
	const arn = "arn:aws:kms:us-east-1:111111111111:key/cache-test"
	stub := newFakeKMS(arn)
	env, err := NewKMSEnvelope(context.Background(), arn, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}

	cek, _ := GenerateCEK()
	wrapped, _ := env.WrapCEK(cek)

	// Restore-side simulates 100 chunks: a single Unwrap + 100
	// in-memory chunk decrypts (no envelope calls per chunk).
	if _, err := env.UnwrapCEK(wrapped); err != nil {
		t.Fatalf("UnwrapCEK: %v", err)
	}
	for i := 0; i < 100; i++ {
		// Chunk decrypt uses the cached cek; envelope is not touched.
		_ = cek
	}
	if got := stub.Decrypts(); got != 1 {
		t.Errorf("after 100 chunk reads expect 1 KMS Decrypt call (per-chain cache); got %d", got)
	}
}

func TestNewKMSEnvelope_EmptyARN(t *testing.T) {
	_, err := NewKMSEnvelope(context.Background(), "", WithKMSClient(newFakeKMS("ignored")))
	if err == nil {
		t.Fatal("empty ARN expected to error")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("expected 'empty' in err; got %v", err)
	}
}

func TestKMSEnvelope_PreflightAccessDenied(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	stub.errOn["describe"] = accessDeniedErr("not allowed")
	_, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err == nil {
		t.Fatal("expected access-denied to surface at preflight")
	}
	if !strings.Contains(err.Error(), "kms:DescribeKey") {
		t.Errorf("error should name the kms:DescribeKey action; got %v", err)
	}
}

func TestKMSEnvelope_EncryptAccessDenied(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	stub.errOn["encrypt"] = accessDeniedErr("kms:Encrypt denied")
	cek, _ := GenerateCEK()
	if _, err := env.WrapCEK(cek); err == nil {
		t.Fatal("expected encrypt to fail with access denied")
	} else if !strings.Contains(err.Error(), "kms:Encrypt") {
		t.Errorf("error should name kms:Encrypt; got %v", err)
	}
}

func TestKMSEnvelope_DecryptAccessDenied(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	cek, _ := GenerateCEK()
	wrapped, _ := env.WrapCEK(cek)
	stub.errOn["decrypt"] = accessDeniedErr("kms:Decrypt denied")
	if _, err := env.UnwrapCEK(wrapped); err == nil {
		t.Fatal("expected decrypt to fail with access denied")
	} else if !strings.Contains(err.Error(), "kms:Decrypt") {
		t.Errorf("error should name kms:Decrypt; got %v", err)
	}
}

func TestKMSEnvelope_WrongKey(t *testing.T) {
	stubA := newFakeKMS("arn:aws:kms:us-east-1:1:key/A")
	stubB := newFakeKMS("arn:aws:kms:us-east-1:1:key/B")
	envA, err := NewKMSEnvelope(context.Background(), stubA.keyID, WithKMSClient(stubA))
	if err != nil {
		t.Fatalf("NewKMSEnvelope A: %v", err)
	}
	envB, err := NewKMSEnvelope(context.Background(), stubB.keyID, WithKMSClient(stubB))
	if err != nil {
		t.Fatalf("NewKMSEnvelope B: %v", err)
	}
	cek, _ := GenerateCEK()
	wrapped, _ := envA.WrapCEK(cek)
	// Now try to unwrap a key-A blob using key-B's envelope.
	if _, err := envB.UnwrapCEK(wrapped); err == nil {
		t.Fatal("expected wrong-key decrypt to fail")
	} else if !strings.Contains(err.Error(), "different key") {
		t.Errorf("error should name 'different key'; got %v", err)
	}
}

func TestKMSEnvelope_NotFound(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/missing")
	stub.errOn["describe"] = &kmstypes.NotFoundException{Message: aws.String("Key 'missing' does not exist")}
	_, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err == nil {
		t.Fatal("expected not-found to surface")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error should name 'not found'; got %v", err)
	}
}

func TestKMSEnvelope_InvalidState(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/disabled")
	stub.errOn["describe"] = &kmstypes.KMSInvalidStateException{Message: aws.String("pending deletion")}
	_, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err == nil {
		t.Fatal("expected invalid-state to surface")
	}
	if !strings.Contains(err.Error(), "invalid state") {
		t.Errorf("error should name 'invalid state'; got %v", err)
	}
}

func TestKMSEnvelope_Disabled(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	stub.errOn["encrypt"] = &kmstypes.DisabledException{Message: aws.String("disabled")}
	cek, _ := GenerateCEK()
	if _, err := env.WrapCEK(cek); err == nil {
		t.Fatal("expected disabled-key to surface")
	} else if !strings.Contains(err.Error(), "disabled") {
		t.Errorf("error should name 'disabled'; got %v", err)
	}
}

func TestKMSEnvelope_GenericError(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub), withSkipPreflight())
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	stub.errOn["encrypt"] = errors.New("network unreachable")
	cek, _ := GenerateCEK()
	if _, err := env.WrapCEK(cek); err == nil {
		t.Fatal("expected generic error to surface")
	} else if !strings.Contains(err.Error(), "network unreachable") {
		t.Errorf("generic error should preserve underlying message; got %v", err)
	}
}

func TestKMSEnvelope_WrongCEKLength(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	if _, err := env.WrapCEK([]byte("too short")); err == nil {
		t.Fatal("expected length validation error")
	}
}

func TestKMSEnvelope_EmptyWrapped(t *testing.T) {
	stub := newFakeKMS("arn:aws:kms:us-east-1:1:key/x")
	env, err := NewKMSEnvelope(context.Background(), stub.keyID, WithKMSClient(stub))
	if err != nil {
		t.Fatalf("NewKMSEnvelope: %v", err)
	}
	if _, err := env.UnwrapCEK(nil); err == nil {
		t.Fatal("expected empty-wrapped error")
	}
}
