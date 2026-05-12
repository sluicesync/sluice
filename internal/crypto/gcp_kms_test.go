// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeGCPKMS is a deterministic in-memory KMS stub satisfying
// [GCPKMSAPI]. Encrypt prepends a fixed key-tagged marker so a
// Decrypt call against the wrong instance surfaces as InvalidArgument
// (Cloud KMS's actual shape when the ciphertext doesn't match the
// key). Mirrors fakeKMS in aws_kms_test.go.
type fakeGCPKMS struct {
	keyResource string
	encrypts    int64
	decrypts    int64
	describes   int64
	closes      int64

	// errOn lets a test seed an error to surface on the next call to
	// the matching op. Cleared after one use.
	errOn map[string]error
}

func newFakeGCPKMS(resource string) *fakeGCPKMS {
	return &fakeGCPKMS{keyResource: resource, errOn: map[string]error{}}
}

func (f *fakeGCPKMS) Encrypt(_ context.Context, in *kmspb.EncryptRequest, _ ...gax) (*kmspb.EncryptResponse, error) {
	atomic.AddInt64(&f.encrypts, 1)
	if err := f.popErr("encrypt"); err != nil {
		return nil, err
	}
	blob := append([]byte("gcpkms:"+f.keyResource+"|"), in.Plaintext...)
	return &kmspb.EncryptResponse{Ciphertext: blob, Name: f.keyResource}, nil
}

func (f *fakeGCPKMS) Decrypt(_ context.Context, in *kmspb.DecryptRequest, _ ...gax) (*kmspb.DecryptResponse, error) {
	atomic.AddInt64(&f.decrypts, 1)
	if err := f.popErr("decrypt"); err != nil {
		return nil, err
	}
	prefix := "gcpkms:" + f.keyResource + "|"
	if !bytes.HasPrefix(in.Ciphertext, []byte(prefix)) {
		// Cloud KMS surfaces ciphertext/key mismatches as
		// InvalidArgument; the translator branches on that.
		return nil, status.Error(codes.InvalidArgument, "ciphertext was not produced by key "+f.keyResource)
	}
	plain := in.Ciphertext[len(prefix):]
	return &kmspb.DecryptResponse{Plaintext: plain}, nil
}

func (f *fakeGCPKMS) GetCryptoKey(_ context.Context, _ *kmspb.GetCryptoKeyRequest, _ ...gax) (*kmspb.CryptoKey, error) {
	atomic.AddInt64(&f.describes, 1)
	if err := f.popErr("describe"); err != nil {
		return nil, err
	}
	return &kmspb.CryptoKey{Name: f.keyResource, Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT}, nil
}

func (f *fakeGCPKMS) Close() error {
	atomic.AddInt64(&f.closes, 1)
	return nil
}

func (f *fakeGCPKMS) popErr(op string) error {
	err, ok := f.errOn[op]
	if !ok {
		return nil
	}
	delete(f.errOn, op)
	return err
}

func (f *fakeGCPKMS) Encrypts() int64  { return atomic.LoadInt64(&f.encrypts) }
func (f *fakeGCPKMS) Decrypts() int64  { return atomic.LoadInt64(&f.decrypts) }
func (f *fakeGCPKMS) Describes() int64 { return atomic.LoadInt64(&f.describes) }

func TestGCPKMSEnvelope_RoundTrip(t *testing.T) {
	const resource = "projects/test/locations/us-east1/keyRings/r/cryptoKeys/k"
	stub := newFakeGCPKMS(resource)
	env, err := NewGCPKMSEnvelope(context.Background(), resource, WithGCPKMSClient(stub))
	if err != nil {
		t.Fatalf("NewGCPKMSEnvelope: %v", err)
	}
	if env.Mode() != KEKModeGCPKMS {
		t.Errorf("Mode() = %q; want %q", env.Mode(), KEKModeGCPKMS)
	}
	if env.KeyResource() != resource {
		t.Errorf("KeyResource() = %q; want %q", env.KeyResource(), resource)
	}
	if stub.Describes() != 1 {
		t.Errorf("Describes() = %d; want 1 (preflight)", stub.Describes())
	}

	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if stub.Encrypts() != 1 {
		t.Errorf("Encrypts() = %d; want 1", stub.Encrypts())
	}

	out, err := env.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("UnwrapCEK: %v", err)
	}
	if !bytes.Equal(out, cek) {
		t.Errorf("UnwrapCEK = %x; want %x", out, cek)
	}
	if stub.Decrypts() != 1 {
		t.Errorf("Decrypts() = %d; want 1", stub.Decrypts())
	}
}

func TestGCPKMSEnvelope_EmptyResource(t *testing.T) {
	_, err := NewGCPKMSEnvelope(context.Background(), "")
	if err == nil {
		t.Fatal("err = nil; want error for empty resource")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("err = %v; want mention of \"empty\"", err)
	}
}

func TestGCPKMSEnvelope_PreflightNotFound(t *testing.T) {
	const resource = "projects/test/locations/us/keyRings/r/cryptoKeys/missing"
	stub := newFakeGCPKMS(resource)
	stub.errOn["describe"] = status.Error(codes.NotFound, "key not found")
	_, err := NewGCPKMSEnvelope(context.Background(), resource, WithGCPKMSClient(stub))
	if err == nil {
		t.Fatal("err = nil; want preflight failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v; want \"not found\" hint", err)
	}
}

func TestGCPKMSEnvelope_PermissionDeniedErrorMessages(t *testing.T) {
	const resource = "projects/test/locations/us/keyRings/r/cryptoKeys/k"
	for _, c := range []struct {
		op     string
		err    error
		expect string
	}{
		{"encrypt", status.Error(codes.PermissionDenied, "no permission"), "Encrypter"},
		{"decrypt", status.Error(codes.PermissionDenied, "no permission"), "Decrypter"},
		{"describe", status.Error(codes.PermissionDenied, "no permission"), "Viewer"},
	} {
		c := c
		t.Run(c.op, func(t *testing.T) {
			got := translateGCPKMSError(c.err, resource, c.op)
			if got == nil {
				t.Fatal("translateGCPKMSError returned nil")
			}
			if !strings.Contains(got.Error(), c.expect) {
				t.Errorf("err = %v; want substring %q", got, c.expect)
			}
			if !strings.Contains(got.Error(), resource) {
				t.Errorf("err = %v; want key resource %q in message", got, resource)
			}
		})
	}
}

func TestGCPKMSEnvelope_UnauthenticatedHint(t *testing.T) {
	got := translateGCPKMSError(
		status.Error(codes.Unauthenticated, "no auth"),
		"projects/x/locations/y/keyRings/z/cryptoKeys/k",
		"encrypt",
	)
	if !strings.Contains(got.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Errorf("err = %v; want GOOGLE_APPLICATION_CREDENTIALS hint", got)
	}
}

func TestGCPKMSEnvelope_WrongKeyDecryptInvalidArgument(t *testing.T) {
	const resA = "projects/p/locations/us/keyRings/r/cryptoKeys/a"
	const resB = "projects/p/locations/us/keyRings/r/cryptoKeys/b"
	stubA := newFakeGCPKMS(resA)
	stubB := newFakeGCPKMS(resB)

	envA, err := NewGCPKMSEnvelope(context.Background(), resA, WithGCPKMSClient(stubA))
	if err != nil {
		t.Fatalf("NewGCPKMSEnvelope(A): %v", err)
	}
	envB, err := NewGCPKMSEnvelope(context.Background(), resB, WithGCPKMSClient(stubB))
	if err != nil {
		t.Fatalf("NewGCPKMSEnvelope(B): %v", err)
	}

	cek, _ := GenerateCEK()
	wrappedA, err := envA.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK(A): %v", err)
	}
	_, err = envB.UnwrapCEK(wrappedA)
	if err == nil {
		t.Fatal("UnwrapCEK(B) = nil; want failure (wrong key)")
	}
	if !strings.Contains(err.Error(), "invalid argument") && !strings.Contains(err.Error(), "InvalidArgument") {
		t.Errorf("err = %v; want InvalidArgument mention", err)
	}
}

func TestCryptoKeyForResource(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{
			"projects/p/locations/us/keyRings/r/cryptoKeys/k",
			"projects/p/locations/us/keyRings/r/cryptoKeys/k",
		},
		{
			"projects/p/locations/us/keyRings/r/cryptoKeys/k/cryptoKeyVersions/3",
			"projects/p/locations/us/keyRings/r/cryptoKeys/k",
		},
		{"", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			got := cryptoKeyForResource(c.in)
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}
