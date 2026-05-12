// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
)

// fakeAzureKMS is a deterministic in-memory Key Vault stub
// satisfying [AzureKMSAPI]. WrapKey prepends a fixed key-tagged
// marker so an UnwrapKey call against the wrong instance surfaces
// as a BadParameter error (Azure's actual shape when the ciphertext
// doesn't match the key). Mirrors fakeKMS / fakeGCPKMS.
type fakeAzureKMS struct {
	keyName string
	wraps   int64
	unwraps int64
	gets    int64
	errOnOp map[string]error
}

func newFakeAzureKMS(keyName string) *fakeAzureKMS {
	return &fakeAzureKMS{keyName: keyName, errOnOp: map[string]error{}}
}

func (f *fakeAzureKMS) WrapKey(_ context.Context, name, _ string, params azkeys.KeyOperationParameters, _ *azkeys.WrapKeyOptions) (azkeys.WrapKeyResponse, error) {
	atomic.AddInt64(&f.wraps, 1)
	if err := f.popErr("wrap"); err != nil {
		return azkeys.WrapKeyResponse{}, err
	}
	if name != f.keyName {
		return azkeys.WrapKeyResponse{}, fakeAzureAPIError("KeyNotFound", 404, "key not found")
	}
	blob := append([]byte("azkv:"+f.keyName+"|"), params.Value...)
	kid := azkeys.ID("https://stub.vault.azure.net/keys/" + f.keyName + "/v1")
	return azkeys.WrapKeyResponse{
		KeyOperationResult: azkeys.KeyOperationResult{
			Result: blob,
			KID:    &kid,
		},
	}, nil
}

func (f *fakeAzureKMS) UnwrapKey(_ context.Context, name, _ string, params azkeys.KeyOperationParameters, _ *azkeys.UnwrapKeyOptions) (azkeys.UnwrapKeyResponse, error) {
	atomic.AddInt64(&f.unwraps, 1)
	if err := f.popErr("unwrap"); err != nil {
		return azkeys.UnwrapKeyResponse{}, err
	}
	if name != f.keyName {
		return azkeys.UnwrapKeyResponse{}, fakeAzureAPIError("KeyNotFound", 404, "key not found")
	}
	prefix := "azkv:" + f.keyName + "|"
	if !bytes.HasPrefix(params.Value, []byte(prefix)) {
		// Azure surfaces wrong-key unwrap as a BadParameter; the
		// translator branches on that error code.
		return azkeys.UnwrapKeyResponse{}, fakeAzureAPIError("BadParameter", 400, "ciphertext was not wrapped by this key")
	}
	plain := params.Value[len(prefix):]
	kid := azkeys.ID("https://stub.vault.azure.net/keys/" + f.keyName + "/v1")
	return azkeys.UnwrapKeyResponse{
		KeyOperationResult: azkeys.KeyOperationResult{
			Result: plain,
			KID:    &kid,
		},
	}, nil
}

func (f *fakeAzureKMS) GetKey(_ context.Context, name, _ string, _ *azkeys.GetKeyOptions) (azkeys.GetKeyResponse, error) {
	atomic.AddInt64(&f.gets, 1)
	if err := f.popErr("get"); err != nil {
		return azkeys.GetKeyResponse{}, err
	}
	if name != f.keyName {
		return azkeys.GetKeyResponse{}, fakeAzureAPIError("KeyNotFound", 404, "key not found")
	}
	kid := azkeys.ID("https://stub.vault.azure.net/keys/" + f.keyName + "/v1")
	enabled := true
	return azkeys.GetKeyResponse{
		KeyBundle: azkeys.KeyBundle{
			Key: &azkeys.JSONWebKey{
				KID: &kid,
			},
			Attributes: &azkeys.KeyAttributes{
				Enabled: &enabled,
			},
		},
	}, nil
}

func (f *fakeAzureKMS) popErr(op string) error {
	err, ok := f.errOnOp[op]
	if !ok {
		return nil
	}
	delete(f.errOnOp, op)
	return err
}

func (f *fakeAzureKMS) Wraps() int64   { return atomic.LoadInt64(&f.wraps) }
func (f *fakeAzureKMS) Unwraps() int64 { return atomic.LoadInt64(&f.unwraps) }
func (f *fakeAzureKMS) Gets() int64    { return atomic.LoadInt64(&f.gets) }

// fakeAzureAPIError produces an *azcore.ResponseError shaped like
// what the real SDK surfaces, so translateAzureKMSError's ErrorCode
// branches fire. The `_` msg parameter is kept at the call site for
// inline documentation of what each fake error represents; the
// azcore.ResponseError shape doesn't surface custom messages
// (Error() reads from RawResponse) so we don't thread it further.
func fakeAzureAPIError(code string, status int, _ string) error {
	return &azcore.ResponseError{
		ErrorCode:   code,
		StatusCode:  status,
		RawResponse: &http.Response{StatusCode: status, Body: http.NoBody},
		// The underlying error must carry the message for callers
		// that log the wrapped error. errors.New is enough here.
	}
}

func TestAzureKMSEnvelope_RoundTrip(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/test-key"
	stub := newFakeAzureKMS("test-key")
	env, err := NewAzureKMSEnvelope(context.Background(), keyID, WithAzureKMSClient(stub))
	if err != nil {
		t.Fatalf("NewAzureKMSEnvelope: %v", err)
	}
	if env.Mode() != KEKModeAzureKMS {
		t.Errorf("Mode() = %q; want %q", env.Mode(), KEKModeAzureKMS)
	}
	if env.KeyID() != keyID {
		t.Errorf("KeyID() = %q; want %q", env.KeyID(), keyID)
	}
	if stub.Gets() != 1 {
		t.Errorf("Gets() = %d; want 1 (preflight)", stub.Gets())
	}

	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if stub.Wraps() != 1 {
		t.Errorf("Wraps() = %d; want 1", stub.Wraps())
	}

	out, err := env.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("UnwrapCEK: %v", err)
	}
	if !bytes.Equal(out, cek) {
		t.Errorf("UnwrapCEK = %x; want %x", out, cek)
	}
	if stub.Unwraps() != 1 {
		t.Errorf("Unwraps() = %d; want 1", stub.Unwraps())
	}
}

func TestAzureKMSEnvelope_EmptyKeyID(t *testing.T) {
	_, err := NewAzureKMSEnvelope(context.Background(), "")
	if err == nil {
		t.Fatal("err = nil; want error for empty key ID")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("err = %v; want \"empty\" hint", err)
	}
}

func TestAzureKMSEnvelope_PreflightNotFound(t *testing.T) {
	stub := newFakeAzureKMS("test-key")
	stub.errOnOp["get"] = fakeAzureAPIError("KeyNotFound", 404, "missing")
	_, err := NewAzureKMSEnvelope(context.Background(),
		"https://test-vault.vault.azure.net/keys/test-key",
		WithAzureKMSClient(stub))
	if err == nil {
		t.Fatal("err = nil; want preflight failure")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("err = %v; want \"not found\" hint", err)
	}
}

func TestAzureKMSEnvelope_WrongKeyDecryptBadParameter(t *testing.T) {
	stubA := newFakeAzureKMS("key-a")
	stubB := newFakeAzureKMS("key-b")
	envA, err := NewAzureKMSEnvelope(context.Background(),
		"https://v.vault.azure.net/keys/key-a", WithAzureKMSClient(stubA))
	if err != nil {
		t.Fatalf("NewAzureKMSEnvelope(A): %v", err)
	}
	envB, err := NewAzureKMSEnvelope(context.Background(),
		"https://v.vault.azure.net/keys/key-b", WithAzureKMSClient(stubB))
	if err != nil {
		t.Fatalf("NewAzureKMSEnvelope(B): %v", err)
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
	if !strings.Contains(err.Error(), "rejected") && !strings.Contains(err.Error(), "BadParameter") {
		t.Errorf("err = %v; want BadParameter mention", err)
	}
}

func TestAzureKMSEnvelope_ForbiddenErrorMessage(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/test-key"
	got := translateAzureKMSError(
		fakeAzureAPIError("Forbidden", 403, "no permission"),
		keyID, "encrypt",
	)
	if got == nil {
		t.Fatal("translateAzureKMSError returned nil")
	}
	if !strings.Contains(got.Error(), "Crypto User") {
		t.Errorf("err = %v; want \"Crypto User\" hint", got)
	}
	if !strings.Contains(got.Error(), keyID) {
		t.Errorf("err = %v; want key ID in message", got)
	}
}

func TestAzureKMSEnvelope_DescribeForbiddenWantsReader(t *testing.T) {
	const keyID = "https://test-vault.vault.azure.net/keys/test-key"
	got := translateAzureKMSError(
		fakeAzureAPIError("Forbidden", 403, "no permission"),
		keyID, "describe",
	)
	if !strings.Contains(got.Error(), "Crypto Reader") {
		t.Errorf("err = %v; want \"Crypto Reader\" hint for describe", got)
	}
}

func TestAzureKMSEnvelope_StatusCodeFallback(t *testing.T) {
	// ErrorCode empty → status-code fallback fires.
	const keyID = "https://test-vault.vault.azure.net/keys/test-key"
	cases := []struct {
		name   string
		status int
		expect string
	}{
		{"401 unauthenticated", 401, "credentials"},
		{"403 forbidden", 403, "Crypto User"},
		{"404 not found", 404, "not found"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := translateAzureKMSError(
				&azcore.ResponseError{StatusCode: c.status, RawResponse: &http.Response{StatusCode: c.status, Body: http.NoBody}},
				keyID, "encrypt",
			)
			if !strings.Contains(got.Error(), c.expect) {
				t.Errorf("err = %v; want substring %q", got, c.expect)
			}
		})
	}
}

func TestAzureKMSEnvelope_NonResponseErrorFallthrough(t *testing.T) {
	// Errors that aren't *azcore.ResponseError fall through with the
	// key ID + op preserved.
	const keyID = "https://test-vault.vault.azure.net/keys/test-key"
	got := translateAzureKMSError(errors.New("connection refused"), keyID, "encrypt")
	if !strings.Contains(got.Error(), keyID) {
		t.Errorf("err = %v; want key ID in fallthrough message", got)
	}
	if !strings.Contains(got.Error(), "encrypt") {
		t.Errorf("err = %v; want op in fallthrough message", got)
	}
}

func TestParseAzureKeyID(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		wantVault   string
		wantName    string
		wantVersion string
		wantErr     bool
	}{
		{
			"versioned",
			"https://my-vault.vault.azure.net/keys/my-key/abc123",
			"https://my-vault.vault.azure.net",
			"my-key",
			"abc123",
			false,
		},
		{
			"latest (no version)",
			"https://my-vault.vault.azure.net/keys/my-key",
			"https://my-vault.vault.azure.net",
			"my-key",
			"",
			false,
		},
		{
			"managed HSM",
			"https://my-hsm.managedhsm.azure.net/keys/my-key/v1",
			"https://my-hsm.managedhsm.azure.net",
			"my-key",
			"v1",
			false,
		},
		{"empty", "", "", "", "", true},
		{"http scheme rejected", "http://v.vault.azure.net/keys/k", "", "", "", true},
		{"missing /keys/ prefix", "https://v.vault.azure.net/notkeys/k", "", "", "", true},
		{"empty key name", "https://v.vault.azure.net/keys/", "", "", "", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			vault, name, version, err := parseAzureKeyID(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("err = nil; want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAzureKeyID: %v", err)
			}
			if vault != c.wantVault {
				t.Errorf("vault = %q; want %q", vault, c.wantVault)
			}
			if name != c.wantName {
				t.Errorf("name = %q; want %q", name, c.wantName)
			}
			if version != c.wantVersion {
				t.Errorf("version = %q; want %q", version, c.wantVersion)
			}
		})
	}
}
