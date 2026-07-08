// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	if len(cek) != CEKLen {
		t.Fatalf("GenerateCEK length = %d; want %d", len(cek), CEKLen)
	}
	plaintext := []byte("the quick brown fox jumps over the lazy dog")
	ct, err := EncryptChunk(plaintext, cek)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	if len(ct) < NonceLen+AuthTagLen {
		t.Fatalf("ciphertext too short: %d", len(ct))
	}
	if bytes.Contains(ct, plaintext) {
		t.Fatalf("ciphertext contains plaintext bytes — encryption did nothing")
	}
	pt, err := DecryptChunk(ct, cek)
	if err != nil {
		t.Fatalf("DecryptChunk: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatalf("round-trip mismatch: got %q want %q", pt, plaintext)
	}
}

func TestDecrypt_WrongCEK(t *testing.T) {
	cek1, _ := GenerateCEK()
	cek2, _ := GenerateCEK()
	ct, err := EncryptChunk([]byte("payload"), cek1)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	if _, err := DecryptChunk(ct, cek2); err == nil {
		t.Fatalf("DecryptChunk with wrong cek expected to fail; got nil")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	cek, _ := GenerateCEK()
	plaintext := []byte("authentic payload")
	ct, err := EncryptChunk(plaintext, cek)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}
	// Flip a bit in the ciphertext body.
	tampered := append([]byte(nil), ct...)
	tampered[NonceLen+1] ^= 0x01
	if _, err := DecryptChunk(tampered, cek); err == nil {
		t.Fatalf("tampered ciphertext expected to fail auth-tag check; got nil")
	}
}

func TestDecrypt_ShortCiphertext(t *testing.T) {
	cek, _ := GenerateCEK()
	if _, err := DecryptChunk([]byte("too-short"), cek); err == nil {
		t.Fatalf("short input expected to error; got nil")
	}
}

func TestPassphraseEnvelope_RoundTrip(t *testing.T) {
	params, err := DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	env, err := NewPassphraseEnvelope("correct horse battery staple", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	if env.Mode() != KEKModePassphrase {
		t.Fatalf("Mode() = %q; want %q", env.Mode(), KEKModePassphrase)
	}
	cek, err := GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}
	wrapped, err := env.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	got, err := env.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("UnwrapCEK: %v", err)
	}
	if !bytes.Equal(got, cek) {
		t.Fatalf("unwrap mismatch")
	}
}

func TestPassphraseEnvelope_WrongPassphrase(t *testing.T) {
	params, _ := DefaultArgon2idParams()
	env1, err := NewPassphraseEnvelope("right", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope right: %v", err)
	}
	env2, err := NewPassphraseEnvelope("wrong", params)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope wrong: %v", err)
	}
	cek, _ := GenerateCEK()
	wrapped, err := env1.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if _, err := env2.UnwrapCEK(wrapped); err == nil {
		t.Fatalf("UnwrapCEK with wrong passphrase expected to fail; got nil")
	}
}

func TestPassphraseEnvelope_EmptyPassphrase(t *testing.T) {
	params, _ := DefaultArgon2idParams()
	if _, err := NewPassphraseEnvelope("", params); err == nil {
		t.Fatalf("empty passphrase expected to error; got nil")
	}
}

func TestPassphraseEnvelope_BadParams(t *testing.T) {
	cases := []struct {
		name   string
		params Argon2idParams
	}{
		{"empty salt", Argon2idParams{Salt: nil, Memory: 1024, Iterations: 1, Parallelism: 1, KeyLen: KEKLen}},
		{"zero memory", Argon2idParams{Salt: []byte("abcdefghijklmnop"), Memory: 0, Iterations: 1, Parallelism: 1, KeyLen: KEKLen}},
		{"zero iterations", Argon2idParams{Salt: []byte("abcdefghijklmnop"), Memory: 1024, Iterations: 0, Parallelism: 1, KeyLen: KEKLen}},
		{"zero parallelism", Argon2idParams{Salt: []byte("abcdefghijklmnop"), Memory: 1024, Iterations: 1, Parallelism: 0, KeyLen: KEKLen}},
		{"wrong key len", Argon2idParams{Salt: []byte("abcdefghijklmnop"), Memory: 1024, Iterations: 1, Parallelism: 1, KeyLen: 16}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPassphraseEnvelope("pass", tc.params); err == nil {
				t.Fatalf("expected error; got nil")
			}
		})
	}
}

// TestArgon2idParams_UntrustedManifestBounds pins the audit N-7 clamp:
// Argon2id params arrive VERBATIM from a backup manifest and hit
// argon2.IDKey before any authentication, so each bound must refuse
// loudly (naming the param, value, and bound) while the at-bound value
// still passes. The at-bound cases go through validateArgon2idParams
// directly — deliberately not NewPassphraseEnvelope — so the memory
// boundary (2 GiB) is pinned without allocating 2 GiB in a unit test;
// the over-bound cases go through the real constructor to prove the
// refusal fires before the derivation.
func TestArgon2idParams_UntrustedManifestBounds(t *testing.T) {
	base := func() Argon2idParams {
		return Argon2idParams{
			Salt:        []byte("abcdefghijklmnop"),
			Memory:      1024,
			Iterations:  1,
			Parallelism: 1,
			KeyLen:      KEKLen,
		}
	}

	t.Run("at-bound values pass validation", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(*Argon2idParams)
		}{
			{"memory at 2 GiB", func(p *Argon2idParams) { p.Memory = MaxArgon2idMemoryKiB }},
			{"iterations at bound", func(p *Argon2idParams) { p.Iterations = MaxArgon2idIterations }},
			{"parallelism at bound", func(p *Argon2idParams) { p.Parallelism = MaxArgon2idParallelism; p.Memory = 8 * MaxArgon2idParallelism }},
			{"salt at bound", func(p *Argon2idParams) { p.Salt = make([]byte, MaxArgon2idSaltLen) }},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				p := base()
				tc.mutate(&p)
				if err := validateArgon2idParams(p); err != nil {
					t.Fatalf("at-bound params refused: %v", err)
				}
			})
		}
	})

	t.Run("over-bound values refuse via the constructor", func(t *testing.T) {
		cases := []struct {
			name    string
			mutate  func(*Argon2idParams)
			wantSub string
		}{
			// The headline bomb: memory_kib maxed out asks for ~4 TiB.
			{"memory bomb", func(p *Argon2idParams) { p.Memory = 4294967295 }, "memory 4294967295 KiB exceeds"},
			{"memory one over bound", func(p *Argon2idParams) { p.Memory = MaxArgon2idMemoryKiB + 1 }, "memory"},
			{"iterations bomb", func(p *Argon2idParams) { p.Iterations = MaxArgon2idIterations + 1 }, "iterations 65 exceeds"},
			{"parallelism over bound", func(p *Argon2idParams) { p.Parallelism = MaxArgon2idParallelism + 1 }, "parallelism 33 exceeds"},
			{"oversized salt", func(p *Argon2idParams) { p.Salt = make([]byte, MaxArgon2idSaltLen+1) }, "salt length 1025 exceeds"},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				p := base()
				tc.mutate(&p)
				_, err := NewPassphraseEnvelope("pass", p)
				if err == nil {
					t.Fatal("over-bound params accepted; the pre-auth KDF-bomb clamp is gone")
				}
				if !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("error %q missing substring %q (must name the param + value + bound)", err.Error(), tc.wantSub)
				}
			})
		}
	})
}

func TestArgon2idParams_Determinism(t *testing.T) {
	// Same passphrase + same salt + same params → same KEK.
	params := Argon2idParams{
		Salt:        []byte("deterministic-sa"),
		Memory:      1024,
		Iterations:  1,
		Parallelism: 1,
		KeyLen:      KEKLen,
	}
	env1, err := NewPassphraseEnvelope("p", params)
	if err != nil {
		t.Fatalf("env1: %v", err)
	}
	env2, err := NewPassphraseEnvelope("p", params)
	if err != nil {
		t.Fatalf("env2: %v", err)
	}
	cek := bytes.Repeat([]byte{0x42}, CEKLen)
	w1, err := env1.WrapCEK(cek)
	if err != nil {
		t.Fatalf("w1: %v", err)
	}
	// Wrap output is non-deterministic (random nonce per call) but
	// unwrap-of-each-other's-output must succeed since the KEKs match.
	if got, err := env2.UnwrapCEK(w1); err != nil {
		t.Fatalf("env2 unwrap of env1 wrap: %v", err)
	} else if !bytes.Equal(got, cek) {
		t.Fatalf("unwrap mismatch")
	}
}

func TestArgon2idParams_JSONRoundTrip(t *testing.T) {
	in := Argon2idParams{
		Salt:        []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		Memory:      65536,
		Iterations:  3,
		Parallelism: 4,
		KeyLen:      32,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), "memory_kib") {
		t.Fatalf("expected memory_kib field in JSON; got %s", b)
	}
	var out Argon2idParams
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Equal(out.Salt, in.Salt) ||
		out.Memory != in.Memory ||
		out.Iterations != in.Iterations ||
		out.Parallelism != in.Parallelism ||
		out.KeyLen != in.KeyLen {
		t.Fatalf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
}

func TestEncryptChunk_BadCEK(t *testing.T) {
	if _, err := EncryptChunk([]byte("x"), []byte("short-key")); err == nil {
		t.Fatalf("short cek expected to error; got nil")
	}
}

func TestPassphraseEnvelope_WrapBadCEK(t *testing.T) {
	params, _ := DefaultArgon2idParams()
	env, _ := NewPassphraseEnvelope("p", params)
	if _, err := env.WrapCEK([]byte("too-short")); err == nil {
		t.Fatalf("wrap bad cek expected to error; got nil")
	}
}

func TestEncryptChunk_DifferentNoncePerCall(t *testing.T) {
	cek, _ := GenerateCEK()
	plaintext := []byte("same input")
	ct1, err := EncryptChunk(plaintext, cek)
	if err != nil {
		t.Fatalf("ct1: %v", err)
	}
	ct2, err := EncryptChunk(plaintext, cek)
	if err != nil {
		t.Fatalf("ct2: %v", err)
	}
	if bytes.Equal(ct1, ct2) {
		t.Fatalf("expected different ciphertexts (nonce should differ); got identical bytes")
	}
}
