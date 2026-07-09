// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// TestDeriveManifestHMACKey_Deterministic pins that the derivation is
// deterministic and HKDF-separated from the KEK (the derived key is not
// the KEK, and is 32 bytes).
func TestDeriveManifestHMACKey_Deterministic(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, KEKLen)
	k1, err := DeriveManifestHMACKey(kek)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := DeriveManifestHMACKey(kek)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(k1, k2) {
		t.Fatal("derivation is not deterministic")
	}
	if len(k1) != ManifestSigKeyLen {
		t.Fatalf("key len %d != %d", len(k1), ManifestSigKeyLen)
	}
	if bytes.Equal(k1, kek) {
		t.Fatal("derived signing key equals the KEK (must be HKDF-separated)")
	}
}

// TestDeriveManifestHMACKey_GoldenLabel pins the derived key for a fixed
// KEK: the HKDF label is on-disk contract (it keys every signature), so a
// change to the label / derivation strands every existing signature and
// must be caught here.
func TestDeriveManifestHMACKey_GoldenLabel(t *testing.T) {
	kek := make([]byte, KEKLen)
	for i := range kek {
		kek[i] = byte(i)
	}
	got, err := DeriveManifestHMACKey(kek)
	if err != nil {
		t.Fatal(err)
	}
	// Golden: HKDF-SHA256(ikm=0x00..0x1f, salt=nil, info="sluice-manifest-sig/v1"), 32 bytes.
	const want = "1e289eacbde1f8aebe1193a2ae824f32f510fa8ca15dad6d544d8a597f6c7aa1"
	if hex.EncodeToString(got) != want {
		t.Fatalf("derived key drift (label/derivation changed — this strands every signature):\n got  %s\n want %s",
			hex.EncodeToString(got), want)
	}
}

// TestSignVerifyManifestHMAC pins the sign/verify roundtrip and that a
// wrong key or tampered payload fails.
func TestSignVerifyManifestHMAC(t *testing.T) {
	key := bytes.Repeat([]byte{0x22}, ManifestSigKeyLen)
	payload := []byte("canonical-manifest-bytes")
	mac := SignManifestHMAC(key, payload)
	if !VerifyManifestHMAC(key, payload, mac) {
		t.Fatal("valid mac did not verify")
	}
	if VerifyManifestHMAC(key, append(payload, '!'), mac) {
		t.Fatal("tampered payload verified")
	}
	wrong := bytes.Repeat([]byte{0x33}, ManifestSigKeyLen)
	if VerifyManifestHMAC(wrong, payload, mac) {
		t.Fatal("wrong key verified")
	}
}

// TestManifestSigKeyID_StableAndNonSecret pins that the key-id is stable
// and does not leak the key.
func TestManifestSigKeyID_StableAndNonSecret(t *testing.T) {
	key := bytes.Repeat([]byte{0x44}, ManifestSigKeyLen)
	id1 := ManifestSigKeyID(key)
	id2 := ManifestSigKeyID(key)
	if id1 != id2 {
		t.Fatal("key id not stable")
	}
	if id1 == hex.EncodeToString(key) || bytes.Contains(key, []byte(id1)) {
		t.Fatal("key id leaks key material")
	}
}

// TestPassphraseEnvelope_ManifestSigner pins that PassphraseEnvelope
// implements ManifestSigner and derives the same key HKDF gives for its
// KEK, and that two DIFFERENT chains (different salts) derive DIFFERENT
// signing keys.
func TestPassphraseEnvelope_ManifestSigner(t *testing.T) {
	params, err := DefaultArgon2idParams()
	if err != nil {
		t.Fatal(err)
	}
	env, err := NewPassphraseEnvelope("correct horse battery staple", params)
	if err != nil {
		t.Fatal(err)
	}
	var ms ManifestSigner = env
	got, err := ms.ManifestSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	want, err := DeriveManifestHMACKey(env.kek)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("PassphraseEnvelope signing key != DeriveManifestHMACKey(kek)")
	}

	params2, err := DefaultArgon2idParams() // fresh salt
	if err != nil {
		t.Fatal(err)
	}
	env2, err := NewPassphraseEnvelope("correct horse battery staple", params2)
	if err != nil {
		t.Fatal(err)
	}
	k2, err := env2.ManifestSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, k2) {
		t.Fatal("different chains (salts) derived the same signing key")
	}
}
