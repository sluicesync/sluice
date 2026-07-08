// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Package crypto carries the envelope-encryption primitives that gate
// Phase 6 of the logical-backup feature
// (`docs/dev/design/logical-backups-phase-6.md`). Phase 6.1 ships the
// passphrase-mode implementation; Phase 6.2/6.3 add KMS-backed modes
// behind the same [EnvelopeEncryption] interface so the chunk
// writer/reader paths don't change when those land.
//
// The cryptographic shape is standard envelope encryption with
// AES-256-GCM as the bulk cipher:
//
//   - A Content Encryption Key (CEK) — 32 random bytes — encrypts each
//     chunk's bytes via AES-256-GCM with a per-chunk random 12-byte
//     nonce. The composed ciphertext is `[nonce | ciphertext | authtag]`.
//   - A Key Encryption Key (KEK) — derived from the operator's
//     passphrase via Argon2id in Phase 6.1, or fetched from a cloud KMS
//     in Phase 6.2/6.3 — wraps the CEK. The wrapped CEK is what lands
//     in the manifest.
//   - On restore, the operator's passphrase (or KMS handle) re-derives
//     the KEK, unwraps the CEK, and the chunk reader uses the CEK to
//     decrypt each chunk's bytes.
//
// Per-chain CEK is the default: one CEK is generated when the chain
// starts and reused across every chunk. Argon2id (the expensive op in
// passphrase mode) runs once per restore, not once per chunk —
// per-chain CEK is the load-bearing performance choice. Per-chunk CEK
// (`--encrypt-mode=per-chunk`) is opt-in for operators who want
// defense-in-depth at the cost of one wrap operation per chunk.
//
// See `docs/dev/design/logical-backups-phase-6.md` for the full design,
// threat model, and operator-facing UX.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"

	"golang.org/x/crypto/argon2"
)

// Constants for the AES-256-GCM cipher chosen for Phase 6.1. These are
// declared as exported constants so callers (manifest writer / chunk
// codec / tests) can pin them rather than hard-coding magic numbers.
const (
	// NonceLen is the per-chunk random nonce size for AES-GCM.
	// 12 bytes (96 bits) is NIST SP 800-38D's recommended size and the
	// only nonce length the stdlib's [cipher.NewGCM] accepts as the
	// default. Per-chunk random nonces with this size keep the
	// birthday-bound at ~2^48 chunks per CEK — well beyond any
	// realistic chain.
	NonceLen = 12

	// AuthTagLen is the AES-GCM authentication tag size.
	// 16 bytes is the AES-GCM standard.
	AuthTagLen = 16

	// CEKLen is the byte length of a Content Encryption Key —
	// AES-256-GCM uses a 32-byte (256-bit) key.
	CEKLen = 32

	// KEKLen is the byte length of a Key Encryption Key. Same as
	// [CEKLen] because the KEK is itself an AES-256 key (used to wrap
	// CEKs via AES-GCM in passphrase mode).
	KEKLen = 32

	// SaltLen is the byte length of the per-chain Argon2id salt
	// recorded in the manifest. 16 bytes is the standard recommendation
	// — long enough that two independent chains never collide in
	// practice, short enough that the manifest stays compact.
	SaltLen = 16
)

// KEK-mode strings recorded in [backup.ChainEncryption.KEKMode]. String
// literals are part of the on-disk format; renaming requires a manifest
// format-version bump.
const (
	// KEKModePassphrase is Phase 6.1's KEK mode: KEK is derived from a
	// passphrase via Argon2id with the chain's recorded salt + cost
	// params.
	KEKModePassphrase = "passphrase-argon2id"

	// AlgorithmAESGCM is the bulk-cipher algorithm tag used in both
	// [backup.ChainEncryption] and [backup.ChunkEncryption]. Phase 6.1 ships
	// only this algorithm; future revisions may add ChaCha20-Poly1305.
	AlgorithmAESGCM = "AES-256-GCM"
)

// Encryption-mode strings recorded in [backup.ChainEncryption.Mode].
const (
	// EncryptModePerChain wraps a single CEK into the chain manifest;
	// every chunk in the chain uses the same CEK with its own random
	// nonce. Default — minimises KMS / Argon2id calls on restore.
	EncryptModePerChain = "per-chain"

	// EncryptModePerChunk wraps a fresh CEK per chunk; each chunk's
	// [backup.ChunkEncryption.WrappedCEK] carries its own wrap. Opt-in via
	// `--encrypt-mode=per-chunk`. Costs one wrap-per-chunk.
	EncryptModePerChunk = "per-chunk"
)

// Argon2idParams matches `backup.Argon2idParams` shape verbatim. Re-declared
// here so the crypto package doesn't depend on the manifest IR (avoids
// an import cycle: the manifest IR imports nothing in this package, but
// future phases may flip that). Marshalled into the manifest's
// [backup.ChainEncryption] field on backup write; re-read on restore.
type Argon2idParams struct {
	Salt        []byte `json:"salt"`
	Memory      uint32 `json:"memory_kib"`
	Iterations  uint32 `json:"iterations"`
	Parallelism uint8  `json:"parallelism"`
	KeyLen      uint32 `json:"key_len"`
}

// DefaultArgon2idParams returns the Phase 6.1 NIST-recommended starting
// parameters for Argon2id KEK derivation. Memory=64 MiB, iterations=3,
// parallelism=4, key length matches AES-256 (32 bytes). Operators
// concerned about brute-force can raise these via flag (future
// enhancement); chains record the actual params used so older chains
// stay decryptable when defaults rotate forward.
//
// The salt is generated fresh — DefaultArgon2idParams returns a unique
// salt per call. Callers that need a deterministic salt (tests) build
// the params struct explicitly.
func DefaultArgon2idParams() (Argon2idParams, error) {
	salt := make([]byte, SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return Argon2idParams{}, fmt.Errorf("argon2id default params: read salt: %w", err)
	}
	return Argon2idParams{
		Salt:        salt,
		Memory:      64 * 1024, // 64 MiB
		Iterations:  3,
		Parallelism: 4,
		KeyLen:      KEKLen,
	}, nil
}

// Upper bounds on Argon2id params accepted at KEK-derivation time.
//
// The params are copied VERBATIM from a backup manifest's JSON and fed
// to argon2.IDKey BEFORE anything about the manifest is authenticated —
// the KEK the auth tags depend on is itself derived from them — so
// without bounds a tampered manifest is a pre-auth memory/CPU bomb on
// restore: `memory_kib: 4294967295` asks for ~4 TiB, and a huge
// iteration count spins for hours (the classic unauthenticated-KDF-param
// bomb; audit N-7). The bounds sit far above anything sluice ever
// writes: the only construction site is [DefaultArgon2idParams]
// (64 MiB / 3 iterations / parallelism 4 / [SaltLen]-byte salt) and
// there is no flag to raise those, so a legitimate sluice-written chain
// can never trip them.
const (
	// MaxArgon2idMemoryKiB caps the memory cost at 2 GiB.
	MaxArgon2idMemoryKiB = 2 * 1024 * 1024

	// MaxArgon2idIterations caps the time cost.
	MaxArgon2idIterations = 64

	// MaxArgon2idParallelism caps the lane count.
	MaxArgon2idParallelism = 32

	// MaxArgon2idSaltLen caps the salt length at 1 KiB; sluice writes
	// [SaltLen] (16) bytes.
	MaxArgon2idSaltLen = 1024
)

// validateArgon2idParams enforces the non-zero floors and the
// untrusted-manifest ceilings above. Split from [NewPassphraseEnvelope]
// so the bounds are unit-testable without paying an actual (potentially
// multi-GiB) Argon2id derivation at the boundary values.
func validateArgon2idParams(params Argon2idParams) error {
	if len(params.Salt) == 0 {
		return errors.New("crypto: argon2id salt is empty")
	}
	if len(params.Salt) > MaxArgon2idSaltLen {
		return fmt.Errorf("crypto: argon2id salt length %d exceeds the %d-byte bound (tampered manifest?)",
			len(params.Salt), MaxArgon2idSaltLen)
	}
	if params.Memory == 0 {
		return errors.New("crypto: argon2id memory is zero")
	}
	if params.Memory > MaxArgon2idMemoryKiB {
		return fmt.Errorf("crypto: argon2id memory %d KiB exceeds the %d KiB (2 GiB) bound (tampered manifest?)",
			params.Memory, MaxArgon2idMemoryKiB)
	}
	if params.Iterations == 0 {
		return errors.New("crypto: argon2id iterations is zero")
	}
	if params.Iterations > MaxArgon2idIterations {
		return fmt.Errorf("crypto: argon2id iterations %d exceeds the %d bound (tampered manifest?)",
			params.Iterations, MaxArgon2idIterations)
	}
	if params.Parallelism == 0 {
		return errors.New("crypto: argon2id parallelism is zero")
	}
	if params.Parallelism > MaxArgon2idParallelism {
		return fmt.Errorf("crypto: argon2id parallelism %d exceeds the %d bound (tampered manifest?)",
			params.Parallelism, MaxArgon2idParallelism)
	}
	if params.KeyLen != KEKLen {
		return fmt.Errorf("crypto: argon2id key_len %d does not match KEKLen %d", params.KeyLen, KEKLen)
	}
	return nil
}

// EnvelopeEncryption abstracts CEK wrap/unwrap so Phase 6.1's
// passphrase mode and Phase 6.2/6.3's KMS modes plug into the same
// chunk writer/reader. Implementations are responsible for any caching
// (e.g. passphrase mode caches the derived KEK after first use; KMS
// mode caches the unwrapped CEK after first KMS Decrypt call).
type EnvelopeEncryption interface {
	// WrapCEK encrypts cek with the implementation's KEK and returns
	// the wrapped (opaque) bytes that should be recorded in the
	// manifest. cek must be exactly [CEKLen] bytes.
	WrapCEK(cek []byte) ([]byte, error)

	// UnwrapCEK is the inverse of WrapCEK: takes the wrapped bytes
	// from the manifest and returns the plaintext CEK. Returns an
	// error if the unwrap fails (wrong passphrase / wrong KMS key /
	// tampered wrap).
	UnwrapCEK(wrapped []byte) ([]byte, error)

	// Mode returns a tag identifying the implementation —
	// "passphrase-argon2id" / "aws-kms" / etc. — for recording in the
	// manifest's [backup.ChainEncryption.KEKMode] field. Operators
	// inspecting an encrypted manifest see this value; restore-side
	// validation matches it against the supplied envelope's Mode().
	Mode() string
}

// PassphraseEnvelope is the Phase 6.1 implementation of
// [EnvelopeEncryption]. The KEK is derived once via Argon2id from the
// supplied passphrase + the chain's salt; subsequent WrapCEK /
// UnwrapCEK calls reuse the cached KEK. CEK wrapping uses AES-256-GCM
// with a fresh per-wrap random nonce embedded in the wrapped bytes:
// `[nonce (12B) | ciphertext (32B) | authtag (16B)]` = 60 bytes per
// wrapped CEK.
//
// Lifecycle: NewPassphraseEnvelope (validates inputs, derives KEK
// eagerly so that an operator-typo passphrase fails fast) → Wrap/Unwrap
// as needed.
type PassphraseEnvelope struct {
	params Argon2idParams
	kek    []byte
}

// NewPassphraseEnvelope constructs a PassphraseEnvelope by deriving the
// KEK from passphrase + params.Salt via Argon2id. Returns an error if
// passphrase is empty or params are malformed (zero salt, zero
// memory/iterations/parallelism, key length mismatch) — or exceed the
// untrusted-manifest ceilings (Max* consts above): restore-side callers
// feed manifest-recorded params straight in, so this constructor is the
// single chokepoint where a KDF-param bomb must be refused before
// argon2.IDKey runs.
//
// The derivation runs eagerly — at NewPassphraseEnvelope time, not at
// first Wrap/Unwrap — so a typo passphrase fails before the chain
// writer or reader has done any work. The cost is one Argon2id call
// (~64 MiB, ~tens of ms with default params); subsequent
// Wrap/Unwrap calls reuse the cached KEK.
func NewPassphraseEnvelope(passphrase string, params Argon2idParams) (*PassphraseEnvelope, error) {
	if passphrase == "" {
		return nil, errors.New("crypto: passphrase is empty")
	}
	if err := validateArgon2idParams(params); err != nil {
		return nil, err
	}
	kek := argon2.IDKey(
		[]byte(passphrase),
		params.Salt,
		params.Iterations,
		params.Memory,
		params.Parallelism,
		params.KeyLen,
	)
	return &PassphraseEnvelope{params: params, kek: kek}, nil
}

// Mode returns [KEKModePassphrase].
func (e *PassphraseEnvelope) Mode() string { return KEKModePassphrase }

// Params returns the Argon2id params the envelope was built with.
// Callers (chain writer) use this to populate
// [backup.ChainEncryption.Argon2id] in the manifest.
func (e *PassphraseEnvelope) Params() Argon2idParams { return e.params }

// WrapCEK encrypts cek with the cached KEK via AES-256-GCM. The
// returned bytes are `[nonce | ciphertext | authtag]` and are what the
// caller records in the manifest's [backup.ChainEncryption.WrappedCEK] (or
// [backup.ChunkEncryption.WrappedCEK] for per-chunk mode).
func (e *PassphraseEnvelope) WrapCEK(cek []byte) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: wrap cek length %d != %d", len(cek), CEKLen)
	}
	return EncryptChunk(cek, e.kek)
}

// UnwrapCEK is the inverse of WrapCEK. Returns an error if the wrapped
// bytes were produced by a different KEK (wrong passphrase) or
// tampered with (auth-tag mismatch).
func (e *PassphraseEnvelope) UnwrapCEK(wrapped []byte) ([]byte, error) {
	cek, err := DecryptChunk(wrapped, e.kek)
	if err != nil {
		return nil, fmt.Errorf("crypto: unwrap cek: %w", err)
	}
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: unwrapped cek length %d != %d", len(cek), CEKLen)
	}
	return cek, nil
}

// GenerateCEK returns a fresh 32-byte random Content Encryption Key
// suitable for AES-256-GCM. Backed by [crypto/rand] so the bytes are
// cryptographically secure.
func GenerateCEK() ([]byte, error) {
	cek := make([]byte, CEKLen)
	if _, err := rand.Read(cek); err != nil {
		return nil, fmt.Errorf("crypto: generate cek: %w", err)
	}
	return cek, nil
}

// EncryptChunk encrypts plaintext with cek via AES-256-GCM and returns
// `[nonce (12B) | ciphertext | authtag (16B)]`. cek must be exactly
// [CEKLen] bytes; nonce is generated fresh per call via [crypto/rand].
//
// The composed shape is what the chunk writer hands to
// [backup.Store.Put]; the chunk reader splits it back into
// nonce + ciphertext on the way out.
func EncryptChunk(plaintext, cek []byte) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: encrypt cek length %d != %d", len(cek), CEKLen)
	}
	block, err := aes.NewCipher(cek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: new gcm: %w", err)
	}
	nonce := make([]byte, NonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("crypto: read nonce: %w", err)
	}
	// gcm.Seal appends ciphertext+tag onto the destination; passing
	// nonce as both prefix and the nonce arg gives us
	// `[nonce | ciphertext | authtag]` in one allocation.
	ct := gcm.Seal(nonce, nonce, plaintext, nil)
	return ct, nil
}

// DecryptChunk is the inverse of [EncryptChunk]. Parses
// `[nonce | ciphertext | authtag]`, runs AES-GCM.Open, returns the
// plaintext on success. Returns an error if the input is too short to
// contain a valid envelope or if the auth-tag check fails (wrong CEK
// or tampered ciphertext).
func DecryptChunk(ciphertext, cek []byte) ([]byte, error) {
	if len(cek) != CEKLen {
		return nil, fmt.Errorf("crypto: decrypt cek length %d != %d", len(cek), CEKLen)
	}
	if len(ciphertext) < NonceLen+AuthTagLen {
		return nil, fmt.Errorf("crypto: ciphertext too short (%d bytes); minimum %d", len(ciphertext), NonceLen+AuthTagLen)
	}
	block, err := aes.NewCipher(cek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: new gcm: %w", err)
	}
	nonce := ciphertext[:NonceLen]
	ct := ciphertext[NonceLen:]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		// AES-GCM Open returns an opaque error on auth-tag failure;
		// wrap with operator-actionable context. Distinct from
		// short-input above so callers can branch on shape vs auth.
		return nil, fmt.Errorf("crypto: aes-gcm open: %w (wrong key or tampered ciphertext)", err)
	}
	return pt, nil
}
