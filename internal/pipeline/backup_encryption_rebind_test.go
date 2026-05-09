// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"bytes"
	"errors"
	"testing"

	"github.com/orware/sluice/internal/crypto"
	"github.com/orware/sluice/internal/ir"
)

// TestBackupEncryption_RebindForChain_NilSafe exercises the safe
// no-ops in [BackupEncryption.rebindForChain]: nil receiver, nil
// parent params, and missing builder fall through without error.
func TestBackupEncryption_RebindForChain_NilSafe(t *testing.T) {
	var nilEnc *BackupEncryption
	if err := nilEnc.rebindForChain(&ir.Argon2idParams{}); err != nil {
		t.Errorf("nil receiver: unexpected error: %v", err)
	}

	encNoBuilder := &BackupEncryption{}
	if err := encNoBuilder.rebindForChain(&ir.Argon2idParams{Salt: []byte("x")}); err != nil {
		t.Errorf("no builder: unexpected error: %v", err)
	}

	calls := 0
	encWithBuilder := &BackupEncryption{
		RebuildForChain: func(_ *ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
			calls++
			return nil, nil
		},
	}
	if err := encWithBuilder.rebindForChain(nil); err != nil {
		t.Errorf("nil params: unexpected error: %v", err)
	}
	if calls != 0 {
		t.Errorf("nil params should skip builder; got %d calls", calls)
	}
}

// TestBackupEncryption_RebindForChain_SwapsEnvelope confirms that a
// successful rebuild swaps the receiver's Envelope to the rebuilt
// instance. This is the load-bearing fix for Bug 43: the orchestrator
// must use the rebuilt envelope (KEK derived against the chain's
// recorded salt) rather than the cold-start envelope (KEK derived
// against a freshly-minted salt) when unwrapping the chain's
// WrappedCEK.
func TestBackupEncryption_RebindForChain_SwapsEnvelope(t *testing.T) {
	freshParams, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	coldEnv, err := crypto.NewPassphraseEnvelope("hunter2", freshParams)
	if err != nil {
		t.Fatalf("cold envelope: %v", err)
	}

	chainParams, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams chain: %v", err)
	}
	chainEnv, err := crypto.NewPassphraseEnvelope("hunter2", chainParams)
	if err != nil {
		t.Fatalf("chain envelope: %v", err)
	}

	enc := &BackupEncryption{
		Envelope: coldEnv,
		RebuildForChain: func(p *ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
			if p == nil {
				return nil, errors.New("nil params")
			}
			return chainEnv, nil
		},
	}
	parentParams := &ir.Argon2idParams{
		Salt:        chainParams.Salt,
		Memory:      chainParams.Memory,
		Iterations:  chainParams.Iterations,
		Parallelism: chainParams.Parallelism,
		KeyLen:      chainParams.KeyLen,
	}
	if err := enc.rebindForChain(parentParams); err != nil {
		t.Fatalf("rebindForChain: %v", err)
	}
	if enc.Envelope != chainEnv {
		t.Fatalf("rebindForChain did not swap envelope: got %p want %p", enc.Envelope, chainEnv)
	}
}

// TestBackupEncryption_RebindForChain_EnablesUnwrap is the end-to-end
// shape pin for Bug 43: a chain CEK wrapped under the chain's salt
// can be unwrapped only by an envelope whose KEK was derived against
// that same salt. The cold-start envelope (fresh salt) MUST fail, and
// the rebuilt envelope (chain's salt) MUST succeed. This nails down
// the asymmetry the v0.22.0 cycle reproduced.
func TestBackupEncryption_RebindForChain_EnablesUnwrap(t *testing.T) {
	const passphrase = "correct horse battery staple"

	// Step 1: simulate the chain root's full backup. Mint a
	// chain-bound salt, derive the KEK, generate + wrap a CEK.
	chainParams, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("chain params: %v", err)
	}
	chainEnv, err := crypto.NewPassphraseEnvelope(passphrase, chainParams)
	if err != nil {
		t.Fatalf("chain envelope: %v", err)
	}
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("generate cek: %v", err)
	}
	wrapped, err := chainEnv.WrapCEK(cek)
	if err != nil {
		t.Fatalf("wrap cek: %v", err)
	}

	// Step 2: simulate the CLI-time "fresh salt" envelope the
	// extending writer (incremental / stream) starts with. KEK is
	// derived against a different salt → unwrap MUST fail.
	freshParams, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("fresh params: %v", err)
	}
	freshEnv, err := crypto.NewPassphraseEnvelope(passphrase, freshParams)
	if err != nil {
		t.Fatalf("fresh envelope: %v", err)
	}
	if _, err := freshEnv.UnwrapCEK(wrapped); err == nil {
		t.Fatal("Bug 43 contract: fresh-salt envelope must fail to unwrap chain-salt CEK; got nil error")
	}

	// Step 3: route the fresh envelope through rebindForChain with
	// the chain's recorded params. Post-rebind, the Envelope must
	// unwrap cleanly and recover the original CEK.
	enc := &BackupEncryption{
		Envelope: freshEnv,
		RebuildForChain: func(p *ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
			params := crypto.Argon2idParams{
				Salt:        p.Salt,
				Memory:      p.Memory,
				Iterations:  p.Iterations,
				Parallelism: p.Parallelism,
				KeyLen:      p.KeyLen,
			}
			return crypto.NewPassphraseEnvelope(passphrase, params)
		},
	}
	recordedParams := &ir.Argon2idParams{
		Salt:        chainParams.Salt,
		Memory:      chainParams.Memory,
		Iterations:  chainParams.Iterations,
		Parallelism: chainParams.Parallelism,
		KeyLen:      chainParams.KeyLen,
	}
	if err := enc.rebindForChain(recordedParams); err != nil {
		t.Fatalf("rebindForChain: %v", err)
	}
	got, err := enc.Envelope.UnwrapCEK(wrapped)
	if err != nil {
		t.Fatalf("post-rebind unwrap: %v", err)
	}
	if !bytes.Equal(got, cek) {
		t.Fatalf("post-rebind CEK mismatch")
	}
}

// TestBackupEncryption_RebindForChain_BuilderError surfaces any
// builder error to the caller verbatim. Used for "wrong passphrase
// shape" edge cases (e.g. KeyLen mismatch in the recorded params).
func TestBackupEncryption_RebindForChain_BuilderError(t *testing.T) {
	sentinel := errors.New("wrong passphrase shape")
	enc := &BackupEncryption{
		RebuildForChain: func(_ *ir.Argon2idParams) (crypto.EnvelopeEncryption, error) {
			return nil, sentinel
		},
	}
	err := enc.rebindForChain(&ir.Argon2idParams{Salt: []byte("x")})
	if !errors.Is(err, sentinel) {
		t.Fatalf("rebindForChain: got %v; want %v", err, sentinel)
	}
}
