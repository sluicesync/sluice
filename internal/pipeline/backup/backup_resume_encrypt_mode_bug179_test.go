// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestBackup_SetupChainEncryption_ResumeModeConflict_Bug179 pins the
// full-resume edition of Bug 179 (the value-fidelity review's third-site
// finding): resuming a crashed in-progress full must keep the mode the chain
// root already committed. An explicit --encrypt-mode that disagrees with the
// in-progress chain's recorded mode is refused loudly at build; an omitted
// mode inherits it — otherwise the resumed manifest records one mode while the
// prior chunks are the other, an un-restorable chain identical in shape to the
// chain-extend bug (see chain_encrypt_mode_bug179_test.go for the
// incremental/stream side).
func TestBackup_SetupChainEncryption_ResumeModeConflict_Bug179(t *testing.T) {
	passphraseEnv := func(t *testing.T) crypto.EnvelopeEncryption {
		t.Helper()
		p, err := crypto.DefaultArgon2idParams()
		if err != nil {
			t.Fatalf("DefaultArgon2idParams: %v", err)
		}
		env, err := crypto.NewPassphraseEnvelope("resume-pass", p)
		if err != nil {
			t.Fatalf("NewPassphraseEnvelope: %v", err)
		}
		return env
	}

	// A synthetic in-progress chain root recording only the mode the guard
	// reads (the refuse path fires before any CEK unwrap).
	priorWithMode := func(mode string) *irbackup.Manifest {
		return &irbackup.Manifest{ChainEncryption: &irbackup.ChainEncryption{
			Algorithm: crypto.AlgorithmAESGCM, Mode: mode,
		}}
	}

	t.Run("explicit conflicting mode on resume → LOUD refuse", func(t *testing.T) {
		cases := []struct{ chain, flag string }{
			{crypto.EncryptModePerChunk, crypto.EncryptModePerChain},
			{crypto.EncryptModePerChain, crypto.EncryptModePerChunk},
		}
		for _, c := range cases {
			b := &Backup{Encryption: &lineage.BackupEncryption{Envelope: passphraseEnv(t), Mode: c.flag}}
			_, err := b.setupChainEncryption(&irbackup.Manifest{}, priorWithMode(c.chain))
			if err == nil {
				t.Fatalf("chain=%q resume --encrypt-mode=%q built OK; want LOUD refuse (Bug 179 full-resume: un-restorable)", c.chain, c.flag)
			}
			if !strings.Contains(err.Error(), "conflicts with the in-progress chain's mode") {
				t.Errorf("chain=%q flag=%q: err = %q; want 'conflicts with the in-progress chain's mode'", c.chain, c.flag, err.Error())
			}
		}
	})

	t.Run("omitted mode on resume → inherit per-chunk (no unwrap, nil CEK)", func(t *testing.T) {
		b := &Backup{Encryption: &lineage.BackupEncryption{Envelope: passphraseEnv(t), Mode: ""}}
		m := &irbackup.Manifest{}
		cek, err := b.setupChainEncryption(m, priorWithMode(crypto.EncryptModePerChunk))
		if err != nil {
			t.Fatalf("inherit per-chunk: unexpected err: %v", err)
		}
		if cek != nil {
			t.Errorf("per-chunk inherit → cek = %v; want nil", cek)
		}
		if m.ChainEncryption == nil || m.ChainEncryption.Mode != crypto.EncryptModePerChunk {
			t.Errorf("resumed manifest mode = %v; want inherited %q", m.ChainEncryption, crypto.EncryptModePerChunk)
		}
	})

	t.Run("explicit matching mode on resume → accepted", func(t *testing.T) {
		b := &Backup{Encryption: &lineage.BackupEncryption{Envelope: passphraseEnv(t), Mode: crypto.EncryptModePerChunk}}
		if _, err := b.setupChainEncryption(&irbackup.Manifest{}, priorWithMode(crypto.EncryptModePerChunk)); err != nil {
			t.Errorf("explicit matching per-chunk resume: err = %v; want accept", err)
		}
	})
}
