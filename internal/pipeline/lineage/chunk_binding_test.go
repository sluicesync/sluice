// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Unit pins for the ADR-0152 CEK-binding chokepoints
// ([WrapChainCEK] / [UnwrapChainCEK] / [RebindEnvelopeKEK] /
// [ResolvedKEKRef]) and the sidecar-normalization keep-above-v3 rule
// in manifest_io.go.

package lineage

import (
	"bytes"
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

func bindingTestEnvelope(t *testing.T) *crypto.PassphraseEnvelope {
	t.Helper()
	p, err := crypto.DefaultArgon2idParams()
	if err != nil {
		t.Fatalf("DefaultArgon2idParams: %v", err)
	}
	p.Memory, p.Iterations, p.Parallelism = 1024, 1, 1
	env, err := crypto.NewPassphraseEnvelope("chokepoint-pass", p)
	if err != nil {
		t.Fatalf("NewPassphraseEnvelope: %v", err)
	}
	return env
}

func bindingOwner(version int) *irbackup.Manifest {
	return &irbackup.Manifest{
		FormatVersion: version,
		CreatedAt:     time.Date(2026, 7, 8, 1, 2, 3, 0, time.UTC),
		SourceEngine:  "postgres",
		Kind:          irbackup.BackupKindFull,
	}
}

// TestWrapUnwrapChainCEK_VersionGatedSymmetry pins the chokepoint
// contract: the OWNING manifest's recorded FormatVersion decides
// bound-vs-legacy identically on both sides, so a wrap always unwraps
// with the same owner and never with a different identity.
func TestWrapUnwrapChainCEK_VersionGatedSymmetry(t *testing.T) {
	env := bindingTestEnvelope(t)
	cek, err := crypto.GenerateCEK()
	if err != nil {
		t.Fatalf("GenerateCEK: %v", err)
	}

	t.Run("v5 owner: bound round-trip; foreign identity refused", func(t *testing.T) {
		owner := bindingOwner(irbackup.FormatVersionEncryptedChunkBinding)
		wrapped, err := WrapChainCEK(env, cek, owner)
		if err != nil {
			t.Fatalf("WrapChainCEK: %v", err)
		}
		got, err := UnwrapChainCEK(env, wrapped, owner)
		if err != nil {
			t.Fatalf("UnwrapChainCEK: %v", err)
		}
		if !bytes.Equal(got, cek) {
			t.Error("round-trip returned a different CEK")
		}
		other := bindingOwner(irbackup.FormatVersionEncryptedChunkBinding)
		other.CreatedAt = other.CreatedAt.Add(time.Hour)
		if _, err := UnwrapChainCEK(env, wrapped, other); err == nil {
			t.Error("a v5 chain CEK unwrapped under a DIFFERENT backup identity; the cross-chain CEK class (audit N-8) is back")
		}
		// And the legacy plain unwrap must not open it either.
		if _, err := env.UnwrapCEK(wrapped); err == nil {
			t.Error("a v5-bound chain CEK unwrapped via the legacy path")
		}
	})

	t.Run("pre-v5 owner: legacy round-trip preserved", func(t *testing.T) {
		owner := bindingOwner(irbackup.FormatVersionStandaloneSequences)
		wrapped, err := WrapChainCEK(env, cek, owner)
		if err != nil {
			t.Fatalf("WrapChainCEK: %v", err)
		}
		// Byte-compatible with what the OLD binary wrote: the plain
		// unwrap opens it, and the chokepoint routes it there.
		if _, err := env.UnwrapCEK(wrapped); err != nil {
			t.Fatalf("pre-v5 wrap is not the legacy unbound shape: %v", err)
		}
		if got, err := UnwrapChainCEK(env, wrapped, owner); err != nil || !bytes.Equal(got, cek) {
			t.Errorf("pre-v5 chokepoint round-trip: got=%v err=%v", got, err)
		}
	})
}

// rebindingEnvelope is a fake implementing [crypto.ChainKEKRebinder] +
// [crypto.ResolvedKEKReferencer] so the chokepoint's rebind dispatch is
// pinnable without an Azure stub.
type rebindingEnvelope struct {
	crypto.EnvelopeEncryption
	rebinds  []string
	resolved string
}

func (r *rebindingEnvelope) RebindChainKEKRef(ref string) { r.rebinds = append(r.rebinds, ref) }
func (r *rebindingEnvelope) ResolvedKEKRef() string       { return r.resolved }

func TestUnwrapChainCEK_RebindsKEKRefFirst(t *testing.T) {
	inner := bindingTestEnvelope(t)
	env := &rebindingEnvelope{EnvelopeEncryption: inner}
	cek, _ := crypto.GenerateCEK()
	owner := bindingOwner(irbackup.FormatVersionStandaloneSequences)
	owner.ChainEncryption = &irbackup.ChainEncryption{KEKRef: "https://v.vault.azure.net/keys/k/v3"}
	wrapped, err := inner.WrapCEK(cek)
	if err != nil {
		t.Fatalf("WrapCEK: %v", err)
	}
	if _, err := UnwrapChainCEK(env, wrapped, owner); err != nil {
		t.Fatalf("UnwrapChainCEK: %v", err)
	}
	if len(env.rebinds) != 1 || env.rebinds[0] != "https://v.vault.azure.net/keys/k/v3" {
		t.Errorf("rebinds = %v; want exactly the recorded KEKRef, before the unwrap (audit N-9)", env.rebinds)
	}
	if got := ResolvedKEKRef(env, "fallback"); got != env.resolved && got != "fallback" {
		t.Errorf("ResolvedKEKRef = %q", got)
	}
	env.resolved = "https://v.vault.azure.net/keys/k/v9"
	if got := ResolvedKEKRef(env, "fallback"); got != "https://v.vault.azure.net/keys/k/v9" {
		t.Errorf("ResolvedKEKRef = %q; want the envelope's resolved ref", got)
	}
	if got := ResolvedKEKRef(inner, "fallback"); got != "fallback" {
		t.Errorf("ResolvedKEKRef(non-referencer) = %q; want the fallback", got)
	}
}

// TestReplayManifestProgress_KeepsAboveSidecarVersions pins the
// normalization rule in replayManifestProgress: a recorded in-progress
// version ABOVE the sidecar tier (the writer's finalVersion showing
// through the max(sidecar, final) stamp) survives the in-memory
// normalization — recomputing from the schema would strip the v5 stamp
// and mis-route bound chunks down the nil-AAD path.
func TestReplayManifestProgress_KeepsAboveSidecarVersions(t *testing.T) {
	ctx := context.Background()
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "t", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}}}

	t.Run("encrypted v5 in-progress keeps v5", func(t *testing.T) {
		m := &irbackup.Manifest{
			FormatVersion:   irbackup.FormatVersionEncryptedChunkBinding,
			PartialState:    irbackup.BackupStateInProgress,
			ProgressSidecar: &irbackup.ProgressSidecarRef{File: "manifest.progress.jsonl", AttemptID: "a"},
			Schema:          schema,
		}
		if err := replayManifestProgress(ctx, newMemStore(), m); err != nil {
			t.Fatalf("replayManifestProgress: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionEncryptedChunkBinding {
			t.Errorf("normalized FormatVersion = %d; want %d kept (bound chunks would be read nil-AAD)",
				m.FormatVersion, irbackup.FormatVersionEncryptedChunkBinding)
		}
	})

	t.Run("plain sidecar-tier in-progress recomputes from the schema", func(t *testing.T) {
		m := &irbackup.Manifest{
			FormatVersion:   irbackup.FormatVersionProgressSidecar,
			PartialState:    irbackup.BackupStateInProgress,
			ProgressSidecar: &irbackup.ProgressSidecarRef{File: "manifest.progress.jsonl", AttemptID: "a"},
			Schema:          schema,
		}
		if err := replayManifestProgress(ctx, newMemStore(), m); err != nil {
			t.Fatalf("replayManifestProgress: %v", err)
		}
		if m.FormatVersion != irbackup.FormatVersionLegacy {
			t.Errorf("normalized FormatVersion = %d; want the schema-appropriate %d", m.FormatVersion, irbackup.FormatVersionLegacy)
		}
	})
}
