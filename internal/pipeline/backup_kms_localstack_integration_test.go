//go:build kmsverify

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase 6.2 AWS KMS integration tests against a localstack KMS
// container. Runs ONLY under the `kmsverify` build tag — the
// localstack KMS image is heavy + has historically been the
// bottleneck for CI throughput, so the main `integration` build tag
// stays focused on real-database scenarios. Operators (and the AI
// agent that ran Phase 6.2) verify these locally before tagging.
//
// To run:
//
//	# Linux / macOS (testcontainers manages the container lifecycle)
//	go test -tags=kmsverify ./internal/pipeline/...
//
//	# Windows + Rancher Desktop: also export
//	#   TESTCONTAINERS_RYUK_DISABLED=true
//	# (see CLAUDE.md for the full PATH override)
//
// The localstack image starts in <30s on a warm cache; cold-pull adds
// ~1m. Tests budget for cold-pull via the testcontainers default
// startup-timeout shape.
//
// What's covered:
//
//   1. Round-trip — encrypted full backup + chain restore against
//      same-engine PG; chunks are ciphertext on disk; data round-trips.
//   2. Chain extension — encrypted full + encrypted incremental +
//      chain restore; verifies the Bug-43 chain-extension pattern works
//      for KMS too (KMS unwrap doesn't depend on a chain-recorded
//      Argon2id salt; the orchestrator's `rebindForChain` is a no-op
//      for KMS envelopes).
//   3. Wrong-key refusal — chain wrapped under KMS-key-A, restored
//      with KMS-key-B → fails with KMS Decrypt error (clear message).
//   4. Missing-key refusal — encrypted chain restored without
//      `--kms-key-arn` → fails with operator-actionable error citing
//      the chain's KEKMode + KEKRef.
//
// IMPORTANT: this file is a **harness skeleton**. Live execution
// against localstack requires the testcontainers + AWS-config
// plumbing (region, fixed credentials, custom endpoint). The file
// compiles under `-tags=kmsverify` but is intentionally tagged with
// `t.Skip("kmsverify scaffolding")` until the harness lands. Phase
// 6.2's primary gates are the unit-level tests in
// `backup_kms_test.go` (which run on every CI cycle and pin the
// load-bearing manifest integration + per-chain caching contract).

package pipeline

import (
	"testing"
)

// TestBackup_KMSEncryption_RoundTrip skeleton — full backup + chain
// restore against localstack KMS. See file-level doc comment for the
// shape; live wiring is operator-run.
func TestBackup_KMSEncryption_RoundTrip(t *testing.T) {
	t.Skip("kmsverify scaffolding — wire localstack + AWS config to enable")
}

// TestBackup_KMSEncryption_ChainExtension skeleton — encrypted full +
// encrypted incremental + chain restore against localstack KMS.
func TestBackup_KMSEncryption_ChainExtension(t *testing.T) {
	t.Skip("kmsverify scaffolding — wire localstack + AWS config to enable")
}

// TestBackup_KMSEncryption_MissingKey skeleton — encrypted chain;
// restore without `--kms-key-arn` → operator-actionable error.
func TestBackup_KMSEncryption_MissingKey(t *testing.T) {
	t.Skip("kmsverify scaffolding — wire localstack + AWS config to enable")
}

// TestBackup_KMSEncryption_WrongKey skeleton — chain with key A;
// restore with key B → KMS Decrypt error.
func TestBackup_KMSEncryption_WrongKey(t *testing.T) {
	t.Skip("kmsverify scaffolding — wire localstack + AWS config to enable")
}

// TestBackup_KMSEncryption_AccessDenied skeleton — IAM policy denies
// kms:Decrypt; restore fails with translated AccessDeniedException.
func TestBackup_KMSEncryption_AccessDenied(t *testing.T) {
	t.Skip("kmsverify scaffolding — wire localstack + AWS config to enable")
}
