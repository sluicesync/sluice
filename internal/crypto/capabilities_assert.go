// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package crypto

// Compile-time declarations of the OPTIONAL [EnvelopeEncryption]
// extensions each concrete envelope intentionally implements.
//
// Why this file exists (audit ARCH-F1): the backup encryption + signing
// paths discover these surfaces by runtime type-assertion
// (`env.(BoundEnvelope)`, `env.(ManifestSigner)`, …) in
// internal/pipeline/lineage/encryption.go and signing.go. A method-set
// break — a signature change, a renamed method, a value/pointer receiver
// flip — doesn't fail the build; it makes the assertion quietly stop
// matching, and the path SILENTLY falls back (legacy unbound CEK wrap, no
// key-version rebind, an unsigned successor). The blank-var assertions
// below turn that silent downgrade into a compile error in this package.
//
// The set is deliberately a SUBSET per type — each envelope implements only
// the extensions its scheme needs (e.g. Azure uses key-version rebind /
// resolved-ref rather than the identity-bound wrap the passphrase and
// AWS/GCP envelopes use). When removing an interface from a type ON
// PURPOSE, delete its line here in the same commit and call out the
// downgrade in the commit message.
var (
	// BoundEnvelope — identity-bound CEK wrap/unwrap (ADR-0152 SEC-F1).
	// AWS/GCP KMS and the passphrase envelope bind; Azure uses the
	// key-version rebind path below instead.
	_ BoundEnvelope = (*PassphraseEnvelope)(nil)
	_ BoundEnvelope = (*KMSEnvelope)(nil)
	_ BoundEnvelope = (*GCPKMSEnvelope)(nil)

	// ChainKEKRebinder / ResolvedKEKReferencer — the Azure key-version
	// retarget (audit N-9): a recorded kek_ref pins the exact key version.
	_ ChainKEKRebinder      = (*AzureKMSEnvelope)(nil)
	_ ResolvedKEKReferencer = (*AzureKMSEnvelope)(nil)

	// ManifestSigner — HMAC-off-KEK manifest signing (ADR-0154 Phase 1);
	// the passphrase envelope derives the signing key from its KEK.
	_ ManifestSigner = (*PassphraseEnvelope)(nil)
)
