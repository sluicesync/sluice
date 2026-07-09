# ADR-0154 — Signed backup manifests (whole-manifest authentication)

Status: **Accepted** (2026-07-09; decisions §3/§4/§7 ratified by the operator — build in the §6 phased order, Phase 1 first). **Phase 1 implemented (unreleased)** — see §8.
Date: 2026-07-09
Supersedes/extends: ADR-0152 (backup-encryption integrity — chunk AAD binding, KMS EncryptionContext, chunk-header + SchemaHash verification)
Audit origin: `workspace/repo-audit-2026-07-08-fable-crosscheck.md` finding N-8, "honest boundary" residual

---

## 1. Context

ADR-0152 (shipped v0.99.202) cryptographically bound every encrypted **chunk** to its manifest identity and recorded position, closing chunk splice / replay / reorder / cross-backup substitution by a store-level adversary. It deliberately left one residual documented in SECURITY.md and its own text:

> the manifest is UNSIGNED, so presenting a complete older (manifest + chunks) pair — a whole-backup rollback — remains possible, and change-list tail truncation by a store adversary remains out of scope until a signed-manifest design lands.

This ADR is that design. It is written **for operator review before any implementation** — the load-bearing decisions (what key signs, and what verification refuses) are UX-and-trust trade-offs, not purely technical ones.

### 1.1 What the manifest is, and what an adversary can do to it today

`internal/ir/backup/manifest.go` — the manifest is plaintext, unsigned JSON at the root of every backup / chain, carrying: chunk paths + per-chunk SHA-256, row counts, the source→target table mapping, `SchemaHash`, the `ChainEncryption` descriptor (algorithm, KEK mode, KEK ref, Argon2id params), and lineage/parent pointers. The threat model in scope is a **store-level adversary** — someone with read/write to the backup bucket but WITHOUT the encryption key (the ADR-0152 threat actor).

Against that adversary, after ADR-0152, the following are **already closed** and must stay closed:
- chunk content swap / splice / reorder / cross-backup replay (chunk GCM AAD binds ciphertext to manifest-identity + path [+ ordinal for change chunks]);
- chunk-header column-set mismatch (restore validates it, ADR-0152);
- corruption of `SchemaHash` relative to the schema (chain-restore recomputes+compares it, ADR-0152 — corruption detection, not tamper-proofing, because the hash shares the unsigned manifest — *this ADR is what would make it tamper-proofing*);
- KDF-param bomb from a tampered manifest (N-7 clamps Argon2id params pre-derivation).

The residuals this ADR targets:
- **R1 — whole-manifest substitution / rollback.** The adversary replaces the current manifest+chunks with a *complete, internally-consistent, genuinely-older* backup of the same chain (or a forked one they captured earlier). Every ADR-0152 check passes — the chunks are authentic, just *stale*. A restore silently recovers old data. For a chain, they can also drop the newest links (roll the chain back to an earlier tip).
- **R2 — change-list tail truncation.** For a CDC/incremental chain, the adversary removes the last N change chunks from a manifest's list. Each remaining chunk is authentic and correctly bound; the manifest is internally consistent; the restore silently lands a target that is missing the tail of the change stream — a silent-data-loss outcome from an authentic-looking backup.

Both are **authenticity-of-the-whole** gaps: every *part* is authentic, but nothing proves the *set* and its *freshness* are what sluice wrote.

### 1.2 What signing does NOT address (and why that matters for scoping)

The Vitess CVE pair that motivated this review is instructive precisely because **sluice does not share its worst class:**

- **CVE-2026-27965 (command injection):** Vitess's MANIFEST could name an external *decompressor command* that restore executed — a store-write adversary got RCE in the restoring process. **sluice has no command-like manifest field.** The codec is a closed enum (`zstd`/`gzip`/`none`) resolved to in-process code; N-14 (v0.99.204) hardened even the *inference* of it (magic-byte sniff, never an embedded instruction). This RCE class is absent **by construction**, and signing is not what would prevent it — *not having executable fields* is. Design principle to preserve: **the manifest is data, never instructions.**
- **CVE-2026-27969 (path traversal on restore):** manifest-named files written outside the restore root. **sluice already defends this** — `LocalStore.absPath` rejects `..` segments and re-checks root-containment via `filepath.Rel`; `sanitiseBlobKey` mirrors it; both are pinned (`blob_store_test.go`, `local_store_test.go`). Signing would *also* stop a traversal-injecting adversary, but the path-sanitisation is the correct primary defense and stays independent of signing (defense in depth; a bug in one shouldn't open the other).

**The honest framing for the ADR and any release notes:** signing closes R1/R2 (whole-manifest rollback + tail truncation) — a narrower, lower-frequency class than the CVEs, requiring a store-write adversary AND a motive to substitute-not-corrupt. It is defense-in-depth, not a patch for an active RCE. We should not oversell it.

---

## 2. Decision (proposed)

Add an **optional** detached signature over the manifest (and, for chains, over the lineage tip) at backup write time, verified at restore/verify/prune/compact time, gated by a new `FormatVersion` bump and an opt-in signing mode. The two decisions requiring operator input are **§3 (what key signs)** and **§4 (what verification refuses)**; everything else follows from those.

### 2.1 Shape (independent of the key choice)

- **What is signed:** a canonical serialization of the manifest's security-relevant fields — chunk list (paths + SHAs + ordinals), row counts, table mapping, `SchemaHash`, `ChainEncryption`, lineage parent pointer, and a monotonic **sequence number / high-water position** (this is what makes R1 rollback and R2 truncation detectable: a verifier that has seen sequence N refuses a presented sequence < N, and the tip's sequence + chunk count is itself signed so a truncated list fails). Canonicalization must be deterministic and versioned (the same discipline as ADR-0152's AAD strings being on-disk contract).
- **Where the signature lives:** a detached `manifest.sig` (and `lineage.sig`) sibling object — NOT inside the manifest it signs. Detached keeps the signed bytes byte-stable and lets an old reader ignore it.
- **Algorithm:** Ed25519 (asymmetric options B/C) or HMAC-SHA-256 (symmetric option A) — see §3.
- **FormatVersion:** a new manifest `FormatVersion 6`, stamped only when signing is enabled (the ADR-0152 proportionality discipline — unsigned backups keep their version; a v6 manifest REQUIRES a valid signature to restore under the strict policy).

### 2.2 The freshness anchor (the part that actually closes R1)

A signature alone does **not** stop rollback — the adversary presents an *old but validly-signed* manifest. Closing R1 requires the verifier to know "what sequence should I expect." Options, in increasing strength / cost:
- **(a) None (integrity only).** Signing proves authenticity+integrity of the presented manifest but not freshness. Stops R2 (tail truncation — the signed chunk-count/tip won't match a truncated list) and forgery, but NOT R1 (rollback to a fully-signed older backup). Cheapest; honest about the gap.
- **(b) Sequence-in-signature + operator-recorded high-water.** The signed payload carries a monotonic sequence; the operator (or an out-of-band store, e.g. the sync's own control tables, or a tiny local `.hwm` file the runbook keeps off the backup bucket) records the highest sequence seen. Verify refuses a lower one. Closes R1 for anyone who keeps the high-water somewhere the store-adversary can't also rewrite. This is the classic "the anchor must live outside the untrusted store" requirement.
- **(c) Chain-internal monotonicity only.** Within a chain restore, verify that sequence numbers are gap-free and the tip matches the signed count — closes R2 and *intra-chain* rollback (dropping newest links), but a whole-chain swap to an older chain still passes unless (b)'s external anchor exists.

**Recommendation:** ship (c) as the always-on behavior when signing is enabled (it's free once you're signing and closes R2 + intra-chain rollback), and document (b) as the operator-opt-in for full R1 coverage, with the honest note that R1's residual without an external anchor is inherent to any backup system (you cannot detect rollback purely from artifacts the attacker controls).

---

## 3. DECISION 1 — What key signs the manifest? (needs operator input)

This is the crux. Three options, with the usability/security trade the operator asked to balance.

### Option A — Symmetric HMAC keyed off the existing passphrase/KEK
- **How:** derive an HMAC-SHA-256 key from the same Argon2id passphrase (or the KMS-unwrapped CEK) that already protects the chain, via a distinct HKDF label (never reuse the encryption key material directly). Sign = HMAC; verify = recompute HMAC with the same derived key.
- **Usability: excellent.** Zero new key management — anyone who can *decrypt* the backup can *verify* it; anyone with the passphrase can *write* a signed backup. No new flags beyond enabling signing. Falls out naturally for the already-encrypted case.
- **Security trade:** symmetric — the verifier and the signer hold the *same* secret, so it authenticates "written by someone who knew the passphrase," not "written by a specific principal." Good enough for the store-adversary model (the adversary has neither the passphrase nor the derived MAC key). **But:** it only helps **encrypted** backups — a plaintext backup has no passphrase to key off, so signing would be unavailable exactly where the manifest is most exposed (plaintext-on-shared-storage). And it does not give non-repudiation or multi-party verification (a CI system that should *verify* backups must then also hold the *signing* secret).
- **Verdict:** the low-friction default for encrypted chains; insufficient as the only option because it strands plaintext backups.

### Option B — Asymmetric Ed25519 with an operator-managed keypair
- **How:** operator generates an Ed25519 keypair (`sluice backup keygen` or bring-your-own); the private key signs at backup time (`--sign-key priv.pem` / `env:SLUICE_SIGN_KEY`), the public key verifies (`--verify-key pub.pem`, distributable freely). Independent of encryption — works for plaintext AND encrypted backups.
- **Usability: moderate.** One new concept (a keypair) and its distribution: the private key must be available wherever backups are written, the public key wherever they're verified/restored. This is standard signing UX (like signing releases), but it IS new key management the operator must not lose (lose the private key → can't write new signed backups in the chain; lose/rotate the public key → document which key signed which backup, i.e. a `key id` in the signed payload, which we'd include).
- **Security trade: strongest for the stated model.** Verification requires only the *public* key, so a CI/restore host that must verify never holds a signing secret — the separation Option A can't give. Non-repudiation and multi-signer stories become possible. Rotation is clean (key id in payload + a small trust list).
- **Verdict:** the right answer when backups are plaintext, when verification happens somewhere that shouldn't hold write-secrets, or when the operator wants signing decoupled from encryption. More setup.

### Option C — KMS-backed signing (reuse the ADR-0152 KEK providers' Sign/Verify)
- **How:** AWS KMS, GCP KMS, and Azure Key Vault all expose asymmetric Sign/Verify with the private key never leaving the HSM. Reuse the existing `crypto` KMS provider surface (already wired for wrap/unwrap) to sign the manifest digest; verify calls the KMS Verify (or fetches the public key and verifies locally).
- **Usability: low friction FOR operators already using KMS mode** (they've configured the provider; signing is one more key + IAM grant), high friction otherwise (requires a cloud KMS). Verification needs KMS *read/verify* access (or just the exported public key — cheaper, offline-verifiable).
- **Security trade: strongest key custody** (private key in an HSM, IAM-audited, CloudTrail-logged signing events — a real audit trail of who signed what). Azure caveat carries over from ADR-0152/N-9: version-pinning discipline applies to the signing key too.
- **Verdict:** the enterprise answer; natural to offer *only* to operators already in KMS mode, layered on Option B's mechanism (KMS is just a different keystore for the same Ed25519/ECDSA verify).

### Recommended composite (for operator ratification)
**Ship B as the core mechanism, with A as the zero-config convenience for encrypted chains, and C as a KMS keystore option layered on B.** Concretely:
- `--sign` with no key + an encrypted chain → **Option A** (HMAC off the chain KEK; zero new key management; the "it just works if you're already encrypting" path).
- `--sign --sign-key <ed25519 priv | kms://...>` → **Option B/C** (explicit keypair or KMS; works for plaintext too; verification via `--verify-key <pub | kms://...>`).
- The signed payload records a `key id` and the scheme (`hmac-kek` | `ed25519` | `kms`) so a verifier knows what to check and rotation is expressible.

This balances the operator's ask: the common encrypted case gets **security with zero added friction** (A); the cases that need real key separation or plaintext coverage have a **standard, well-understood path** (B/C) they opt into.

---

## 4. DECISION 2 — What does verification refuse, and when? (needs operator input)

The usability hazard of signing is **breaking restores of legitimately-unsigned older backups** and **turning a recoverable situation into a refusal during a disaster.** Proposed policy, mirroring the ADR-0152 FormatVersion-gated discipline:

- **Unsigned pre-v6 backups:** restore normally, forever (no retroactive requirement — the FormatVersion gate means "this backup predates signing," not "this backup is untrusted"). This is non-negotiable for not breaking existing/mixed chains.
- **A v6 (signing-enabled) manifest with a MISSING or INVALID signature:** this is the real decision. Two sub-policies:
  - **Strict (default when a `--verify-key` / verification context is supplied):** refuse loudly with a coded error (`SLUICE-E-BACKUP-SIGNATURE-INVALID` / `-MISSING`) naming the manifest and the expected key id. A v6 backup asserting it was signed, presented without a valid signature, is exactly the tamper signal — refuse.
  - **Warn-and-proceed (when NO verification key is configured):** if the operator restores a v6 backup but supplies no verify key, WARN that the signature is present-but-unverified and proceed — do not hard-fail a disaster-time restore just because the verify key wasn't passed. (An operator who wants strict-always sets a config flag; the DR-path default must not be "your restore fails because you forgot a flag during an outage.")
- **Freshness (§2.2):** intra-chain monotonicity (option c) is always checked when signing is on (a gap or a tip-count mismatch → refuse; this is the R2 tail-truncation catch). The external high-water anchor (option b) is opt-in; when configured, a below-high-water sequence refuses.
- **`backup verify`** (the dedicated command) always checks signatures strictly when a verify key is available and reports signature status in its output regardless — verify is where you *want* the loud answer.

**The balance:** strict where the operator has expressed intent (they passed a verify key, or they're running `backup verify`), forgiving where a hard-fail would turn a recoverable outage into an unrecoverable one (a v6 backup, no verify key, mid-disaster). Never silent: the warn path is loud in the logs even when it proceeds.

---

## 5. Trade-offs (the honest ledger the operator asked for)

**Positive:**
- Closes R2 (change-list tail truncation — a silent-data-loss class) and intra-chain rollback outright once signing is on.
- Closes R1 (whole-backup rollback) *for operators who keep an external high-water anchor* — and honestly documents that R1 is otherwise inherent.
- Makes `SchemaHash` verification (ADR-0152) actually tamper-proof rather than corruption-detecting, because the hash is now under a signature.
- Independent of, and complementary to, the path-sanitisation and no-executable-fields invariants that keep sluice clear of the Vitess CVE classes.
- Option-A path adds security to the common encrypted case with **zero new operator concepts.**

**Negative / cost:**
- **New key-management surface** for Options B/C — the single biggest usability cost, and the one that can bite an operator (lost signing key, undistributed verify key). Mitigated by making A the zero-config default and B/C explicit opt-ins, and by `key id` + rotation support.
- **A new FormatVersion (6)** — old sluice binaries refuse v6 backups loudly (the existing unknown-version discipline); another rung on the compatibility ladder to document.
- **Verification-key distribution** becomes an operational concern for anyone using B/C (public keys to restore/CI hosts).
- **Does not address the RCE/traversal CVE classes** — those are (already) handled by not-having-executable-fields and path-sanitisation; we must be careful in messaging not to imply signing is what makes sluice safe from the Vitess-style bugs. It isn't; those are separate, already-held invariants.
- **Complexity** in the restore/verify/prune/compact paths (each must know the signing policy; compact must re-sign a merged manifest, which means the compacting host needs signing capability — a real design point for Options B/C: can a prune/compact run that lacks the private key still operate? Proposed: compact/prune that can't re-sign must refuse loudly rather than emit an unsigned successor to a signed chain).

**Explicitly NOT in scope (this ADR):**
- Signing individual chunks (ADR-0152's AAD already binds them; a per-chunk signature adds cost without closing a residual the manifest signature doesn't).
- Encrypting the manifest (it's metadata; confidentiality of paths/row-counts/table-names is a different, lower-priority ask — flag if wanted, separate ADR).
- A full PKI / certificate chain for signing keys (bring-your-own-key + a trust list is sufficient at this maturity; revisit if multi-tenant signing appears).

---

## 6. Rollout (if ratified)

1. FormatVersion 6 + the detached-signature read/write plumbing, Option A (HMAC-off-KEK) first — smallest surface, covers the encrypted common case, exercises the canonicalization + verification-policy machinery.
2. Option B (Ed25519 keypair + `keygen`) — unlocks plaintext + key-separated verification.
3. Option C (KMS Sign/Verify) — layered on B's verify path.
4. `backup verify` signature reporting + the strict/warn policy + the coded errors, wired the same release as (1).
5. Freshness: intra-chain monotonicity (c) with (1); external high-water anchor (b) as a documented opt-in, possibly a later increment.
- Each step: the ADR-0152 discipline — golden canonicalization pins (the signed bytes are on-disk contract), a tamper matrix (bad sig / missing sig / rolled-back sequence / truncated tail / wrong key id), old-format-still-restores compat pins, and the compact/prune re-sign-or-refuse pin. `-race` gate for anything touching the concurrent backup/restore paths.

---

## 7. Resolved decisions (ratified by the operator, 2026-07-09)

1. **Key mechanism → the §3 composite is adopted.** Option A (HMAC-off-KEK) is the zero-config default for encrypted chains; Option B (Ed25519 keypair) is the explicit opt-in that also covers plaintext and key-separated verification; Option C (KMS Sign/Verify) is layered on B's verify path for operators already in KMS mode. The signed payload records `key id` + scheme (`hmac-kek` | `ed25519` | `kms`).
2. **Verification policy → strict-where-intent-is-expressed, warn-where-a-hard-fail-would-break-a-disaster-restore.** A v6 manifest with a missing/invalid signature refuses loudly (`SLUICE-E-BACKUP-SIGNATURE-INVALID` / `-MISSING`) when a `--verify-key`/verification context is supplied OR when running `backup verify`; with NO verify key configured, restore WARNs (present-but-unverified) and proceeds. An operator wanting strict-always sets a config flag; the DR-path default must never fail a restore for a forgotten flag. Pre-v6 unsigned backups always restore (the FormatVersion gate means "predates signing," not "untrusted").
3. **Freshness → ship (c) always-on, (b) opt-in.** Intra-chain monotonicity (gap-free sequence + signed tip-count) is always checked when signing is on — this closes R2 (tail truncation) and intra-chain rollback for free. The external high-water anchor (b, full R1 coverage) is a documented opt-in; R1's residual without an external anchor is honestly documented as inherent to any backup system.
4. **Compact/prune without the signing key → refuse loudly** rather than emit an unsigned successor to a signed chain (never silently break the chain's signing invariant). The documented operable path for an automated maintenance cron that shouldn't hold raw key material is Option C (KMS Sign — the cron signs via IAM-granted KMS without holding the private key); this is called out in the C rollout notes.
5. **Priority → build now, in the §6 phased order, Phase 1 first.** No production users means it can be sequenced deliberately (not rushed) but the operator has greenlit starting; Phase 1 (FormatVersion 6 + HMAC-off-KEK + verification policy + `backup verify` reporting + intra-chain monotonicity) lands as its own release, Phases 2–3 (Ed25519, KMS) as follow-ups.

---

## 8. Phase 1 implementation notes — **implemented (unreleased)**

Phase 1 (§6 step 1 + step 4 + freshness option (c)) is implemented on `main`'s worktree, gated behind the CI `-race` Integration job (it touches the concurrent backup/restore paths) and unreleased. Shape as built:

- **FormatVersion 6** (`irbackup.FormatVersionSignedManifest`), the new `BackupFormatVersion` ceiling. Stamped ONLY when `--sign` is set on an encrypted backup (or when extending an already-signed chain); an unsigned encrypted backup stays on 5 (Bug-116 proportionality). The RECORDED version decides signed-vs-unsigned verification — never try-both.
- **Signing key** — HMAC-SHA-256 keyed off a key HKDF-derived from the chain KEK via label `"sluice-manifest-sig/v1"` (`crypto.DeriveManifestHMACKey`), surfaced through a new optional envelope interface `crypto.ManifestSigner` implemented only by `PassphraseEnvelope`. **Phase 1 signs only passphrase-encrypted chains**: a KMS-encrypted chain's KEK never leaves the HSM, so `--sign` on one refuses loudly (KMS Sign is Phase 3). `--sign` on a plaintext backup refuses ("needs --encrypt … not available until Phase 2"). Scheme tag `hmac-kek`; the detached sig records `key_id` (a non-secret fingerprint) + scheme.
- **Canonical serialization** — `irbackup.CanonicalManifestBytes(m, seq)`, versioned `sluice-manifest-canon/v2`, a provably-INJECTIVE **length-prefixed** token stream (`<len>:<bytes>\n`) over format version, identity, parent pointer, SchemaHash, resume anchors (start/end position), ChainEncryption (incl. Argon2id params), the table→row-count map (schema and name as SEPARATE tokens), the full chunk list (row chunks by path; change chunks by ordinal), **SchemaDelta** (restore drives DDL from it — folded as each before/after table's round-trip-stable fingerprint), **SchemaHistory** (folded verbatim), and the freshness anchors (sequence + chunk-count). Length-prefixing closes the raw-concatenation forgery where an embedded `\n`/`=`/`:`/`.` in a source-derived table name or chunk path let two distinct manifests collide under one MAC. Golden-pinned (exact minimal + SHA-of-full + an injectivity/collision property pin).
- **Detached signatures** — `<manifest>.sig` per manifest and `lineage.json.sig` for the chain tip (never inside the manifest). The `lineage.sig` authenticates the link ENUMERATION (closes dropped-newest-link, which the per-manifest sigs alone can't see).
- **Freshness (option c)** — the per-manifest signed `sequence` is the manifest's flat position in the lineage; chain-restore verifies each link's signed sequence equals its walked position (gap-free — a dropped/reordered middle link fails) and each link's signed chunk-count equals its actual list length (a truncated change-list fails). `lineage.sig`'s signed link count closes the dropped tail.
- **Verification policy (§4)** — the restore-side "is this chain signed?" decision is made from the **PRESENCE of signature objects** (`lineage.json.sig` / any `<manifest>.sig`), NEVER from the MAC-covered `FormatVersion` field. This closes the self-inflicted downgrade hole: an adversary flipping every `"format_version": 6` → `5` (v5/v6 decrypt identically — the chunk-binding gate is `>=5`) no longer makes the verifier skip its checks; with the `.sig` files present, verification still runs and refuses, because `format_version` is inside the signed canonical bytes. A signed chain restored with a KEK-holding envelope (encrypted restore always has one) verifies strictly and refuses on missing/invalid/rolled-back/truncated/downgraded with `SLUICE-E-BACKUP-SIGNATURE-INVALID` / `-MISSING`. No verify key available → WARN-and-proceed by default, refuse under `--require-signature`. A backup with no signature objects at all restores (genuinely unsigned) unless `--require-signature` forces verification.
- **Compact/prune (Q4)** — a signed chain refuses to compact/prune without the key; with `--encrypt` supplied it re-signs the whole restructured survivor set + lineage at the new positions.
- **`backup verify`** — reports each manifest's signature status (valid / invalid / unsigned) + the lineage status; an invalid signature is a verify failure (non-zero exit).

**Residual (honestly documented, not a hole):** stripping BOTH the version stamp AND every `.sig` object (and no `--require-signature`) evades verification — this is the inherent external-anchor gap (option b): you cannot detect a whole-backup deletion-to-unsigned purely from artifacts the attacker controls. `--require-signature` closes it (the operator asserts the chain SHOULD be signed → a missing signature refuses). This is distinct from the now-CLOSED downgrade hole (v6→v5 with sigs present), which was self-inflicted (using a MAC-covered field to decide whether to check the MAC).

**Deferred to a follow-up (flagged, not silently dropped):** (1) `backup stream` does not yet sign its rollover manifests — it refuses to extend a signed chain rather than emit an un-restorable tail (the rotation/CDC re-sign needs its own `-race` gating); use `backup incremental --sign`. (2) The external high-water anchor (option b, full R1 coverage) — documented opt-in, unbuilt. (3) The write-side signing `sequence` assumes the lineage's `RestorableFromSegment` is 0 at sign time (true for fresh chains + post-prune/compact, which reset it); a signed chain restored from a `RestorableFromSegment > 0` catalog would mis-number and refuse — acceptable (loud) for Phase 1, to revisit if that shape appears.

*Prior art note:* Vitess — a mature, widely-deployed project — shipped backup manifests without cryptographic signing for its entire history; the trust-the-manifest model produced CVE-2026-27965 (manifest-embedded decompressor command → RCE) and CVE-2026-27969 (manifest path traversal → arbitrary file write on restore), both patched July 2026 in v22.0.4 / v23.0.3 by *removing the executable field* and *sanitising paths* — i.e. the two invariants sluice already holds — not by adding signatures. This is direct evidence for the ADR's framing: **the first-order defense against malicious-manifest-field attacks is "the manifest is data, never instructions, and never an unsanitised path"; signing is the second-order defense against whole-manifest substitution.** sluice should keep both, and not conflate them.
