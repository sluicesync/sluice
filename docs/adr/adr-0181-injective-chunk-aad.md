# ADR-0181: make the encrypted-chunk AAD injective (FormatVersion 9)

## Status

**Accepted — implemented 2026-07-27** (roadmap item 88). Raised as SEC-2 by the blind audit of 2026-07-26 and deliberately kept out of the v0.103.x fix batches: it changes an on-disk contract, and a format change rushed into a correctness release is how format changes go wrong. This ADR was written as the design so the implementation could be one focused piece of work rather than a decision made under release pressure. See "Implementation notes" at the end for the two places the design was underspecified and what was done instead.

## Context

An encrypted row chunk is sealed with AES-GCM under additional authenticated data that binds it to its manifest and its parent table, so a chunk cannot be silently relocated — the SEC-F1 parent-table binding. That AAD is built by raw concatenation (`internal/ir/backup/chunk_binding.go`):

```
"created_at=" + … + "\nsource_engine=" + … + "\nkind=" + … + "\nschema=" + … + "\ntable=" + …
```

**This encoding is not injective.** A value containing `\nschema=` forges a structural boundary, so two distinct (schema, table) parents can render to identical AAD bytes. The audit observed it directly: a chunk sealed under one parent opened cleanly under another (`err=<nil>`), and the refuter reproduced the harmful direction end to end — seal under a crafted parent, forge the manifest's `File` field, inject attacker rows into a legitimate `public.orders`.

What makes this awkward rather than merely wrong is that the correct encoding already exists **in the adjacent file**. `signature.go`'s `CanonicalManifestBytes` emits length-prefixed tokens (`<len>:<bytes>\n`) and its doc comment states the reason in as many words: length-prefixing closes "the raw-concatenation forgery where an embedded newline / delimiter in a source-derived table name or chunk path let two distinct manifests collide". The signing path learned this lesson; the sealing path next door did not.

### Severity, honestly

The audit rated it Medium, not High, and the reasoning is worth preserving because it shapes the urgency:

- The **exfiltration** direction is impossible. A sluice-generated chunk path contains no `\nschema=` delimiter, so victim rows can never be reassigned to an attacker-owned table. The refuter brute-forced this and could not produce it.
- The reachable primitive is **attacker-rows-IN only**, and it is gated behind adversary source DDL *plus* store write access *plus* an unsigned chain. `--sign` closes the whole class, and ADR-0152 already documents and accepts that residual.

So this is not a release blocker. It is worth doing because the AAD is on-disk contract, and **the cost of deferring grows with every chain written** — every chunk sealed under the old encoding is one more that a future reader must keep being able to open.

## Decision

Introduce `FormatVersionInjectiveChunkAAD = 9` and render the AAD with the same length-prefixed token encoding the signature path uses.

`ChunkAAD` is already the single version gate for both sides — writers call it with the freshly-stamped manifest, readers with the manifest as recorded — so it grows a third state rather than a new branch structure:

| recorded FormatVersion | AAD |
|---|---|
| `< 5` (pre-binding) | `nil` — chunks were written unbound and must decrypt via the legacy path |
| `5 … 8` | raw concatenation, **exactly as today**, byte-for-byte |
| `>= 9` | length-prefixed tokens |

The dual-version rule is the one ADR-0154 established for signature canonicalization and which its own downgrade-oracle pin protects: **recompute at the version the artifact records, never at the version the reader prefers.** A chain written before this ships keeps opening forever; a chain written after cannot be opened by a reader that would recompute the old way, because the tag will not verify.

### Why not migrate existing chains

Re-sealing every chunk in an existing chain would require the CEK and a full rewrite, turning a defect fix into a data migration with its own failure modes — for a primitive whose harmful direction requires an unsigned chain and store write access. Old chains stay readable under their recorded version; the exposure ends for everything written from v9 onward. Operators who want the property on existing data have a documented path already: re-run the backup with `--sign`.

## Consequences

**On-disk contract, so it is golden-pinned.** The v9 encoding gets a golden test in the same shape as `CanonicalManifestBytes`', and the v5–v8 encoding keeps its existing pins unchanged — a regression that "fixes" the old path would break every chain ever written, so those pins are load-bearing in the opposite direction.

**The property test is the point, not a golden alone.** A golden proves one input renders to one output; it says nothing about injectivity. The gate this finding deserves is a property test: for randomly generated field tuples containing delimiter bytes (`\n`, `=`, `:`, `|`) in every position, distinct tuples must render to distinct AAD. That is the assertion that would have failed on the current encoding, and a golden would not have.

**A downgrade-oracle pin, mirroring ADR-0154's.** A v9 manifest relabelled to v8 must fail to open, proving the dual-version path is not itself an oracle.

## Alternatives considered

**Escape the delimiters instead of length-prefixing.** Cheaper to write and strictly worse: escaping is a second encoding whose correctness has to be argued separately, while length-prefixing is injective by construction and already exists in this package with a doc comment explaining why. Two encodings for the same job in one package is how the two paths diverged in the first place.

**Reuse `CanonicalManifestBytes` directly.** Tempting, but it renders the *whole manifest* for signing; the AAD binds a specific chunk to a specific parent and must stay small and stable. Sharing the token-encoding primitive is right; sharing the field list is not.

**Do nothing, and rely on `--sign`.** Defensible today — `--sign` genuinely closes the class — but it makes a security property depend on an opt-in flag, and the deferral cost compounds with every chain written.

## Implementation notes

Four things the design above left implicit, resolved during implementation.

**The decision table describes the READER; it does not say who stamps 9.** Reading the recorded version correctly changes nothing if nothing ever records 9. Three write sites stamp it: the fresh encrypted full (previously v7), and the encrypted incremental and stream-rollover segment manifests (previously v5 — both build a fresh manifest per window and seal every chunk in that window, so there is nothing older to stay compatible with). The full-backup resume ladder grew a fourth tier, `resumingPreInjectiveAAD`: a run resumed from a v7/v8 chain keeps v7, because its already-written chunks carry the raw-concatenation binding. That is the same Bug-179 inherit-the-chain's-shape rule the v5 and v7 tiers already followed, one step up.

**The CEK binding changes encoding too, and that makes the downgrade oracle fire a layer earlier than expected.** `CEKBinding` shares `bindingIdentity`, so it moves to length-prefixed tokens at v9 as well (tagged `sluice-cek-binding/v2`). A relabelled manifest therefore fails at the chain-CEK *unwrap*, before any chunk is read — a louder refusal than the GCM tag mismatch the ADR anticipated, but a different error surface, and worth knowing when reading a support report. It also means any fixture that rewinds a manifest's stamp to simulate an older binary must re-wrap the CEK at that version; a rewind alone fails for the encoding reason rather than the tier reason, which would make a tier test green for the wrong cause.

**Stamping 9 enrols encrypted manifests in the item-57 `BackupID` fold.** `ComputeBackupID` gates the `cdc_position_commits_after_rows` field on `FormatVersion >= FormatVersionCDCPositionBinding`, which is a `>=`, so a v9 encrypted full now folds that field (as `false`) where a v7 one did not. This is self-consistent — every writer stamps the version before computing the id, and every recompute keys on the manifest's own recorded version — so no chain, mixed or otherwise, is affected. But it does mean a fresh encrypted backup's `BackupID` differs from the one v0.103.x computed for byte-identical inputs. Noted because "nothing else changes" would have been wrong.

**The class is wider than the (schema, table) parent.** Row chunks and change chunks share one chain CEK, and under raw concatenation a change chunk's recorded path can also collide with a *row* chunk of a table named `…\nindex=0` — a cross-KIND relabel that evades the change-chunk ordinal binding entirely. Same defect, same fix; the property test renders every binding kind into a single collision map rather than one map per kind, so cross-kind forgeries are covered rather than assumed away. The corresponding non-finding: the ordinal itself is not forgeable, because it is rendered from an `int`.
