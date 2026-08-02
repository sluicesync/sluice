# ADR-0183 — Sign the manifest's schema bytes (canon v5)

Status: **Accepted — implemented 2026-08-01** (audit-2026-08-01 C1). See Implementation notes.

## Context

The ADR-0154 manifest signature covers **zero bytes** of `Manifest.Schema`. It folds only the *recorded* `schema_hash` **string** (`internal/ir/backup/signature.go:329`). Schema authenticity is therefore transitive: it holds only if something recomputes the fingerprint from the schema and compares it to the signed value.

That recomputation — `verifySchemaHashes` (`internal/pipeline/backup/chain_restore.go:894`) — is gated behind `needsWalk` (`:802`), and `verifyLineageNeedsWalk` returns **false** for a single segment with zero incrementals (`restore.go:317-330`). `backup verify` shares the identical predicate (`restore.go:1717-1723`), and `export-as-parquet` never runs it on any shape (`export_parquet.go:210,230`).

So on a **signed bare full** — the ordinary product of `backup full --encrypt --sign` — nothing authenticates the schema.

**Demonstrated** (audit-2026-08-01, probe against unmodified code): planting `Index.Method = "btree; CREATE ROLE attacker SUPERUSER; --"` into a signed manifest's schema left the canonical signed bytes **unchanged**, the HMAC signature **still verified**, and the recorded `BackupID` still matched. A second probe stripping RLS + policies + CHECK and injecting an expression `DEFAULT` behaved identically.

**Attacker model.** Write access to the backup store — a compromised S3 prefix, a shared NFS backup directory, a DR chain handed over by a third party. **No key material, no database credentials, no source access.** Explicitly in scope per `SECURITY.md`'s store-level-adversary section.

**The primitive is arbitrary SQL, not corruption.** All Postgres DDL runs `ExecContext(ctx, stmt)` with zero arguments, and pgx v5 forces `QueryExecModeSimpleProtocol` when `len(arguments) == 0` (`conn.go:516-518`). Simple protocol permits `;`-separated statements.

**The exposure is the whole schema, not one field.** Three tiers:

1. **Bare-identifier positions → arbitrary SQL**, no validator exists for any: `Index.Method` (`postgres/ddl_emit.go:1595`), `IndexColumn.OperatorClass` (`:1470`), `Sequence.DataType` (`sequence_ddl.go:240`), `Policy.Command` (`rls_emit.go:125`), MySQL `Charset`/`Collation` (`mysql/ddl_emit.go:585`).
2. **Verbatim-expression positions** — CHECK bodies, column DEFAULTs, generated-column and index expressions, RLS `USING`/`WITH CHECK`, `ExcludeConstraint.Definition`, `VerbatimType.Definition`. `SECURITY.md:47` carves these out **under the trusted-source model**; there is no source database on this path, so the premise the exemption rests on is absent. This is the larger surface in practice, because CHECK and DEFAULT are populated on ordinary schemas while `Index.Method` is nearly always empty.
3. **No injection required** — `RLSEnabled`/`RLSForced`/`Policies` flipped means the restored target comes up with row-level security silently off; CHECK/EXCLUDE/FK/unique/`Nullable`/types dropped or widened means integrity controls silently absent. Restore's chunk-header check compares column **name sets** only (`restore.go:1219-1233`), so preserving the names leaves everything else free.

ADR-0154 line 25 names this class exactly: corruption of `SchemaHash` relative to the schema is *"corruption detection, not tamper-proofing … this ADR is what would make it tamper-proofing."* It does not.

## Decision

**Fold the manifest's raw serialized `schema` subtree bytes into a new canonical serialization, `sluice-manifest-canon/v5`.**

### Why not simply run the recomputation on the bare-full path

This was the obvious cheap fix and it is **wrong**, for a reason written down at the scoping site (`chain_restore.go:783-787`): *"the schema fingerprint is a CHAIN-path check — the single-manifest path never walks links and never fingerprints them, which is why **a bare full legitimately crosses a fingerprint epoch**."*

`ComputeSchemaHash` is an IR-struct fingerprint. When a field is added to `ir.Column` or `ir.Index`, the fingerprint of the same logical schema changes — a *fingerprint epoch*. A bare full written by an older binary carries a hash computed under the old algorithm; recomputing it with a newer binary yields a different value. Enabling the check unconditionally would refuse legitimate old backups en masse.

That property is precisely what makes the hash the wrong thing to authenticate through. Signing the **bytes on disk** has no epoch, because the bytes are whatever was written, and a signature is only ever verified against the manifest it was written with.

### What gets folded

The **raw serialized bytes of the manifest's `schema` member**, exactly as they appear on disk — not a re-marshal of the decoded struct.

A re-marshal would reintroduce the epoch problem in a worse form: adding a field to `ir.Column` would change the re-marshalled bytes and invalidate every existing signature, turning a routine field addition into a fleet-wide restore outage.

**Precedent, in the same function:** `SchemaHistoryEntry.TableJSON` is already folded verbatim for exactly this reason — *"the already-marshalled bytes recorded in the manifest (byte-identical across a JSON round-trip via base64), so it is folded verbatim"* (`signature.go:440-443`).

This requires `Manifest` to retain the raw `schema` bytes across decode. The mechanism is a `json.RawMessage` captured at unmarshal time and preserved alongside the decoded `*ir.Schema`; the decoded form remains the one every consumer reads, and the raw form is used only for canonicalization. Writers capture the same bytes they marshal.

### Version handling

`manifestCanonVersions` (`signature.go:298`) already implements the dual-version rule: the verifier keys on the signature's **own recorded** `CanonVersion`, so each retired version keeps its exact rendering forever. v5 adds one feature flag:

```go
type manifestCanonFeatures struct {
    scheme              bool  // v3+
    rowChunkParentTable bool  // v4+
    schemaBytes         bool  // v5+
}
```

v2/v3/v4 renderings are unchanged and keep verifying byte-identically. No chain is re-signed and no re-seal migration exists.

### Composed with, not replaced by

Two independent changes ship alongside, because neither subsumes the other and both stand on their own:

1. **Validate or quote the five Tier-1 bare-identifier positions.** `Sequence.DataType` → `{smallint, integer, bigint}`; `Policy.Command` → `{ALL, SELECT, INSERT, UPDATE, DELETE}` (already upper-cased at `rls_emit.go:118`, so a three-line allowlist); the remainder quoted, or refused on `^[A-Za-z_][A-Za-z0-9_$]*$`. This reduces the primitive from *arbitrary SQL* to *a refused DDL* **regardless of how the hostile value arrived** — including from a live source that is less trusted than the operator believes.
2. **Route `export-as-parquet` through `restoreManifestIntegrityPreflights`.** It currently runs signature + interrupted-manifest checks and neither recompute, on any chain shape. Bug 218's shared-list lesson, one command further out.

## Consequences

**What this closes.** A signed backup of any shape gets its schema authenticated by the signature directly, not transitively through a check that may not run. The demonstrated forgery fails at signature verification.

**What it does not close, stated plainly.** An **unsigned** chain remains fully forgeable on every shape — `ComputeSchemaHash` is a keyless public hash, so an adversary edits the schema and recomputes it. Nothing in this ADR changes that, and nothing can: integrity without a secret is not achievable. `SECURITY.md` must say so directly rather than leaving a reader to infer that a hash implies protection. This is the same residual ADR-0152 documents for the chunk-AAD class, and `--sign` is the answer to both.

**What it does not close, second residual: every chain signed BEFORE this ADR.** "No chain is re-signed and no re-seal migration exists" is stated above as a compatibility property, and it is also a security boundary — one this ADR's first cut left implicit and a reader could not have inferred. A manifest signed under canon v4 or earlier — everything written by v0.107.0 and below — verifies GREEN on a v5 binary with its schema still fully forgeable, because the MAC covers the bytes it covered. The regression cycle demonstrated it end to end: a v4-signed bare full, forged on disk with no key material, verifies `signature valid` at exit 0 under `--require-signature` and restores at exit 0 with CHECK and UNIQUE constraints stripped and RLS flipped off, after which the target accepts a negative amount and a duplicate key the source forbade. The design is right and does not change; the defect was silence. Every verification now emits `lineage.WarnPreSchemaCanonSignature` naming the recorded canon version, what it does not authenticate, and the only remedy (a fresh full with this release — a chain cannot be re-signed into coverage). It is wired inside `lineage.VerifyManifest`, the chokepoint all three verification entry points share, so a fourth cannot be added without it. Bug 220.

**A strict "refuse anything below v5" policy is deliberately NOT wired.** It would be a reasonable flag and it is not this change: making refusal the default breaks every existing chain on upgrade day, which is the trade ADR-0181 got right by warning, and a strict flag has to reach `restore`, `backup verify`, `export-as-parquet` and the broker together or it becomes the item-111 accident (a policy knob on one entry point and silently missing on its siblings). Recorded here as an absence with a reason rather than implied. `backup verify` plus the WARN answers "do I still have pre-v5 chains?" today.

**Cost.** One new canon version and a `json.RawMessage` on `Manifest`. No re-seal, no chain migration, no format-version bump on the chunk encoding (this is the signature canon, not `FormatVersion`), so the ADR-0154 §11 "a chain's floor is its newest link" consequence does not apply.

**Interaction with format-version floors.** None. Canon version and `FormatVersion` are independent axes; a v5 signature can accompany any `FormatVersion` the writer would otherwise have stamped.

## Gates

Three, and the first is the one that would have caught this.

1. **Field-coverage gate on the canonical bytes.** `TestCanonicalManifestBytes_TamperSensitivity` (`signature_test.go:265`) is a **curated allowlist**, which is exactly why `Schema` and four other fields are absent from it and nothing failed. Replace with a reflect-driven test over `irbackup.Manifest`: every exported field needs either a mutator that provably changes the signed bytes, or a `canonExempt` entry carrying a written reason. Direct transplant of the fail-by-default shape in `internal/pipeline/copy_phase_flag_parity_test.go:64-70`. Mutation-run in both directions.
   Fields confirmed outside the signed bytes today, each of which needs a verdict: `Schema`, `PartialState`, `Tables[].Partial`, `CDCPositionCommitsAfterRows`, `SluiceVersion`, `ChunkInfo.Encryption`. `PartialState` gates a real decision (`refuseInterruptedManifest`) and is unreachable today only because signing runs *after* it is set to complete (`backup.go:594` → `:619`) — an ordering invariant with no test naming it.
2. **A forgery pin.** Plant a hostile value into a signed manifest's schema, leave `schema_hash` untouched, and assert verification **fails** at v5 and — as the permanent non-vacuity guard — **succeeds** at v4. The v4 leg is what proves the test is exercising the new binding rather than agreeing with itself.
3. **Consumer-coverage gate.** Assert every manifest consumer routes through `restoreManifestIntegrityPreflights`, with reasoned exemptions. Catches the `export-as-parquet` gap and the next one like it.

Per the 2026-07-29 lesson, every gate above is mutation-run in both directions before it is trusted — three gates in this project have already passed against the defect they were named for.

## Implementation notes

Things the design left implicit, decided during implementation.

**The captured bytes are COMPACTED, not the literal on-disk slice.** Manifests are written with `json.MarshalIndent`, so the raw `schema` sub-slice carries the writer's indentation. `Manifest.captureRawSchema` runs the captured member through `json.Compact` on both sides of the boundary. This is still "the bytes on disk, not a re-marshal" — the input is the document's own `schema` member, never the decoded struct, so an unknown field written by another binary survives (pinned by `TestManifestSchemaBytesArePreservedNotRemarshalled`). What compaction buys is that the **three** renderings agree: the write capture, the read capture, and the fallback `json.Marshal(m.Schema)` used by an in-memory manifest that has never crossed the JSON boundary. Whitespace between JSON tokens carries no information and `json.Compact` removes exactly that, so signing before a write and verifying after a read fold identical bytes. Without it, an in-memory-signed manifest would fold compact bytes and its own read-back would fold indented ones — a mismatch that fails loudly but needlessly.

**Every manifest WRITE now goes through `irbackup.MarshalManifest`.** Four sites did their own `json.MarshalIndent`: `lineage.WriteManifest`, `lineage.WriteManifestAt`, the full-backup `manifestCommitter`, and compaction's staged-incremental write. All four now call the helper, which renders and records in one step. The fallback keeps a hypothetical fifth site correct as long as the struct round-trips, but a site that marshalled a manifest decoded from an OLDER binary would fold bytes that dropped its unknown fields; the helper is the contract.

**Ordering already held: every writer signs AFTER the manifest is durable.** `backup full` (`backup.go`: finalize → `signBackupArtifacts`), `backup incremental`, `backup stream` and `ResignLineage` (which reads from the store) all write-then-sign, so the captured bytes are always the bytes on disk at sign time. Nothing had to be reordered.

**`PartialState` was pinned rather than folded, and the pin is a guard.** The ADR's gate list flagged it as unreachable-today-by-ordering with no test naming the invariant. v5 folds only the schema bytes, as decided; the exemption is now enforced by `lineage.refuseSigningInProgressManifest`, which refuses to produce a signature over an `in_progress` manifest at the single choke point every writer passes through. With no valid signature over an in-progress manifest to forge from, `in_progress`→`complete` is unreachable and `complete`→`in_progress` lands on `refuseInterruptedManifest`. Pinned by `TestSignManifestRefusesInProgressManifest`, mutation-run.

**Verdicts on the other unfolded fields** (each carries its reason in `canonExempt`, `internal/ir/backup/manifest_canon_coverage_test.go`, and each is asserted to be genuinely invisible to the canonical bytes by `TestCanonicalManifestBytes_ExemptFieldsStayInvisible` — an exemption that quietly became false fails the build): `SluiceVersion` — informational, no decision reads it; `ProgressSidecar` — nil on every signed manifest AND cleared by the read path's sidecar replay, so folding it would break a legitimate decode; `CDCPositionCommitsAfterRows` — bound transitively *and checkably*, because `ComputeBackupID` folds it at FormatVersion ≥ 8, `BackupID` is itself folded, and the shared preflight list recomputes the id on both chain shapes (flip the flag alone → the recompute refuses; flip it and re-stamp the id → the MAC fails); `Tables[].Partial` — read only by the backup resume classifier, which reads the never-signed in-progress manifest; `ChunkInfo.Encryption` — the chunk's SHA256 is folded, and any reinterpretation of those bytes fails the ADR-0152 authenticated open.

**Tier-1 validation refuses rather than quotes, at all five positions.** The ADR allowed either. Refusal was chosen because it changes **zero emitted bytes for every legitimate value**, where quoting would change the DDL for every charset-bearing column and every index with an access method — and `USING "BTREE"` is not the same statement as `USING BTREE` on a value that arrived upper-cased. The shared guard is `internal/sqlident`: a shape test (`^[A-Za-z_][A-Za-z0-9_$]*$`) for the three name positions, so `ivfflat`/`hnsw`/`gin_trgm_ops` and every future extension name keep working, and a closed allowlist for the two keyword positions (`AS <type>`, `FOR <command>`) where the grammar itself is closed. New code `SLUICE-E-SCHEMA-IDENTIFIER-INVALID`. It lives in one package rather than two engine-local copies because these are one class across two engines.

**`export-as-parquet` passes `needsWalk` from restore's own predicate, which is `false` for a bare full.** Forcing it true would make export refuse the fingerprint-epoch-crossing chains restore accepts (roadmap item 102). That leaves a bare full's schema authenticated by the **signature** rather than by a recompute — which is the v5 binding, and the reason the fingerprint was the wrong thing to authenticate through in the first place. On an unsigned chain the schema stays forgeable there, exactly as it does for restore. The routing also **widens** the interrupted-manifest check from the selected full to every link, matching restore and verify.
