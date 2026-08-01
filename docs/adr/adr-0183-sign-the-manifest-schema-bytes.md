# ADR-0183 — Sign the manifest's schema bytes (canon v5)

Status: **Accepted — design settled 2026-08-01; implementation pending** (audit-2026-08-01 C1)

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

**Cost.** One new canon version and a `json.RawMessage` on `Manifest`. No re-seal, no chain migration, no format-version bump on the chunk encoding (this is the signature canon, not `FormatVersion`), so the ADR-0154 §11 "a chain's floor is its newest link" consequence does not apply.

**Interaction with format-version floors.** None. Canon version and `FormatVersion` are independent axes; a v5 signature can accompany any `FormatVersion` the writer would otherwise have stamped.

## Gates

Three, and the first is the one that would have caught this.

1. **Field-coverage gate on the canonical bytes.** `TestCanonicalManifestBytes_TamperSensitivity` (`signature_test.go:265`) is a **curated allowlist**, which is exactly why `Schema` and four other fields are absent from it and nothing failed. Replace with a reflect-driven test over `irbackup.Manifest`: every exported field needs either a mutator that provably changes the signed bytes, or a `canonExempt` entry carrying a written reason. Direct transplant of the fail-by-default shape in `internal/pipeline/copy_phase_flag_parity_test.go:64-70`. Mutation-run in both directions.
   Fields confirmed outside the signed bytes today, each of which needs a verdict: `Schema`, `PartialState`, `Tables[].Partial`, `CDCPositionCommitsAfterRows`, `SluiceVersion`, `ChunkInfo.Encryption`. `PartialState` gates a real decision (`refuseInterruptedManifest`) and is unreachable today only because signing runs *after* it is set to complete (`backup.go:594` → `:619`) — an ordering invariant with no test naming it.
2. **A forgery pin.** Plant a hostile value into a signed manifest's schema, leave `schema_hash` untouched, and assert verification **fails** at v5 and — as the permanent non-vacuity guard — **succeeds** at v4. The v4 leg is what proves the test is exercising the new binding rather than agreeing with itself.
3. **Consumer-coverage gate.** Assert every manifest consumer routes through `restoreManifestIntegrityPreflights`, with reasoned exemptions. Catches the `export-as-parquet` gap and the next one like it.

Per the 2026-07-29 lesson, every gate above is mutation-run in both directions before it is trusted — three gates in this project have already passed against the defect they were named for.
