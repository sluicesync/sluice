# Backup format versioning

How sluice's backup chain manifest versioning works, what it
guarantees, and why a backup that includes security metadata
(row-level security, EXCLUDE constraints, etc.) refuses to restore
on an older sluice binary instead of silently dropping the metadata.

This page is the canonical reference for the contract. The mechanic
is small but load-bearing — it prevents a specific class of silent
data-integrity loss that's hard to detect from the operator side
unless you know to look for it.

## The short version

Every backup chain root manifest carries a `FormatVersion` field:

- **`FormatVersion=1`** — an "innocent" backup. The schema uses none
  of the gated security-or-correctness features. Any sluice version
  from v0.16.x onward restores it.
- **`FormatVersion=2`** — the schema contains at least one of:
  - Row-level security enabled on a table (`RLSEnabled`)
  - Row-level security forced (`RLSForced`)
  - One or more RLS policies attached to a table (`Policies`)
  - One or more EXCLUDE constraints on a table (`ExcludeConstraints`)
  - Only sluice v0.94.1 or newer restores it. Older binaries refuse
  loudly at preflight rather than silently dropping the gated fields.
- **`FormatVersion=3`** — an *in-progress* full backup in the
  sidecar-checkpoint layout (v0.99.39+, ADR-0086). Never stamped on
  finalized manifests; older binaries refuse to resume it rather than
  mis-resume off a base manifest that under-reports progress.
- **`FormatVersion=4`** — the schema carries one or more standalone
  sequences (`Schema.Sequences`, v0.99.175+ / roadmap item 51). Older
  binaries would silently restore a target without the sequence
  object — its custom `START`/`INCREMENT` options and `nextval()`
  topology gone — so they refuse loudly at preflight instead.

If your backups don't use RLS, EXCLUDE constraints, or standalone
sequences, you'll never see a version above 1 on a finalized manifest
and cross-version restore behaves exactly as it did pre-v0.94.1. If
your backups *do* use them, you get the guarantee that older sluice
can't silently land a restored target with those invariants stripped.

## Why this exists — the silent-loss class

Sluice's manifest format predates RLS and EXCLUDE support. The
original contract was a common one: *field-additions are forward-
compatible — older sluice ignores unknown fields.* That's the right
contract for behaviorally-idempotent additions (operator hints,
performance metrics, restore-time tuning suggestions). It's the
**wrong** contract for fields that change what the restored target
*means*.

The fields that broke the contract:

- **`Schema.Tables[].RLSEnabled` + `RLSForced`** — row-level security
  was active on the source. Without these on the target, the
  restored table accepts every query regardless of the user — the
  multi-tenant isolation invariant is gone.
- **`Schema.Tables[].Policies`** — the actual access predicates
  attached to RLS-enabled tables. Without these, an RLS-enabled table
  becomes a deny-all-by-default surface (or, if RLS isn't enabled on
  the target either, an allow-all surface) — neither matches the
  source.
- **`Schema.Tables[].ExcludeConstraints`** — PG's EXCLUDE constraints
  (e.g. `EXCLUDE USING gist (room_id WITH =, period WITH &&)` to
  prevent room-booking overlaps). Without these on the target, the
  source-side invariant disappears and the target accepts row pairs
  the source would reject.

All three are **operator-visible data-integrity invariants**.
Silently dropping them on restore produces a target that diverges
from the source's data model without any error or warning. Catalogued
as **Bug 116** in the project's internal regression catalog — closed in
v0.94.1.

## The proportional rule

The fix isn't "always bump FormatVersion to the highest the build
supports." That would invalidate every old chain every time sluice
ships a new version — operationally awful, especially in multi-tier
deployments where different regions might run different sluice
versions.

The rule shipped in v0.94.1 is **proportional**: a manifest gets the
*minimum* FormatVersion safe for its actual contents.

```
ir.FormatVersionFor(schema) →
    Walk schema.Tables:
      If any RLSEnabled || RLSForced || len(Policies) > 0 || len(ExcludeConstraints) > 0 →
        return FormatVersionSecurityMetadata (= 2)
    return FormatVersionLegacy (= 1)
```

Real-world implications:

- A backup of a typical CRUD database with no RLS and no EXCLUDE
  constraints → `FormatVersion=1`. Restorable on any sluice from
  v0.16.x onward.
- A backup of a multi-tenant SaaS application with RLS policies →
  `FormatVersion=2`. Only sluice v0.94.1+ can restore it; older
  sluice refuses loudly.
- A backup of a partial-failure-handling app with EXCLUDE
  constraints on time-range columns (the classic
  no-overlapping-bookings shape) → `FormatVersion=2`.

The proportional rule means **most operators see no version bump**
across the v0.94.1 upgrade. Operators who use the gated features
get the bump on the chains where the security guarantee matters.

## How older sluice refuses

The mechanic is a preflight check that runs at restore time, before
any DDL or data lands on the target:

```go
// internal/pipeline/backup.go (paraphrased)
if m.FormatVersion > ir.BackupFormatVersion {
    return nil, fmt.Errorf(
        "backup: manifest format version %d is newer than this build supports (%d); upgrade sluice",
        m.FormatVersion, ir.BackupFormatVersion,
    )
}
```

`ir.BackupFormatVersion` is the build-time constant naming the
highest FormatVersion this sluice binary knows about. v0.94.1+ has
it at `2`; pre-v0.94.1 has it at `1`.

If you point a pre-v0.94.1 sluice at a chain with
`FormatVersion=2`, this preflight trips. Sluice exits with the
documented message, leaves the target untouched, and creates **zero
relations** on the destination. The refuse-before-touch property is
load-bearing: there is no code path on the older binary where the
chain is partially applied with the security metadata dropped. The
silent-loss class is **structurally impossible**.

## What operators see

Practically, the FormatVersion contract surfaces in two places:

### 1. Backup-time stamping (silent for most operators)

Sluice computes the right FormatVersion at chain root creation time.
Operators don't pass a flag; the value is derived from the schema's
contents. The manifest's `format_version` field records the result:

```json
{
    "format_version": 2,
    "schema": { ... },
    "chain_encryption": null,
    ...
}
```

Most operators never look at the manifest by hand and don't care
which version landed. Operators auditing chain compatibility can
`jq .format_version manifest.json` to check.

### 2. Cross-version restore refusal (visible when it fires)

If you try to restore a `FormatVersion=2` chain with a pre-v0.94.1
sluice, you'll see the documented message:

```
$ ./sluice restore --from-dir /var/backups/myapp ...
restore: read root manifest: backup: manifest format version 2 is
newer than this build supports (1); upgrade sluice
exit status 1
```

The fix is in the message: **upgrade sluice**. The chain itself is
fine; it's just authored by a sluice version newer than the binary
trying to restore it. Operators in this situation either upgrade the
restoring sluice, or (if upgrading isn't immediately possible)
re-take the backup with the older sluice (which will silently lose
the gated fields, but at least the chain restores — only do this if
you've consciously accepted the security-metadata drop for that DR
exercise).

## What going forward looks like

The FormatVersion contract is forward-extending. The principle is
captured in CLAUDE.md tenet 1 and in the code review heuristic:

**If older binaries dropping this field silently would cost the
operator a security or correctness invariant, bump the version.
Otherwise let them ignore it.**

Adding a new gated field means:

1. Add the field to the IR (`internal/ir/...`).
2. Add it to `ir.FormatVersionFor`'s detection logic.
3. If a higher tier than v2 is needed, introduce
   `FormatVersionSecurityMetadataPlusFooBar = 3` and bump
   `BackupFormatVersion` to match.
4. Add per-field sub-pins to `TestChooseFormatVersion_Bug116`
   covering the new shape.
5. Add an integration-side sub-pin to `TestBackup_FormatVersion_Bug116`
   at the orchestrator boundary.

If the new field is purely informational (operator hint, performance
metric, restore-time tuning suggestion), no version bump is needed —
the original field-additions rule still applies for those.

## The test suite pins

The contract is pinned by three integration tests:

- **`TestChooseFormatVersion_Bug116`** in `internal/ir/backup_test.go`
  — 10 sub-pins covering:
  - Nil schema → legacy (v1)
  - Empty-tables schema → legacy
  - Each of the four gated fields independently triggering the bump
  - Multi-table mixed innocent+RLS picks the security version
  - Nil-element tolerance in the table slice
  - Agreement between the internal `chooseFormatVersion` and the
    exported `FormatVersionFor`
  - The constant invariant `BackupFormatVersion ==
    FormatVersionSecurityMetadata` (defense-in-depth for the
    "constants drift on a future bump" failure mode)
- **`TestBackupFormatVersion_Bumped`** — pins the constant invariant
  explicitly.
- **`TestBackup_FormatVersion_Bug116`** at the pipeline-orchestrator
  boundary — 4 sub-pins: innocent → v1, RLSEnabled → v2, Policies →
  v2, EXCLUDE → v2. Catches regressions where the per-field check
  gets refactored at the IR layer but the orchestrator wiring drifts.

If a future contributor adds a security-relevant field but forgets
to wire it into `FormatVersionFor`, the per-field sub-pin in
`TestChooseFormatVersion_Bug116` is the first catch.
`TestBackup_FormatVersion_Bug116` is the second catch at the
orchestrator boundary, which exists to detect refactor drift.

## Round C cross-version validation

The v0.97.1 post-arc validation matrix exercised this contract
end-to-end on real binaries (not just in the test suite):

1. **v0.97.1-binary backs up** a PG source with an RLS-bearing table
   (`d2_rls` with 2 rows + `tenant_isolate` policy + RLS enabled +
   forced). The chain manifest is stamped `FormatVersion=2` with the
   populated security fields.

2. **v0.94.0-binary attempts restore**. Pre-v0.94.1; its
   `BackupFormatVersion` constant is `1`. The preflight at the
   manifest-read step trips: `manifest format version 2 is newer
   than this build supports (1); upgrade sluice`. Exit 1.

3. **Verification on the destination**: zero relations created;
   `dst.pg_tables WHERE schemaname='public'` returns zero rows. The
   refuse-before-touch property holds — no partial application.

4. **Control: v0.97.1-binary restores the same chain** (correct
   binary version): exit 0, target gets the table + the RLS policy +
   the RLS enabled/forced flags + the 2 rows. Round-trip
   byte-identical.

Documented in the v0.97.1 round-C cross-version-matrix regression
cycle (sub-case 2). The control sub-case 1 covered the symmetric forward-
compat direction (v0.94.0 takes an innocent FormatVersion=1 chain;
v0.97.1 restores it cleanly), proving the proportional rule keeps
the boring path working.

## See also

- [`docs/architecture.md`](architecture.md) — how the backup chain fits
  into the broader system architecture.
- [`docs/cookbook/recipe-backup-encrypted.md`](cookbook/recipe-backup-encrypted.md)
  — the operator-facing recipe for backup chains, including the
  Bug 117 verify-path probe + ingestion-path probe stories.
- ADR-0046 in [`docs/adr/`](adr/) — the inline backup chain rotation
  ADR that defines the chain root + segment + lineage model the
  FormatVersion field lives in.
- Bug 116 in the project's internal regression catalog — the original
  silent-loss class report that drove the v0.94.1 fix.
