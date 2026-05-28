# sluice v0.85.2 — CRITICAL hotfix: silent UPDATE/DELETE loss under REPLICA IDENTITY FULL

**Headline:** Patch release fixing **Bug 92**, a CRITICAL silent-data-loss bug in the core `postgres` (slot-based) engine. A PostgreSQL source table set to `REPLICA IDENTITY FULL` could **silently drop CDC UPDATEs** (and, latently, DELETEs) when a row carried a rich-type column — jsonb, timestamptz, bytea, or high-precision numeric. No error, no WARN, exit 0; the target simply diverged from the source. **Any operator running a PG source on a `REPLICA IDENTITY FULL` table with such columns should upgrade.**

This is a **pre-existing latent bug** (not introduced in the v0.85.x line). It was found by the postgres-trigger Phase-2 readiness gate's new congruence test — which runs the same workload through the slot-based `postgres` engine and the `postgres-trigger` engine and compares the targets byte-for-byte. On its very first run it caught the parent engine dropping writes that the trigger engine applied correctly.

## Fixed

- **`fix(engines/postgres): Bug 92 — narrow UPDATE/DELETE Before to true PK under REPLICA IDENTITY FULL`**

  ### The failure

  Under `REPLICA IDENTITY FULL`, pgoutput sets the per-column "key" flag on **every** column (FULL makes the whole row the replica identity). The CDC reader trusted that wire flag to build `Update.Before` / `Delete.Before`, so the applier's `WHERE` clause spanned **all** columns including rich types. A rich-type OLD value (jsonb / timestamptz / bytea / numeric) failed the `=` predicate after the pgoutput decode→rebind round-trip → the UPDATE (or DELETE) matched **zero rows** → ADR-0010's idempotency tolerance silently absorbed it. The result was silent target divergence with no operator-visible signal.

  - **UPDATE and DELETE both affected.** The DELETE path used the same flawed key resolution; it only appeared correct historically because the prior FULL test corpus used int/varchar-only tables, whose values round-trip exactly through pgoutput and so always `=`-matched.
  - **Latent, not new.** The bug predates v0.85.x. It surfaced now only because the new congruence test is the first to exercise FULL + UPDATE/DELETE across rich-type columns.

  ### The fix

  - New `IdentityKeyCols` resolved per relation message: `REPLICA IDENTITY DEFAULT` / `USING INDEX` keep the pgoutput wire-flagged key columns (unchanged behavior); **`REPLICA IDENTITY FULL` now narrows `Before` to the table's TRUE PRIMARY KEY**, read from `pg_index WHERE indisprimary` via the reader's live DB handle. The applier `WHERE` is therefore `id = $N` for both UPDATE and DELETE under FULL — robust to the rich-type decode→rebind round-trip.
  - **PK-less FULL tables** fall back to the full-row image (unchanged from prior behavior — there is no narrower identity available).
  - **Bonus bytea fix:** the family-matrix pin surfaced a separate bytea CDC silent-corruption — the CDC path delivers bytea as pgoutput `\x`-hex text, which the shared decoder copied verbatim (`\xcafebabe` became 10 literal ASCII bytes instead of the intended 4). A new `decodeBytea` hex-decodes the `\x`-prefixed even-length-hex CDC shape while leaving the row-reader raw-bytes path (and JSON/JSONB text payloads) untouched.

  ### Tests (Bug-74 "pin the class" discipline)

  - `TestCDCReader_UpdateUnderReplicaIdentityFull_FamilyMatrix` — UPDATE under FULL across int / bigint / high-precision numeric / text / varchar / boolean / timestamp / timestamptz / bytea / jsonb, asserting `Before` is key-only AND the new value lands on the target for every family, including an unchanged-rich-column UPDATE (the exact shape that silently dropped).
  - `TestMigratePGTrigger_CongruenceVsParent` — differential test: identical workload through `postgres → postgres` (slot) and `postgres-trigger → postgres-trigger`, asserting the two targets are byte-identical (per-column ordered MD5 across the value-family matrix). This is the test that caught Bug 92.
  - `TestDecodeBytea` + reworked `TestFilterBeforeToKeyCols` unit pins.

## Compatibility

- **Drop-in upgrade from v0.85.1.** No config / schema / IR changes. The only behavior change is that PG-source CDC under `REPLICA IDENTITY FULL` now correctly applies UPDATEs and DELETEs that were previously (silently) dropped when rich-type columns were present.
- **Patch version bump (v0.85.2)** — bug fix only.
- **Severity CRITICAL** — silent data loss on the core PG CDC path. PG operators with any `REPLICA IDENTITY FULL` table (set explicitly, or implicitly when a table lacks a usable PK/unique index) carrying jsonb / timestamptz / bytea / high-precision numeric columns should upgrade and, if a prior migration may have been affected, re-verify target state.

## Who needs this

- **PG-source operators using `REPLICA IDENTITY FULL`** with rich-type columns — the canonical audience. Pre-v0.85.2, UPDATEs/DELETEs on those tables could silently fail to apply. v0.85.2 applies them correctly.
- **Anyone relying on bytea fidelity through CDC** — the `\x`-hex decode fix ensures bytea values land byte-exact.
- **Operators on v0.85.0/v0.85.1** — drop-in patch; no reason not to take it, and a real reason to if you use FULL replica identity.
