# sluice v0.94.0

# sluice v0.94.0 — backup-family arc, step 1: Bug 110 incremental schema-read scope

**Headline:** First fix in the v0.94.x backup-family arc. Pre-fix, an incremental backup chain originally taken with `--include-table=X` would silently re-read the entire source schema at end-position recording, so a single unrelated table carrying a verbatim-eligible column type (`xml` / `money` / `interval` / `tsvector` / etc.) would fail the whole incremental with `read source schema (end): postgres: read columns: table "Y" column "Z": postgres: unsupported data_type "..."` — a previously-working chain broke because an unrelated table was added to the source. v0.94.0 derives the table-name scope from the parent manifest's recorded schema and threads it through the engine's optional `ir.TableScoper` surface (PostgreSQL today) so the end-position read restricts itself to the chain's original table set.

## Fixed

- **`fix(pipeline): scope incremental backup's end-position schema-read to the parent chain's table set (Bug 110 closure)`** — pre-fix `IncrementalBackup.readSourceSchema` called the schema reader's unscoped `ReadSchema`, which iterated every table in the source. A chain originally taken with `--include-table=X` would silently re-read every table at end-position recording, and a single unrelated table carrying a verbatim-eligible column type (`xml` / `money` / `interval` / `tsvector` / etc.) failed the whole incremental at `read source schema (end): postgres: read columns: table "Y" column "Z": postgres: unsupported data_type` — a previously-working chain broke because an unrelated table was added to the source. v0.94.0 derives a table-name predicate from the parent manifest's recorded `Schema.Tables` at `Run` start and threads it through `readSourceSchema` so on engines that implement `ir.TableScoper` (PostgreSQL today; MySQL falls through to the unscoped read because MySQL has no verbatim-type-in-schema problem to begin with), the end-position read restricts itself to the chain's original table set. A parent manifest with no recorded table list (corrupt / pre-v0.94 fallback) leaves the scope nil and preserves the historical unscoped behaviour. Pinned by `TestIncrementalBackup_ScopeFromParentManifest` (5 sub-pins: nil-schema unscoped fallback, empty-schema unscoped fallback, single-table admit, multi-table exact set with the Bug 110 false-positive cases (`unrelated_xml`, `unrelated_money`), nil-element tolerance in the table slice).

## Compatibility

- **Minor bump (v0.94.0).** Drop-in from v0.93.0 except for the behavior change below.
- **Behavior change:**
  - `sluice backup incremental` against a chain whose parent manifest carries a non-empty `Schema.Tables` list now restricts the end-position schema-read to that table set. Pre-v0.94.0 the read iterated every table in the source. Operators on chains originally taken without `--include-table` (so the parent's table list reflects the full source) see no behavioral change. Operators on chains taken with `--include-table=X` get the Bug 110 fix automatically — the incremental no longer fails when an unrelated table with a verbatim-eligible column is added to the source.

## Who needs this

- **Anyone running `sluice backup incremental` against a chain originally taken with `--include-table=X`** — Bug 110's silent break-on-unrelated-table-addition is closed. **Upgrade.**
- **Everyone else** — no action needed; chains taken without `--include-table` are byte-identical to pre-v0.94.0 behavior.

## Coming next

The v0.94.x backup-family arc continues with:
- **Bug 117** — `backup full` + `backup incremental` with `--encrypt-mode=per-chunk` silently accepts a different passphrase on each incremental; `backup verify` reports OK; `restore` partial-fails at the rotated chunk leaving the target with a partial dataset and no rollback. Needs a verify-side decrypt probe.
- **Bug 116** — Older sluice binary restoring a chain whose manifest was written by a newer binary silently drops schema-metadata fields added in the newer manifest (RLS, policies, exclude-constraints). CRITICAL security-policy silent-loss. Needs a manifest-format-version preflight.
- **Bug 118 re-verify** — likely already closed by v0.92.4's verbatim-type wire-encoding fix; a quick confirmation step.

After v0.94.x, v0.95.x ships the PG IR-carry additions (Bug 113 DOMAIN constraint, Bug 115 operator-class on indexes), and v0.96.x covers operator-quality-of-life (Bugs 108 / 114). Open backlog trajectory to zero across the next three arcs.
