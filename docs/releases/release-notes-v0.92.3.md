# sluice v0.92.3

> **Update 2026-05-31:** Bug 97's wire-encoding gap — the second wire-format layer this release's `$N::TYPE` SQL cast missed — is now closed in [v0.92.4](https://github.com/orware/sluice/releases/tag/v0.92.4). Operators with `money` / `pg_lsn` columns in PG → PG sync should upgrade directly to v0.92.4.

---

# sluice v0.92.3 — Bug 97 wire-encoding closure + Bug 121 slot-name normalization

**Headline:** Two surgical fixes that close the partial gaps the v0.92.2 verification cycle surfaced. **Bug 97 wire-encoding** — v0.92.2's applier-side translator fix landed the column type correctly, but `money` and `pg_lsn` rows still failed at runtime because pgx fell back to bytea binary encoding for the unknown PG type. v0.92.3 emits explicit `$N::TYPE` casts in INSERT VALUES + UPDATE SET, and `col::text = $N::TYPE::text` in WHERE equality predicates, so every `ir.VerbatimType` family round-trips on the sync apply path. **Bug 121** — `--slot-name=NAME` was auto-prefixed `sluice_` by `backup full` but taken literally by `backup incremental` and `backup stream run`; v0.92.3 normalizes all three commands through `pipeline.ResolveSlotName`.

## Fixed

- **`fix(postgres): emit $N::TYPE casts for ir.VerbatimType columns in apply SQL (Bug 97 wire-encoding closure — finalised in v0.92.4)`** — v0.92.2 closed the applier-side translator gap (the column type now lands as `ir.VerbatimType`), but the v0.92.2 verification cycle found that `money` and `pg_lsn` rows still failed at runtime. v0.92.3 adds explicit `$N::<verbatim-type>` casts in INSERT VALUES + UPDATE SET clauses, and `col::text = $N::TYPE::text` in WHERE equality predicates (canonical text comparison so REPLICA IDENTITY FULL matches cleanly). New `applyPlaceholder` + `verbatimPlaceholder` helpers in `internal/engines/postgres/change_applier.go`. Pinned by `TestBuildSQL_VerbatimTypeCasts` (4 sub-pins covering INSERT VALUES, UPDATE SET, WHERE equality both-sides cast, and the non-verbatim plain-`$N` path). **v0.92.4** closes the second wire-format layer (the `[]byte` → `string` value-shaping needed for pgx to bind as text instead of bytea).

- **`fix(cli): route --slot-name through pipeline.ResolveSlotName for backup incremental + backup stream (Bug 121)`** — `sluice backup full --slot-name=X` correctly applied the sluice-prefix convention (`X` → `sluice_X`); `sluice backup incremental --slot-name=X` and `sluice backup stream run --slot-name=X` took `X` literally. Operators following a chain workflow with consistent `--slot-name=X` across commands hit `position references slot "sluice_X" but reader is configured with slot "X"` and the chain stalled. v0.92.3 routes both remaining call sites through `pipeline.ResolveSlotName(...)` so the convention applies uniformly across all backup commands. Same kong-tag-drift class as v0.92.1's `--mysql-sql-mode` typo — found by the v0.92.2 verification cycle.

## Compatibility

- **Patch bump (v0.92.3).** Drop-in from v0.92.2 except for the behavior changes below.
- **Behavior changes:**
  - PG → PG sync apply translates `money` / `pg_lsn` / `xml` / `tsvector` / `int4range` / etc. correctly on the apply path. v0.92.4 closes the wire-encoding follow-up that this release's SQL cast didn't fully address.
  - `sluice backup incremental --slot-name=X` and `sluice backup stream run --slot-name=X` now auto-prefix `sluice_X` to match `backup full`'s existing behavior. Operators scripted against the pre-v0.92.3 literal interpretation must drop the explicit `sluice_` prefix from `--slot-name` if they want the same effective slot name.

## Who needs this

- **Anyone running PG → PG sync with `money` / `pg_lsn` columns** — upgrade directly to v0.92.4 for the fully-closed fix.
- **Anyone using `sluice backup` chains with `--slot-name=<custom>`** — Bug 121's inconsistent interpretation is closed. **Upgrade.**
- **Everyone else** — no action needed.

## Coming next

The CDC schema-race family (Bugs 112 + 119 + 120) is queued for v0.93.0. It's concurrency-adjacent and needs the `-race` integration gate before tag cut. After v0.93.0, the v0.94+ arcs will work through the backup-family (Bugs 110 / 116 / 117), the silent-correctness-loss PG types (Bugs 113 / 115), and operator-quality-of-life (Bugs 108 / 114) — the open backlog has a tractable path to zero.
