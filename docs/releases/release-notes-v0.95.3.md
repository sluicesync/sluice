# sluice v0.95.3

# sluice v0.95.3 — Bug 113 round-trip carrier UNBLOCKED (Bug 122 closure)

**Headline:** v0.95.2 wired the schema half of the Bug 113 round-trip carry (reader populates `ir.Domain`, writer Phase 1a' emits `CREATE DOMAIN`, column references render as the DOMAIN name). The post-release regression cycle's `gl_users` repro confirmed the schema half lands correctly on PG dst (`typtype='d'`, column typed `email_address`, CHECK regex preserved, dst rejects `NOT-AN-EMAIL`) — but `bulk_copy` aborted on the first row with `postgres: no decoder for IR type ir.Domain`. The row-stream value codec dispatch had no `ir.Domain` case, so DOMAIN-typed columns surfaced a loud-failure migrate exit 1 with zero rows carried. v0.95.3 closes Bug 122 by adding `ir.Domain` cases to PG's `decodeValue` + `prepareValue` and to MySQL's `prepareValue` as defense-in-depth. With this fix the Bug 113 round-trip carry is fully end-to-end functional on PG→PG: dst has the DOMAIN preserved, the CHECK enforced, AND the row data carried — all five expected outcomes from the v0.95.2 regression cycle's Focus A now pass.

## Fixed

- **`fix(postgres,mysql): dispatch ir.Domain value codec to base type (Bug 122 closure — v0.95.2 round-trip carrier unblocker)`** — v0.95.2 wired the schema half of Bug 113's round-trip carry (reader populates `ir.Domain`, writer Phase 1a' emits `CREATE DOMAIN`, `emitColumnType` references the DOMAIN name). The post-release cycle's `gl_users` repro confirmed the schema half lands correctly on PG dst (`typtype='d'`, column typed `email_address`, CHECK regex preserved, dst rejects `NOT-AN-EMAIL`), but `bulk_copy` aborted on the first row with `postgres: no decoder for IR type ir.Domain` — the PG row-stream value codec dispatch had no `ir.Domain` case, so DOMAIN-typed columns surfaced a loud-failure migrate exit 1 with zero rows carried. Generic across base types (DOMAIN over `text` and `numeric` both reproduced); plain `text` negative control was unaffected. Same shape on cross-engine PG→MySQL (MySQL writer silently downgraded the column to the base type's MySQL DDL, but the source PG row reader's decoder hit the same dispatch gap before the value ever crossed). v0.95.3 adds an `ir.Domain` case to `internal/engines/postgres/value_decode.go` (`decodeValue` recurses against `Domain.BaseType` — PG's wire / text I/O for a DOMAIN-typed column is byte-identical to its base type) and to `internal/engines/postgres/row_writer.go::prepareValue` (same recursion shape) so every downstream specialization (Array / Geometry / Bit / Extension / Verbatim / scalar passthrough) reaches its existing branch. Defense-in-depth `ir.Domain` case added to `internal/engines/mysql/row_writer.go::prepareValue` (synthesize a `*ir.Column` with the base type and recurse), covering the scenario where ir.Domain leaks past the retarget layer into the MySQL applier's value-prep path. With this fix the v0.95.2 round-trip carry's `gl_users` repro is end-to-end functional: dst has DOMAIN preserved + invalid email rejected + rows carried.

## Compatibility

- **Patch bump (v0.95.3).** Drop-in from v0.95.2.
- **Behavior change:**
  - `sluice migrate` / `sluice sync start` PG→PG of any schema with a `CREATE DOMAIN`-bearing column now succeeds end-to-end: schema lands with the DOMAIN preserved AND the row data carries to dst. v0.95.2 produced the schema correctly but aborted bulk_copy on the first DOMAIN-column row; v0.95.3 closes that loud-failure gap.
- No effect on schemas that don't use DOMAINs (the common case).

## Who needs this

- **Anyone running `sluice migrate` PG→PG against a source schema that uses `CREATE DOMAIN`** — Bug 122 was a v0.95.2 round-trip blocker; v0.95.3 unblocks. **Upgrade.**
- **Everyone else** — drop-in upgrade, no action needed.

## Coming next

After v0.95.x, **v0.96.x** covers operator-quality-of-life (Bugs 108 / 114). Open backlog post-v0.95.3: 108 / 114 = 2. PG→MySQL DOMAIN CHECK table-level emit tracked as a v0.96+ follow-up (partial close documented on v0.95.2).
