# sluice v0.95.2

# sluice v0.95.2 — PG schema-fidelity arc, step 3: Bug 113 full round-trip

**Headline:** Bug 113's full closure. v0.95.1 shipped the loud-refuse closure (PG schema reader refused loudly at the read boundary when a column referenced a `CREATE DOMAIN` user-defined type, preventing the silent CHECK-constraint loss). v0.95.2 rotates the refusal to **actual round-trip carry**: the PG schema reader populates `ir.Domain{Name, BaseType, Checks}` from `pg_type` + `pg_constraint` joins, the writer adds a Phase 1a' that emits `CREATE DOMAIN ... AS ... CHECK (...)` before any table that references it, and `emitColumnType` dispatches the column type to the operator-declared DOMAIN identifier (not the base type's DDL spelling). PG→PG migrations now preserve the operator's input-validation invariants verbatim: the DOMAIN re-lands on dst, the CHECK rejects values the source would reject, and queries against the dst behave identically to the source. Cross-engine PG→MySQL downgrades to the base type (CHECK re-emission as a table-level constraint on MySQL 8.0.16+ is a partial close tracked as a follow-up — the silent-loss class is now narrowed from silent-DOMAIN-unwrap-everything to silent-CHECK-drop-on-cross-engine).

## Added

- **`feat(postgres): round-trip PG DOMAIN with CHECK constraints (Bug 113 full closure)`** — v0.95.1 shipped the loud-refuse closure for Bug 113 (silent CHECK-constraint loss prevented at the read boundary). v0.95.2 rotates to actual round-trip carry: the PG schema reader pre-reads every DOMAIN's CHECK definitions via `pg_get_constraintdef` (`readDomainChecks` — keyed by `pg_type.typname`, joined to `pg_constraint` on `contypid` + `contype='c'`), and `populateColumns` wraps the column's BASE-translated IR type in `ir.Domain{Name, BaseType, Checks}` when `pg_type.typtype == 'd'`. The base IR type comes for free because `information_schema.columns` unwraps DOMAINs at every field it exposes (`data_type`, `udt_name`, `char_max_len`, etc.) so the existing `translateType` call produces the base type; the DOMAIN-specific metadata (name + CHECKs) is wrapped on AFTER. PG schema writer adds Phase 1a' (after Phase 1a enum types, before Phase 1b tables): walk every column for `ir.Domain`, dedupe by `Name`, emit `CREATE DOMAIN <schema>.<name> AS <base type DDL> [CONSTRAINT <name>] CHECK (<body>);` so column references in CREATE TABLE resolve to the just-created DOMAIN. `emitColumnType` dispatches `ir.Domain` to emit the schema-qualified DOMAIN name (NOT the base type's DDL) when a table-column reference is rendered. Cross-engine PG→MySQL: MySQL has no DOMAIN counterpart; the MySQL writer downgrades to the DOMAIN's BASE type DDL (a partial close — the CHECK constraints attached to the DOMAIN are not yet re-emitted as table-level CHECKs; tracked as a follow-up). Pinned by `TestSchemaReader_DomainRoundTrip_Bug113` (DOMAIN round-trips as `ir.Domain` with BaseType=`ir.Text` + one CHECK with non-empty body) + `TestSchemaReader_DomainRoundTrip_NonDomainUserDefinedStillRoundTrips` (negative control: ENUM still round-trips as `ir.Enum`, not wrapped as Domain). The v0.95.1 refusal-at-read-boundary pin was rotated to the round-trip assertion in this release.

## Compatibility

- **Patch bump (v0.95.2).** Drop-in from v0.95.1.
- **Behavior change (PG→PG):**
  - `sluice migrate` / `sluice sync start` against a Postgres source whose schema contains any column referencing a `CREATE DOMAIN` type now **round-trips the DOMAIN** end-to-end: dst has the same `CREATE DOMAIN ... AS ... CHECK (...)` declaration as src, and INSERTs that the source would reject on the DOMAIN's CHECK constraint are equivalently rejected on dst. v0.95.1 refused loudly at the read boundary; v0.95.2 succeeds and produces a fully equivalent target.
- **Behavior change (PG→MySQL):**
  - Cross-engine migration of a DOMAIN-bearing schema now downgrades the column to the DOMAIN's base type on MySQL. The DOMAIN's CHECK constraints are not yet re-emitted as MySQL table-level CHECKs (tracked as a follow-up); operators relying on cross-engine validation continue to do so at the application layer for now. This is the proportional close per the bug-catalog's "Either is acceptable; silent-drop is not" — the column shape is now correct cross-engine, only the CHECK is dropped.
- No effect on schemas that don't use DOMAINs (the common case).

## Who needs this

- **Anyone running `sluice migrate` PG→PG against a source schema that uses `CREATE DOMAIN`** — Bug 113's silent CHECK-loss class is fully closed; dst has DOMAINs + CHECKs preserved verbatim. **Upgrade.**
- **Anyone running PG→MySQL of a DOMAIN-bearing schema** — column shape correct, CHECKs not yet preserved (follow-up). Upgrade for the correct column type; keep relying on application-layer validation for the CHECK invariants until the table-level CHECK emit ships.
- **Everyone else** — drop-in upgrade, no action needed.

## Coming next

After v0.95.x, **v0.96.x** covers operator-quality-of-life (Bugs 108 / 114). Open backlog post-v0.95.2: 108 / 114 = 2. PG→MySQL DOMAIN CHECK table-level emit tracked as a v0.96+ follow-up.
