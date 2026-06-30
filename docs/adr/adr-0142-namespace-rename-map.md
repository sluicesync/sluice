# ADR-0142: Per-namespace target rename for multi-namespace migrate/sync (`--map-database` / `--map-schema`)

- Status: Accepted
- Date: 2026-06-29
- Deciders: sluice maintainers
- Relates: ADR-0074 (MySQL multi-database fan-out), ADR-0075 (Postgres multi-schema fan-out), ADR-0031 (`--target-schema` single-source namespacing)

## Context

ADR-0074/0075 let one `sluice migrate` / `sluice sync` copy N source namespaces (MySQL databases / Postgres schemas) to N target namespaces in a single run — but each source namespace always routes to a **same-named** target namespace. There is no way to *rename* a namespace in a fan-out (e.g. migrate source `app` → target `app_prod`). The single-source rename already exists — ADR-0031's `--target-schema NAME` (PG target) or simply pointing the target DSN at a different database (MySQL target) — but it takes one value and so can't express a per-source-namespace mapping across a fan-out. Operator demand surfaced 2026-06-29 (staging/prod namespace suffixes, consolidation renames).

## Decision

Introduce a **source → target namespace rename map** (default identity), applied at the single point where the fan-out derives the target namespace from the source namespace, in both the migrate and sync paths.

**CLI.** `--map-database OLD=NEW` and `--map-schema OLD=NEW`, repeatable / comma-separated, on both `migrate` and `sync` (mirroring the `--include-database`/`--include-schema` pair). `--map-database` and `--map-schema` are **engine-agnostic synonyms** for the same map (as include/exclude already are); supplying both spellings in one invocation is an error (same rule the existing synonyms enforce). Each entry is an exact `OLD=NEW` pair (no globs in keys — globs select, rename keys are exact to avoid ambiguous overlap). Also settable in `sluice.yaml` as a `namespace_map:` block (`OLD: NEW`) via the existing koanf merge, so the map is expressible in config, not just on the command line.

**Semantics.**
- **Rename, not select.** When a selection is given (`--all-databases`/`--all-schemas` or `--include-*`/`--exclude-*`), the map renames *within* that selection; unmapped selected namespaces keep their source name (identity). A map key that is **not in the resolved selection is a loud error** (typo guard — refuses before any data moves).
- **Map-only convenience.** When ONLY `--map-*` is given (no `--all`/`--include`), the **map keys are the selection** — `--map-database app=app_prod,billing=billing_prod` migrates exactly `app`→`app_prod` and `billing`→`billing_prod`. (`--map-*` engages multi-namespace mode the same way `--include-*` does.)
- **Many-to-one is refused.** Two source namespaces mapping to the same target identifier is rejected up front — both at map-parse time (two keys, one value) and by extending the existing ADR-0075 fold-collision preflight to run on the **mapped** target names (so a case-fold collision between a mapped and an unmapped name is also caught). sluice never silently merges two source namespaces into one target.
- **Reads are unchanged.** The source is still read from `OLD`; `Table.Schema`/`View.Schema` are still stamped with the source name; only the **target** namespace identifier (the PG schema via `--target-schema`, or the MySQL target database via the derived DSN) becomes `NEW`. The cross-database FK carve-out and deferred-FK pass operate on the source names exactly as before.

**Coverage.** Both `migrate` (one-shot) and `sync` (continuous). In `sync`, the rename applies at the same target-namespace derivation AND at the per-source-database **CDC change-routing** (a captured change for source namespace `OLD` is applied into target namespace `NEW`).

## Mechanism

A small `NamespaceRenameMap` value (constructed once in the CLI from the resolved pairs, default empty = identity) carried on `Migrator`/`Streamer`. A single `targetNamespace(src string) string` helper (identity when the map is empty or the key is absent) replaces the bare `database` at the target-derivation sites:

- `migrate_multidb.go`: the `EnsureDatabase` / target `WithDatabase` calls and the `perDB.TargetSchema = database` fallback; the `preflightNamespaceFoldCollisions` input; the `validateMultiDatabase`/selection cross-check (map-key-not-selected refusal).
- `streamer_multidb.go`: the same target-namespace derivation, plus the CDC change-router that maps an incoming change's source namespace to its target namespace.

The per-database `MigrationID`/stream-id derivation stays keyed on the **source** namespace (resumable state is per source, unaffected by the target rename).

## Correctness / safety

- Identity default ⇒ every existing run is byte-for-byte unchanged (no map ⇒ `targetNamespace` returns its input).
- The map-key-not-selected error and the many-to-one refusal are both loud and fire before any data moves (loud-failure-first), so a typo or an accidental merge can't silently corrupt a target.
- No value-codec / row path is touched — this is namespace routing only.

## Alternatives considered

- **YAML-only or flag-only.** Chose both (flag for one-off + scriptability, YAML for many stable mappings) — consistent with how other sluice options are expressible in either.
- **Globs in rename keys** (`app_*=prod_*`): deferred — ambiguous (overlapping patterns, capture-group target rewriting) and not yet demanded; exact keys cover the surfaced need. Selection still supports globs.
- **Map implies nothing about selection** (require `--include` always): rejected as needlessly verbose for the common "migrate these few with new names" case; the map-only-selects rule is the ergonomic default while the map-key-not-selected error preserves the typo guard when a selection IS given.
- **`--map-database` as a single-source rename** (alias for `--target-schema`): out of scope — single-source rename already has `--target-schema` / the target DSN; this ADR is specifically the multi-namespace generalization.
