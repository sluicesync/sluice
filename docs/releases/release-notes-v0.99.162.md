# sluice v0.99.162

**New: rename namespaces during a multi-namespace migration or sync — `--map-database OLD=NEW` / `--map-schema OLD=NEW`. Migrate `app`,`billing` into `app_prod`,`billing_prod` in one run, instead of only ever same-named targets. A feature release; no fixes, fully backward-compatible (no map ⇒ unchanged behavior).**

## Added

**Per-namespace target rename for multi-namespace `migrate` / `sync` (ADR-0142).** The multi-namespace fan-out — ADR-0074 `--all-databases` (MySQL source) / ADR-0075 `--all-schemas` (Postgres source) / `--include-*` — copies N source namespaces in one run, but until now each one landed in a **same-named** target namespace. The single-source rename (`--target-schema`) couldn't express a per-namespace mapping across the fan-out. `v0.99.162` adds that:

```
sluice migrate --all-databases --map-database app=app_prod,billing=billing_prod ...
sluice sync    --include-schema app,billing --map-schema app=app_prod ...
```

- **Flags.** `--map-database OLD=NEW` and `--map-schema OLD=NEW`, repeatable / comma-separated, on both `migrate` and `sync`. They are engine-agnostic synonyms for the same map (MySQL-database vs PG-schema spelling) — supplying both in one run is an error, exactly like the existing `--include-database`/`--include-schema` pair. Also settable in `sluice.yaml`:
  ```yaml
  namespace_map:
    app: app_prod
    billing: billing_prod
  ```
- **Covers continuous sync too**, including the per-source-database CDC change-routing — a captured change for source `app` is applied into target `app_prod`.

## Semantics

- **Rename within the selection.** With a selection (`--all-*`/`--include-*`/`--exclude-*`), the map renames the listed namespaces and leaves the rest identity-named. A map key that isn't in the resolved selection is a **loud error before any data moves** (a typo guard — `--map-database typoo=x` won't silently rename nothing).
- **Map-only is a shorthand.** With no selection flag, the map keys ARE the selection — `--map-database app=app_prod` migrates exactly `app` → `app_prod`.
- **No silent merges.** Two source namespaces mapping to one target is refused loudly — both at parse time and via the case-fold-collision preflight (extended to the mapped names, so a mapped-vs-unmapped fold collision on a case-folding MySQL target is caught too).

## Safety / compatibility

The rename changes **only the target namespace identifier**. Reads, `Table.Schema`/`View.Schema` stamping, the cross-namespace FK carve-out, the deferred-FK pass, **`--redact` rule matching**, and the per-source migration-/stream-id all stay keyed on the **source** name. Notably, the CDC change-router applies the rename only for the physical write/PK path and never rewrites the change's own schema — so a source-keyed redaction rule (`--redact app.users.email`) keeps matching after `app=app_prod` instead of silently ceasing to redact (PII-leak-safe by design). The identity default — no `--map-*`, no `namespace_map:` — is **byte-for-byte unchanged** behavior. This generalizes ADR-0031's single-source `--target-schema` to the fan-out. The `-race` integration gate passed before tagging (the rename rides the concurrent CDC apply path).

## Who needs this

Anyone running a multi-database / multi-schema `migrate` or `sync` who wants the target namespaces named differently from the source — staging→prod suffixes, environment renames, consolidation. Single-namespace migrations are unaffected (use `--target-schema` / the target DSN's database as before).

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.162 · **Container:** ghcr.io/sluicesync/sluice:0.99.162
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
