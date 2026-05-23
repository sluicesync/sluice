# sluice v0.17.3

Single-bug patch from the v0.17.2 test cycle. v0.17.2 shipped Phase 3.3's PG soft-warning preflights including Patroni / HA-managed source detection; cycle testing on PlanetScale Postgres surfaced that the three v0.17.2 detection signals all systematically miss on tenant-isolated managed PG services. The operators who most need the idle-slot trap warning (managed-PG users who can't tune their own slot retention) got nothing. Fix is option (c) from the BUG-CATALOG analysis: ship broader heuristics as the new default, plus an explicit override flag for cases the heuristics still miss.

## Fixed

- **Bug 36 — Patroni / managed-PG idle-slot trap warning does not fire on PlanetScale Postgres (or other tenant-isolated managed PG services).** v0.17.2's `detectPatroniSource` heuristic checked three signals: (1) `pg_settings WHERE name ILIKE '%patroni%'`, (2) `pg_stat_replication.application_name ILIKE 'patroni%'`, (3) `pg_roles WHERE rolname IN ('patroni', 'replicator')`. All three miss on PS-PG specifically (and likely on most tenant-isolated managed PG services): Patroni sets *standard* PG GUCs via `ALTER SYSTEM` (not Patroni-prefixed ones), `pg_stat_replication` is permission-restricted on per-tenant roles (returns 0 rows even when Patroni IS using it), and PS creates tenant-prefixed roles like `hzi_xgsa060j2bbb_role` (so the literal-name check doesn't match). v0.17.3 adds two engine-side signals plus a streamer-layer DSN hostname-pattern signal:

  - **Signal 4 — non-temporary physical replication slots present.** Standby physical slots are a strong HA-cluster signal — most non-HA PG deployments don't carry them.
  - **Signal 5 — `cluster_name` GUC populated.** Patroni convention sets this; many managed services follow suit.
  - **DSN hostname pattern** (streamer layer; the IR preflight interface deliberately doesn't carry the DSN). Six known managed-PG suffixes: `*.psdb.cloud` (PlanetScale Postgres), `*.aws.prod.archil.com` and `*.gcp.prod.archil.com` (Archil), `*.cluster*.rds.amazonaws.com` (Aurora cluster endpoints; vanilla RDS instances are excluded because they're not always HA), `*.postgres.database.azure.com` (Azure Database for PostgreSQL), `*.cloudsql.google.internal` (Cloud SQL via private IP). Patterns are intentionally narrow — false positives on non-HA setups would erode the warning's signal value.

  Permission-denied errors on any individual SQL signal degrade gracefully via the existing `isPermissionDenied` helper — managed services that restrict `pg_replication_slots` or `pg_settings` won't break the preflight; the heuristic just skips the affected signal and continues.

## Added

- **`sluice sync start --patroni-mode=auto|on|off`.** New flag pairing with the broader heuristics. `auto` (default) runs the engine heuristics + DSN hostname-pattern check and warns if any of the six signals fires; `on` skips the heuristics and forces the warning (operator opts in regardless of detection — the canonical override for tenant-isolated managed PG where the heuristics still miss); `off` skips the heuristics and suppresses the Patroni warning entirely (operator confirmed self-hosted single-node PG without HA, doesn't want the noise). Combine `--patroni-mode=on` with `--strict-preflight=true` to make the warning a hard refusal. Validation: unknown values are rejected at flag parse time with a clear error.

## Compatibility

- **No IR / CLI / manifest schema changes.** Fix is additive: two new engine-side SQL queries inside `detectPatroniSource`, a new streamer-layer hostname matcher, and a new `--patroni-mode` flag with a sensible default (`auto`) that preserves v0.17.2 behaviour for any setup the v0.17.2 signals were already catching. The IR `PositionFromManifestPreflight` interface is unchanged. Format version stays at 1.
- **Existing v0.17.2 setups behave the same way under `--patroni-mode=auto`.** If your source was tripping signals 1–3 in v0.17.2 (self-hosted Patroni with the canonical role names + GUCs), it still does. The new signals 4–5 only widen the catch surface; they don't change the behaviour where the old signals already fired. Other warnings (wal_keep_size sufficiency) are unaffected.
- **The slot-existence / `wal_status='lost'` refusal is unaffected by `--patroni-mode`.** Those are always refusals — the slot can't deliver what's needed regardless of the operator's view on Patroni detection.

## Who needs this

- **Anyone running `sluice sync start --position-from-manifest` against PlanetScale Postgres, Aurora, Azure Database for PostgreSQL, Cloud SQL, Archil, or any other tenant-isolated managed PG service** — Bug 36 affects you. v0.17.2 silently let the run proceed without the idle-slot-trap warning even when the cluster was Patroni-managed underneath. v0.17.3 catches this via the new heuristics (or via `--patroni-mode=on` if your service uses a hostname pattern not on the list yet).
- **Anyone running self-hosted Patroni with the canonical role / GUC convention** — you're unaffected by Bug 36 (signals 1–3 already fire on your setup) but you still want v0.17.3 because the new flag adds the explicit-override option for CI gates and scripted runbooks where you want to force or suppress the warning regardless of detection.
- **Anyone running self-hosted single-node PG without HA who finds the v0.17.2 warning noisy** (e.g. when role names happen to include `replicator` for unrelated reasons) — `--patroni-mode=off` suppresses the warning cleanly.

## What's next

Phase 4 (continuous-incremental backup) is the next chunk on the backup track; the snapshot-anchored EndPosition gap-fix is queued separately. See `docs/dev/roadmap.md` for the full picture.
