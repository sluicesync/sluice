# psverify dispatch runbook

The `psverify.yml` workflow runs the live-PlanetScale verification suites (12 tests across 5 packages) with `-race`, on demand via `workflow_dispatch`. It FAILS if any psverify test skips — a green run means everything actually ran. The durable token secrets are standing; the infra-pointing secrets are refreshed per dispatch from throwaway databases. First executed 2026-07-16 (three runs; every suite passed live at least once, zero skips).

## Per-dispatch steps

1. **Provision** (pscale CLI, service token from `C:\code\PLANETSCALE_SLUICESYNC.env` — parse with awk, never `source`; all PS_10, us-east):
   - `sluice-psv-mysql` (mysql) — safe migrations OFF (default). Serves `SLUICE_MYSQL_SOURCE`/`SLUICE_MYSQL_DESTINATION` (same DSN works — destination is only presence-checked) + `PLANETSCALE_METRICS_DATABASE`.
   - `sluice-psv-ec` (mysql) — create table `sluice_ec_items` (BIGINT PK AUTO_INCREMENT + a VARCHAR) with ≥1 row FIRST, then `pscale branch safe-migrations enable <db> main` (the toggle propagates in ~10s; DDL after enable fails "direct DDL is disabled"). **Use a FRESH database every dispatch** — sluice ≥ v0.99.258 restarts the backfill when its expand leg deploys, but a fresh DB keeps the run hermetic either way.
   - `sluice-psv-pg-src` + `sluice-psv-pg-dst` (postgresql, `--replicas 0`) — two separate DBs; wal_level=logical + REPLICATION come free with the default postgres role.
2. **DSNs:** MySQL/EC = `pscale password create <db> main <name> --format json` → `user:plain_text@tcp(access_host_url)/<db>?tls=true` (the `?tls=true` param is required). Do NOT add `multiStatements=true`: sluice's `openDB` force-clears it on every connection it opens (audit H-3 — it is the MySQL counterpart of the PostgreSQL single-statement door, so a recorded/tampered CHECK body cannot smuggle a second statement past the restore path), and a DSN that sets it is silently overridden. PG = `pscale role reset-default <db> main --force --format json` → `database_url` with sslmode rewritten to `require`.
3. **Secrets** (pipe values via stdin to `gh secret set <NAME> --repo sluicesync/sluice`, never echo): the 4 `SLUICE_*` DSNs, `PLANETSCALE_EC_{DSN,DATABASE,BRANCH=main,TABLE=sluice_ec_items,ORG=sluicesync}`, `PLANETSCALE_METRICS_{DATABASE,BRANCH=main}`. The 7 durable token secrets are already set.
4. **Dispatch:** `gh workflow run psverify.yml --repo sluicesync/sluice`. Green = job success AND the fail-on-skip step reports "no skipped tests".
5. **Teardown, always:** `pscale database delete <db> --force --org sluicesync` ×4, verify by listing, and reset the 11 infra secrets to the literal `unset-pending-next-dispatch` (keeps the env block resolving; a dispatch against them fails loudly at connect).

## Known behavior

- PS-PG holds a just-closed CDC reader's walsender "active" for >40s; the tests poll-until-inactive in their slot pre-cleans (fixed `edde8af5`), which is also why the postgres/pipeline packages run sequentially in the workflow.
- The PS deploy call can 422 "currently validating…" during the safe-migrations settling window; sluice ≥ v0.99.258 retries that class (bounded ~90s).
- The workflow scopes to `-run '^TestPS'` — a bare `./internal/...` sweep would trip the fail-on-skip gate on the unit suite's permanent placeholder skips.

## When to dispatch

Cron stays OFF (operator call — quota predictability). Dispatch when a PS-touching change ships without a manual live leg, or when a `-race` pass over the live control-plane paths is wanted (this box cannot run `-race`). Cost: 4 × PS_10 for ~1 hour.
