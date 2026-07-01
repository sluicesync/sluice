# sluice v0.99.169

**Two operator-facing touches from a real PlanetScale soak — a preflight warning that flags PlanetScale-Postgres's ephemeral-role table-ownership trap before it bites, and the end of a spurious per-apply DEBUG line on MySQL→Postgres — plus a round of documentation corrections and seven new guides.**

## Added

**PlanetScale-Postgres target ownership advisory.** On a PlanetScale Postgres, the per-connection *user-defined role* (`pscale_api_*`, which inherits `postgres`) is distinct from the Default `postgres` role — and whichever role sluice connects as **owns every table it creates**. Owning your migrated tables with an ephemeral `pscale_api_*` role is a trap: delete that role, or run later DDL as a *different* `pscale_api_*` role, and you hit ownership/permission errors even though both inherit `postgres`. `migrate` and `sync start` now emit a preflight `WARN` when they detect this, naming the pitfall and the fix — connect as the Default `postgres` role upfront (`pscale role reset-default`), or reassign afterwards (`pscale role reassign`, or the PlanetScale UI's "Reassign objects", which works even for an expired role). It is **advisory only**: sluice never refuses and never auto-reassigns ownership — auto-`ALTER … OWNER` is a privileged action the "contain Postgres complexity" tenet keeps out of sluice's hands. No-op on non-PostgreSQL targets and on the Default `postgres` role.

## Fixed

**No more applied-LSN parse-failure spam on MySQL→Postgres.** The Postgres applier's applied-LSN slot-ack feedback is a Postgres-*source* mechanism. Against a MySQL / VStream source it was handed the source's JSON-array position token, tried to parse it as a Postgres LSN, and logged `applied-LSN report skipped (parse failure)` at `DEBUG` on **every single apply**. It now recognizes the non-Postgres (array-shaped) token and no-ops silently, while a genuinely malformed Postgres-source token still surfaces the error. This is `DEBUG`-only log noise with no data-path effect — but it removes a persistent, misleading line from every MySQL→Postgres debug log.

## Documentation

- **How sluice copies your data** — a new README section and docs-site guide on the two internal copy paths: the typed IR path (every cross-engine copy, and where type translation, redaction, and value-fidelity checks live) versus the same-engine fast lane (Postgres→Postgres byte-pipes the native `COPY` stream; MySQL→MySQL writes through the native `LOAD DATA` loader). It is careful to frame the fast lane as *the same fidelity, less work* — not a "more exact" copy — with the auditable gate that falls back to the IR path the moment any transform (redaction, type/expr override, shard injection, an OID-sensitive type) is present.
- **PlanetScale source-selection corrected.** A `*.connect.psdb.cloud` host does not auto-select VStream — VStream requires `--source-driver planetscale`; that host under `--source-driver mysql` gets binlog CDC with the Vitess `_vt_*` shadow tables auto-excluded.
- **VStream idle/heartbeat troubleshooting corrected** (from the same soak): on real PlanetScale the heartbeat-log cadence is ~60s and goes fully silent under a throttle / large-transaction stall, and a *genuinely idle* source does **not** fire the soft-idle WARN (vtgate's idle VGTIDs re-arm the timer) — so that WARN is a throttle/stall signature, not an idle one.
- **Redaction docs de-drifted.** `verify` has no redaction awareness (`--depth=sample` hashes the full row, so verify a redacted target with `--depth=count`), and `--require-redactions` is not a real flag — both were removed from the redaction cookbook recipe.
- **Seven new docs-site guides** — PII redaction, Postgres source prep, encrypted backups, schema changes during a live sync, operating a sync fleet, PlanetScale & Vitess, and verify & reconcile.

## Compatibility

No behavior change to any data path. The ownership advisory is a `WARN` only — it never refuses and never mutates ownership — and is a no-op on non-PostgreSQL targets and on the Default `postgres` role. The applied-LSN fix removes a `DEBUG`-level log line and is otherwise byte-identical. Both were surfaced by a real PlanetScale soak, not a code-review hypothesis.

## Who needs this

Operators running sluice against **PlanetScale Postgres** (the ownership advisory) and anyone reading **MySQL→Postgres** debug logs (the applied-LSN line). Everyone else is unaffected on the runtime side — but the new same-engine copy guide and the documentation corrections are worth a look if you run same-engine copies or PlanetScale / VStream sources.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.169 · **Container:** ghcr.io/sluicesync/sluice:0.99.169
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
