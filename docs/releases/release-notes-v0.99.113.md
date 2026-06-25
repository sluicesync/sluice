# sluice v0.99.113

**PlanetScale telemetry flags renamed to one word (`--planetscale-*`), and a second storage-grow face (the Postgres read-only reparent window) is now ridden on the cold-copy path.** A branding-consistency rename plus the follow-up the live item-38 re-validation surfaced.

## Changed

**PlanetScale flags are now `--planetscale-*` (one word), matching PlanetScale's own branding** — `--planetscale-org`, `--planetscale-metrics-token-id`, `--planetscale-metrics-token`, `--planetscale-metrics-db`, `--planetscale-metrics-branch`, on both `sync start` and `diagnose`. The previous `--planet-scale-*` forms were kong's automatic camelCase split of the Go field names; this pins the brand-correct spelling explicitly via the flag definitions. This is **breaking** for anyone who scripted the hyphenated forms — but a clean rename with no compatibility shim, per the zero-users policy. The env-var forms (`PLANETSCALE_METRICS_TOKEN_ID` / `PLANETSCALE_METRICS_TOKEN`) were already one word and are unchanged. The old flag now errors helpfully: `unknown flag --planet-scale-org, did you mean "--planetscale-org"?`.

## Fixed

**A PlanetScale Postgres serving-transition read-only window during a storage auto-grow is now retriable on the cold-copy path (item 38 follow-up).** v0.99.111 made the PG cold-copy `COPY` ride the `53100` could-not-extend (disk-full) face of a non-Metal PlanetScale-Postgres storage auto-grow. The full-corpus re-validation (43 GB MySQL → fresh PS-160 Postgres) confirmed that works — it rode **22 grow-gate windows** retrying the 53100s — but surfaced a *second* face of the same grow: while the new primary takes over, the cluster is briefly promoted **read-only**, and an in-flight chunk `COPY` fails with `pg_readonly: invalid statement because cluster is read-only` (SQLSTATE `XX000`). That wasn't in the retriable classifier, so the cold-copy terminated loudly on it (no data loss, but not resilient).

It's now classified retriable — the exact Postgres twin of the MySQL `Error 1290 --read-only` reparent face fixed in v0.99.101 — so the grow-gate + chunked-COPY retry rides the read-only window and resumes once the new primary serves read-write. The match is on the message (`cluster is read-only` / `pg_readonly`), **not** the bare `XX000` code (which is Postgres's generic `internal_error` catch-all), so a non-read-only `XX000` stays terminal — no over-match. Pinned in the classifier test set (read-only `XX000` → retriable; generic `XX000` → terminal).

With this, a full MySQL→PlanetScale-Postgres cold-copy rides **both** storage-grow faces — disk-full (`53100`) and the read-only reparent (`pg_readonly`/`XX000`) — to completion, the way MySQL targets already do.

## Compatibility

The flag rename is breaking only for scripts using the old `--planet-scale-*` spelling (zero-users clean break; helpful error on the old form). The classifier change is additive (one more retriable face on the PG cold-copy/apply path, message-scoped) — no effect on any non-PlanetScale target, and the exactly-once contract is unchanged.

## Who needs this

Anyone scripting the PlanetScale telemetry flags should switch to the `--planetscale-*` spelling. Anyone running a `sluice sync` / `migrate` into a **PlanetScale Postgres** target whose cold-copy crosses a storage auto-grow now rides the read-only reparent window as well as the disk-full window — to a complete copy.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.113
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.113
```
