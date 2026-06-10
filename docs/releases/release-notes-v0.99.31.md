# sluice v0.99.31

**New: MySQL secondary indexes now build in one combined `ALTER` per table (−18.1 % median index-phase on top of v0.99.30's overlap win) and `--log-format=json` lands for structured log ingestion — plus a security hardening: local backup stores and crash bundles are now written owner-only (0600/0700).** The combined-`ALTER` change collapses one-InnoDB-scan-per-index into one scan per table on any MySQL-target `migrate`, on both the overlapped and serial index paths. Separately, this release closes a CI testing gap: the `postgres-trigger` engine's integration suite — including the capture-payload silent-loss pin — runs in CI for the first time, with new guards so a gap like it can't reopen. **Drop-in from v0.99.30 — no flag or behavior change to existing invocations; the perms hardening affects only newly created backup files.**

## Features

- **Combined-`ALTER` MySQL secondary-index builds (ADR-0080 follow-up).** When `sluice migrate` builds a table's secondary indexes on a MySQL target, all combinable indexes — regular + `UNIQUE`, BTREE/HASH — are now created in **one** `ALTER TABLE … ADD INDEX a (…), ADD UNIQUE INDEX b (…)` statement, so InnoDB scans the table once and takes one metadata lock per table instead of once per index. `FULLTEXT` and `SPATIAL` each keep their own statement (InnoDB permits only one `ADD FULLTEXT` per `ALTER`, and `SPATIAL` can't share a `LOCK=NONE` group — folding either in would error or downgrade the whole statement's algorithm). The job model is one-per-table, so both the serial post-copy `CreateIndexes` and the v0.99.30 overlapped per-table workers route through the same combined emit; idempotent resume is preserved via per-index detect-then-skip (the combined `ALTER`'s clause list is the filtered survivor set of missing indexes). Measured on the `benchmarks/mysql/` harness: **−18.1 % median index-phase time** on top of the v0.99.30 overlap win, zero-loss verified. Pinned by a grouped-emit unit matrix (all-combinable collapse, mixed-kind split, two-`FULLTEXT`-never-combine, single == standalone), an all-index-kinds integration family guard (the Bug-74 pin-the-class discipline), and a partial-resume integration test (drop one index, rebuild only the missing one). PlanetScale/Vitess targets keep declining the overlap entirely, exactly as in v0.99.30.

- **`--log-format=json`.** New global flag emitting one JSON object per line on stderr (slog's JSONHandler) instead of the human-readable text format — the shape Loki / Datadog / CloudWatch agents ingest natively. The long-running `sync` mode already ships `/metrics` + `/healthz` + `/readyz` and a Kubernetes-probed container image; the structured log stream was the missing third leg for running it under an orchestrator. Default remains `text` — existing invocations are unchanged.

## Security

- **Local backup stores and crash bundles are now written owner-only — 0600 files / 0700 directories (previously 0644/0755).** Backup chunks contain full row data and `--encrypt` is opt-in, so a world-readable backup directory handed any local user on the host the entire dataset; crash bundles can likewise carry row data. Only **newly created** files and directories get the tighter mode — sluice does not retroactively chmod existing stores, and restore reads them regardless of mode. No effect on Windows (Go approximates Unix permission bits there). Pinned by an owner-only-permissions unit test. Local backup stores have written 0644/0755 since they were introduced in v0.15.0 (crash bundles since v0.75.0) — operators with existing stores on shared multi-user hosts should tighten them by hand (see below). This is a local-disclosure hardening, not a data-integrity issue: no migration or backup contents were ever wrong.

## Fixed

- **Failed backup-compact orphan sweeps now leave a WARN breadcrumb.** After `sluice backup compact` commits the merged catalog, a post-commit sweep deletes the superseded segment files. That sweep was silently best-effort: a failure (permissions, transient FS error) leaked backup-store disk with no log line at any level. It stays non-fatal by design — the catalog swap has already committed and the chain remains correct either way — but failures now WARN with the segment directory and merge target so the leak is visible and reclaimable. Operator-visibility fix only; **no data was ever lost or corrupted.** Affected releases: v0.77.0 (where `backup compact` shipped) through v0.99.30.

## CI

- **The `postgres-trigger` engine's integration tests now run in CI — for the first time.** The package landed after the integration-shard split and no shard listed it, so its suite — including `TestCapturePayload_EndToEnd_AllModes`, the pin for the PK-changing-UPDATE silent-loss class caught in the ADR-0068 review — had never executed in CI (it had been run locally and in pre-release validation, but no merge gate enforced it). Two new guards keep the gap closed: a Lint-job shard-coverage check fails CI if a package with integration-tagged tests is ever outside the shard matrix again, and a tags-vet matrix type-checks every `//go:build` tag combination — including tagged *test* files, which `go build -tags=…` skips — on every PR. This is a testing-gap closure, not a product bug: no shipped pgtrigger behavior changed, and the suite passes.

## Compatibility

- **No breaking changes. Drop-in from v0.99.30.** No existing flag, default, or invocation changes behavior.
- **Combined-`ALTER` index builds** engage automatically on any MySQL-target `migrate` (MySQL→MySQL and PG→MySQL), on both the overlapped and serial post-copy index paths. The resulting indexes are identical — only the DDL statement shape changes. PlanetScale/Vitess targets are **unaffected** (they decline the overlap and defer to platform online-DDL, as in v0.99.30). PG targets are unaffected.
- **`--log-format`** defaults to `text`; output is byte-identical unless you opt into `json`.
- **Backup-store permissions:** only newly created files/dirs get 0600/0700. Existing stores keep their current permissions; restore and verify read stores of either mode. Windows is unaffected.
- **Unaffected:** sync/CDC paths, resume, restore, all PG-target index builds, and all cross-engine value translation are untouched.

## Who needs this — action required

- **Anyone migrating to a MySQL target with multi-index tables** — the combined `ALTER` cuts the index phase further on top of v0.99.30's overlap. **No action required:** it engages automatically.
- **Anyone running `sluice sync` under Kubernetes / a log aggregator** — add `--log-format=json` to get natively ingestible structured logs. Opt-in.
- **Anyone with an existing local backup store or crash bundles on a shared multi-user Unix host** — files created by prior releases (backup stores since v0.15.0, crash bundles since v0.75.0) are world-readable (0644/0755) and contain full row data unless you used `--encrypt`. v0.99.31 fixes new writes only; **action: tighten existing stores by hand** (e.g. `chmod -R go-rwx <backup-dir>`). Single-user hosts and Windows: nothing to do.
- **Anyone who has run `sluice backup compact`** — if a sweep ever failed, superseded segment files may be silently occupying disk; v0.99.31 will WARN on future failures. Optional: check the backup-store directory size against the catalog if disk usage looks high. The chain itself is correct either way.
- **No re-verification of prior migrations is needed.** This release contains **no silent data-loss fix** — the security item is a local file-permissions disclosure hardening, the orphan-sweep fix is operator visibility for a disk leak, and the pgtrigger-CI item is a testing-gap closure (the suite passes; no shipped behavior changed).

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.31`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.31`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
