# sluice v0.75.0 — `sluice diagnose` operator-bundle subcommand + auto-on-crash hook

**Headline:** First minor bump after the v0.74.x patch chain. Ships the `sluice diagnose` subcommand — a `cockroach debug zip`-shape operator-bundle assembler for filing GitHub issues — plus an opt-in `--diagnose-on-crash-dir` auto-write hook that captures a bundle when sluice exits via a loud-failure path. Closes the last item in the pre-public-release prep cluster (alongside the already-shipped `tmp/` → `docs/releases/` rename and the Vultr/AURORA-R11 identifier scrub).

## Why this is the minor bump that opens the public-release window

Three of the items from the 2026-05-22 public-release audit have now shipped on released binaries: `#17` (folder rename, v0.74.1), `#19` (identifier scrub, v0.74.1), and now `#18` (diagnose subcommand, v0.75.0). With v0.75.0 the repo is structurally ready to flip from private to public — the audit doc's named gating items are all closed.

This release is **behavior-additive** on top of v0.74.2 (new subcommand, new opt-in flag, new optional interfaces); no existing operator workflow changes.

## Added

- **`sluice diagnose`** *(new top-level subcommand; Task #18; ADR-0056)*. Assembles a ZIP bundle containing the operator-visible state needed to triage a sluice GitHub issue. Three privacy levels:

  - **`basic`** *(safest, default for unattended crash-writes)*: state tables only — per-stream `sluice_cdc_state` row, last 100 `sluice_cdc_schema_history` rows for the stream, `sluice_shard_consolidation_lease` rows for the target tables. **No** version metadata, **no** DSN, **no** engine names in manifest, **no** logs, **no** row counts. Row-level data is excluded.

  - **`standard`** *(default for operator-initiated bundles)*: `basic` + sluice version + commit + build date + Go runtime + GOOS/GOARCH + DSN-redacted CLI argv + redacted source/target DSN + engine `Capabilities()` declarations + per-engine `ir.DiagnoseProber` snapshots (PG slot state + version; MySQL master-status + GTID) + `sync health`-shape cross-engine probe.

  - **`verbose`** *(largest; slow on big targets)*: `standard` + per-table `COUNT(*)` on the target + last 200 lines of sluice's own log file (requires `--log-file` to be set).

  Privacy contract per level is pinned by separate unit tests applying the Bug 74 "pin the class, not the representative" discipline.

- **`--diagnose-on-crash-dir <path>`** *(new opt-in flag on `sync start` and `migrate`)*. Installs a runtime hook that writes a `crash-bundle-<timestamp>.zip` to the configured directory if sluice exits with an error. **Opt-in only** — default off (writing diagnostic bundles to local disk without operator action is a privacy risk; the operator must explicitly enable). Default privacy for crash-writes is `basic` (safest unattended); opt up via `--diagnose-on-crash-privacy=standard|verbose`. **The hook NEVER masks the original error** — bundle-assembly failures are logged at WARN and the original error propagates unchanged (loud-failure tenet). Pinned by `TestCrashHook_BundleWriteFailureDoesNotMaskOriginalError`.

- **`ir.DiagnoseProber` interface** *(new optional engine surface)*. `DiagnoseBundle(ctx, streamID) (DiagnoseSnapshot, error)` returns a structured per-engine snapshot. PG + MySQL both implement.

- **`ir.SchemaHistoryReader` interface** *(new optional applier surface)*. `ListSchemaHistory(ctx, streamID, limit) ([]SchemaHistoryRow, error)` returns the per-stream rows from `sluice_cdc_schema_history` in created_at-desc order. PG + MySQL both implement.

## Docs

- **`docs/adr/adr-0056-sluice-diagnose-operator-bundle.md`** *(new ADR)*. Documents the design rationale: scope decision (cockroach-debug-zip-shape vs minimal), privacy levels (exact inclusion/exclusion contracts per level), redaction-helper boundary (`internal/redact` is row-value-level, NOT for diagnose), crash-hook safety semantics (NEVER masks original error), and the `--log-file` requirement for `verbose`-level log inclusion.

## Tests

Privacy-level pins applying the Bug 74 "pin the class" discipline (separate test per level), plus the crash-hook never-masks-original-error pin, plus four integration pins against a real PG container.

## Compatibility

- **Drop-in upgrade from v0.74.2.** No CLI surface change for existing flows. `sluice diagnose` is a new top-level subcommand. `--diagnose-on-crash-dir` is opt-in.
- **MySQL `SchemaReader` gains a `flavor` field** — additive only.
- **New optional engine interfaces** — engines implementing them get richer bundles; engines not implementing them get partial bundles (graceful degrade).

## Who needs this

- **Anyone filing or triaging GitHub issues against sluice.** Drop the bundle as an attachment.
- **Operators running unattended sluice deployments.** Set `--diagnose-on-crash-dir=...` and the crash hook captures bundles on loud failure.
- **Anyone NOT using either surface** sees no observable change.

## Public-release readiness

With v0.75.0 the [2026-05-22 public-release audit](https://github.com/orware/sluice)'s gating items are all closed. The repo is structurally ready for the private→public flip. The v0.74.x → v0.75.0 chain covers: the entire 2026-05-22 PG-internals research backlog (F1–F9), the post-public-release prep cluster (#17 / #18 / #19), and the v0.73.0 → v0.73.2 hotfix chain that closed the ADR-0054 Shape A Phase 2 misship + its follow-on bugs.

## Cross-references

- [ADR-0056 — sluice diagnose operator-bundle](https://github.com/orware/sluice/blob/main/docs/adr/adr-0056-sluice-diagnose-operator-bundle.md) *(new)*
- [ADR-0049 — CDC schema history](https://github.com/orware/sluice/blob/main/docs/adr/adr-0049-cdc-schema-history.md)
- [ADR-0054 — Shape A Phase 2 live cross-shard DDL coordination](https://github.com/orware/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md)
- Prior art: `cockroach debug zip` from CockroachDB
