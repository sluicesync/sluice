# ADR-0056: `sluice diagnose` — operator-bundle for GitHub issue support

## Status

Accepted. Implemented in `internal/diagnose/` (bundle assembler), `cmd/sluice/diagnose.go` (kong subcommand), `cmd/sluice/diagnose_crash_hook.go` (auto-on-crash flag wiring), `internal/engines/postgres/diagnose.go` and `internal/engines/mysql/diagnose.go` (engine-side probes). Pinned by `internal/diagnose/bundle_test.go` + `internal/diagnose/crash_hook_test.go` (unit, per-privacy-level contracts + crash-hook safety) and `internal/engines/postgres/diagnose_integration_test.go` (live-PG end-to-end).

## Context

sluice is heading into public release (audit `C:\code\sluice-public-release-audit-2026-05-22.md`); the cluster of pre-release prep tasks #17 + #19 (the renames and the scrub pass) are shipped, and #18 — the `sluice diagnose` operator-bundle — was named as the last item before flipping the GitHub repo visibility.

The shape is well-trodden: `cockroach debug zip` is the prior art every database operator who has ever filed a CockroachDB issue is familiar with. The recipient (a sluice maintainer triaging a GitHub issue) asks the operator to "attach a diagnose bundle" and gets a single ZIP carrying every piece of server-state diagnostic information needed to reproduce the bug, in one structured archive.

Two invocation paths matter:

1. **Operator-initiated.** The operator runs `sluice diagnose --stream-id X --output bundle.zip` after their stream wedges; the bundle attaches to their GH issue. This is the happy path.
2. **Automatic on crash.** A long-running `sluice sync start` or `sluice migrate` that exits via a loud-failure path SHOULD be able to drop a bundle on the operator's behalf — when the operator isn't watching the terminal at the moment of the crash, the post-mortem still has every signal it needs. This is the unattended path.

The naive implementation of either path has three pitfalls the design must avoid:

- **Credentials in bundles.** Source / target DSNs and keyset-source URLs carry passwords. A bundle that lands on a public GH issue with a clear DSN is a security incident. Every DSN-shaped value must be redacted before write.
- **Row data leaking out.** A bundle that includes `SELECT * FROM users LIMIT 100` would be tempting on the engineer's side (it makes bug repros trivial) but is operator-data exfiltration. The bundle is for **server-state diagnostics, not data dumps** — row data MUST stay out.
- **Default-on auto-write.** If `--diagnose-on-crash-dir` defaulted to a real path, an unattended bundle would land on disk every time sluice crashed even when the operator never asked for the feature. The directory has to be opt-in.

There is also a Bug-74 trap latent in any feature with privacy levels: a single representative test ("verbose-level produces row counts") doesn't pin the **contract for EACH privacy level**. The reviewer corollary applies — the inclusion / exclusion contract for `basic` vs `standard` vs `verbose` must be pinned per level, not just tested via the most-inclusive level.

## Decision

Implement `sluice diagnose` as a `cockroach debug zip`-shape ZIP bundle with **three concentric privacy levels** and **two invocation paths** (operator-initiated + opt-in auto-on-crash). The full inclusion / exclusion contract is pinned per level below.

### Privacy levels (exact inclusion / exclusion contract)

The three levels are concentric — each higher level is a strict superset of the levels below. Pinned by per-level unit tests (`TestBundle_BasicLevel_*`, `TestBundle_StandardLevel_*`, `TestBundle_VerboseLevel_*`).

| Subsystem | `basic` | `standard` | `verbose` |
|---|:---:|:---:|:---:|
| `sluice_cdc_state` row for `--stream-id` | yes | yes | yes |
| `sluice_cdc_schema_history` rows (most-recent 100, per `SchemaHistoryRowCap`) | yes | yes | yes |
| `sluice_shard_consolidation_lease` rows (ADR-0054) | yes | yes | yes |
| sluice version + commit + build date | no | yes | yes |
| Go runtime + GOOS + GOARCH | no | yes | yes |
| DSN-redacted CLI argv | no | yes | yes |
| Redacted source/target DSN locator in manifest | no | yes | yes |
| Engine `Capabilities()` declarations | no | yes | yes |
| Engine `ir.DiagnoseProber` snapshot (PG: slot state + version; MySQL: master-status + GTID + version) | no | yes | yes |
| Source + target cross-engine health probe (mirrors `sluice sync health` JSON) | no | yes | yes |
| Per-table `COUNT(*)` on the target (one query per filtered table — slow path) | no | no | yes |
| Last 200 lines of `--log-file` (operator opts in via existing `--log-file` flag) | no | no | yes |
| Row-level data (any column values) | NEVER | NEVER | NEVER |

The `basic` level is the **safest possible bundle** — state tables only, no version metadata, no DSN even redacted, no logs. This is the level the auto-on-crash hook defaults to: an unattended bundle landing on disk should never carry signals the operator hasn't explicitly authorised.

The `standard` level is what an operator filing a GH issue typically wants: enough metadata for the recipient to reproduce the bug (version, runtime, declared capabilities, slot state) without leaking either credentials or row data.

The `verbose` level adds **two opt-in signals** that operators sometimes need but often don't want: per-table row counts (slow path on large tables — the operator should know this is a `COUNT(*)`-per-table query) and a tail of the operator's slog log file (requires the operator to have wired `--log-file` separately; sluice does NOT inspect the parent process's stderr).

### Bundle format

ZIP archive. Top-level files:

- `bundle.json` — manifest, version-bumped via `ManifestVersion` (currently 1) when an incompatible shape change is needed.
- `state/cdc_state.json` — scoped to `--stream-id` (NOT every stream — privacy-by-scoping).
- `state/schema_history.json` — capped at `SchemaHistoryRowCap` (100) most-recent rows, ordered by `created_at DESC`.
- `state/shard_consolidation_lease.json` — every row in the per-target lease table (ADR-0054 §6 operator-visibility surface).
- `config/cli_args.json` — redacted CLI argv (standard+).
- `engine/capabilities.json` — declared shapes (standard+).
- `engine/{source,target}_diagnose.json` — `ir.DiagnoseProber` snapshots (standard+).
- `health/sync_health.json` — cross-engine probe (standard+).
- `verbose/row_counts.json` — per-table counts (verbose only).
- `logs/log_tail.txt` — tail of `--log-file` (verbose only).

Sections that the engine doesn't support (e.g. MySQL targets and `ir.ShardConsolidationLeaseLister`) surface as a `<section>/__skipped.txt` reason file rather than missing entirely — the recipient can tell "absent because not supported" from "absent because of a probe failure".

### Auto-on-crash hook

When the operator passes `--diagnose-on-crash-dir=PATH` to a long-running subcommand (`sync start`, `migrate`), sluice installs an `internal/diagnose.CrashHook` at startup. The hook wraps the subcommand's `Run` return: if `Run` returns a non-nil error, the hook writes a bundle named `crash-bundle-<RFC3339-timestamp>-<stream-id>.zip` to the directory and **returns the ORIGINAL error unchanged**. The bundle is best-effort instrumentation — the loud-failure tenet says the original error is authoritative; a bundle-write failure is logged at WARN and never propagated.

Companion flag: `--diagnose-on-crash-privacy=basic|standard|verbose` (defaults to `basic`). The crash-hook default-`basic` decision is deliberate — an unattended bundle on disk should never include version metadata or DSN locators unless the operator explicitly opted up.

Hook installation fails LOUDLY at startup (not at crash time) when:

- `--diagnose-on-crash-dir` points at a path that doesn't exist or isn't a directory.
- `--diagnose-on-crash-privacy` is set to an unrecognised value.

Failing fast beats failing mid-crash when the operator most needs the bundle.

### Redaction

DSN redaction at the **config / locator level**, not the row-value level. Specifically:

- DSN userinfo (`user:password@`) is stripped from URI-form DSNs (`postgres://`, `mysql://`).
- The same is stripped from go-sql-driver DSNs (`user:password@tcp(...)/db`).
- Query strings are dropped (some keyset / object-store URLs carry credentials in `?key=...`).
- Database NAMES are preserved deliberately — operators filing GH issues need to identify which DB the bug reproduced against, and the DB name is metadata, not a credential.
- CLI argv is redacted using a curated list of DSN-bearing flags (`--source`, `--target`, `--keyset-source`, `--backup-target`, `--position-from-manifest`). Both `--flag value` and `--flag=value` forms are handled.

The redaction helpers live in `internal/diagnose/redact.go` as **siblings** to the existing `internal/redact.redactDSNForAudit` (the keyset-source audit-log helper) and `internal/pipeline.redactBlobURL` (the blob-store URL helper). The siblings mirror the existing helpers' shape but stay independent so the diagnose-bundle surface can evolve without coupling to the audit-log or blob-store hot paths.

**Critical: `internal/redact` is NOT diagnose redaction.** The `internal/redact` package implements per-column row-value redaction (`hash:sha256`, `mask:pan`, `tokenize:dict`, etc.) — that's data-level PII protection inside the streamer's hot path. Diagnose redaction is a DIFFERENT contract operating at a different layer; conflating them would couple unrelated invariants.

## Cross-references

- **ADR-0007** (position persistence) — defines the `sluice_cdc_state` row shape the bundle's basic-level state dump embeds.
- **ADR-0049** (CDC schema-history) — defines the `sluice_cdc_schema_history` table the bundle's basic-level dump enumerates (via the new `ir.SchemaHistoryReader` interface).
- **ADR-0054** (Shape A live cross-shard DDL coordination) — defines `sluice_shard_consolidation_lease` and `ir.ShardConsolidationLeaseLister`, which the basic-level dump embeds.
- **`C:\code\sluice-public-release-audit-2026-05-22.md`** — the audit doc that named `sluice diagnose` as the last public-release prep item (Task #18) and cross-linked the scrub findings (Vultr identifiers, internal paths) to diagnose's redaction patterns.
- **`internal/redact` package** — row-value redaction (NOT used by diagnose). See package comment on `internal/diagnose/redact.go` for the boundary.
- **`cmd/sluice/sync_health.go`** — the existing `sluice sync health` probe whose JSON shape the diagnose bundle's `health/sync_health.json` mirrors.

## Consequences

**Public-release readiness.** With the diagnose bundle in place, operators filing GitHub issues against sluice can attach a single ZIP carrying enough signal for the maintainer to triage without back-and-forth. This is a missing piece every operational-database OSS project has — its absence in v0.74.x would have been a noticeable gap on day one of public visibility.

**Engine-neutrality kept.** The bundle assembler lives in `internal/diagnose/` and depends only on `internal/ir/` interfaces — no engine-specific imports. Engines plug in their own `DiagnoseProber` + `SchemaHistoryReader` implementations. The orchestrator never knows whether the target is PG or MySQL; the snapshot is engine-opaque JSON the assembler embeds verbatim.

**Privacy levels are operator-facing, not maintainer-facing.** The operator chooses what to share; the maintainer asks for `basic` / `standard` / `verbose` as the bug-triage situation warrants. This matches the cockroach-debug-zip flow and gives operators an in-band escape hatch (run a `basic` bundle first; promote to `standard` if the maintainer asks for more after the first round of triage).

**No prometheus endpoint for diagnose.** Option C from the design dialogue (expose the diagnose payload over the existing `--metrics-listen` Prometheus endpoint) was rejected. Diagnose is a forensic operator-initiated tool, not a continuous monitoring surface; the existing `sluice sync health` already covers the monitoring path. Conflating them would have widened the metrics endpoint's surface for no operator-facing win.

**Bug-74 discipline applied.** Per-level inclusion / exclusion is pinned by separate unit tests (one per level), and the DSN-redaction contract is pinned across each DSN family (URI-form Postgres, URI-form MySQL, go-sql-driver form). The reviewer corollary applies: when reviewing a future change to either the privacy-level contract or the redaction helpers, re-derive the per-level matrix yourself; "the verbose-level test is green" is insufficient cover.
