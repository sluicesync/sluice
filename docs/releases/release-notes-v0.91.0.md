# sluice v0.91.0

# sluice v0.91.0 — Stage 2 verbatim-carry + `--poll-interval` + `trigger teardown --yes`

**Headline:** Same-engine PG → PG `sluice migrate` / `sync` of columns whose `pg_catalog` type is `xml`, `money`, `pg_lsn`, `txid_snapshot`, or `pg_snapshot` now **preserves the column type and round-trips the value byte-equal**, instead of refusing loudly at translate time. Plus a new `--poll-interval` flag for tuning postgres-trigger CDC cadence, and a small CLI papercut fix on `trigger teardown`. Drop-in from v0.90.0; no config / schema / IR / lineage-format changes.

## Features

- **Stage 2 verbatim-carry promote — `xml` / `money` / `pg_lsn` / `txid_snapshot` / `pg_snapshot` ([ADR-0070](https://github.com/orware/sluice/blob/v0.91.0/docs/adr/adr-0070-stage-2-verbatim-carry-promote.md)).** The [ADR-0051 §"Stage 2 candidates"](https://github.com/orware/sluice/blob/v0.91.0/docs/adr/adr-0051-core-pg-type-verbatim-carry.md) deferred these five types pending per-type round-trip integration evidence. v0.90.0 shipped the per-type pins (xml / money / pg_lsn / txid_snapshot / pg_snapshot); this release promotes them into `coreVerbatimEligibleTypes`. The pins automatically flip from path (a) refuse-loudly to path (c) preserve. **Cross-engine PG → MySQL behaviour is unchanged** — `ir.VerbatimType` continues to refuse loudly at preflight; the cross-engine safety is doubly enforced.
- **`--poll-interval` flag on `sluice sync start` (roadmap item 18(c) part 1).** Operator-tunable cadence for poll-based CDC readers (today: `postgres-trigger`; default 1 s). Set to e.g. `--poll-interval=250ms` to tighten apply latency on a write-heavy postgres-trigger stream, or `--poll-interval=5s` to trade latency for source load. Push-based engines (postgres pgoutput, mysql binlog, planetscale VStream) have no poll loop and silently ignore the flag. Wired via a new `pollIntervalSetter` optional interface on the CDC reader, type-asserted by the streamer between open and `StreamChanges`; the `ir.Engine` interface is unchanged.

## Fixed (papercut surfaced in v0.90.0 cycle)

- **`sluice trigger teardown --yes` now does the right thing.** Previously the command had no confirmation prompt at all, but rejected `--yes` with help-text output, which surprised operators (and the v0.90.0 post-release cycle subagent) into thinking `--yes` was required and then silently no-op'd on the first attempt. The command now mirrors `sluice slot drop`: confirmation prompt by default (`Tear down the sluice trigger engine on the source ...? [y/N]`), `--yes` / `-y` skips it for scripted / CI use.

## Compatibility

- **Minor version bump (v0.91.0)** — additive, drop-in from v0.90.0. No config / schema / IR / lineage-format changes.
- **Two behavior changes**, both PG-only:
  - Same-engine PG → PG migrations involving `xml` / `money` / `pg_lsn` / `txid_snapshot` / `pg_snapshot` columns now succeed (previously refused with `postgres: unsupported data_type`). Operators relying on the refusal as a guard should declare the column explicitly via `--exclude-table` or `--type-override`.
  - `sluice trigger teardown` now prompts for confirmation. Scripted invocations should add `--yes` (or `-y`); the prompt is on `os.Stdin` and will block in non-TTY environments without it.

## Internal cleanup (no operator-visible effect)

- Roadmap `docs/dev/roadmap.md` reflects Item 14 (backup chain retention) as fully shipped — chunks 14a–14e (chain.json catalog, rotation, prune, naive compact, smart compaction) are all complete. Moved to "Recently landed" with a one-line SHIPPED pointer in the "Next up" section, matching Items 1/3/4/6's pattern.
- 14 stale `//nolint:unused` directives removed (7 mysql + 7 postgres) on `internal/engines/{mysql,postgres}/schema_history.go`. These were placed when ADR-0049 Chunk A landed as storage-only; Chunks B/C/D have since wired every consumer. `golangci-lint run` stays at 0 issues without the suppressions.
- `docs/dev/notes/path-d-phase-a-status.md` annotated as historical (ADR-0036 Phase A closed v0.32.0; the Vultr-box recipes are superseded by the local Hyper-V validation flow).

## Who needs this

- **Operators with PG-source schemas containing `xml`, `money`, `pg_lsn`, `txid_snapshot`, or `pg_snapshot` columns** — same-engine PG → PG migrations involving those columns just work now, no `--exclude-table` workaround needed. Common in audit tables, financial systems (money), legacy schemas (txid_snapshot), and PG 13+ snapshot capture (pg_snapshot).
- **Operators tuning postgres-trigger CDC apply latency** — `--poll-interval` is the cleanest dial. Push-based CDC users see no change.
- **Operators teardown-scripting `trigger teardown`** — add `--yes` / `-y` to scripted invocations; the prompt-by-default change matches `slot drop`'s established pattern.
- **Everyone else:** no action needed.
