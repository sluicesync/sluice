# sluice v0.99.236

**`sync start` — the continuous-sync command — gets a live status panel at an interactive terminal, and `slot list` gets an on-brand table. Both are TTY-additive; every piped / CI / JSON invocation is byte-identical to prior releases.**

## Added

- **A TTY-aware live status panel for `sync start` (ADR-0156 phase 1).** Run `sluice sync start` at an interactive terminal and you get a live, in-place status view instead of a wall of log lines:
  - an **initial-copy checklist** with a per-table progress bar during the snapshot;
  - a **CDC body** once the snapshot hands off — last-applied position, **freshness** (seconds since last apply, the load-bearing cross-engine lag signal), and connection health;
  - a bounded **recent-events** region that surfaces WARN/ERROR live as they occur (a days-long run never buffers them to a summary);
  - a `q` / ctrl+c footer that triggers a **graceful drain-and-stop**, wired to the exact `RequestStop` write `sync stop` performs — in-flight changes drain rather than being dropped.

  The renderer is isolated from the stream: a panel failure falls back to structured logging and never aborts the sync. Gating matches the rest of the pretty rollout exactly — the panel renders only when stdout is a terminal **and** `--log-format=text` **and** `--no-progress` is unset, for a single-namespace, non-`--format json`, non-`--dry-run` run. Every other invocation — piped, CI, `--log-format=json`, `--no-progress`, or a multi-namespace fan-out — keeps the byte-identical structured `slog` stream, so log ingestion and automation are unchanged.

  **Named phase-1 gap:** cumulative rows-applied and throughput render `n/a (phase 1)` rather than a fabricated number — the control table carries no applied-row counter yet, and sluice refuses to invent one (loud-failure discipline). A truthful counter is the immediate next follow-up. The broker (`sync from-backup run`), `backup stream run`, and `metrics-watch` adopt the same panel in ADR-0156 phases 2–3.

- **`slot list` renders an on-brand bordered table at a TTY (ADR-0155, report-shaped).** `sluice slot list` now shows a rounded-border grid with the ACTIVE column colour-coded so a slot with a live consumer stands out at a glance. Piped / CI / `--log-format=json` / `--no-progress` output is the exact `tabwriter` table sluice has always emitted, byte-for-byte. `schema preview` and `schema diff` deliberately stay plain — they're the most copy-paste- and CI-oriented commands, where a box would get in the way.

## Changed

- **golangci-lint no longer descends into the gitignored `.claude/` agent-worktree scratch area** — a local whole-tree lint could otherwise be polluted by a background agent's in-progress worktree. CI is unaffected (it lints a fresh checkout).

## Compatibility

- No format change; no behavior change to any command. Both views are additive on top of a TTY; non-TTY / `--log-format=json` / `--no-progress` output is identical to prior releases, exit codes included.

## Who needs this

Anyone who runs `sync start` interactively — the continuous-sync stream is now a legible live view rather than a scrolling log. Anyone who runs `slot list` interactively gets the clearer table. If you script either, or ship their logs to an aggregator, nothing changes (or add `--no-progress` to force plain output at a terminal).

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
