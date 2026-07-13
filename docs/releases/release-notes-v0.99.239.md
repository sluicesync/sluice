# sluice v0.99.239

**The live status panel now covers the last of the continuous commands — the `sync from-backup` broker, `backup stream run`, and `metrics-watch` — completing the ADR-0156 rollout across every long-running command.**

## Added

- **TTY-aware live status panel for `sync from-backup run`, `backup stream run`, and `metrics-watch` (ADR-0156 phases 2–3).** Following `sync start` (v0.99.236), these long-running commands now render a live, in-place status panel at an interactive terminal instead of a wall of log lines:
  - **`sync from-backup run`** (the broker) — chain position / backup id, incrementals replayed, chunks applied, per-poll activity.
  - **`backup stream run`** — rollovers rolled, current position, cadence.
  - **`metrics-watch`** — the live CPU / memory / storage (used/capacity) / lag / connections sample as it's polled.

  Each panel has a header with the command's mode, a mode-appropriate readout body that updates in place, a bounded **recent-events** region that surfaces WARN/ERROR live (and, for `metrics-watch`, **threshold breaches** — even with no external `--notify-*` sink configured, since the panel is a delivery target of its own), and a `q` / ctrl+c **drain-and-stop** footer wired to each command's graceful shutdown. The renderer is isolated from the command's work — a panel panic falls back to structured logging and never aborts the broker, stream, or watch.

  Gating matches the rest of the pretty rollout exactly: the panel renders only when stdout is a terminal **and** `--log-format=text` **and** `--no-progress` is unset (and, for `metrics-watch`, not `--once`). Every other invocation — piped, CI, `--log-format=json`, `--no-progress` — keeps the **byte-identical** structured output these commands have always emitted, so log ingestion and automation are unchanged.

## Compatibility

- No format or behavior change. The panel is additive on top of a TTY; non-TTY / `--log-format=json` / `--no-progress` output is identical to prior releases for all three commands (and for `sync start`, whose panel is unchanged).

## Who needs this

Anyone who runs the broker, `backup stream run`, or `metrics-watch` interactively — a legible live view instead of a scrolling log. Anyone who scripts them or ships their logs to an aggregator sees no change.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
