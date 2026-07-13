# sluice v0.99.233

**`migrate` now shows a clean live progress view at an interactive terminal — a phase checklist, a per-table progress bar, and a summary panel — while every piped/CI/JSON invocation keeps the exact structured logs, byte-for-byte.**

## Added

- **A TTY-aware pretty view for `migrate` (ADR-0155).** Run `sluice migrate` at an interactive terminal and you now get a legible live view instead of a wall of log lines: a phase checklist (Tables → Bulk copy → Indexes → Identity → Constraints → Views) that fills in as each phase completes, a per-table progress bar during the bulk copy, and a final summary panel showing tables, rows, duration, and any warnings (rendered inside the panel, long ones truncated to fit the width). It's all ASCII-safe, so it renders correctly on every terminal, Windows included.

  The structured-log path is **completely unchanged**. The pretty view appears only when stdout is a terminal **and** `--log-format=text` (the default) **and** `--no-progress` is not set. Piped output, CI, `--log-format=json`, and the new **`--no-progress`** flag all emit the exact `slog` records sluice has always emitted — byte-for-byte — so Loki/Datadog/CloudWatch ingestion, `| tee`, and every automation keep working identically. `--no-progress` is the explicit opt-out for a plain terminal.

  Small nicety: for a MySQL source, InnoDB's row-count estimate routinely undershoots, so the bar reads **`100%+`** with the live row count once the copy passes the estimate — it never shows a nonsensical percentage, and it stays obviously "still progressing" rather than looking stuck.

## Compatibility

- No format change; no behavior change to any migration. The pretty view is additive on top of a TTY, and the non-TTY / `--log-format=json` output is identical to prior releases.

## Who needs this

Anyone who runs `migrate` interactively — it's a much clearer read. If you script `migrate` or ship its logs to an aggregator, nothing changes (or add `--no-progress` to force plain logs at a terminal). This is the first command in a rollout: `verify`, `backup`, `restore`, `trigger`, and others get the same treatment next, and the long-running commands (`sync start`, the broker, `backup stream`) get a live status panel.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
