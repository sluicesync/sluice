# sluice v0.99.241

**`restore` no longer leaks a "starting…" INFO line above its live panel at a TTY — completing the pre-panel-INFO-leak sweep across every command that renders a live view.**

## Fixed

- **`restore` no longer leaks a "restore: starting full restore" INFO line above the live panel (ADR-0155 polish).** The pre-run `slog` line fired before `runWithProgress` installed the TTY slog gate, so on the pretty path it printed above the restore checklist panel. This is the same class of leak v0.99.238 fixed for `backup full` / `backup incremental` and v0.99.240 fixed for `sync start` / `metrics-watch` — `restore` was simply missed in that earlier sweep. The pretty gate is now computed up front, and both the "starting" INFO and the optional PlanetScale telemetry-enabled INFO (when `--planetscale-org` is set) are suppressed on the interactive path; the panel is the output there, and its header carries the same context. With this, every command that renders a live view is leak-free.

## Compatibility

- No format or behavior change off the interactive panel path. Non-TTY / `--log-format=json` / `--no-progress` `restore` output is identical to prior releases — the INFO lines still emit exactly as before when not rendering the live view. The restore itself, and the telemetry provider, are unchanged.

## Who needs this

Anyone who runs `sluice restore` interactively — a clean checklist panel with no stray log line above it. Everyone else, and anyone scripting or shipping logs, sees no change.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
