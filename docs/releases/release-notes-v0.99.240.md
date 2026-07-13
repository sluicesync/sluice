# sluice v0.99.240

**A small polish fix: `sync start` and `metrics-watch` no longer leak a "telemetry enabled" INFO line above the live status panel at a TTY — the ADR-0156 counterpart to v0.99.238's `backup` fix.**

## Fixed

- **`sync start` and `metrics-watch` no longer leak a "PlanetScale target-health telemetry enabled" INFO line above the live panel (ADR-0156 polish).** When target-health telemetry is opted in with `--planetscale-org` (ADR-0107), the shared telemetry-provider constructor logs a one-line "telemetry enabled" INFO on success. For the two panel commands that build the provider *before* rendering their panel, that line fired before the panel's TTY slog gate installed, so on the pretty path it printed above the panel — visible for `metrics-watch` (which always builds the provider) and latent for `sync start` (only with `--planetscale-org`). The panel gate is now computed up front and passed to the constructor, which suppresses that one INFO line on the panel path; the panel's own header carries the same org / database context. This is the ADR-0156 analogue of the v0.99.238 `backup` "starting…" fix.

## Compatibility

- No format or behavior change anywhere off the interactive panel path. Non-panel callers (fleet `sync run`, `restore`, `diagnose`) and every non-TTY / `--log-format=json` / `--no-progress` invocation still emit the "telemetry enabled" INFO exactly as before. The telemetry provider itself is constructed identically in all cases — only the one log line is gated.

## Who needs this

Anyone who runs `sync start` or `metrics-watch` interactively with `--planetscale-org` telemetry enabled — a clean panel with no stray log line above it. Everyone else, and anyone scripting or shipping logs, sees no change.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
