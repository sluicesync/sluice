# sluice v0.99.235

**The live progress view now covers the last of the one-shot commands — `cutover`, `matview refresh`, and `trigger setup` / `teardown` / `prune` — completing the ADR-0155 rollout across every one-shot command.**

## Added

- **TTY-aware pretty output for `cutover`, `matview refresh`, and `trigger setup` / `teardown` / `prune` (ADR-0155 phase 2, continued).** Following `migrate` (v0.99.233) and `verify` / `restore` / `backup` (v0.99.234), these commands now render a phase checklist and a command-appropriate summary panel when run at an interactive terminal: `cutover` shows its Connect → Read → Prime progression and a primed / noop / skipped / refused rollup with the source→target engine pair; `matview refresh` shows refreshed / skipped; `trigger setup` shows statements applied, DDL-detection mode, and PG version; `trigger teardown` shows statements and keep-data; `trigger prune` shows deleted / remaining / vacuumed. It's ASCII-safe and renders correctly on every terminal. As always, the pretty view is additive on a TTY only — piped output, CI, `--log-format=json`, `--format json`, `--dry-run`, and `--no-progress` emit the exact structured records and exit codes these commands have always emitted, byte-for-byte, so automation and log ingestion are unchanged (verified byte-identical against a real Postgres 16). Destructive `teardown` confirmation prompts and `--dry-run` DDL previews stay outside the live view, and any refusal prints after the view tears down — never mid-render.

  This completes the ADR-0155 rollout across the one-shot commands. The remaining presentation surfaces are the report-shaped commands (`preview`, schema diff, `slot list`) and the continuous commands (`sync start`, the broker, `backup stream`, `metrics-watch`) under ADR-0156.

## Compatibility

- No format change; no behavior change to any command. The pretty view is additive on top of a TTY, and non-TTY / `--log-format=json` / `--format json` / `--dry-run` / `--no-progress` output is identical to prior releases, including exit codes (the destructive-teardown prompt and every coded refusal are unchanged).

## Who needs this

Anyone who runs `cutover`, `matview refresh`, or the `trigger` lifecycle commands interactively gets the clearer view; anyone who scripts them sees no change (or adds `--no-progress` to force plain logs at a terminal).

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
