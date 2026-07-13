# sluice v0.99.238

**A small ADR-0155 polish: `backup full` / `backup incremental` no longer print a stray "starting…" INFO line above their live summary panel at an interactive terminal.**

## Fixed

- **`backup full` / `backup incremental` pretty view no longer leaks a pre-run INFO line above the panel.** The `backup: starting full backup` / `backup: starting incremental` line (and the keyset / redaction config lines) fired *before* `runWithProgress` installed the TTY slog gate, so on the pretty path they printed above the summary box. The pretty gate is now computed up front and those pre-run INFO lines are suppressed when rendering the live view — the summary panel is the interactive output there. **Non-TTY / `--log-format=json` output is unchanged**: the lines still emit exactly as before whenever the live view isn't rendered, so logs and automation see no difference.

## Who needs this

Anyone who runs `backup full` or `backup incremental` interactively — a slightly cleaner panel. No behavior change to backups themselves, and no change to scripted / piped output.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
