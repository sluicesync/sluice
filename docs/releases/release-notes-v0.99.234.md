# sluice v0.99.234

**The live progress view extends to `verify`, `restore`, and the `backup` commands, and `backup verify` now returns the coded refusal on a corrupt chunk (matching `restore`).**

## Added

- **TTY-aware pretty output for `verify`, `restore`, `backup full`, `backup incremental`, and `backup verify` (ADR-0155 phase 2).** Following `migrate` in v0.99.233, these commands now render a phase checklist and a command-appropriate summary panel when run at an interactive terminal: `verify` reports tables checked / clean / mismatched / skipped; `backup` reports tables, rows, chunks, encrypted?, signed?, EndPosition; `restore` reports tables and rows. It's ASCII-safe (renders correctly on every terminal). As always, the pretty view is additive on a TTY only — piped output, CI, `--log-format=json`, and `--no-progress` emit the exact structured records these commands have always emitted, byte-for-byte, so automation and log ingestion are unchanged. (Per-table live progress bars for these commands are a follow-up; this phase renders the phase progression and the summary panel.)

## Fixed

- **`backup verify` returns a coded refusal (exit 3) on a corrupt chunk, matching `restore` (Bug 185).** v0.99.232 coded `restore`'s chunk-corruption refusal (`SLUICE-E-BACKUP-CHUNK-CORRUPT`) but `backup verify` still exited `1` uncoded on the aggregate failure, contradicting the release note that operators could script `backup verify` against the code. It now exits `3` with the coded refusal — `SLUICE-E-BACKUP-CHUNK-CORRUPT` for a SHA-256 mismatch, `SLUICE-E-BACKUP-CHUNK-AUTH-FAILED` for a decrypt/splice failure. The refusal was always loud and data-safe; this makes the exit code match `restore` and the docs.

## Compatibility

- No backup-format change. **One exit-code change:** a script that keyed on `backup verify` exiting `1` on a chunk failure will now see `3` (the Refusal class, matching `restore`) — the refusal itself is unchanged, and `verify` still never reports a corrupt chunk as valid.

## Who needs this

Anyone who runs `verify`/`backup`/`restore` interactively gets the clearer view; anyone who scripts `backup verify` against its exit code should note the `1`→`3` change (and can now match the coded error). Non-TTY / `--log-format=json` output is unchanged.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
