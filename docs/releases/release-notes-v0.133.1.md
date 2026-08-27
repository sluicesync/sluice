# sluice v0.133.1

The Bug 257 fast-follow, caught by the v0.133.0 regression cycle within hours of publish. Drop-in from v0.133.0.

## Fixed

**`trigger setup` re-runs no longer poison their own stream (Bug 257, new in v0.133.0, loud — no silent loss).** Re-running `trigger setup` over an existing *streamed* install emitted the ADR-0185 meta-migration `ALTER` (and, under the opt-in, the two `ENABLE ALWAYS` ALTERs), which the install's own pre-existing DDL event trigger recorded as `op='X'` rows — so the next warm resume refused "observed source-side DDL", misattributing sluice's own statements to operator DDL and steering to a needless full re-copy. Fresh installs were unaffected. The fix suppresses capture of setup's own session only: the setup plan is bracketed by a session GUC on one pinned connection, the capture-DDL function returns early while it is set, and the function replace is now the plan's *first* DDL statement so the very first re-setup over a pre-fix install is already suppressed. Operator DDL from every other session — including DDL against sluice's own tables — records unchanged (pinned in the over-reach direction). The root test gap is closed as a gate: the re-setup repair cells now *consume* the change log and warm-resume rather than just opening the reader, across plain, opt-in, converge-back, and old-binary shapes — mutation-verified against the exact filed refusal. The gate cell is `TestSetup_ReRunOverStreamedInstall_WritesNoSelfDDL` (real PG, all four re-setup shapes), and the design note lives in `adr-0185-pgtrigger-capture-replicated-writes.md`. Already-poisoned v0.133.0 installs keep their X rows (prevention, not retroactive healing): recover once via `sync start --restart-from-scratch`.

## Compatibility

Drop-in from v0.133.0 — no schema, format, or flag change. If you re-ran `trigger setup` on v0.133.0 over a live stream and hit the DDL refusal: upgrade, run `trigger setup` once more (now self-suppressing), and restart the affected stream from scratch once.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.133.1
```

Container images: `ghcr.io/sluicesync/sluice:0.133.1` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
