# sluice v0.99.38

**Crash-resumed anchored backups now keep a gap-free incremental chain (ADR-0085). Previously, resuming an interrupted `backup full` silently re-anchored the chain at a NEW snapshot position while keeping already-completed tables from the OLD one — writes to those kept tables in between landed in neither the full nor any incremental, with exit 0 everywhere. Found by code-reading during the ADR-0083 work and closed before any release carried a worse variant.**

## Fixed

- **Silent-loss class closed: resume adopts the FIRST attempt's anchor (Bug-class: chain gap on crash-resume).** The in-progress manifest now records the snapshot anchor from the moment it is first committed, and a resumed run records *that* position as the chain handoff — kept tables replay exactly, and tables re-streamed under the newer snapshot are healed by the first incremental's idempotent replay (the ADR-0010 applier convergence argument, now formally load-bearing for backup chains). Loud refusals guard the cases where that argument doesn't hold: a truly keyless table needing a re-stream, or schema DDL between the attempts (both name `--force-overwrite` as the escape hatch). Manifests from older binaries (no recorded anchor) fall back to re-streaming everything, with a WARN.
- **`--chain-slot` crash recovery reversed: the resume now ADOPTS the surviving chain slot instead of telling you to drop it.** The leaked slot after a hard crash is not debris — it is the only thing retaining the WAL that makes a sound resume possible. The already-exists refusal previously advised `sluice slot drop` + retry, which destroyed exactly that retention and funneled into the silent gap; the resume now verifies the slot can serve the original anchor (the chain preflight) and proceeds, and the refusal message names re-running the same command as the recovery.
- **Incrementals and streams refuse to chain off an in-progress (crashed, unfinished) full** — previously this produced a confusing "from now" fallback; with anchors now recorded early it would have been silently wrong instead. The refusal says to finish or resume the full first.
- Test-infrastructure: the ADR-0046 crash-injection matrix no longer flakes on the post-crash walsender race (deterministic slot handoff between the crash and recovery runs).

## Compatibility

- A resumed anchored full's `EndPosition` now means "the FIRST attempt's anchor" (the chain-sound choice). Per-table data in a resumed full remains mixed-consistency across attempts exactly as before — but now the first incremental genuinely converges it.
- Resumes of backups taken by pre-v0.99.38 binaries re-stream all tables once (the old anchor is unknowable); subsequent crashes/resumes get the new table-granular behavior.

## Who needs this

- Anyone using backup chains whose fulls can be interrupted — especially `--chain-slot` users, whose documented crash recovery previously led into the gap.

## Install

Binaries for Linux/macOS/Windows (x86_64 + arm64) attached; container image `ghcr.io/sluicesync/sluice:0.99.38`. Verify with `checksums.txt`.
