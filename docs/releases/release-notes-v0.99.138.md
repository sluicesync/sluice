# sluice v0.99.138

**Fixes a silent sync-death on a target outage in the default concurrent CDC apply path: a target-side write failure was masked as a clean context-cancel, so a sync whose target went unreachable parked itself permanently with no error and no restart instead of recovering. Availability/observability fix — no data loss. Operators running CDC sync with the default concurrent apply should upgrade. Fully drop-in over v0.99.137.**

## Fixed

**Concurrent CDC apply — silent sync-death on a target outage (`internal/laneapply`).** On the default concurrent apply path (`--apply-concurrency > 1`, the `auto:N` default), when a lane's write to the target failed — e.g. the target database became unreachable mid-CDC — the key-hash lane-apply orchestrator recorded the real error and then cancelled its own internal context to unwind the other lanes. `Orchestrator.Run`'s final error resolution then let that **self-inflicted `context.Canceled` mask the recorded error** (the `getErr() != nil && loopErr == nil` guard gave the internal cancel precedence). The masked cancel propagated up as a clean `nil` return, so the streamer and supervisor read an *uncommitted target outage* as a *graceful drain*: the sync was parked in `stopped` — **no restart, no `last_error`** — so it never recovered when the target came back, and the dashboard / `sync status` showed nothing wrong.

The fix makes the **recorded run error authoritative** over the orchestrator's own internal cancel: a real target-side failure now surfaces as a non-nil (retriable) error, so the sync recovers (the streamer/supervisor retry + restart) and surfaces `last_error` for the operator. A genuine operator stop — which records no lane error — still returns `context.Canceled` and drains cleanly, so a clean Ctrl-C is unchanged and never spuriously restarts.

**Scope and severity.** It is **engine-neutral** — the orchestrator is shared by the MySQL (ADR-0104) and PostgreSQL (ADR-0105) concurrent-apply paths — and **latent since that path shipped**; a *source* outage was always handled correctly (it surfaces through a separate side-channel), and the serial path (`--apply-concurrency 1`) was unaffected. **Exactly-once was never at risk:** the durable resume position only ever advances on fully-durable work, so a warm-resume after the outage re-streams and idempotently re-applies the remainder — this is a loss of availability + observability, **not data**. Pinned by Run-level orchestrator unit tests (a target failure and an exhausted in-lane retry each surface the recorded error rather than a masked clean cancel; a genuine outer cancel still returns `context.Canceled`) that fail without the fix, under the CI `-race` integration gate. Surfaced by the sync fleet-dashboard demo (a stopped target parked the sync silently instead of backing off).

## Compatibility

Strictly safer and fully drop-in over v0.99.137: no flag, config, or data-path change. The only behavior change is the intended one — an uncommitted target outage on the concurrent apply path now recovers (with a surfaced error) instead of silently parking the sync. A clean operator stop is byte-identical to before.

## Who needs this — action required

Operators running a continuous `sync` (or a `sync run` fleet) with the **default concurrent apply** (`--apply-concurrency` unset / `auto:N` / any value > 1) against a target that could ever blip should upgrade: before this fix, a transient target outage could permanently park the sync with no surfaced error. Anyone on the serial apply path (`--apply-concurrency 1`) or who only runs `migrate` (cold-copy, no CDC apply lanes) is unaffected. No re-verification of already-migrated data is needed — exactly-once held throughout.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.138 · **Container:** ghcr.io/sluicesync/sluice:0.99.138
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
