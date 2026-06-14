# ADR-0090: VStream liveness/progress watchdog timeouts are retriable

## Status

Accepted. Amends the loud-failure handling introduced by
[ADR-0073](adr-0073-vstream-liveness-watchdog.md) (the two-phase VStream
liveness/progress watchdog): the watchdog's hard timeouts now surface as
[ir.RetriableError] (ADR-0038) rather than terminal errors. The windows
themselves are unchanged (Phase-1 30s, Phase-2 45s, COPY 10m;
`vstream_liveness_timeout` / `vstream_progress_timeout` / etc. still override).

## Context

The two-phase watchdog (ADR-0073) fails LOUD when a VStream produces no
serving-proof event within Phase-1 (stream open) or goes totally silent
within Phase-2 (mid-stream). Its errors were **terminal by construction** —
the reasoning being that the primary-only-wedge (no serving replica tablet)
never heals within a retry.

The first real PlanetScale long-haul soak (2026-06-13; `sluice-testing`
Bug 141, with a self-hosted Vitess-24 reproduction) showed the cost of that
choice in the wild. A heavy source write-burst tripped the source-tablet
**throttler**; under throttle vtgate withholds change events (and, near its
own 10-minute `fullyThrottledTimeout`, even heartbeats), so the watchdog's
Phase-1/Phase-2 windows fire — **misdiagnosing a transient throttle as a
failover hang**. Because the error was terminal, the `sync` process exited;
under a supervisor (systemd, the soak's Vultr host) it restarted, warm-resumed
to the **same** throttled position, re-stalled, and exited again — a tight,
non-converging **crash-loop** (`RestartCount=5 in 70s`), the headline finding.
Crucially, the throttle is transient: it clears when the replica catches up,
and the stream then converges. The terminal exit prevented that recovery; a
backed-off in-process reconnect would have ridden it out (raising the windows
manually to survive the throttle was the soak's confirmed workaround).

The pipeline already has the right machinery: ADR-0038's bounded
exponential-backoff retry reconnects a fresh `StreamChanges` from the last
persisted position on any [ir.RetriableError]. vtgate's own
"fully throttled or otherwise hung" error is already retriable and reconnects
in-process — only sluice's own watchdog error escaped that path.

## Decision

Wrap both `vstreamLivenessTimeoutError` (Phase-1) and
`vstreamProgressTimeoutError` (Phase-2) as `ir.RetriableError`. The streamer's
ADR-0038 retry then handles a watchdog timeout **in-process** with exponential
backoff — reconnecting from the last position — which is the correct recovery
for **both** real causes of a Phase-2 silence (a post-failover hung Recv *and*
a sustained throttle / large-transaction stall) and for a throttle-at-open
Phase-1 timeout. The Phase-2 error message is broadened to name the throttle /
large-transaction cause alongside failover/reparent (they are indistinguishable
from the stream alone — vtgate erases the in-band `throttled` flag, item 19(a)).

The **primary-only-wedge** (a genuinely non-healing Phase-1 case) is still
caught: the retry budget is bounded (`--apply-retry-attempts`, default 8), so
it now retries with backoff and then fails LOUD on budget exhaustion — the
same actionable error (both candidate causes + the `vstream_tablet_type=primary`
remedy), just not in a tight loop. Trading a fast-fail for a bounded
backed-off-then-fail on that one case is worth eliminating the crash-loop on
the common transient (throttle/failover) cases.

## Consequences

- A transient source throttle or tablet failover is **ridden out in-process**
  and converges on recovery, instead of a supervisor crash-loop that never
  converges (Bug 141 closed).
- The primary-only-wedge fails loud after the bounded retry budget rather than
  immediately — slightly slower diagnosis, same actionable message; mitigated
  by `vstream_tablet_type=primary`.
- Applies uniformly to all three VStream pumps (standalone CDC tail, the
  snapshot post-COPY tail, and the COPY pump) since they share the builders —
  a watchdog stall during cold-start COPY now also retries rather than exits.
- No window/default changes; operators who tuned the windows are unaffected.

## Alternatives considered

- **Raise the default windows** (e.g. above vtgate's 10-minute throttle
  tolerance): blunt — it slows genuine-failover detection for everyone and a
  long throttle still eventually trips it. Rejected in favour of retriable,
  which keeps detection prompt while recovering in-process.
- **Throttle-aware watchdog** (an out-of-band `SHOW VITESS_THROTTLED_APPS`
  check to relabel the stall): only a partial discriminator (it reflects an
  explicit per-app deny, not the common lag-metric throttle — item 19(a)), and
  more complex. The retriable approach recovers correctly without needing to
  *distinguish* the causes.
