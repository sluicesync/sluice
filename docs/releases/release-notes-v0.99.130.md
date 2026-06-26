# sluice v0.99.130

**CRITICAL fix — PostgreSQL CDC: close a silent data-loss window on warm-resume. The slot-ack-after-apply wiring (Bug 15 / ADR-0020) was a silent no-op on the streamer path, so the replication slot acked the DECODED position instead of the APPLIED position — a crash/restart while the reader ran ahead of the applier could silently drop the decoded-but-unapplied changes. Operators running PostgreSQL CDC with batched or concurrent apply should upgrade.**

## Fixed

**CRITICAL — PostgreSQL CDC silent data loss on warm-resume (slot-ack-after-apply was never engaged on the streamer path).** sluice's PostgreSQL CDC is supposed to advance the replication slot's `confirmed_flush_lsn` only as far as changes have been durably APPLIED to the target (Bug 15 / ADR-0020's slot-ack-after-apply) — so that a crash and warm-resume can always re-stream anything not yet applied. That wiring was silently broken on the live streamer paths: the reader's `AttachLSNTracker` method took the engine's concrete `*lsnTracker` type, which did not satisfy the pipeline's engine-neutral `AttachLSNTracker(any)` interface, so the streamer's type-assertion failed silently and the applied-LSN tracker was never attached on the cold-start, warm-resume, or multi-database paths. With the tracker absent, the keepalive fell back to acking the STREAMED (decoded) LSN, so `confirmed_flush_lsn` advanced past changes the target had not yet committed. PostgreSQL is then free to recycle that WAL — and a warm-resume after an ungraceful stop (which requests sluice's durable applied position) gets fast-forwarded past the recycled window, dropping those changes with no error.

The window size equals how far the reader runs ahead of the applier. The legacy lockstep per-change apply kept decoded ≈ applied (a near-zero window), which is why this stayed latent — but the default batched (ADR-0052 AIMD) and concurrent key-hash (ADR-0104) apply paths run the reader well ahead of the applier, so the window was real on a normal modern configuration. It evaded the test suite because the confirmed-flush invariant test attached the tracker manually, validating the tracker's logic while bypassing the broken production wiring.

The fix changes the reader's `AttachLSNTracker` to take `any` (matching the pipeline interface) with an internal type-assert, finally engaging ADR-0020's slot-ack-after-apply on the streamer path. Two regression guards prevent recurrence: a compile-time contract pin (`var _ interface{ AttachLSNTracker(any) } = (*CDCReader)(nil)` — a drift back to a concrete-typed signature fails the build) and a runtime wiring test that asserts the tracker actually engages (the gap the invariant test missed). The change is strictly safer: the slot now retains WAL up to the applied position rather than the decoded position. It was found while building the upcoming delayed-replica `--apply-delay` mode (ADR-0121), which runs the reader far ahead of the applier and turned the latent window into reproducible loss.

## Compatibility

No configuration changes, no API changes, no flags added. The only behavioral change is the intended one: the PostgreSQL replication slot now advances to the applied position instead of the decoded position — strictly safer (it retains slightly more source WAL, up to what has actually been applied, which is exactly Bug 15 / ADR-0020's intent). Affects **PostgreSQL CDC only**; MySQL and VStream (Vitess/PlanetScale) sources use a different position-ack path and are unaffected. Fully drop-in over v0.99.129.

## Who needs this — action required

**Upgrade if you run PostgreSQL CDC (`sluice sync`) with batched or concurrent apply** (`--apply-batch-size` > 1 or the default `--apply-concurrency` auto path) — i.e. essentially every modern PostgreSQL CDC configuration. The loss only manifests on an ungraceful stop / crash + warm-resume while a backlog of decoded-but-unapplied changes existed and PostgreSQL had recycled the gap WAL; a graceful `sync stop --wait` drain was never affected (apply caught up before exit). There is nothing to re-run or reconfigure — upgrading engages the correct slot-ack behavior automatically. Operators on MySQL/VStream sources are unaffected and need not act.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.130 · **Container:** ghcr.io/sluicesync/sluice:0.99.130
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
