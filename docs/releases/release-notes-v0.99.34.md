# sluice v0.99.34

**Performance + reliability — migration-state checkpoints are now O(1) in table count (ADR-0082): measured on real Postgres at 10k tables, 31.7 ms → 377 µs per checkpoint (84×), and a 10k-table migration's total state writes drop from ~17 GB to ~1.3 MB. Alongside it, a reliability fix for PG streams: warm resumes and crash recoveries no longer fail on a replication slot the dead prior owner hasn't released yet — `START_REPLICATION` now retries with bounded backoff, while a genuinely concurrent second writer still gets the loud two-writers refusal. Drop-in from v0.99.33; in-flight migrations upgrade their on-target state transparently and crash-safely on first resume — just don't downgrade the binary past an upgraded state (see Compatibility).**

## Fixed

- **Warm resumes and crash recoveries of PG syncs no longer fail on a not-yet-released replication slot.** Restarting a PG stream moments after the prior owner stopped or crashed could hit `replication slot is active for PID N` (SQLSTATE 55006) and fail loudly for a condition that self-heals as soon as Postgres reaps the dead walsender — the race that bites hardest in orchestrated environments (k8s restarting a sync pod). `START_REPLICATION` now retries with bounded backoff (8 attempts, 0.5–8 s, each wait logged at INFO so a recovering stream is visibly waiting): a dead owner's walsender is reaped well inside the budget; a *genuinely* concurrent second writer holds the slot past it and the original loud refusal propagates unchanged. The two-writers guard is preserved and pinned (`TestCDCReader_SlotActiveRetry_LiveOwnerStillRefuses` asserts the retry branch fires *and* the refusal survives the budget against a live-held slot); the stop/restart integration test now races resume directly against slot release, exercising the retry end-to-end. This was always a loud failure — affected restarts exited non-zero with the slot-active error; no data was ever lost or skipped. Affects all prior versions with PG slot-based CDC.

## Performance

- **Migration-state checkpoints are O(1) in table count (ADR-0082).** Through v0.99.33, every per-table breadcrumb, per-5000-row resume cursor, and per-chunk checkpoint deep-cloned the entire progress map, JSON-encoded all of it, and upserted the full blob into the one `sluice_migrate_state` row — O(N²) in table count, hammering a single hot row's MVCC/TOAST path (~856 KB × ≥20k writes ≈ 17 GB at 10k-table scale), with an O(N) clone inside every state-lock hold. Now a small header row carries phase/format/timestamps and each table gets its own `sluice_migrate_table_progress` row; hot-path checkpoints are one single-row upsert with encoding moved outside the lock. Measured at 10k tables: in-process 11.69 ms → 945 ns per checkpoint (~12,400×); real-PG end-to-end 31.7 ms → 377 µs (84×); payload 855,535 B → 67 B. Legacy state upgrades on first resume inside one transaction (crash any time before commit simply re-runs the upgrade), and the old blob is replaced with a sentinel so a downgraded binary fails loudly instead of silently reading "no progress" and re-copying every table. Pinned by a cross-version test on both engines against a byte-captured v0.99.x blob covering every `TableProgress` field family × shape.

## Internal

- **Applier control-plane extraction arc complete (ADR-0081, tiers a–d).** The duplicated batch loop, control-table CRUD, and lease-row IR conversion now live once in `internal/appliershared`; both engines' files are thin SQL shims, and ADR-0081 records what stays engine-specific and why. Behavior-identical; no user action.
- **Random-op sync-convergence property test** (`pgregory.net/rapid`) joins the suite: random transaction interleavings against live PG/MySQL syncs must converge to exact content equality — smoke budget in PR CI, env-knobbed deep runs.

## Compatibility

- **No breaking changes.** Drop-in from v0.99.33 for all engines and flavors — no flag, default, or invocation changes.
- **Migration-state format is upgraded one-way (state format 2).** Existing in-flight migrations upgrade transparently and crash-safely on the first resume under v0.99.34 — no action needed in the upgrade direction. But once a migration's state has been upgraded, **do not downgrade to an older binary mid-migration**: the old binary fails loudly at state decode (by design — the alternative was silently seeing "no progress" and re-copying every table). Finish the migration or clear its state before downgrading. Fresh migrations started on older binaries are unaffected.
- **The slot-retry change affects only the PG slot-based CDC source path.** MySQL CDC, the postgres-trigger path, and all migrate/restore paths are untouched. The refuse-loudly contract for a genuinely concurrent second writer on the same slot is preserved — only the transient dead-owner case now self-heals.

## Who needs this — action required

- **Anyone running PG syncs in orchestrated environments (k8s, supervisors, anything that restarts processes)** — restarts that previously died with `replication slot is active for PID N` now wait out the walsender reap and proceed. Action: upgrade; remove any external retry/sleep wrappers you added to paper over this. No data was ever affected — the old behavior was a loud exit, never silent loss.
- **Anyone migrating many-table schemas** — the checkpoint win scales with table count (84× per checkpoint at 10k tables; smaller schemas see proportionally less). Action: just upgrade; in-flight migrations resume and upgrade their state automatically.
- **Anyone who might roll back** — finish or clear an upgraded migration before downgrading the binary (see Compatibility). Everything else requires no action.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.34`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.34`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
