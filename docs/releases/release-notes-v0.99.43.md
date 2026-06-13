# sluice v0.99.43

**Two MySQL/Vitess improvements: (1) `backup full` now reads tables in parallel on MySQL — ~2.6× faster dump, ~2.3× faster restore (ADR-0088); (2) a throttled or idle Vitess VStream is now surfaced loudly instead of stalling silently (observability).** Drop-in from v0.99.42 — no format, flag, or breaking changes; `migrate`, the Postgres paths, and cross-engine behavior are untouched.

## Added

- **MySQL `backup full`: coordinated parallel backup snapshot (ADR-0088) — dump 2.63× / restore 2.26× faster.** sluice's MySQL backup table sweep was *serial* (a single `START TRANSACTION WITH CONSISTENT SNAPSHOT` connection) because MySQL — unlike Postgres — has no shareable *exported* snapshot to lazily import onto N parallel readers. It now opens N reader transactions whose consistent snapshots **coincide** under a brief `FLUSH TABLES WITH READ LOCK` window (the same mechanism mydumper uses), so `--table-parallelism > 1` (default auto = 4) overlaps cross-table reads on a vanilla MySQL source. Measured on a 16.25 GB / 33 M-row corpus: **dump 184 s → 70 s (2.63×), restore 404 s → 179 s (2.26×)**, artifact byte-unchanged. Cross-table consistency and the anchored `EndPosition` are preserved — the N readers' snapshots are identical by construction, so a backup taken under concurrent writes is point-in-time consistent across every table. Falls back — **loudly** — to the serial single-reader path when the source role lacks the `RELOAD` privilege (most managed tiers), so it never silently produces an inconsistent parallel read. PlanetScale/Vitess sources are unaffected (they keep the VStream-COPY backup path). See `docs/comparison-backup.md` for the full fair-fight vs `mysqldump`/`mydumper`.

- **Vitess/MySQL CDC: a throttled-or-idle VStream is surfaced, not silently stalled (observability; roadmap item 19(a)).** When a Vitess source's tablet throttler engages mid-stream, vtgate withholds row/change events but keeps sending ~5 s heartbeats *and strips the tablet's in-band `VEvent.throttled` flag* — so the stream correctly stays alive (the progress watchdog re-arms on heartbeats) but the stall was **silent**: unbounded replication lag with zero diagnostic. Three observability-only changes (the resilient streaming behavior is unchanged — sluice still waits and catches up when the throttle clears):
  1. the at-stream-open liveness error now names the **source throttler** as a candidate cause alongside the primary-only topology wedge (so operators aren't sent down the wrong path);
  2. a new **soft idle-WARN** fires once per quiet spell — *"alive (heartbeats flowing) but no change events for N s — the source may be throttled or genuinely idle; check `SHOW VITESS_THROTTLED_APPS` on the primary"* — cleared by the next real change event, default 30 s, tunable per-DSN via `vstream_idle_warn_timeout` (`0` disables the WARN only; the hard liveness guards are unaffected);
  3. `docs/vitess-vstream-troubleshooting.md` documents the mechanism, detection, and the real-world triggers (the prime one being a **co-tenant Vitess migration** — `OnlineDDL`/`MoveTables`/`Reshard` — on the same keyspace, which moves the shared lag metric and throttles unrelated streams; *not* your own write rate).

## Compatibility

- **Drop-in from v0.99.42 — no format or breaking changes.** On a vanilla MySQL `backup full`, `--table-parallelism > 1` (default auto = 4) now engages the ADR-0088 coordinated FTWRL path instead of sweeping serially; the artifact is byte-equivalent and the recorded position is unchanged. A source role without `RELOAD` transparently keeps the prior serial behavior (loud INFO).
- **New opt-in DSN param:** `vstream_idle_warn_timeout` (default `30s`, `0` disables the idle WARN only). No existing DSN needs changing.
- **No on-disk/chunk-format change**; existing backups restore unchanged. `migrate`, Postgres sources/targets, and cross-engine translation are untouched.

## Who needs this

- **Anyone backing up MySQL with sluice** — the parallel sweep is automatic at the default `--table-parallelism` on a vanilla MySQL source with `RELOAD`; just upgrade. (No `RELOAD`? You keep today's serial behavior, loudly logged.)
- **Anyone running `sluice sync` against a Vitess/PlanetScale source** — you now get a loud, actionable WARN when the source throttler pauses your stream, instead of a silent lag climb. If you watch for it, the `docs/vitess-vstream-troubleshooting.md` guide tells you the likely cause (usually a co-tenant migration) and that no data is lost — the stream converges once the throttle clears.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.43`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.43`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
