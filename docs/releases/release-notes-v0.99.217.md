# sluice v0.99.217

**Performance fix from a fresh full-codebase audit (PERF-P1) — no behavior or value change. The VStream-COPY exact-FLOAT repair now corrects target rows in batched `UPDATE`-against-`VALUES`-join statements instead of one `UPDATE` per row, cutting the network round-trips ~500×: a repair that took hours-to-days on a WAN PlanetScale target now takes minutes, so you no longer have to disable exact-FLOAT (`--no-float-exact-reread`) and re-inherit the display rounding. The repaired values are byte-identical to before — verified against real Postgres 16 and MySQL 8 across the full type × PK-shape matrix.**

## Changed

- **Batched exact-FLOAT repair (audit PERF-P1).** When a PlanetScale/Vitess cold-start COPY lands single-precision FLOAT columns display-rounded to 6 significant digits, sluice re-reads them exactly and corrects the target rows before CDC begins. That correction previously issued one `UPDATE … SET floats WHERE pk` per row — one round-trip each — so at a ~100 ms WAN RTT a 5M-row FLOAT table needed roughly 5–6 days of repair, which in practice pushed operators to turn the fix off. It now batches 500 rows into a single statement:
  - **Postgres:** `UPDATE <t> AS tgt SET c = v.c FROM (VALUES …) AS v(pk…, c…) WHERE tgt.pk = v.pk`
  - **MySQL:** `UPDATE <t> AS tgt JOIN (SELECT ? AS pk, ? AS c … UNION ALL …) AS v ON tgt.pk = v.pk SET tgt.c = v.c`

  This collapses the round-trips from O(rows) to O(rows/500) — a benchmark shows 50,000 rows going from 50,000 statements to 100 — and replaces the per-batch `BEGIN`/`COMMIT` with one atomic autocommit statement per batch. A `VALUES`-join (not `INSERT … ON CONFLICT`/`ON DUPLICATE KEY`) is used deliberately so a row deleted between copy and re-read stays a clean join-miss no-op, exactly as before, rather than a failed partial insert.

- **Shared repair skeleton (audit ARCH-F3).** The two ~95%-identical MySQL and Postgres FLOAT-repair writers are unified into one engine-neutral `internal/engines/internal/floatrepair` skeleton with thin per-engine statement builders, so the batching (and any future change to the repair loop) lands in both engines at once rather than drifting between siblings.

## Compatibility

- **No behavior or value change.** The repaired FLOAT values are byte-for-byte identical to v0.99.216 — the batched path reuses the exact same value-shaping, and correctness was verified against real Postgres 16 and MySQL 8 across REAL / DOUBLE / NUMERIC targets × single- and composite-PK × NULL / −0.0 / overflow. This affects only the *speed* of the exact-FLOAT repair on a VStream (PlanetScale/Vitess) cold-start; no migrate, sync, backup, or non-VStream path is touched.

## Who needs this — action required

- **If you run continuous sync from a PlanetScale/Vitess source with single-precision FLOAT columns and a large table:** upgrade — the exact-FLOAT repair (on by default) is now fast enough to leave enabled on a WAN target, where before you may have had to pass `--no-float-exact-reread` (and accept the display rounding) to avoid a multi-day pre-CDC repair.
- **Everyone else: no action.** Nothing changes for UTC/non-VStream sources, DOUBLE columns, or any table without single-precision FLOAT.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.217 · **Container:** ghcr.io/sluicesync/sluice:0.99.217
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
