# sluice v0.99.49

**New: pipelined Postgres CDC apply — ~70× higher apply throughput on latency-bound (cross-region/cross-cloud) links.** If you run `sluice sync` with the Postgres target in a different region or cloud from sluice, apply throughput was capped at ~1/RTT regardless of batch size or DB tier; this lifts that ceiling. Drop-in from v0.99.48 — no flag, config, or format change, default-on for the batch apply path (the `--apply-batch-size=auto` default), value encoding byte-identical to the prior path.

## Improved

- **Postgres CDC apply is now pipelined (ADR-0092).** The batch apply path used to send a batch of N changes as **N serial `Exec` round trips inside one transaction**, plus a position upsert and a commit (N+2 round trips). Batching ([ADR-0017]/[ADR-0089]) amortized the commit fsync but did nothing for the N data round trips — they stayed serial, so steady-state apply throughput was bounded by `1 / per_row_exec_latency`, which on any non-co-located link is dominated by network RTT. The data statements **and** the position upsert are now queued onto a single `pgx.Batch` and sent in one pipelined flush; **round trips per batch drop from N+2 to O(1)** (begin + flush + commit), independent of N. Throughput becomes bounded by the *server's* execution rate rather than `N × RTT`. The win scales with RTT.
  - **Measured on a live PlanetScale soak:** apply was pinned at **~90 rows/s on a ~7 ms cross-cloud link** (sluice on a Vultr VM → PlanetScale `us-east-2`), and a **PS-10 → PS-80 database upsize moved it 0%** — proving the bottleneck was the wire, not the database. Pipelining lifts this ~70× on that link (`1/0.011s ≈ 90 rows/s` → bounded by server execution rate instead).
  - **Co-located / low-latency deployments** were already fast (batching amortized the commit and sub-ms execs reached thousands of rows/s); they see a smaller gain — the collapse of the serial-exec span into a fixed handful of flushes — never a regression.
  - **What pins it:** the pipelined pool runs in pgx `QueryExecModeDescribeExec`, so `SendBatch` describes each distinct statement fresh against the live catalog and binds + executes with the **real described parameter OID in BINARY format** — byte-identical encoding to the serial `CacheStatement` path. The existing `buildInsertSQL` / `buildUpdateSQL` / `buildDeleteSQL` builders and the `prepareValue` codec path are reused byte-for-byte; pipelining changes *when* statements are sent, never *how a value is encoded*. The value-fidelity matrix (full type-family × shape, src==dst ground-truthed) and a differential pin (pipelined vs serial → byte-identical target state) are run through the batch path, and `-race` integration ran before the tag.

## Compatibility

- **No breaking changes. Drop-in from v0.99.48** — no flag, config, or on-disk/format change.
- **Default-on** for the Postgres batch apply path (`--apply-batch-size` > 1, i.e. the `auto` default). `--apply-batch-size=1` keeps the serial per-change exec path verbatim (a batch of one has nothing to pipeline).
- **Value encoding is byte-identical to the prior path** (pgx `DescribeExec` → real described OID, binary format, same per-OID codecs). Durability and atomicity are unchanged: the position upsert rides the same transaction as the data (now in the same flush), `synchronous_commit = on` is still pinned, and a crash before the single commit rolls back both ([ADR-0007] holds). Error classification ([ADR-0038] retriable/fatal) is unchanged; the only difference is *when* an exec error surfaces (commit time vs mid-accumulation) — the batch outcome (full rollback + reclassify + retry) is identical.
- **Postgres-only.** MySQL apply is unchanged (still serial); the [ADR-0081] seam was generalized so MySQL can adopt pipelining later, but nothing about MySQL behavior changes here. `migrate` (snapshot/bulk-copy) and cross-engine translation are untouched.
- **Loud fall-back, never silent:** if the underlying pgx-conn escape is ever unavailable (a non-pgx driver, a wrapped conn), the path falls back to serial `*sql.Tx` exec with a one-time WARN and makes no throughput claim.

## Known limitation (pre-existing, unchanged by this release)

- **Geometry over CDC is refused loudly on the applier path.** The CDC applier registers no binary geometry codec, so PostGIS `geometry` is refused (`parse error - invalid geometry`) — **identically on the serial and pipelined paths**; this is a pre-existing applier gap, not introduced by ADR-0092. The snapshot/migration COPY path handles geometry fine (it writes EWKB in COPY-binary). Registering a geometry binary codec on the applier conns is tracked as a separate follow-up. pgvector and hstore over CDC round-trip identically on both paths.

## Who needs this — action required

- **No action required — this is a performance improvement, not a correctness fix.** No data was lost or mis-applied on the old path; it was simply slow on high-latency links. Nothing to re-verify or re-run.
- **You benefit most if:** you run `sluice sync` (continuous CDC) into a **Postgres target that is not co-located** with sluice (different region, cloud, or provider — the common topology) and previously saw apply throughput capped well below your DB tier's capacity. Upgrade and the ceiling lifts automatically with the `auto` default; no flag to set.
- **Co-located / low-latency Postgres targets** were already fast and need do nothing — the gain there is small.
- **MySQL-target users** are unaffected (apply path unchanged).

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.49`  ·  **Container:** `ghcr.io/sluicesync/sluice:v0.99.49`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
