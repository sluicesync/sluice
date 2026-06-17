# sluice v0.99.64

**New: `sluice migrate` now parallelizes a large single table across N connections even when its primary key is a UUID/string/binary/decimal/temporal value or a composite key — not just a single integer PK (ADR-0096).** Drop-in: no flag or config change, and the strategy is derived automatically from the table's PK type. This closes the cross-region single-connection tail (e.g. a UUID-keyed table copied into PlanetScale, where `LOAD DATA` is blocked and one pinned connection is round-trip-bound) — those tables now fan out the same way integer-keyed tables have since v0.5.0.

## Features

- **Within-table parallel bulk copy for non-integer and composite PKs (ADR-0096).** Since v0.5.0 (ADR-0019) `sluice migrate` has split a large single table into N PK-range chunks copied concurrently across N connections — but a table was eligible only when it had a single integer primary key; anything else (a UUID/string surrogate key, a `(tenant_id, id)` composite key) fell back to a single-connection copy. On a cross-region PlanetScale target that single connection is round-trip-bound (PlanetScale blocks `LOAD DATA LOCAL INFILE`, so the copy uses one pinned-connection batched-`INSERT` stream), so a UUID-keyed table single-streamed while the integer-keyed tables next to it ran N-way. This release lifts the limit: a single non-integer *orderable* PK (string/varchar/char, uuid, inet/cidr/macaddr, decimal/numeric, binary/bytea, temporal) or a composite PK whose columns are all orderable is now chunk-eligible via a new **sampled-keyset** boundary strategy — a single windowed `ROW_NUMBER()` scan over the PK index that splits the table by *actual row count*, so each chunk gets ~total/N rows regardless of how clustered or sparse the keyspace is (skew-free, unlike the integer path's MIN/MAX-divide on a non-uniform key). The strategy is chosen automatically from the table's PK type at decision time — there is no new flag, no config bool, and therefore no zero-value-default trap; the existing `--bulk-parallelism` / `--bulk-parallel-min-rows` knobs now simply engage on a strictly wider set of tables, drawing from the same connection budget (ADR-0076) the integer path already respects.
- **Exactly-once chunk coverage, by construction.** A row must land in exactly one chunk. Both bounds of each chunk are pushed into SQL in the PK column's own collation — the per-chunk read emits `WHERE (pk) > ($lower) AND (pk) <= ($upper) ORDER BY pk`, so the read order and the boundary partition always agree (no Go-side comparison sits in the coverage path). The half-open `(lower, upper]` convention over the engine's own `ORDER BY` total order partitions the keyspace with no gap and no overlap: a boundary value is the inclusive upper of one chunk and the exclusive lower of the next, and the open ends (first chunk has no lower bound, last has no upper) capture everything below the minimum and above the sampled maximum, including rows inserted past the sampled max during the copy. This composes cleanly with the idempotent cold-copy writer (Bug 125 — disjoint chunk ranges never key-collide, and a re-run re-copies a chunk idempotently) and with resume (ADR-0072 — boundaries are computed once on the first attempt, persisted as the already-`[]any` chunk-progress tuples, and read verbatim on resume, never recomputed). The boundary comparator is pinned across every orderable PK family × {single, composite}, with the partition invariants (coverage + disjointness) asserted directly against the database `ORDER BY` on real PostgreSQL and MySQL containers for UUID, string-under-non-C-collation, numeric, composite, and the no-PK single-reader fallback.

## Compatibility

- **No breaking changes; drop-in upgrade.** There is no new flag or config key. Strategy selection is derived from each table's PK type, so every code path (CLI, tests, future callers) classifies identically — the change only widens which tables the existing `--bulk-parallelism` machinery engages on.
- **Scope: the `sluice migrate` within-table parallel-copy path.** This speeds up migrating a large single table keyed by a UUID/string/binary/decimal/temporal value or a composite key (the common case when copying into PlanetScale, where the single-connection tail was most visible). Tables with no usable PK — no primary key, or a PK whose column is a non-orderable type (`JSON`/array/geometry) — still take the safe single-reader path; sluice never invents a chunking that could miss or double-copy rows. A table whose PK is a shard-injected discriminator (`--inject-shard-column`) also stays on the single-reader path (the injected column is not readable from the source and is constant across a per-shard run, so it is not a partitioning key) — same behavior as before this release.
- **No effect on the `sluice sync` / VStream snapshot path.** This change is to the `migrate` within-table copy only; the VStream cold-copy path is a separate enhancement (auto-shard-by-table landed in v0.99.63) and is untouched here.

## Who needs this — action required

- **No action required, and no re-verification of prior migrations.** This is a performance optimization to *how fast* an eligible table copies, not a fix to a path that was producing wrong or missing data — completed migrations on prior versions are unaffected and need no re-check. The slower single-connection copy that previously ran for non-integer/composite-keyed tables was correct; it was just slow.
- **Anyone migrating a large table keyed by a UUID/string/composite primary key — especially into PlanetScale or any cross-region target:** upgrade to v0.99.64 and re-run the migration to get the N-way fan-out. There is nothing to configure; the speed-up engages automatically once the table crosses the existing `--bulk-parallel-min-rows` threshold.

---

## Install

```
brew install sluicesync/tap/sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.99.64
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.64
```

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
