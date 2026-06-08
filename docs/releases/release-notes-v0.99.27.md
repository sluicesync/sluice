# sluice v0.99.27

**`--type-override COL=interval` carries a MySQL `TIME` *duration* to a Postgres `INTERVAL`.** A MySQL `TIME` is a duration (`-838:59:59…838:59:59`) — wider than Postgres `time`'s `00:00–24:00` time-of-day range — so a column used to store a real duration couldn't be migrated faithfully before. Now it can, on both `migrate` and continuous `sync`. **Drop-in from v0.99.26.**

## Added

- **MySQL `TIME` duration → Postgres `INTERVAL` (Vector C).** A MySQL `TIME` column spans `-838:59:59…838:59:59` and is semantically a *duration*; a value beyond Postgres `time`'s `00:00–24:00` (a >24h span, or a negative offset) has no home in the default `TIME → time` mapping. `--type-override TABLE.COL=interval` maps the column to PG `INTERVAL`, which holds the full range — the value is carried as its textual form (`838:59:59`, `-12:30:00`, `12:34:56.789012`) and PG's interval input parser accepts it. Verified end-to-end against real Postgres on both paths:
  - **`migrate`** — max-positive, negative, fractional-second, zero, and NULL durations all round-trip exactly onto a PG `interval` column.
  - **`sync` (CDC)** — cold-start plus continuous-CDC `INSERT`/`UPDATE` of an interval-overridden column round-trip exactly (the applier resolves the `interval` target column and binds the textual value).

  `interval` is now a first-class Postgres type in sluice's IR (a new `ir.Interval`, distinct from `ir.Time` the time-of-day): a PG→PG migration/sync round-trips a native `interval` column too. A **non-Postgres target** — MySQL has no `INTERVAL` — is **refused loudly** (at schema emit and at the cross-engine pre-flight) rather than silently degraded back to `TIME`, which would re-lose the very range the override exists to preserve.

## Compatibility

- No breaking changes. Drop-in from v0.99.26. The default MySQL `TIME → time` mapping is unchanged; `interval` is strictly opt-in per column. A native PG `interval` column — previously refused as an unsupported type — now migrates/syncs (PG→PG); a PG `interval` → MySQL is refused loudly (no silent loss; no MySQL equivalent).

## Who needs this

- **Anyone migrating a MySQL `TIME` column that stores a duration** (an elapsed-time / offset value that can exceed 24h or go negative) **to Postgres.** Add `--type-override TABLE.COL=interval` to land it as PG `INTERVAL` instead of hitting the out-of-range `time` failure.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.27`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.27`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
