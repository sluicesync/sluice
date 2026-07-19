# sluice v0.99.280

> ⚠️ **Correction (2026-07-19):** a fresh independent audit found additional silent-loss in `sync --where` string/float filters (`_as_cs`/`char(n)`/PAD-SPACE collations and float ordering) that this remediation wave did not catch. **Fixed in v0.99.282.** If you use `sync --where`, upgrade to **≥ v0.99.282**.

**Audit-tail hardening of `--where` filtering.** The second wave of the 2026-07-18 audit remediation (batch A) — correctness and quality on top of the v0.99.279 silent-loss fix. Fully additive: nothing that doesn't use `--where` changes.

## Added

### Deterministic Postgres named collations work again for `sync --where`

v0.99.279 closed a real bug — Postgres *non-deterministic* ICU collations (`ks-level`) were byte-compared as if deterministic, which could delete an in-scope row — but it did so conservatively, refusing **all** named PG collations because it couldn't tell deterministic from non-deterministic by name alone. That over-refused legitimate deterministic collations (`"C"`, `"POSIX"`, libc `en_US`).

sluice now reads `pg_collation.collisdeterministic` into the IR and uses it: a **deterministic** named collation's `=` is byte equality, so it's admitted as byte-exact; a **non-deterministic** one still refuses loudly (its collation-aware `=` can't be faithfully reproduced client-side). The database default collation is unchanged. This is ground-truthed by a new **real-Postgres collation family matrix** — it boots an actual Postgres and asserts sluice's classification equals PG's own `WHERE col = 'lit'`, including that the non-deterministic ICU case refuses *because* PG's `=` under it would diverge from a byte compare.

## Fixed / Changed

- **Throughput:** an `IN`-list filter on a case/accent-insensitive column now short-circuits on the first match instead of running the (allocating) collation compare against every member for every streamed CDC row.
- **Loud guard for a filtered cold-start:** a source engine that supports table-scoped snapshots but not the filtered-snapshot open now refuses a filtered `sync --where` at start rather than silently opening an unfiltered stream. No current engine hits this — it fences a future flavor from a silent scope-escape.
- **Docs + hardening:** documented the move-OUT→`DELETE`-under-`--allow-degraded-fks` orphan hazard (intrinsic to filtering a parent table in *continuous* sync); documented the client-side collation set's MySQL-8.0.30 version pin and the `--where-strict-collation` escape hatch for an exotic self-hosted Vitess; recorded the filtered-sync stream-reduction technique (and its per-engine gaps) in the perf-parity matrix; added compile-time asserts so the CDC readers' full-before-image capability can't silently drift; and added a tag-push CI gate that runs the Vitess-cluster filtered move-OUT test before a release tag publishes (so a VStream filtered-sync change can't ship un-validated).

## Compatibility

**No behavior change without `--where`.** For `sync --where`: deterministic named Postgres collations now work (were refused in v0.99.279); non-deterministic ones still refuse loudly. Everything else is internal hardening, documentation, and CI.

## Who needs this

Anyone using `sync --where` on a Postgres source with an explicit deterministic collation (`"C"`, `en_US`, …) on the filtered column — refused in v0.99.279, working here. Everyone on v0.99.279 can upgrade at leisure for the throughput and hardening.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.280
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.280`
