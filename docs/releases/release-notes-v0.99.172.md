# sluice v0.99.172

**Two PlanetScale-at-scale improvements found via live 49 GB testing: `verify`'s row count no longer dies on the max-statement-execution-time wall (it runs under OLAP now), and a new opt-in `--upfront-indexes` mode lets a large migrate build secondary indexes during the copy so the post-copy `ADD INDEX` can't hit that same wall.**

## Added

**`--upfront-indexes` migrate mode.** By default sluice defers secondary-index creation to a post-copy phase — a sort-based bulk `ADD INDEX` that is far cheaper than maintaining indexes per row during the load. But on a large PlanetScale-MySQL target that deferred `ALTER … ADD INDEX` can run past PlanetScale's max-statement-execution-time limit and **fail with errno 3024** (~900 s; a 49 GB table's index build failed at ~901 s in testing), leaving the data copied but the declared secondary indexes uncreated — recoverable only via `--resume`, which just re-attempts the same failing ALTER.

`--upfront-indexes` builds the secondary indexes **before** the bulk copy, so the INSERTs maintain them and no post-copy ALTER is ever issued — the wall is never reached. It reuses the existing index phase (engine-neutral, MySQL and Postgres), keeps foreign keys deferred to the constraints phase (indexes-only reorder → no copy-ordering change), and is opt-in with the default staying deferred.

It is a **reliability escape hatch, not a speed knob.** A local-MySQL benchmark (3.2 M rows, 4 secondary indexes) measured **deferred at 29 s vs upfront at 333 s** — deferred is ~11× faster, because a post-copy sort-based build beats per-row B-tree maintenance handily. So keep the default (deferred) for speed; reach for `--upfront-indexes` on a PlanetScale target large enough that the deferred `ADD INDEX` would otherwise fail outright — there the real choice is "fails vs completes."

## Changed

**`verify`'s exact row count on PlanetScale/Vitess now runs under OLAP workload mode (ADR-0147).** A full `COUNT(*)` on a large wide/clustered table runs long enough to hit PlanetScale's max-statement-execution-time limit (**errno 3024**, ~900 s; a 49 GB PK-only table's OLTP `count(*)` failed at ~661 s in testing). sluice only survived this for single-integer-PK tables (via a chunked-PK count); composite/string/UUID/no-PK tables fell back to a plain `COUNT(*)` that failed at scale.

On vtgate flavors (PlanetScale and self-hosted Vitess) the count now runs as `SET workload='olap'; SELECT COUNT(*)` on a dedicated connection (never session-wide — the v0.99.15 lesson). OLAP streams, so it is not bound by the OLTP statement-time limit; the optimizer still auto-narrows to a small secondary index when one exists; and it works for every PK shape — closing the gap where non-int-PK tables simply couldn't be counted at scale. In testing, OLAP and the chunked-PK approach counted the same 49 GB table in an identical 1264 s, so chunking is not faster — the win is generality. The existing chunked/single-shot path is retained as a WARN-logged fallback, and vanilla MySQL is unchanged.

## Compatibility

`--upfront-indexes` is opt-in and defaults off, so existing migrates are unchanged. The OLAP-count change affects only PlanetScale/Vitess targets and is a no-op on vanilla MySQL and Postgres; `count(*)` is exact in any workload mode, so `verify` results are unchanged where the old path already succeeded — the difference is that large non-int-PK tables now succeed instead of failing with errno 3024. No flag removed, no default flipped.

## Who needs this

Operators migrating large tables into **PlanetScale-MySQL** — big enough that a post-copy `ADD INDEX` or a full `COUNT(*)` crosses PlanetScale's ~900 s statement-time limit. `--upfront-indexes` gets the indexes built for such migrates; the OLAP-count change lets `sluice verify` count those tables (including composite/string/UUID/no-PK ones) instead of erroring. Everyone on smaller tables or non-PlanetScale targets is unaffected and should keep the fast deferred default.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.172 · **Container:** ghcr.io/sluicesync/sluice:0.99.172
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
