# sluice v0.111.1

**The first production field report, and the four queue items that were waiting behind it.**

Someone moved a 2.64 GB / ~16M row MariaDB database to PlanetScale with sluice and wrote up what happened. `migrate` completed in 45 minutes with no errors and the data verified clean. `sync` failed every time they tried it. Three of the fixes below come straight from that report; two of the three are defects that only a real production shape surfaces, and one of them had a written justification that turned out to be false in the field.

## Fixed

**A dropped vtgate connection ended a cold copy instead of being retried, when vtgate reported it in its own wording.** sluice already treats a lost vtgate→tablet connection as a transient and retries the batch on a fresh connection — but only when vttablet frames it (`code = Unavailable desc = connection reset by peer`). The same loss reported by vtgate itself (`Error 1105: internal: vtgate connection error (read: connection reset by peer)`) was not recognised, so one condition had two verdicts depending on which layer happened to report it — and the terminal one is the framing a cold copy actually meets.

The report: `sync` failing on all five attempts, always on the widest table, always 90–130 seconds in, at a *varying* chunk index. Confirmed by feeding both wordings to the classifier rather than by reading it.

It is anchored on vtgate's own wording rather than on the generic "connection reset by peer" tail, deliberately: an `Error 1105` message can echo the statement that failed, and a false transient is worse than a missed one here — it also arms a duplicate-tolerance path on the retry. One classifier serves both the cold-copy retry and the CDC applier, so both are covered.

**What is not established, and is worth saying:** *why* their connection dropped. A storage-grow reparent fits the timing, but their logs were not available and the classifier gap is provable without them. One theory is ruled out by their own numbers — the ~4,000-row statements are exactly the placeholder ceiling for that table's column count, so the statement was well under the 1 MiB byte target and this was not an oversized write. Whether the retry rides their specific drop out is unverified until they re-run.

**A PlanetScale branch that misreported its buffer pool was throttled to a quarter of its parallelism, silently.** sluice infers a copy-parallelism ceiling from `@@innodb_buffer_pool_size`, which scales by plan tier and is the only credential-free tier signal available. On a large branch that returned 32 MiB — far below the smallest tier sluice has ever measured — it inferred the smallest instance and capped table parallelism at 2 instead of 8.

A reading below the smallest known tier is now treated as *no reading* rather than as a tiny instance, which falls back to the connection-derived budget instead of throttling, and warns. When the cap does apply it now says so, naming the observed pool size and the derived ceiling. That provenance was documented as operator-visible for as long as the feature has existed and was never actually printed — which is why "why is this only using 2 workers?" had no answer in the output, and why the reporter had to read the source to find it.

**`backup compact --smart-compaction` did nothing at all on a MySQL-family source, and blamed your schema for it.** The per-row collapse matches a change event to its table by schema-qualified name. A MySQL change event carries the database name, because that is what the binlog gives it; the recorded schema carries no qualifier at all for a single-database source. The comparison could never succeed, every table fell through to a verbatim pass-through, and the report listed tables with a plain `INT PRIMARY KEY` under *"tables without a primary key"*.

Nothing was lost — the chain was correct, just uncompacted — which together with the misleading report is why it survived three releases. Matching now understands an unqualified recorded schema, while an exact qualifier still wins, so Postgres and multi-database MySQL chains are unaffected. If two unqualified tables ever share a name it refuses to pick one rather than guessing: a wrong primary key means a wrong collapse, which would be silent data loss on a backup path and is strictly worse than the inertness. The two report buckets are now separate — "declares no primary key" is expected and yours to act on; "the event named a table the backup does not carry" is a defect, and it now warns.

**Restoring a Postgres backup re-created its enum types under invented names.** sluice records a PG enum column's real type name so a same-engine restore rebuilds `post_status` as `post_status` — but the name was not carried on the manifest wire, so it arrived empty and the writer synthesized one, renaming it to `posts_status_enum`. That breaks casts, any table sharing the enum, and application code that names the type.

The name now rides the manifest. **No existing backup chain is repartitioned by this** — it is deliberately excluded from the chain's schema fingerprint, exactly as the MAC width was in v0.111.0, because Postgres sets it on *every* enum column and folding it into the hash would have moved close to every Postgres chain in existence. The residual is stated where the exclusion lives: a schema diff will not flag an enum type *rename* whose values are unchanged.

**A backup chunk could buffer gigabytes in memory before it rolled.** The chunk writer rolled on row count and nothing else, while the chunk accumulated in an in-memory buffer — so the peak was the row count times the average serialized row size, with the row size unbounded and never consulted. Measured: at a fixed 500 rows, widening rows from 64 B to 64 KiB grew the buffered chunk 644×, which extrapolates to roughly 6.1 GiB for a single chunk at the shipped 100,000-row default. A wide `mediumtext` or `json` column is exactly the shape that reaches it.

There is now a 64 MiB ceiling beside the row cap — the same figure the Postgres chunked `COPY` path already used — and the writer rolls on whichever it hits first. Narrow rows are unaffected: they still roll on the row count, at exactly the boundaries they always did. The ceiling counts bytes *before* compression, deliberately, because chunk boundaries feed the content-addressed same-path upload skip and where a chunk ends must not depend on how well it happened to compress.

**`migrate --dry-run` presented its per-table row counts as if they were exact.** They come from the engine's statistics — MySQL's `information_schema.TABLE_ROWS`, Postgres's `pg_class.reltuples` — which lag real cardinality badly on a table that has not been `ANALYZE`d recently, and can read zero for a freshly loaded one. The report saw a plan say 1,026,026 for a table that copied 1,701,520 rows and reasonably read the gap as sluice reporting a mismatch. The plan now carries `row_count_estimated`, in both the JSON payload and the log line, matching the label the copy-progress line has always had.

Nothing about the counting changed, and **`sluice verify` was never affected** — it counts both sides exactly, which is why it agreed with the target while the plan did not.

## Compatibility

No flags were added, renamed or removed. No error codes changed. **Backup chain fingerprints are unchanged** — the enum type name rides the manifest but is excluded from the hash, so chains written by earlier versions restore exactly as before and no chain crosses an epoch boundary.

One additive JSON field: `row_count_estimated` on each table of the `migrate --dry-run --format json` plan. Existing consumers are unaffected. `backup compact` gains a `tables_unmatched` attribute on its summary log line, beside the `tables_without_pk` it has always had.

Chunk boundaries in *new* backups can differ from earlier releases on tables with wide rows, because the byte ceiling now rolls them earlier. Existing chunks are untouched, restore is unaffected, and resume works at table granularity so nothing keys on a boundary.

## Who needs this

- **Anyone syncing into PlanetScale or another vtgate-fronted MySQL target.** A dropped vtgate connection during the cold copy was fatal where the equivalent failure in vttablet's wording was survivable.
- **Anyone running a large PlanetScale branch** — check whether your copy parallelism is what you expect. If sluice capped it from the buffer-pool signal it now says so in the log; if that signal is masked, it no longer throttles you on it.
- **Anyone using `--smart-compaction` on a MySQL, MariaDB, PlanetScale or Vitess source.** It has been doing nothing. Your chains are correct but larger than they should be; compacting them again will now actually collapse.
- **Anyone restoring a Postgres backup containing enum columns.** Restored types were being renamed. Re-check any enum type name your application refers to directly.
- **Anyone backing up tables with wide `TEXT`/`JSON` columns**, who has seen sluice's memory use climb during a backup.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.111.1_Linux_x86_64.tar.gz --repo sluicesync/sluice
```
