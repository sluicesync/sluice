# sluice v0.99.168

**A SQLite/D1 `.sql`-dump source is now streamed into the staging database instead of read whole into memory — so a large `sqlite3 .dump` no longer needs tens of gigabytes of RAM. Peak memory on the dump path is now a few MiB regardless of dump size.**

## Fixed

**`.sql`-dump materialization is streamed, not buffered.** When a SQLite/D1 source is a SQL text dump (a `wrangler d1 export` or a `sqlite3 .dump`) rather than a binary `.db`, sluice materializes it into a temporary staging database before reading. That step previously read the **entire dump into memory** (`os.ReadFile` + a `string` copy) — fine for a small `wrangler d1 export`, but catastrophic for a large `sqlite3 .dump`: a dump is bigger than the database it came from (a 45 GB database dumps to ~85 GB of SQL), and holding it as a Go string needs roughly twice that again.

It now **streams** the dump:

- Read in bounded 1 MiB blocks; complete top-level statements are split off each block and executed in ~8 MiB multi-statement batches. A statement, string literal, or comment that spans a block boundary is carried into the next block, so the quote/comment/`;` handling stays exactly correct. **Peak memory is a few MiB regardless of dump size.**
- The whole load runs on **one pinned connection**, so a `sqlite3 .dump`'s single wrapping `BEGIN TRANSACTION … COMMIT` commits correctly. (A loader that spread the statements across multiple connections — or, as some tools do, a fresh process per chunk — would roll back the uncommitted prefix and then fail with `no such table`.)

Pinned by a cross-block + transaction test that materializes a `BEGIN…COMMIT`-wrapped dump (with an embedded `;` inside a string, an escaped quote, and line/block comments) **identically at block sizes of 1, 3, 7, 64, and 1 MiB**.

## Compatibility

No behavior change for valid dumps — the staged database is identical, and the binary `.db` and live-D1 (`--source-driver d1`) reader paths are untouched (they were already memory-bounded and remain the recommended route for very large sources). This purely removes the out-of-memory footgun on the `.sql`-dump path. A malformed dump still fails loudly, naming the dump, with no temp-file leak.

## Who needs this

Anyone importing a large SQLite or Cloudflare D1 database from a `.sql` dump (`sqlite3 .dump` / `wrangler d1 export`) — the dump path no longer scales its memory use with the dump's size. If you import from a binary `.db` or a live D1, nothing changes for you.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.168 · **Container:** ghcr.io/sluicesync/sluice:0.99.168
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
