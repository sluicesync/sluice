# Real-world schema corpus — provenance manifest

Publicly-available real-world schemas for exercising sluice's schema
reader + cross-engine translation against operator-shaped reality
(deep FK graphs, partitioning, real default expressions, actual
extension usage). Complements — does not replace — the synthetic fuzz
harness and the sqllogictest DDL corpus. Plan + shortlist:
`docs/dev/notes/prep-new-test-surfaces.md` § "Idea 3".

**Discipline:** schema-only DDL (no data → no data-licensing concern).
The fetched `*.sql` are **gitignored** (large, upstream-owned —
fetch-on-demand via `fetch.sh`, not vendored). This `MANIFEST.md` and
`fetch.sh` are the only tracked files here. `fetch.sh` writes a
`FETCHED.txt` (gitignored) recording the date + resolved upstream
refs of the actual pull, for reproducibility.

`fetch.sh` needs `curl` on PATH. On Windows + Rancher Desktop the
bundled curl is `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\curl.exe`
(often not on PATH — see `docs/dev/development.md`); plain Windows 10+
`curl.exe` also works.

## Iteration 1 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **GitLab** `db/structure.sql` | PostgreSQL | `https://gitlab.com/gitlab-org/gitlab/-/raw/master/db/structure.sql` | MIT (`gitlab-org/gitlab` LICENSE) | ~2.8 MB, schema-only by design (no data). Biggest open real PG schema — partitioning, hundreds of tables. Stresses the PG reader at scale + the PG→MySQL loud-refusal surface. Pulled from `master` HEAD at fetch time (commit recorded in `FETCHED.txt`). |
| **Chinook** (MySQL) | MySQL | `https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql` | Chinook license (`lerocha/chinook-database` LICENSE.md — permissive) | Upstream mixes DDL + data INSERTs; `fetch.sh` strips data → `chinook_mysql.ddl.sql` (schema-only). |
| **Chinook** (PostgreSQL) | PostgreSQL | `https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql` | same as above | Same logical schema as the MySQL file → a **matched cross-engine oracle** (sakila/pagila-class, different shape: decimal/numeric-heavier). Data stripped → `chinook_postgres.ddl.sql`. |

Iteration 2+ (MediaWiki abstract schema, pgloader test corpus,
WordPress, Vitess/PlanetScale samples) will append rows here as
collected — see the prep doc.

## Usage

```sh
cd internal/pipeline/testdata/real-world-corpus
sh fetch.sh        # populates the gitignored *.sql + FETCHED.txt
```

Then the build-tagged corpus harness reads each via sluice's schema
reader and records refuse/translate/break outcomes (added in
iteration 1, task #12).
