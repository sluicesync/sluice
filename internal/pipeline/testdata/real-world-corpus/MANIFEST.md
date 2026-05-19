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
`fetch.sh` are the only tracked files here.

> **LICENSE SAFETY — DO NOT VENDOR THESE FILES.** Some corpora are
> copyleft (MediaWiki = GPL-2.0-or-later; datacharmer test_db =
> CC-BY-SA-3.0). Copyleft obligations attach to *distribution* of the
> work/derivatives. sluice **never distributes** them: they are
> gitignored, fetched on demand, used only as test *inputs* read by
> the schema reader (not linked into / shipped with sluice's
> Apache-2.0 code). That non-distribution posture is the entire reason
> the copyleft never triggers. **Never `git add` a corpus `.sql`** —
> the `.gitignore` exception intentionally tracks only `MANIFEST.md`
> and `fetch.sh`. If a corpus is ever to be *redistributed* (bundled
> in a release / committed), that is a different analysis and only
> the permissively-licensed corpora (MIT / Chinook-permissive) would
> be eligible. Not legal advice; standard OSS hygiene. `fetch.sh` writes a
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

## Iteration 2 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **MediaWiki** `tables-generated.sql` (MySQL) | MySQL | `https://raw.githubusercontent.com/wikimedia/mediawiki/master/sql/mysql/tables-generated.sql` | **GPL-2.0-or-later** (`wikimedia/mediawiki` COPYING) — see LICENSE SAFETY note above; gitignored / not vendored | 64 tables, schema-only by design. |
| **MediaWiki** `tables-generated.sql` (PostgreSQL) | PostgreSQL | `https://raw.githubusercontent.com/wikimedia/mediawiki/master/sql/postgres/tables-generated.sql` | same (GPL-2.0-or-later) | 64 tables. **Both dialects are generated from one abstract schema (`sql/tables.json`) → a *guaranteed-equivalent* matched cross-engine ORACLE** (stronger than independently-authored Chinook/sakila). |
| **datacharmer test_db** `employees_partitioned.sql` | MySQL | `https://raw.githubusercontent.com/datacharmer/test_db/master/employees_partitioned.sql` | **CC-BY-SA-3.0** (`datacharmer/test_db`) — see LICENSE SAFETY note; gitignored / not vendored | 6 tables, real MySQL with `PARTITION BY` (a feature Chinook lacks). Sources data from `.dump` files; `fetch.sh` drops the `source …;` directives → schema-only `employees_mysql_partitioned.ddl.sql`. |

## Iteration 3 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **Joomla** `installation/sql/mysql/base.sql` | MySQL | `https://raw.githubusercontent.com/joomla/joomla-cms/5.4-dev/installation/sql/mysql/base.sql` | **GPL-2.0** (`joomla/joomla-cms`) — see LICENSE SAFETY note; gitignored / not vendored | ~28 tables, real CMS core. Seed INSERTs stripped by `fetch.sh`. |
| **Joomla** `installation/sql/postgresql/base.sql` | PostgreSQL | `https://raw.githubusercontent.com/joomla/joomla-cms/5.4-dev/installation/sql/postgresql/base.sql` | same (GPL-2.0) | ~28 tables. Same logical schema as the MySQL file → a **real-CMS matched cross-engine pair** (independently authored per dialect, like Chinook; not generated-from-one-source like MediaWiki). |
| **WordPress** core schema | MySQL | `https://raw.githubusercontent.com/WordPress/wordpress-develop/trunk/src/wp-admin/includes/schema.php` | **GPL-2.0-or-later** (`WordPress/wordpress-develop`) — see LICENSE SAFETY note; gitignored / not vendored | ~19 tables. Lives in PHP (`wp_get_db_schema()`); `fetch.sh` `extract_wp_schema` pulls the `CREATE TABLE` blocks and substitutes the PHP placeholders (`$wpdb->NAME`→`wp_NAME`, `$max_index_length`→191, `) $charset_collate;`→`);`) to plain MySQL DDL. The canonical operator-brought MySQL shape. |

### Evaluated, deliberately NOT fetched (do not "fix" — wrong shape)

- **pgloader** (`dimitri/pgloader`) — its test corpus is `.load`
  *orchestration* files that connect to live MySQL DSNs, not
  standalone schema `.sql`. It doesn't fit the fetch-on-demand
  schema-corpus pattern. Its genuine value to sluice is its **cast
  ruleset** (MySQL→PG type-mapping prior art) — a *translator-catalog
  reference* (cf. ADR-0016 / `docs/type-mapping.md`), not a corpus.
- **Drupal core** (`drupal/drupal`) — schema is PHP `hook_schema()`
  arrays in `core/modules/*/*.install`, no raw `.sql` anywhere in
  core. Extraction would require running Drupal or parsing PHP arrays
  across dozens of modules — far beyond "grab a schema example", and
  a murky community dump wouldn't meet this corpus's provenance bar.
  Deferred unless a clean raw artifact surfaces.

Iteration 4+ (Vitess/PlanetScale samples; the deeper matched-pair
*congruence* oracle) will append here — see the prep doc + findings.

## Usage

```sh
cd internal/pipeline/testdata/real-world-corpus
sh fetch.sh        # populates the gitignored *.sql + FETCHED.txt
```

Then the build-tagged corpus harness reads each via sluice's schema
reader and records refuse/translate/break outcomes (added in
iteration 1, task #12).
