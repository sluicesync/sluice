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
> be eligible. Not legal advice; standard OSS hygiene. `fetch.sh` fetches
every source **by a pinned upstream commit SHA** (the `PINS` block in
`fetch.sh` is the committed manifest; a bump is a one-line change +
`./fetch.sh --resolve-latest` to get current HEADs) so the corpus is
deterministic; it writes a `FETCHED.txt` (gitignored) recording the
date + the pinned SHAs of the actual pull, for reproducibility.

`fetch.sh` needs `curl` on PATH. On Windows + Rancher Desktop the
bundled curl is `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\curl.exe`
(often not on PATH — see `docs/dev/development.md`); plain Windows 10+
`curl.exe` also works.

## Iteration 1 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **GitLab** `db/structure.sql` | PostgreSQL | `https://gitlab.com/gitlab-org/gitlab/-/raw/master/db/structure.sql` | MIT (`gitlab-org/gitlab` LICENSE) | ~2.8 MB, schema-only by design (no data). Biggest open real PG schema — partitioning, hundreds of tables. Stresses the PG reader at scale + the PG→MySQL loud-refusal surface (incl. the correct partition-aware loud-refusal on GitLab's declaratively-partitioned tables). Fetched by a pinned commit SHA (see the `PINS` block in `fetch.sh`). |
| **Chinook** (MySQL) | MySQL | `https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_MySql.sql` | Chinook license (`lerocha/chinook-database` LICENSE.md — permissive) | Upstream mixes DDL + data INSERTs; `fetch.sh` strips data → `chinook_mysql.ddl.sql` (schema-only). |
| **Chinook** (PostgreSQL) | PostgreSQL | `https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql` | same as above | Same logical schema as the MySQL file → a **matched cross-engine oracle** (sakila/pagila-class, different shape: decimal/numeric-heavier). Data stripped → `chinook_postgres.ddl.sql`. |

## Iteration 2 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **MediaWiki** `tables-generated.sql` (MySQL) | MySQL | `https://raw.githubusercontent.com/wikimedia/mediawiki/master/sql/mysql/tables-generated.sql` | **GPL-2.0-or-later** (`wikimedia/mediawiki` COPYING) — see LICENSE SAFETY note above; gitignored / not vendored | 64 tables, schema-only by design. |
| **MediaWiki** `tables-generated.sql` (PostgreSQL) | PostgreSQL | `https://raw.githubusercontent.com/wikimedia/mediawiki/master/sql/postgres/tables-generated.sql` | same (GPL-2.0-or-later) | 64 tables. Both dialects are generated from one abstract schema (`sql/tables.json`), but this does **NOT** make them column-congruent — the PG adapter renders the abstract binary/blob type as `TEXT` and MW-timestamp as `TIMESTAMPTZ` while MySQL uses `VARBINARY(n)`. The upstream-pair congruence oracle over this member is **retired** (see "Cross-engine congruence: why DumpParity-only …"); DumpParity + the migrate/DryRun legs remain. |
| **datacharmer test_db** `employees_partitioned.sql` | MySQL | `https://raw.githubusercontent.com/datacharmer/test_db/master/employees_partitioned.sql` | **CC-BY-SA-3.0** (`datacharmer/test_db`) — see LICENSE SAFETY note; gitignored / not vendored | 6 tables, real MySQL with `PARTITION BY` (a feature Chinook lacks). Sources data from `.dump` files; `fetch.sh` drops the `source …;` directives → schema-only `employees_mysql_partitioned.ddl.sql`. |

## Iteration 3 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **Joomla** `installation/sql/mysql/base.sql` | MySQL | `https://raw.githubusercontent.com/joomla/joomla-cms/5.4-dev/installation/sql/mysql/base.sql` | **GPL-2.0** (`joomla/joomla-cms`) — see LICENSE SAFETY note; gitignored / not vendored | ~28 tables, real CMS core. Seed INSERTs stripped by `fetch.sh`. |
| **Joomla** `installation/sql/postgresql/base.sql` | PostgreSQL | `https://raw.githubusercontent.com/joomla/joomla-cms/5.4-dev/installation/sql/postgresql/base.sql` | same (GPL-2.0) | ~28 tables. Same logical schema as the MySQL file → a **real-CMS matched cross-engine pair** (independently authored per dialect, like Chinook; not generated-from-one-source like MediaWiki). |
| **WordPress** core schema | MySQL | `https://raw.githubusercontent.com/WordPress/wordpress-develop/trunk/src/wp-admin/includes/schema.php` | **GPL-2.0-or-later** (`WordPress/wordpress-develop`) — see LICENSE SAFETY note; gitignored / not vendored | ~19 tables. Lives in PHP (`wp_get_db_schema()`); `fetch.sh` `extract_wp_schema` pulls the `CREATE TABLE` blocks and substitutes the PHP placeholders (`$wpdb->NAME`→`wp_NAME`, `$max_index_length`→191, `) $charset_collate;`→`);`) to plain MySQL DDL. The canonical operator-brought MySQL shape. |

## Iteration 4 corpora

| Corpus | Engine(s) | Source | License | Notes |
|---|---|---|---|---|
| **Vitess** `examples/local/create_commerce_schema.sql` | MySQL | `https://raw.githubusercontent.com/vitessio/vitess/main/examples/local/create_commerce_schema.sql` | **Apache-2.0** (`vitessio/vitess` LICENSE) — permissive; nonetheless gitignored / fetch-on-demand for corpus consistency (see LICENSE SAFETY note; not vendored) | Vitess's own canonical example keyspace (`commerce`). Characterizes Vitess DDL idioms through sluice's MySQL reader: **no foreign keys** (Vitess discourages cross-shard FKs), small no-partition tables, the reference/sequence-table pattern. Schema-only upstream; run through `strip_data` as a discipline/no-op safety pass. **Runtime Vitess/PlanetScale behaviour (resharding, tx-killer, online DDL) is explicitly OUT OF SCOPE here** — routed to Track-1b per `docs/dev/notes/vitess-local-vs-planetscale-equivalence.md`. This corpus member is the static-DDL-idiom slice only. |

> **Iteration 4 also adds two non-corpus legs** (no new fetched file):
> (1) a **Capabilities-delta** leg — an existing fetched MySQL corpus
> member (WordPress) read+planned with the source engine resolved to
> the **`planetscale`** flavor registration (`engines.Get("planetscale")`,
> `Flavor.String()=="planetscale"`, `internal/engines/mysql/flavor.go`)
> instead of vanilla `mysql`, exercising the PlanetScale `Capabilities`
> declaration (no `LOAD DATA INFILE`, no `PARTITION BY`, no spatial) in
> the read+plan path; (2) a matched-pair **congruence oracle** that
> emitted sluice's translation for real and diffed it against the
> expert-authored other-engine side — **now RETIRED**; see "Cross-engine
> congruence: why DumpParity-only …" below and the file-top doc in
> `migrate_realworld_corpus_congruence_integration_test.go`.

## Cross-engine congruence: why DumpParity-only for Chinook / MediaWiki / Joomla

The iteration-4 **upstream-pair congruence oracle** (emit sluice's
cross-engine translation of one dialect and diff it against the
authored other-dialect file in the same corpus member) has been
**retired**. The three subject DBs — Chinook, MediaWiki, Joomla, the
only members shipping BOTH dialects — are now exercised only by the
migrate/DryRun corpus and the same-engine **DumpParity** corpus. Why:

1. **The upstream pairs diverge by authoring convention, not by any
   sluice error**, so there is no congruent oracle to assert against.
   Chinook uses MySQL PascalCase identifiers (`InvoiceLine`) vs PG
   snake_case (`invoice_line`); MediaWiki's PG adapter renders its
   abstract binary/blob type as `TEXT` and its MW-timestamp as
   `TIMESTAMPTZ` while the MySQL side uses `VARBINARY(n)` (so sluice's
   faithful `VARBINARY → bytea` diverges from the authored PG `TEXT` on
   ~150 columns — "generated from one abstract schema" does **not** mean
   column-congruent); Joomla's PG author uses `idx_`-prefixed / renamed
   indexes plus a PG-only `lower(email)` functional index the MySQL
   author never wrote. sluice cannot and should not reconcile an
   upstream inconsistency.

2. **Golden-output comparison is licensing-blocked.** Capturing sluice's
   own emit once and diffing against it would commit a sluice
   translation *of a GPL-2.0+ schema* (Joomla/MediaWiki/WordPress) into
   this Apache-2.0 repo — a GPL-derivative in an Apache tree — and it
   contradicts the corpus design that `.gitignore`s every fetched schema
   (only `MANIFEST.md` + `fetch.sh` are committed). Nothing derived from
   the fetched schemas may be committed.

3. **Cross-engine translation is already validated authoritatively
   elsewhere:** `migrate_cross_integration_test.go` runs real MySQL↔PG
   round-trips on testcontainers with owned, Apache-clean fixtures, and
   same-engine fidelity on these very corpus schemas is the DumpParity
   corpus. The retired oracle added only false drift-red.

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

Iteration 4 (Vitess sample + PlanetScale-flavor Capabilities-delta +
the deeper matched-pair *congruence* oracle) is recorded above and in
`docs/dev/notes/real-world-corpus-findings.md` § "Iteration 4". Later
iterations append here.

## Usage

```sh
cd internal/pipeline/testdata/real-world-corpus
sh fetch.sh        # populates the gitignored *.sql + FETCHED.txt
```

Then the build-tagged corpus harness reads each via sluice's schema
reader and records refuse/translate/break outcomes (added in
iteration 1, task #12).
