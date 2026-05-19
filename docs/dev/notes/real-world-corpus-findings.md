# Real-world schema corpus — findings

Running log of what the real-world corpus (Idea 3,
`prep-new-test-surfaces.md`) surfaces when run through sluice's schema
reader + cross-engine `Migrator{DryRun:true}` plan. Harness:
`internal/pipeline/migrate_realworld_corpus_integration_test.go`
(build-tagged `integration`; SKIPs when the gitignored corpus isn't
fetched, so it's green on a fresh checkout / in CI). Provenance:
`internal/pipeline/testdata/real-world-corpus/MANIFEST.md`.

## Iteration 1 (2026-05-19) — Chinook (matched MySQL/PG) + GitLab PG

**Verdict: loop proven; ZERO sluice defects.** One corpus-prep fix;
one important *correct-behaviour* characterization.

| Corpus / direction | Outcome | Note |
|---|---|---|
| Chinook **MySQL→PG** DryRun | ✅ PASS | Real (non-synthetic) 11-table schema read + cross-engine plan clean. |
| Chinook **PG→MySQL** DryRun | ✅ PASS *(after corpus fix)* | First run failed `apply ddl: syntax error at or near "\"` — **corpus-prep, not sluice**: lerocha's `Chinook_PostgreSql.sql` is a `psql` script (`\connect` meta-command + `DROP/CREATE DATABASE`). |
| **GitLab** `db/structure.sql` PG→MySQL | ✅ PASS (characterize) | 2.8 MB / 1444-table real PG loaded into vanilla PG; sluice PG→MySQL DryRun **correctly loud-refused**: `unsupported data_type "tsvector"` on `catalog_resources.search_vector` (a genuine `tsvector` column, structure.sql:6104/7827). |

### Key takeaways

- **sluice's reader + refusal are correct on a real production PG
  schema at scale.** GitLab's 1444 tables read fine until a genuine
  unsupported type (`tsvector`, PG full-text search — no faithful
  MySQL equivalent); sluice **loud-refused with a precise message**,
  not a crash or silent drop. This is the loud-failure tenet working
  exactly as designed — *expected, correct, not a bug*. `tsvector`
  is a known unsupported cross-engine type; no action (already the
  documented loud-refuse posture).
- **The corpus surfaced real ingestion friction the fuzz harness
  never would**: vendor sample dumps are often `psql`-client scripts
  (backslash meta-commands, `CREATE DATABASE`), not raw-protocol
  loadable. That is itself valuable signal for operator docs.

### fetch.sh fixes made this iteration

1. **Multi-line INSERT strip.** Chinook uses multi-row
   `INSERT INTO t (...) VALUES` + many `(...),` continuation lines
   until a terminating `;`. Initial strip dropped only the opener →
   ~no shrink. Fixed: skip the whole statement to the `;`-at-EOL.
2. **psql meta + DB-management strip.** Drop lines whose first
   non-space char is `\` (`\connect`/`\encoding`/…) and single-line
   `DROP/CREATE DATABASE` (tests use their own container DB).
3. **Gotcha recorded:** an apostrophe in a comment *inside the
   single-quoted `awk '…'` program* (`Chinook's`) silently closed the
   awk string → `syntax error near unexpected token ')'`. Keep the
   strip awk apostrophe-free.

## Iteration 2 (pending — task #13)

- **MediaWiki abstract schema** — generated MySQL+PG+SQLite from one
  expert schema → a *guaranteed-equivalent* cross-engine oracle
  (stronger than independently-authored Chinook/sakila).
- **pgloader regression/test schemas** — adversarial MySQL→PG prior
  art (the exact job).
- **WordPress** core — canonical operator-brought MySQL shape.
- **Deeper matched-pair oracle:** compare sluice's Chinook MySQL→PG
  *emitted* schema against the upstream Chinook PG schema for
  congruence (iteration 1 only checked both read+plan clean, not that
  the translation equals the hand-authored PG side).
- Then Vitess/PlanetScale sample schemas (Track-1b-adjacent).
