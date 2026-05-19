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

## Iteration 2 (2026-05-19) — MediaWiki oracle + employees + harness hardening

**Verdict: ZERO sluice defects across the whole suite; harness
materially hardened.**

| Corpus / direction | Outcome | Guard |
|---|---|---|
| **MediaWiki MySQL→PG** DryRun | ✅ PASS | 64 tables, non-vacuous |
| **MediaWiki PG→MySQL** DryRun | ✅ PASS | 64 tables, non-vacuous |
| **employees** (partitioned) MySQL→PG | ✅ PASS | 6 tables, non-vacuous |
| GitLab PG (re-run, characterize) | ✅ PASS | 1041 base tables (raw), non-vacuous; sluice correctly loud-refuses `tsvector` — characterized, expected |
| Chinook MySQL→PG / PG→MySQL (re-run) | ✅ PASS | **11 tables each, now PROVABLY non-vacuous** |

### Headline

- **MediaWiki is the strongest signal yet.** Both dialects are
  generated from one abstract schema (`sql/tables.json`), so a clean
  read+cross-engine-plan in *both* directions on the 64-table schema
  is a guaranteed-equivalent-oracle pass — sluice handles a real,
  expert-authored cross-engine schema both ways with zero defects.
- **employees** adds real MySQL `PARTITION BY` coverage (a surface
  Chinook lacks) — read + PG plan clean.

### Harness hardening (the rigor detour — worth it)

- **Non-vacuous guard.** `Migrator.Run` returns `nil` (not an error)
  on a 0-table schema (`migrate.go` "nothing to migrate"), so a
  corpus whose DDL landed in a side DB would pass GREEN without
  sluice ever reading it. Added `corpusAssertTables` (reads via
  sluice's `OpenSchemaReader`/`ReadSchema`, FAILs if `< expected`).
  This **resolved — did not confirm — the vacuous-pass worry**:
  iteration-1 Chinook-MySQL now *provably* reads 11 real tables
  (a real green, not a false one), after the DB-switch strip fix.
- **DB-switch strip fix (`fetch.sh`).** Drop `USE`, `CREATE/DROP
  SCHEMA` (in addition to `DATABASE`), and mysql-client `source …;`
  so every `CREATE TABLE` lands in the connection DB sluice's DSN
  reads. (Chinook-MySQL kept `CREATE SCHEMA/USE` pre-fix → was the
  latent vacuity risk.)
- **Characterization ≠ vacuity.** First full run FAILED GitLab
  because the strict `ReadSchema` guard hit GitLab's *expected*
  `tsvector` loud-refuse — *my harness over-strictness, not sluice*.
  Split it: `corpusRawPGTableCount` (raw `information_schema` count)
  proves GitLab loaded (~1041 tables) without going through
  ReadSchema; the sluice read/translate step then *characterizes*
  the `tsvector` refuse (logged, PASS). "Did the DDL load?" is now
  separate from "can sluice read/translate it?".

### License safety (operator-raised)

GPL-2.0-or-later (MediaWiki) / CC-BY-SA-3.0 (datacharmer employees)
copyleft **never triggers**: corpora are gitignored, fetch-on-demand,
test-input-only — sluice never distributes them. A
`LICENSE SAFETY — DO NOT VENDOR` note is now in
`MANIFEST.md`; the `.gitignore` exception tracks only `MANIFEST.md` +
`fetch.sh`. Redistribution (bundling/committing) would be a different
analysis (only MIT/Chinook-permissive eligible) — not done.

## Iteration 3 (pending — task #13 continuation)

- **pgloader regression/test schemas** — adversarial MySQL→PG prior
  art (the exact job).
- **WordPress** core — canonical operator-brought MySQL shape
  (needs a PHP-`schema.php`-extraction step; note the friction).
- **Deeper matched-pair *congruence* oracle** — compare sluice's
  emitted MySQL→PG schema against the upstream hand/abstract-authored
  PG side (Chinook AND MediaWiki). Iterations 1–2 only assert "both
  read + plan clean + non-vacuous", not that the *translation equals*
  the other engine's authored schema. This is the highest-value next
  signal (true oracle, not just smoke).
- Then Vitess/PlanetScale sample schemas (Track-1b-adjacent).
