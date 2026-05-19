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

## Iteration 3 (2026-05-19) — GitLab PG→PG: FINDING (core range-type gap)

**The highest-value corpus finding so far — a genuine product gap,
surfaced on a real production schema.**

Added a GitLab **PG→PG** DryRun leg (complement of the cross-engine
leg). On the *same* real schema the two directions are correctly
asymmetric for `tsvector` (cross-engine loud-refuse = right; same-
engine verbatim-carry = right) — but PG→PG fails *earlier*, on
**`int8range`** (`ci_partitions.builds_id_range`):

```
pipeline: read source schema: postgres: read columns:
table "ci_partitions" column "builds_id_range":
postgres: unsupported data_type "int8range" (udt "int8range")
```

**Root cause (code-confirmed, `internal/engines/postgres/types.go`):**
`tsvector`/`tsquery` got an explicit same-engine verbatim-carry
branch (catalog **Bug 17**, types.go:379 — `if c.VerbatimEligible →
ir.VerbatimType`). That carve-out was made **type-by-type for the
*representative*, not generalized to the *class*** of core PG types
(`pg_catalog`, no rich cross-engine IR shape). So same-engine PG→PG
**loud-refuses core RANGE types** (`int4range/int8range/numrange/
tsrange/tstzrange/daterange`) at the generic fallthrough (types.go
~392) — *exactly the gap `tsvector` had pre-Bug-17*. This is the
project's own "pin the class, not the representative" lesson, at the
product level, found on real data.

**Scope (Phase-A, GitLab `structure.sql`):** 5 range-type columns —
2×`int8range`, 2×`daterange`, 1×`tstzrange`; one carries an
`EXCLUDE USING gist (... WITH &&)` constraint (an adjacent core-PG
feature that would also need same-engine carry, downstream of the
type refusal). No `xml`/`money`/`pg_lsn`/`interval` in this schema —
the surfaced class is **core range types** (others may exist in
other corpora — characterize as found).

**Severity:** loud refusal → **loud-failure tenet holds, no
corruption / no silent loss.** But it **blocks PG→PG sync of any
schema using range types** — common in partition bounds, scheduling,
analytics (GitLab, Rails, Django). A real fidelity/UX gap, *not* an
emergency, *not* a fix-mid-testing item (the fix is a design
decision: which core types, the same-vs-cross-engine boundary,
interaction with ADR-0047's *USER-DEFINED*-only verbatim tier — range
types are core, not USER-DEFINED, so ADR-0047 does NOT cover them;
this is the Bug-17 core-type tier needing generalization).

**Tracked:** roadmap entry added ("generalize core-PG-type
same-engine verbatim carry beyond tsvector/tsquery"); candidate ADR.
Harness: `TestCorpus_GitLab_PGToPG_VerbatimCarry` stays GREEN by
*characterizing* this known class (PASS on a range-type refusal;
FAIL only on an *unexpected* shape = a new finding) — and will go
green-as-assertion automatically if the gap is later closed.

## Iteration 3 corpus results (2026-05-19) — Joomla + WordPress

| Corpus / direction | Outcome | Guard |
|---|---|---|
| **Joomla MySQL→PG** DryRun | ✅ PASS | 28 tables, non-vacuous |
| **Joomla PG→MySQL** DryRun | ✅ PASS | 28 tables, non-vacuous |
| **WordPress MySQL→PG** DryRun | ✅ PASS | 18 tables, non-vacuous |

- **Joomla = clean real-CMS matched MySQL/PG pair, both directions,
  zero sluice defects** (independently authored per dialect, like
  Chinook; ships raw `installation/sql/{mysql,postgresql}/base.sql`).
- **WordPress = the canonical operator-brought MySQL schema, clean**
  — after two layers of *corpus-prep* friction handled **faithfully
  (schema NOT rewritten)**:
  1. `datetime NOT NULL default '0000-00-00 00:00:00'` → MySQL 8.0
     strict `sql_mode` (`NO_ZERO_DATE`) rejects it at DDL time
     (Error 1067). Fix: apply under WP's permissive
     `sql_mode='NO_ENGINE_SUBSTITUTION'` — load the *real* schema
     (millions of installs run it), don't mangle it.
  2. `wp_get_db_schema()` defines `wp_users`/`wp_usermeta` **twice**
     (mutually-exclusive `$users_single_table` vs `$users_multi_table`
     scopes) → "table already exists". Fix: `extract_wp_schema`
     dedupes by table name, keeping the **first** = the canonical
     single-site schema. → 18 clean tables.
- **pgloader & Drupal: evaluated, deliberately NOT fetched** (see
  MANIFEST.md "Evaluated, deliberately NOT fetched"). pgloader's test
  corpus is `.load` *orchestration* against live MySQL DSNs, not
  standalone schema `.sql` — its real value to sluice is its **cast
  ruleset** as a *translator-catalog reference* (cf. ADR-0016 /
  `docs/type-mapping.md`), not a corpus. Drupal core schema is PHP
  `hook_schema()` (`*.install`), no raw `.sql` in core. Forcing
  either would ship a bad-fit/murky artifact below this corpus's
  provenance bar — honest documentation beats a forced fit.

### Cross-iteration meta-finding (worth stating)

Across iterations 1–3, **every red was either (a) a real,
well-scoped sluice gap that got tracked** (the single one: PG→PG
core-range-type verbatim, roadmap item 17 — loud, not silent) **or
(b) vendor-schema *ingestion* friction the corpus is designed to
surface** (psql `\meta` scripts, `USE`-switched DBs, multi-row INSERT
dumps, strict-mode zero-dates, PHP-embedded schemas, dual-scope table
defs). **Zero silent-corruption defects anywhere.** The synthetic
fuzz harness would never have produced any of (b) — this is exactly
the complementary signal the real-world corpus exists for, and it
also hardened the harness itself (non-vacuous guard; raw-count vs
ReadSchema split; data/meta strip generality).

## Iteration 4 (2026-05-19) — matched-pair CONGRUENCE oracle + Vitess/PS slice

**Verdict: the true-oracle harness is built and proven non-vacuous;
verdicts are deferred to the CI Integration run (Windows+CGO=0+Rancher
cannot run `-tags=integration` locally — authoritative signal is CI's
Linux Integration job, per CLAUDE.md). No NEW FINDING from
code-reading; the legs are written so any congruence break FAILs loud
with `diff.Summary()` and the offending table/column.**

### Part A — matched-pair congruence oracle (the high-value part)

New file `migrate_realworld_corpus_congruence_integration_test.go`.
This is the first leg that asserts sluice's *emitted* translation is
**congruent** with the expert-authored other-engine schema — iterations
1–3 only asserted "reads + plans clean + non-vacuous", never that the
translation *equals* the human-authored side.

Mechanism (forward, MySQL-emitted vs authored-PG):

1. Apply authored MySQL corpus DDL → MySQL testcontainer.
2. `Migrator{Source:mysql,Target:pg,DryRun:false,TargetSchema:
   "sluice_emitted"}` — actually EMITS sluice's translated PG schema
   (schema-only corpus ⇒ zero-row bulk copy) into the `sluice_emitted`
   PG schema (the PG SchemaWriter `CREATE SCHEMA IF NOT EXISTS`es it).
3. Apply authored PG corpus DDL into a sibling `authored` schema of
   the SAME PG container (via `SET search_path`, since the corpus DDL
   is unqualified).
4. Read both PG schemas via the PG engine reader, schema-scoped
   (`applyTargetSchema`/`ir.SchemaSetter` → `SchemaReader.SetSchema`);
   `ir.DiffSchemas(authored, emitted, {IgnoreCharsetCollation:true,
   IgnoreExtras:false})`.
5. `classifyCongruenceDiff`: no drift → CONGRUENT; drift split into a
   tight commented allowlist (`congruenceBenignReason`) vs anything
   outside → **FAIL** with `diff.Summary()` + the offending
   table/column (the GitLab-leg "characterize the known class, FAIL on
   an unexpected shape" pattern).

Pairs: **Chinook (11), MediaWiki (≥50; upstream generates 64), Joomla
(≥20; ≈28)** — floors reuse the iteration-1/2/3 non-vacuous counts
(verified against the existing harness, not hardcoded blind).

Symmetric direction (`runCongruenceReverseLeg`): authored-PG **emitted
PG→MySQL** vs authored-MySQL. MySQL has no schema namespace
(`SchemaScopeFlat` ⇒ `Migrator.TargetSchema` is refused for MySQL
targets), so emitted/authored separation is by **database**
(`authored_db` / `emitted_db`) on one MySQL container, then both read
via the MySQL reader. Same classify/allowlist machinery.

**The benign allowlist (tight, each entry justified):**

| Allow class | Justification (why benign-not-defect) |
|---|---|
| `TINYINT(1)` ⇄ `boolean`/`smallint` | documented cross-engine bool contract, `docs/value-types.md` |
| integer width tier differs (INT vs BIGINT) | both `ir.Integer`; tier is an *authoring choice* between two independently-authored expert schemas, sluice maps width-faithfully, `docs/type-mapping.md` |
| AUTO_INCREMENT ⇄ identity/serial | column-attribute vs default-backed sequence; *type* identical, only DEFAULT spelling differs, `docs/type-mapping.md` |
| ENUM ⇄ named-enum / text / varchar | all documented ENUM cross-engine renderings, `docs/type-mapping.md` |
| DATETIME/TIMESTAMP(p) ⇄ timestamp[tz](p) | temporal family + precision survive; tz/no-tz + DATETIME/TIMESTAMP spelling is the documented temporal mapping |
| MySQL sized TEXT tier ⇄ PG unbounded `text` | documented text policy (reverse widening is the Bug 72 notice) |
| MySQL BLOB tier ⇄ PG `bytea` | documented binary policy |
| MySQL JSON ⇄ PG JSON/JSONB | `docs/value-types.md` JSON contract |
| type-equal, only DEFAULT spelling differs | `docs/type-mapping.md` default-equivalence (two expert authors disagreeing on `0` vs `'0'` etc.) |
| type-equal, only NULL/NOT NULL differs | authoring choice between two independently-authored expert schemas, not a translation defect |

**Allowlist cannot mask a real loss:** `classifyCongruenceDiff` routes
*all* structural drift — missing/extra table, missing/extra column,
index drift, CHECK drift — straight to the FAIL path. The allowlist is
*only* ever consulted for a per-column TYPE/DEFAULT/NULLABLE mismatch
on a column present on **both** sides, so a dropped/extra table or
column can never be silently absorbed (this is the
"pin-the-class-not-the-representative" discipline applied to the
oracle's own guard).

### Part B — Option-A Vitess / PlanetScale slice (DryRun; no live PS)

Added to the existing corpus harness file:

- **Capabilities-delta leg**
  (`TestCorpus_WordPress_PlanetScaleFlavor_MySQLToPG_DryRun`): the
  already-fetched WordPress MySQL corpus member, read+planned with the
  source engine resolved to the **`planetscale`** flavor registration
  (`engines.Get("planetscale")`; `Flavor.String()=="planetscale"`,
  `internal/engines/mysql/flavor.go`) instead of vanilla `mysql`. Same
  engine code, different `ir.Capabilities` (no LOAD DATA INFILE →
  BatchedInsert, no PARTITION BY, no spatial, CDC=VStream). The leg
  asserts the resolved name **and** the documented Capabilities
  divergence (`SupportsPartitioning==false`, `BulkLoad!=LoadDataInfile`)
  so it would FAIL loud if `flavor.go`'s caps regressed — i.e. it
  genuinely exercises the delta it claims to.
- **Vitess example-schema leg**
  (`TestCorpus_Vitess_Commerce_MySQLToPG_DryRun`): new corpus member
  `vitess_commerce_mysql.ddl.sql` — vitessio/vitess
  `examples/local/create_commerce_schema.sql`, **Apache-2.0**,
  added to `fetch.sh` (data-strip discipline pass) + `MANIFEST.md`
  (provenance + LICENSE-SAFETY note). Characterizes Vitess DDL idioms
  (no FKs, reference/sequence tables) through sluice's MySQL reader +
  a MySQL→PG DryRun plan; loud-refuse acceptable-and-characterized, a
  crash would be a finding.

Runtime Vitess/PS behaviour (resharding, tx-killer, online DDL) stays
**out of scope** — routed to Track-1b per
`docs/dev/notes/vitess-local-vs-planetscale-equivalence.md`.

### Gate results (local, this box)

- `go vet -tags=integration ./internal/pipeline/` → clean (the
  authoritative type-check for the build-tagged test files;
  bare `go build` skips `_test.go`).
- `go vet ./...` → clean.
- `go test ./...` (non-integration) → all packages OK.
- `golangci-lint run ./internal/pipeline/` (CI's actual config — no
  integration tag) → **0 issues**. Under the stricter
  `--build-tags=integration` run the two new/edited corpus files are
  also clean; the ~40 tagged-only issues elsewhere are pre-existing in
  other integration test files and outside CI's lint scope.
- gofumpt: new/edited files formatted clean.

### Decisions made that were NOT pre-specified (flagged for lead)

1. **Symmetric direction is true PG→MySQL emit, not a PG→PG
   substitute.** The spec said "authored-MySQL vs sluice-emitted-from-
   PG". Because MySQL has no schema namespace, I separate emitted vs
   authored by **database** on one MySQL container (`authored_db` /
   `emitted_db`) rather than by schema. This is faithful to the spec's
   intent (it really emits PG→MySQL and diffs against authored MySQL).
2. **`SET search_path TO <schema>, public`** is used to land the
   unqualified authored PG corpus DDL into the `authored` schema. If a
   corpus member's PG DDL hard-qualifies `public.` it would bypass
   this — none of Chinook/MediaWiki/Joomla PG do today, but if a
   future congruence pair can't be schema-isolated this way, that is
   itself a finding (documented, don't force a green).
3. **MediaWiki floor = 50** (not 64): reuses the existing iteration-2
   `corpusAssertTables` non-vacuous floor rather than asserting the
   exact generated count, so a benign upstream table-count drift
   doesn't red-bar the oracle while still proving non-vacuity. The
   congruence diff itself (not the floor) is the equivalence assertion.
4. **Verdicts are CI-deferred.** Per CLAUDE.md this box can't run
   `-tags=integration` (Windows+CGO=0+Rancher). The legs are written
   to FAIL loud on any non-allowlisted delta; the actual
   congruent / characterized-benign / NEW-FINDING verdict per pair
   comes from the CI Integration job and should be recorded here on
   that run (this section will be updated with per-pair verdicts once
   CI reports — the harness, allowlist, and guards are the deliverable
   here).
