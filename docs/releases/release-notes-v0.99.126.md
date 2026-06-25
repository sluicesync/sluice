# sluice v0.99.126

**Test-hardening + docs release — no behavior change. A new multi-schema generated-column integration pin makes explicit that sluice detects PostgreSQL `GENERATED … STORED` columns in any source schema, not just `public`. Fully drop-in: no flag, default, value-handling, or wire-path change; the binary behaves byte-identically to v0.99.125.**

## Changed

**New integration pin: `TestMigrate_PG_Generated_NonPublicSchema` (multi-schema generated-column round-trip).** This test migrates a PostgreSQL source whose `GENERATED ALWAYS AS (qty * price) STORED` column lives in a non-`public` schema (`inventory`) end-to-end and asserts the full property chain: the generated column is detected in that non-public schema, dropped from the bulk-copy column list (PostgreSQL rejects writes to a generated column, SQLSTATE 428C9), recreated as a real `GENERATED` column on the target, recomputed during the initial copy *and* on a subsequent `UPDATE` of a dependent column, with a plain table riding along in the same schema to prove non-generated columns still copy normally — and a final assertion that nothing leaked into `public` (no search-path spillover). It guards the class of bug seen in other migration tools (heroku-migrator#11) where generated-column detection was hardcoded to the `public` schema and silently missed generated columns in other schemas, breaking the downstream COPY. sluice never had that bug: its PostgreSQL schema reader is parameterized by schema (every catalog query is `WHERE table_schema = $1`, where `$1` is the schema actually being migrated), so the generated-column flag is read in the same per-schema pass as the column itself and can never cover a different schema set than the copy. This release makes that property an explicit, enforced regression pin rather than an implicit consequence of the reader's shape; it changes no production code.

**Roadmap updates.** Routine roadmap bookkeeping accompanying the test pin.

## Compatibility

No breaking changes; fully drop-in. There is no CLI surface change, no new or changed flag or default, no value-handling change, and no change to any read, write, copy, or CDC path. The only diff that ships in this release is a new `//go:build integration` test file plus documentation; the released binary is byte-identical in behavior to v0.99.125. All engines, directions, and paths are unaffected.

## Who needs this — action required

Nobody needs to act. This is a test + documentation release with no behavior change, so there is nothing to re-verify, re-run, or reconfigure, and no prior release is implicated (sluice's per-schema generated-column detection has always been correct — this pin documents and locks it, it does not fix a regression). For reassurance: anyone migrating PostgreSQL sources that have `GENERATED … STORED` columns spread across multiple schemas (not just `public`) can take this release as explicit, CI-enforced confirmation that those columns are detected and recreated correctly regardless of which schema they live in. Upgrading is optional and safe; staying on v0.99.125 leaves you on identical runtime behavior.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.126 · **Container:** ghcr.io/sluicesync/sluice:0.99.126
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
