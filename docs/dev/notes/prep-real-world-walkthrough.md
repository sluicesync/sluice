# Prep: real-world walkthrough

Roadmap reference: not in the original roadmap; surfaces from the §8 wrap-up conversation about validating the v1 spine against an actual workload before declaring v1 complete.

## Goal

A `docs/examples/quickstart.md` walkthrough that exercises sluice end-to-end against an actual (non-toy) database, using a publicly-available sample dataset that's available for both MySQL and Postgres. The walkthrough doubles as:

1. **A user-facing quickstart** — the doc someone reads before running sluice for the first time. Currently `docs/examples/sluice.yaml` is the only example; there's no narrative walkthrough.
2. **A validation pass** — every step gets executed during writing. Friction we hit (missing CLI flags, unclear error messages, undocumented preconditions, performance on real data) becomes a punch list of small fixes that ship alongside the doc.

Out of scope for this chunk:

- **Performance benchmarking** as a deliverable (numbers in a table). Real numbers will appear incidentally — "this took ~30 seconds for 30 MB of data" — but a structured benchmark harness is a separate chunk if we ever need one.
- **Cloud-specific guides** (PlanetScale, RDS, etc.). Those land separately (§3a-§3b).
- **Multi-tenant or sharded source schemas.** The walkthrough exercises the canonical single-database shape.

## Dataset choice

**Recommend `sakila` / `pagila`.**

- `sakila` is MySQL's reference sample database (DVD rental store). Maintained by Oracle. ~16 tables, ~30 MB, includes `BLOB`s, `ENUM`s, `SET`s, `JSON`-adjacent columns, FK constraints, fulltext indexes, triggers, stored procedures.
- `pagila` is the community-maintained Postgres port of sakila. Same schema shape, populated the same way. Includes PG-native types (UUID, JSONB, arrays in some tables) where the equivalents are available.

Why this dataset:
- Available for both engines → the walkthrough demonstrates all four migration directions with one source-of-truth dataset.
- Small enough to download and load in seconds, large enough that simple-mode bulk-copy isn't instantaneous.
- Real schema with realistic features: FKs, indexes, triggers, ENUM, fulltext. Surfaces real translation issues.
- Well-known among database tutorials → operators recognise it.

Alternative considered: Stack Exchange data dump. Real, much larger (~50 GB compressed for full StackOverflow). Better for performance work; overkill for a quickstart. Punt.

## Walkthrough structure

`docs/examples/quickstart.md` should follow this arc:

```
1. Install sluice
   - go install path
   - verifying with `sluice engines`

2. Set up source and target databases
   - Docker compose snippet for MySQL 8.0 + Postgres 16
   - Loading sakila on MySQL
   - Creating empty target databases

3. Simple-mode migration: MySQL → Postgres
   - Write the YAML config
   - Dry-run first (sluice migrate --dry-run)
   - Execute the migration
   - Spot-check the target

4. Continuous-sync migration: MySQL → Postgres
   - Open the streamer
   - Make a change on the source (INSERT a new film)
   - Watch it propagate to the target
   - Stop the streamer, restart it, watch resume work

5. Reverse direction: Postgres → MySQL
   - Switch source and target in the config
   - Run again

6. What to do next
   - Pointers to docs/architecture.md, ADRs, type-mapping.md
```

Each numbered section includes the actual commands and expected output. The doc is meant to be copy-pasted from.

## Friction we'll likely surface (and fix in-line)

These are anticipated; the real list is whatever shows up while writing:

- **Missing or unclear CLI flags.** A real walkthrough exposes whether `sluice migrate` actually has the flags a user would expect (`--source`, `--target`, `--config`, `--dry-run`). Anything missing gets a small fix.
- **Logging volume.** When a migration takes 10 seconds, what does the user see? Today it's `pipeline: migrated N tables\n` at the end and that's it. A "copying users (1234/50000)" progress line per table — already on the §9 wishlist — would surface here naturally and probably ship in this chunk.
- **Type-translation hits we haven't seen.** Sakila has `YEAR`, `SET`, fulltext indexes; pagila has `tsvector`. We'll find out what survives translation and what doesn't. Per the IR-first tenet, anything that doesn't survive should error loudly with a clear message naming the unsupported feature — fix any silent drops we find.
- **Triggers and stored procedures.** sakila ships with both. Sluice doesn't (and shouldn't) translate procedural code; the walkthrough should document this clearly and exit cleanly when the source has them.
- **README cross-link.** The README's "Status" section says "engine implementations are next." That's stale. The README should reference the quickstart at the top.

## Files to add / touch

- `docs/examples/quickstart.md` — new, the walkthrough.
- `docs/examples/quickstart/docker-compose.yml` — new, container setup.
- `docs/examples/quickstart/sluice.yaml` — new, the config used in the walkthrough.
- `README.md` — update "Status" section, add a Quickstart link near the top.
- `docs/examples/sluice.yaml` — keep; rename references so the existing example file is clearly distinct from the walkthrough's config.
- Inline code fixes for whatever surfaces during the writing pass — likely 2-5 small edits, predictable shape (a missing CLI flag, an error message that should name a column, a stale doc reference).

## Anticipated rough edges

- **Sakila + pagila aren't byte-identical.** Same schema shape but different procedural code, slightly different default values, sometimes different column nullability. The walkthrough should pick one direction (MySQL → PG) as the canonical demo and treat the reverse as a follow-up paragraph.
- **Trigger translation.** sakila's triggers fire on INSERT/UPDATE to maintain `last_update` columns. Sluice doesn't translate triggers. The target's `last_update` column will be populated from the source's row values (which is *correct* — the source's trigger already fired before sluice read the row). Worth a paragraph in the doc explaining this.
- **AUTO_INCREMENT vs IDENTITY.** §7 closed this. Sakila's `actor_id BIGINT NOT NULL AUTO_INCREMENT` should land on PG as `BIGINT GENERATED BY DEFAULT AS IDENTITY` and the post-bulk-copy sequence sync should advance the sequence past the bulk-copied max. The walkthrough should call this out explicitly — it's a load-bearing correctness property and demonstrating it in the doc cements operator confidence.

## Open questions for the user

1. **Walkthrough engines: docker-compose vs cloud account.** docker-compose is reproducible offline; a cloud-account walkthrough is more realistic. *Recommendation:* docker-compose for the canonical quickstart; cloud-specific examples are §3a/§3b's territory. Confirm?
2. **README update scope.** A quickstart reference at the top of the README is uncontroversial. Updating "Status" says we ship past the engine-implementations stage. Confirm both?
3. **Dataset hosting.** Sakila and pagila ship from official sources (Oracle for sakila, GitHub for pagila). The walkthrough should reference upstream URLs, not vendor the data. *Recommendation:* link upstream; include a one-liner `curl` command. Confirm?
4. **Friction-fix scope.** The walkthrough will surface small issues (missing flags, unclear errors). *Recommendation:* fix anything that's obviously broken during the writing pass; defer anything that needs design (like the slog migration) to its own chunk. Confirm?

## Suggested first-cut prompt

> "Read CLAUDE.md, docs/dev/notes/prep-real-world-walkthrough.md, and the current README.md. Propose the walkthrough structure before writing: (1) the docker-compose layout for MySQL 8.0 + Postgres 16 + a sluice container, (2) the section-by-section outline of docs/examples/quickstart.md including the exact commands and expected outputs for each step, (3) the README update for the Status section + Quickstart pointer, (4) a list of friction items we'll likely hit and the fix shape for each. Note any deviation from the prep doc with a why. Stop after the design for review."
