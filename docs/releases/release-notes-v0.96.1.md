# sluice v0.96.1

## v0.96.1 — bulk-copy abort surfaces a resume hint naming the missing-index gap

Closes Bug 114 (MEDIUM operator-friction): when `sluice migrate` aborts mid-bulk-copy, the loud error now tells operators that previously-completed tables hold rows but NOT their declared secondary indexes — and that `--resume` (or `--exclude-table=<name>`) is the next step.

### Fixed

- **Bug 114 — `migrate` partial-failure left operators thinking earlier tables were "done".** Pre-fix the loud error on a mid-bulk-copy abort correctly named the failing table (preserves loud-fail tenet) but said nothing about state of tables 1..N. Because migrate phases run as `tables → bulk_copy → identity_sync → indexes → constraints → views`, an N+1 abort leaves tables 1..N with full row counts AND the PK index (created in the `tables` phase), but WITHOUT any of their declared secondary indexes — the index phase only runs after every table finishes bulk_copy. Operators inspecting `pg_indexes` on the earlier tables saw the PK and concluded "this table migrated cleanly", missing the absent secondaries. Recovery (`--resume`) wasn't surfaced as the next step in the error message. v0.96.1 extends the `hints.go` registry (the existing operator-friendly post-error-hint layer) with a `PhaseBulkCopy` entry matching the standard `pipeline: copy table` wrapper prefix produced by `migrate_bulk.go`'s copy-table failure paths. The hint reads `any earlier tables in this run have data but NOT their declared secondary indexes (the indexes phase runs after ALL tables finish bulk-copy); use --resume to continue after fixing the offending table, or --exclude-table=<name> to skip it`. The existing `does not exist` / `doesn't exist` PhaseBulkCopy entries continue to win first (first-match-wins ordering), so the more-actionable "target table not found" hint still fires for schema-apply mismatches; the new entry catches the residual underlying-engine-error class (e.g. Bug 114's `jsonb[]` COPY-protocol refusal). Pinned by `TestHintForRegistry` adding a new sub-case naming Bug 114 with the catalog's `sentry_releases` repro shape.

### Compatibility

- Pure additive hint-registry change. No schema, no migration semantics, no manifest version bump.
- Existing operator workflows are unaffected; the only observable change is the extra `hint:` line at the bottom of a mid-bulk-copy abort message.

### Who needs this

- **Operators running `sluice migrate` over multiple tables in a single run.** Before v0.96.1, a bulk-copy failure on table N+1 silently left tables 1..N missing their secondary indexes; the operator had to know to use `--resume` or risk migrating "successfully" and discovering missing indexes weeks later when query performance regressed.
- **Migrations involving `jsonb[]` or other COPY-protocol-incompatible types** (the BUG-CATALOG.md Bug 114 repro). Same migration shape, but now with an actionable next step.

### Open backlog after this release

- **PG→MySQL DOMAIN-CHECK silent-downgrade WARN** (cross-engine residual from v0.95.x/v0.96.0 Bug 113 closure — the PG row stream now carries DOMAIN columns to MySQL, but the MySQL writer silently downgrades the column to the base type with no MySQL CHECK and no WARN). Scheduled for v0.96.x successor.
