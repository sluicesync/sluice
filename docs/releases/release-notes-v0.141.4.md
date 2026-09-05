# sluice v0.141.4

**If you run Postgres CDC, the retry budget is not what we told you it was.** sluice's docs have said the applier absorbs about four minutes of transient failure — "enough to ride out vtgate restarts and Patroni failovers." The real figure at shipped defaults is **12.7 seconds**, which does not cover a failover. Nothing changed in the code; the number was wrong from the day it was written, and it is corrected everywhere it appeared. If you sized your operations around it, see below.

This release also fixes a Postgres source that could not be migrated at all, and gives the publication-permission failure a coded refusal with a remedy.

## Fixed

**A Postgres source with an unvalidated `CHECK` on a `DOMAIN` could not be migrated.** The schema reader strips the `CHECK (` wrapper from `pg_get_constraintdef` and trims the closing paren — but an unvalidated constraint renders as `CHECK (VALUE > 0) NOT VALID`, which ends in `D`. The trim matched nothing, the captured body kept an unbalanced parenthesis, and the emitted DDL was a syntax error. A legal source failed in a way that read like an internal sluice bug rather than something about your schema. Measured on PostgreSQL 16.

**`NOT VALID` constraint state is now carried rather than silently dropped.** A source foreign key marked `NOT VALID` was recreated as validating. That is not a stricter copy: the source is telling you it holds rows the predicate rejects, so validating on the target either fails the run or misrepresents the data. Foreign keys now carry it faithfully, and it participates in schema-diff and drift reports as a third constraint-strength axis alongside `MATCH` and `DEFERRABLE`.

Table `CHECK` constraints and domain `CHECK` constraints **warn loudly instead**, because Postgres rejects `NOT VALID` inline in both `CREATE TABLE` and `CREATE DOMAIN` — it is only accepted via `ALTER`, and sluice emits one statement per DDL by design. The warning names the constraint and that `--allow-degraded-fks` does not cover `CHECK`. It is a warning rather than a refusal on purpose: the constraint is created on an empty table, so a source whose data happens to satisfy it migrates fine today and refusing would break that.

**A role that cannot create the publication now gets `SLUICE-E-CDC-PUBLICATION-PERMISSION`.** Previously this surfaced as a raw, uncoded `SQLSTATE 42501` at cold start with no remedy attached, and no hint matched it. Three different grants produce that one SQLSTATE — `CREATE` on the database, ownership of every published table, and superuser for a database-wide `FOR ALL TABLES` publication — so the refusal names all three and carries the server's own message saying which one bit. Every publication DDL door is covered; `TestPublicationPrivilegeRoster_EveryDDLSiteIsClassified` derives them from the source and fails the build if one is added without classification.

## Changed

**The retry-envelope correction, in detail.** The schedule exponentiates from `100ms` and never reaches the `30s` cap at the default eight attempts, so the deliberate backoff is `100ms → 200ms → 400ms → 800ms → 1.6s → 3.2s → 6.4s` — seven sleeps, 12.7 seconds. The eighth failure exhausts the budget without sleeping. The documented "four minutes" came from multiplying the cap by the attempt count, which the schedule never does.

The observed wall clock is longer and varies by failure shape — a half-open target can burn up to `--apply-exec-timeout` (60s) per attempt before the error even arrives — but the *deliberate* wait is those 12.7 seconds. **If you were relying on sluice to ride out a Patroni failover or a vtgate restart, it was not doing so.** `--apply-retry-attempts=20` gives 5m51s and does cover one; that figure is measured, not derived.

The test that was supposed to guard this number could not: `TestComputeRetryBackoff_AttemptsBudget` summed eight terms for seven sleeps, then asserted the total was under four minutes — true by two orders of magnitude, so it never failed while naming the promise it was protecting. It now pins the sequence element by element and the total exactly.

**Managed Postgres: `FOR ALL TABLES` works on RDS and Azure.** Both patch PostgreSQL's superuser check, so a multi-schema `sync start` and `backup full --chain-slot` work with the ordinary admin role even though `pg_roles.rolsuper` is false on both. Measured against live instances on 2026-09-05, with a stock PostgreSQL 18.6 control to confirm the rule had not been relaxed upstream. Google Cloud SQL is unmeasured and the docs say so.

## Compatibility

Drop-in from v0.141.3. One new error code (`SLUICE-E-CDC-PUBLICATION-PERMISSION`), no flag change, no format change.

**Backup chains are unaffected, including for sources that do have unvalidated constraints.** Carrying `NOT VALID` adds a field to the recorded schema, encoded so a schema without one hashes exactly as before. And chain verification recomputes each link's hash from the schema that link itself carries, rather than comparing links to one another — so a chain written by an older release still self-verifies under this one. No repartition, no forced full backup.

No re-run is needed for the retry correction; it is a documentation fix. Whether you want to *act* on it is the question — check whether your `--apply-retry-attempts` was chosen against the four-minute figure.

## Who needs this

Anyone running Postgres CDC, for the retry envelope and the new error code. Anyone migrating a Postgres source that uses `NOT VALID` constraints — those on domains were previously un-migratable. Anyone who avoided a multi-schema sync on RDS or Azure believing it needed a superuser they do not have.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.4
docker pull ghcr.io/sluicesync/sluice:0.141.4
```
