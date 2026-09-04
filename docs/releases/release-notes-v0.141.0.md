# sluice v0.141.0

**Three refusals that reached further than the thing they were guarding, and a warning for damage sluice was doing quietly.** One entry repairs v0.140.0: its new Cloudflare D1 check could fail a migration over a table you had explicitly excluded, with no flag able to get past it. If you migrate D1, run a `d1-trigger` stream, sync more than one Postgres schema at a time, or back up Postgres with `--chain-slot`, one of these is yours.

## Fixed

**A Cloudflare D1 migration no longer refuses over a table you excluded.** v0.140.0 added a byte-sum bracket that catches D1 rewriting invalid UTF-8 in transit, and put it on the staging path as well as the bulk read. Staging replicates the WHOLE database by design — that faithfulness is what lets `--infer-types` treat the local file as indistinguishable from D1 — and it runs before the table filter is consulted at all. So one mangled table anywhere in the database failed the entire run, and neither `--include-table` nor `--exclude-table` could reach it, because by the time either is read the refusal has already happened. `--infer-types` engages staging automatically, so this sat squarely on the path most operators take. The copy is still whole-database; the refusal now fires only for tables the run will actually read, resolved from the CLI flags and the YAML config alike, and a mangled table outside that set warns instead. You still learn your source holds bytes D1 will not return faithfully, and the migration you asked for still finishes.

**An oversized change row no longer wedges a `d1-trigger` stream permanently.** The change-log poll shared the transport's response cap with the bulk reader but not its adaptive page. A poll error kills the pump, the poll batch is not an operator flag, and the batch size was fixed — so a batch whose before-and-after row images exceeded the cap did not degrade the stream, it ended it, and every restart met the identical batch. There was no remedy at all, not a slow one. The poll now halves and re-requests against the same `id >` watermark, which returns a strict prefix of the same rows so nothing can be skipped, and refuses by name only when a single change row is itself too large. The shrink is sticky for the life of the stream and resets on restart.

## Features

**Postgres now tells you which tables outside your scope it is about to break.** A database-wide logical slot needs a `FOR ALL TABLES` publication, and that reaches every table in the database — not only the ones you selected. Postgres refuses UPDATE and DELETE on any published table without a replica identity, while INSERT keeps working, so the breakage is partial and surfaces as an error inside whatever application owns those tables, with nothing connecting it to the run that caused it. sluice now names each at-risk table before it opens the publication, with the mechanism and both remedies.

It warns rather than refuses on purpose: these tables are outside the scope you declared, so refusing would block a run over a schema you deliberately excluded, and a warning demands nothing of you.

Every path that creates such a publication now warns, and there were more of them than the obvious two. The multi-schema `sync start` is the one you would expect. `backup full --chain-slot` is the worse case and had neither a refusal nor a warning: it deliberately keeps the publication so the chain's incrementals can decode through it, so the exposure outlives the run. And a third, found by the pre-tag review: any stream whose publication is MISSING recreates it database-wide on the spot, which includes a warm resume. That last one closed a loop this release had otherwise opened — the new warning tells you that dropping the publication restores the broken writes, and on a single-schema stream the next resume would have silently recreated it `FOR ALL TABLES`.

The warning's scope is exact rather than approximate. The replica-identity preflight that already refuses over your selected tables records precisely what it graded, and the warning reports the complement — so a table you excluded with `--exclude-table` inside a schema you did select, which the preflight never sees, is no longer missed by both.

Measured on PostgreSQL 16.15, 17.11, 18.6 and 19beta1, identical on all four. The at-risk set is narrower than "every table" and the warning is filtered to match: an unlogged table, a partitioned parent, a view and a materialized view are all outside a `FOR ALL TABLES` publication and none of them is ever named; a leaf partition is inside. Both remedies were verified rather than assumed — dropping the publication restores the writes, and so does `REPLICA IDENTITY FULL` with it still in place — and a single-schema sync creates a scoped publication, so it does not cause this — as long as that publication still exists, which is the loop the third path above closes.

## Compatibility

Drop-in from v0.140.0. No flag change, no format change, no new error codes.

One behaviour change worth knowing, and it is a relaxation: a Cloudflare D1 table holding invalid UTF-8 now refuses only when that table is in scope for the run. If v0.140.0 blocked you on a table you had already excluded, that run will now complete.

The new Postgres warning is advisory. It never fails a run, and every failure inside the audit itself is swallowed to a debug line, so an absent warning is not proof of an absent hazard.

## Who needs this

Anyone migrating Cloudflare D1 with `--infer-types` or `--stage-local`, especially with a table filter; anyone running a `d1-trigger` stream over wide rows; and anyone running a multi-schema Postgres sync or a `--chain-slot` Postgres backup against a database that also holds tables they did not select.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.0
docker pull ghcr.io/sluicesync/sluice:0.141.0
```
