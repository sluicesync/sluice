# sluice v0.103.0

A correctness release, and the largest single batch of silent-loss fixes sluice has shipped. Everything here came out of a blind multi-agent audit of the last 88 commits — findings that survived an adversarial refutation pass, each closed with a permanent gate rather than a one-off fix. Drop-in upgrade, no breaking changes; three new loud refusals replace three silent wrong answers.

### Fixed

**A `--where` filter on a GENERATED column silently dropped every matching row on the continuous leg.** This is the serious one, and it was in released code. A predicate referencing a generated / computed column compiled cleanly and the *cold-start copy was correct* — the filter is pushed into the source `SELECT`, which reads a generated column perfectly well — and then the CDC leg dropped every subsequent INSERT for that table, at exit 0, with green `sync status` and `health`. A change stream does not deliver a generated column to the filter — on MySQL the binlog row image carries it but sluice deliberately drops it from the decoded row so the target's own `GENERATED` clause recomputes the value rather than freezing the source's, and on Postgres pgoutput omits it from `RelationMessage` before PG 18 — so either way the decoded row had no such key and every comparison scored "not in scope". The canonical `--where` target — an append-only orders or events table — is precisely the shape that never triggers the loud UPDATE/DELETE sibling, and that sibling's remedy (`binlog_row_image=FULL` / `REPLICA IDENTITY FULL`) could not have fixed it anyway, because sluice's own decoder is what strips the column. Such a predicate is now refused at startup, before any data moves, naming the column and what to write instead; independently, the INSERT arm gained the image-completeness check its three siblings already had.

**Identifier and time-of-day literals compared byte-exactly while every engine coerces them first.** For `uuid`, `inet`, `cidr`, `macaddr` and `time` columns, a `--where` literal was compared against the raw text the operator typed. The source doesn't work that way — it coerces the literal to the column's type before comparing, so `'A0EEBC99-…'`, `'010.000.000.001'`, `'08-00-2B-01-02-03'` and `'08:30'` all match server-side. The client evaluator compares against the decoded value, which is always canonical, so once again the cold start copied the row and the continuous leg then classified every change to it as out-of-scope, leaving the target row permanently stale. Non-canonical literals are now refused with the canonical spelling named. A fractional-second `TIME` column is refused outright: its value renders as `08:30:00.000000` on the snapshot leg and `08:30:00` on the binlog leg, so no single literal is correct on both.

**Foreign-key `MATCH FULL` and `DEFERRABLE` were silently dropped, weakening or strengthening the target's constraints.** They fail in opposite directions. `MATCH FULL` on the source rejects a partially-NULL composite key that the plain constraint sluice landed accepts — the target admits rows the source forbids, permanently. `DEFERRABLE INITIALLY DEFERRED` lets `BEGIN; INSERT child; INSERT parent; COMMIT;` succeed, and against the immediate constraint sluice landed that same transaction *aborts* — a workload that worked before cutover breaks after it. Both are now carried and emitted on Postgres targets. A MySQL-family target can represent neither (InnoDB parses `MATCH FULL` and ignores it, and has no deferred constraints), so it now WARNs with the consequence spelled out rather than shipping the difference unmentioned. The same fix covers `DEFERRABLE PRIMARY KEY`, which a shared internal gate had excluded from both the carry and the warning.

**A Postgres CDC stream died on connection blips it should have ridden out.** The pump parked errors raised while dispatching WAL without classifying them, and that path runs live catalog queries (`pg_attribute` on every relation message, `pg_index` on the identity path). Postgres re-sends relation messages on first touch after every reconnect — exactly when a pooler is least healthy — so a routine fault there was terminal. The same sweep found the server-error path flattening its response to text before classifying, which made the SQLSTATE leg unreachable by construction, and the cold-copy read path parking its errors raw where MySQL has classified them since ADR-0109. That last one is narrower than this entry originally claimed: it makes the Postgres reader classify a mid-table drop as MySQL already did, and ADR-0109's reconnect-and-resume covers the per-table FULL-SCAN read — but the CHUNKED and raw-copy cold-copy paths have no such retry on EITHER engine, so a transient there still aborts the copy and `--resume` is the recovery. Corrected in v0.103.1 after the post-release regression cycle showed a Postgres and a MySQL source failing identically at those sites; the remaining gap is filed as roadmap item 86.

**A warm resume no longer accepts a changed `--where` on MySQL, Vitess, or Postgres below 15.** The drift check existed but covered only the Postgres-pushed subset. A warm resume re-snapshots nothing, so a changed predicate leaves the target holding what the *original* filter copied while the continuous leg classifies under the new one: narrowing strands out-of-scope rows forever, widening never backfills what the cold start skipped.

**`--backfill-added-column` now respects `--where`.** It paginated the entire table on a filtered sync. Nothing leaked, but the bounded source-read volume a filter exists for was silently defeated.

**A telemetry scrape that isn't a Prometheus exposition is now a failed poll.** An HTML or JSON error page served with HTTP 200 parsed to zero samples with no error, and the provider then overwrote its good cached snapshot with an all-unknown one and recorded the poll as successful — strictly worse than an HTTP error, which correctly leaves the last reading in place. Symptoms were `n/a` everywhere, every `--notify-*` threshold silently ceasing to fire, and persisted rows marked fresh with every metric null.

**A half-observed metric no longer reports a fabricated zero.** Active and max connection counts come from independent metric series and either can resolve while the other doesn't; one shared "known" flag published the missing half as `0`. That zero reached the JSONL sink (whose contract promises an unobserved metric serializes as null), the `/metrics` endpoint (where an absent series is how Prometheus says "not observed"), and the history table as a non-NULL value the read side decodes as observed. Operator output rendered `37/0` — a target with no connection budget at all. It now renders `37/?`.

**A short write to `--sink-file` no longer destroys the next record.** A write stopping mid-record left the file ending in a partial line; the next append glued a fully-valid record onto the fragment, producing one unparseable line that swallowed a record the sink had reported as written. Seen tears now roll back to the last complete record; unseen ones (a `kill -9`, a power loss) are terminated on reopen rather than truncated, since those bytes are the operator's.

### Security

**PlanetScale service tokens no longer appear in diagnose bundles.** A bundle exists to be attached to a GitHub issue, and the token was landing in it verbatim — two lines below a correctly-redacted DSN, which is why review missed it: the bundle looked sanitized. The service-token family and `--sink-http` (a signed URL carries its credential in the query string) are now redacted, and a gate walks the real CLI model so a future credential-shaped flag cannot be added without coverage.

### Compatibility

No breaking changes, no flag changes, drop-in upgrade. Three predicate shapes that previously produced silently wrong results now refuse at startup — a generated-column filter, a non-canonical identifier literal, and any filter on a fractional-second `TIME` column — each naming what to write instead. Foreign-key attributes newly emitted on Postgres targets make the target match the source; MySQL targets are unchanged apart from the new warning. Streams established under an earlier version keep resuming: the widened `--where` drift check accepts the older recorded form.

## Who needs this

- **Anyone running `sync --where`** — read the first two entries above. If your predicate references a generated column, or uses a non-canonical uuid / inet / macaddr / time literal, the continuous leg has been dropping rows. Both now refuse at startup, so upgrading tells you immediately whether you were affected.
- **Anyone migrating Postgres schemas with `MATCH FULL` or `DEFERRABLE` constraints** — the target has been getting a constraint of a different strength than the source. Re-run the schema step, or add the attributes on the target by hand.
- **Anyone who has attached a diagnose bundle to an issue while using `metrics-watch` with a service token** — rotate that token.
- **Anyone running Postgres-source CDC through a pooler** — connection blips during relation-message bursts were killing streams that should have reconnected.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.103.0
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.103.0`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
