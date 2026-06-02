# Announcement post — sluice v0.97.x draft

Draft text for a "sluice exists, here's why you might care" post
suitable for Reddit `/r/PostgreSQL` / `/r/mysql` /
`/r/dataengineering`, Hacker News, Lobsters, and the
project's own X/Mastodon. Adapt the framing per platform — HN
prefers technical specificity, Reddit prefers operator-pain
framing, X/Mastodon needs a tight intro + link.

Status: **draft**. The numbers and capabilities are accurate as
of v0.97.1; this is here to be lightly adapted, not posted verbatim.

---

## Tight version (Reddit / Mastodon / X)

> **sluice — an open-source MySQL ↔ Postgres migration + continuous-sync tool**
>
> Single static binary, no daemon, no Perl. All four directions
> (MySQL → MySQL, MySQL → PG, PG → PG, PG → MySQL). PlanetScale-MySQL
> via VStream registered as an engine variant.
>
> Built around three product surfaces — `migrate` (one-shot), `sync`
> (snapshot + CDC catch-up), `backup` (chunked + at-rest encrypted) —
> with a typed dialect-neutral IR so engine-specific knowledge never
> leaks across the layer.
>
> Opinionated about correctness: every silent-loss class we've caught
> has a structured `refuse loudly` message + operator-action recovery
> hint. Recent v0.93.0 → v0.97.1 arc closed nine numbered silent-loss
> classes (DOMAIN constraints dropped on cross-engine, passphrase
> rotation silently accepted in encrypted backup chains,
> mid-CDC-stream RENAME / DROP COLUMN silently lost — the kinds of
> bugs you only find by looking).
>
> Honest comparison vs. Bucardo (the canonical open-source PG → PG
> CDC tool) and against `pg_dump` + `pg_restore` in the docs. Apache
> 2.0. Pre-built binaries on every tagged release.
>
> 🔗 https://github.com/orware/sluice
> 🔗 https://github.com/orware/sluice/blob/main/docs/comparison-bucardo.md
> 🔗 https://github.com/orware/sluice/blob/main/docs/cookbook/case-gitlab.md

## Longer version (Hacker News)

> **Show HN: sluice — opinionated open-source MySQL ↔ Postgres
> migration + continuous-sync**

sluice is the tool I wished existed when I had to move data between
MySQL and Postgres on managed-PG infrastructure. The category-defining
commercial tools (HVR / Striim / Qlik Replicate) sit at enterprise
price points; the open-source landscape is fragmented (Bucardo for
PG-to-PG trigger-based, pgcopydb for fast PG-to-PG snapshot,
Debezium for CDC-into-Kafka, AWS DMS if your target is AWS). What
nobody had was: **single binary, all four directions, managed-PG
friendly, opinionated about loud failure.**

Three product surfaces, each independently runnable:

- **`sluice migrate`** — one-shot snapshot, MySQL ↔ Postgres in any
  direction. Parallel COPY/LOAD-DATA per table; deferred indexes
  and constraints to avoid bulk-load slowdowns; resumable via
  per-target state table.
- **`sluice sync start`** — snapshot + CDC catch-up + cutover.
  Pgoutput on PG; binlog (vanilla MySQL) or VStream
  (PlanetScale-MySQL) on the MySQL side. Position persistence in
  the same transaction as data writes so resume is correct across
  crashes.
- **`sluice backup`** — chunked, resumable, segment-rotated logical
  backups with at-rest encryption (passphrase or AWS KMS), per-chunk
  passphrase rotation, restore on top.

Three opinionated stances that matter more than the feature list:

1. **IR-first.** Every cross-engine translation passes through a
   typed dialect-neutral IR in `internal/ir`. Source-specific
   knowledge lives in readers; target-specific knowledge lives in
   writers; the IR is the only shared contract. No regex over DDL
   strings, no engine-specific imports leaking into the orchestrator.

2. **Loud failure by default.** Every silent-loss class we've caught
   has a structured refuse-loudly message + recovery hint. The recent
   v0.93.0 → v0.97.1 arc closed nine numbered silent-loss classes —
   PG DOMAIN constraints silently dropped to base type on cross-engine
   migrate, passphrase rotation silently accepted in encrypted backup
   chains, mid-CDC-stream `RENAME` / `DROP COLUMN` silently lost,
   etc. The fixes are all in the open; see the BUG-CATALOG.md in
   `orware/sluice-testing` for the full record.

3. **No SaaS dependency.** Single static binary, Apache 2.0, no
   daemon, no control DB, no Kafka, no Perl interpreter. Runs in a
   k8s job, a systemd unit, a one-shot CI step, or interactively
   from your laptop. The author runs it himself on production
   workloads.

Honest comparison docs against the alternatives:

- vs. Bucardo (the canonical OSS PG → PG CDC tool):
  [`docs/comparison-bucardo.md`](https://github.com/orware/sluice/blob/main/docs/comparison-bucardo.md).
  Measured head-to-head. Bucardo wins runtime CDC throughput;
  sluice wins setup, cross-engine, single-binary deploy, and clean
  teardown. The choice is "PG-only and tuned for runtime
  performance" vs "cross-engine, managed-PG-first, single-binary,
  opinionated about teardown."
- vs. `pg_dump` + `pg_restore`:
  [`docs/cookbook/compare-pg-dump.md`](https://github.com/orware/sluice/blob/main/docs/cookbook/compare-pg-dump.md).
  Short version: if your migration fits `pg_dump`'s shape, use
  `pg_dump`. sluice fits the cross-engine, continuous-sync,
  redaction-pipeline, and low-downtime cutover cases.
- Real-world schema case study (GitLab shape — DOMAINs, range
  types, EXCLUDE constraints, partial / covering / functional
  indexes):
  [`docs/cookbook/case-gitlab.md`](https://github.com/orware/sluice/blob/main/docs/cookbook/case-gitlab.md).

I have no commercial interest in sluice; it's a multi-month side
project that reached the point where the public surface is worth
showing. The roadmap is largely shipped — most remaining items are
demand-gated. If you'd run it against an interesting workload and
file the bugs you find, the project would benefit. If you decide
Bucardo or `pg_dump` fits better, the comparison docs walk through
which is right for your shape.

🔗 Repo: https://github.com/orware/sluice
🔗 Quickstart (10-min walkthrough):
   https://github.com/orware/sluice/blob/main/docs/examples/quickstart.md
🔗 Latest release: https://github.com/orware/sluice/releases/latest

## Reddit version (operator-pain framing)

> **TIL: open-source MySQL ↔ Postgres CDC tool that doesn't need
> Kafka or Perl**

Title above is for `/r/dataengineering` / `/r/mysql` / `/r/PostgreSQL`.
Posts to a per-subreddit shape; the body is roughly the same.

Hey — putting this out there because I've spent more time than I'd
like building one-off MySQL ↔ Postgres replication scripts on
managed-PG and figured it was time to ship something I'd actually
want to install.

**sluice**: open-source, Apache 2.0, single static binary, all four
directions (MySQL → MySQL, MySQL → PG, PG → PG, PG → MySQL).
PlanetScale-MySQL and PlanetScale-Postgres flavored as registered
engine variants. No Kafka. No Perl. No daemon. No SaaS dependency.

What you can do with it:

- One-shot migration with `sluice migrate`
- Continuous sync (snapshot + CDC catch-up + cutover) with
  `sluice sync start` and `sluice cutover`
- Chunked encrypted backup chains with `sluice backup full` and
  `backup stream run`
- PII redaction with a persisted keyset for cross-stream
  determinism (26 strategies including format-preserving SSN / PAN
  / email masks)

What it's opinionated about:

- **Loud failure.** Every silent-loss class we've caught has a
  structured refuse-loudly message + operator-action hint. Recent
  v0.93.0 → v0.97.1 arc closed nine numbered silent-loss classes
  in BUG-CATALOG.md — DOMAIN constraints lost on cross-engine
  migrate, passphrase rotation silently accepted in encrypted
  backup chains, mid-CDC-stream `RENAME` silently lost, etc.
- **Managed-PG friendly.** Heroku Postgres / RDS / Crunchy
  Bridge / Supabase Free — sluice's PG source path doesn't need
  superuser or the REPLICATION role attribute for `migrate`-only
  workflows. The trigger-based slot-less engine variant ships
  for the CDC case where the source can't grant `REPLICATION`.

Honest comparisons in the docs:

- vs. Bucardo (the canonical FOSS PG → PG CDC):
  [comparison-bucardo.md](https://github.com/orware/sluice/blob/main/docs/comparison-bucardo.md).
  Measured numbers. Bucardo wins runtime CDC; sluice wins
  setup / cross-engine / clean teardown.
- vs. `pg_dump` + `pg_restore`:
  [compare-pg-dump.md](https://github.com/orware/sluice/blob/main/docs/cookbook/compare-pg-dump.md).
  Short version: if `pg_dump` fits your shape, use it. sluice fills
  the cross-engine + CDC + redaction gap.
- Real-world schema case study (GitLab-shape):
  [case-gitlab.md](https://github.com/orware/sluice/blob/main/docs/cookbook/case-gitlab.md).

If you've been on Bucardo for years and it's working, this isn't a
reason to change. If you've been writing one-off scripts for managed
PG to MySQL migrations, give sluice a look. Bugs welcome —
particularly any silent-loss class you find. They're the only ones
that matter.

🔗 https://github.com/orware/sluice

## Per-platform tweaks

- **HN:** lead with the technical specificity (IR-first, the BUG-CATALOG
  number, the loud-failure tenet). HN respects projects that have
  shipped real silent-loss-class fixes and have the receipts.
- **Reddit:** lead with the operator-pain framing. The TIL framing
  works well in `/r/dataengineering`; the `/r/PostgreSQL` audience
  cares about the Bucardo comparison; `/r/mysql` cares about the
  PlanetScale-MySQL VStream support.
- **Lobsters:** the HN version compressed by ~30% works. Lobsters
  prefers technical specificity over feature lists.
- **X / Mastodon:** the tight version, broken into 2-3 toots with a
  thread linking the comparison docs.

## Things to NOT promise

- **"It will replace Bucardo for you."** It won't if you're on
  PG-only infrastructure with no cross-engine needs.
- **"It's faster than HVR."** It probably isn't on runtime CDC
  throughput; HVR is enterprise-tuned. sluice's "category" is
  open-source single-binary deployment, not commercial throughput.
- **"It supports every PG extension."** It supports an enumerated
  allowlist of 7 (pgvector, pg_trgm, hstore, citext, postgis,
  pgcrypto, uuid-ossp). Uncatalogued extensions get
  `ir.VerbatimType` passthrough on same-engine paths only; cross-
  engine refuses loudly.

## Things to NOT bury

- The **measured** Bucardo comparison — every number is measured,
  the test rig is documented, reproduction notes are in the report.
- The **honest** corrected latency framing — v0.97.0's default-config
  single-change CDC latency is ~0.88s ≈ Bucardo's ~0.95s, not the
  ~5.9s the original benchmark misattributed to sluice (config
  artifact, see comparison-bucardo.md's TL;DR).
- The **roadmap maturity** — most of the roadmap is shipped; the
  remaining items are demand-gated. This is unusual for an OSS
  project at this stage and worth flagging.

## After posting

- Monitor the thread for an hour or two. Bugs / questions get filed
  as GitHub issues with the discussion thread linked.
- Don't argue. If someone says "this is just X," reply with the
  comparison doc link and let them decide. The honest framing is
  load-bearing; arguing erodes it.
- If a real operator workload shows up that would benefit from a
  deferred translator-catalog rule (item 5 in the roadmap) or a
  deferred bug fix (Bug 17, Bug 25), thank them, file the issue,
  treat it as the demand-gating signal.
