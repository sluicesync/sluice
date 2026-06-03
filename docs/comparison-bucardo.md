# sluice vs. Bucardo — the open-source PG → PG comparison

Bucardo is the canonical open-source comparison point. If you've been doing
Postgres replication outside of native logical replication for any length of
time, you know it: trigger-based, mature since 2002, plperl + control DB,
NOTIFY-driven apply. This page is the honest, measured comparison most
operators end up asking for: *"isn't sluice just Bucardo?"*

**Short answer.** No — but Bucardo is genuinely good, and the framing this
page tries to land is *which* of the two you should pick, not which is
"better." The two tools have **different sweet spots** and reach for
**different operator personas**.

This page exists because the canonical critique of sluice would be "ship a
trigger-based PG replicator? Bucardo already exists." That critique is fair
to address head-on, not dodge.

---

## TL;DR

- **You're staying on Postgres forever, on infrastructure you control, and
  CDC latency / runtime throughput matter most:** Bucardo is a legitimately
  good choice and probably what you want.
- **You're on a managed PG (Heroku, RDS, Supabase, Crunchy Bridge,
  PlanetScale-Postgres) without superuser / replication-role, OR you want
  cross-engine to MySQL, OR you want a single static binary with no Perl
  control DB to operate, OR you care about clean teardown:** sluice is the
  better fit.

The choice isn't "mature vs new." It's "PG-only and tuned for runtime
performance" vs "cross-engine, managed-PG-first, single-binary, opinionated
about teardown."

---

## What sluice and Bucardo share

Both are **trigger-based** Postgres change capture replicators. Neither
uses logical replication slots. Both install plpgsql triggers on the
source tables and replicate from a capture log. Both can run on slot-less,
non-superuser PG (this directly contradicts a common assumption that
Bucardo needs superuser on the source — its *source-side* triggers are
plpgsql; only its **control DB** needs superuser + plperl).

Both are correct: every measured run in the head-to-head was
**byte-identical** at the ordered-md5 level across the full table after
the change stream drained. numeric / jsonb / bytea / timestamptz all
preserved exactly by both tools.

So the comparison isn't about correctness. It's about:

1. **Operational shape** — what do you have to install, configure, debug?
2. **Runtime performance** — CDC throughput, latency, source overhead.
3. **Lifecycle hygiene** — what's left behind when you tear it down?
4. **Coverage** — what targets / source environments does each support?

---

## Headline numbers (local controlled, both with their respective defaults)

These are measured against a 1,000,000-row PG-16 source, mixed types
(bigint PK, text, `numeric(20,6)`, boolean, jsonb, timestamptz, bytea,
varchar). Change stream = 110,000 changes (100k UPDATEs + 5k INSERTs +
5k DELETEs). Each tool ran against its own clean target.

| Metric | **sluice postgres-trigger** | **Bucardo 5.6.0** |
|---|---|---|
| Tool install | 93 MB single binary, 0 deps | apt package + plperl-enabled control DB + libdbi-perl + libdbd-pg-perl + daemon |
| Time-to-first-sync (first time) | ~5 s | ~10 min (control-DB blockers, below) |
| Time-to-first-sync (once familiar) | ~5 s | seconds |
| Setup commands | `sluice trigger setup … && sluice migrate …` | `bucardo add db … && add table … && add sync … && start && kick` |
| **Initial copy (1M rows)** | **4.80 s** (~208,000 rows/s, 8 parallel COPY chunks) | 12.9 s (~77,000 rows/s, single COPY) |
| **CDC drain throughput (110k changes)** | ~2,500 changes/s (sluice numbers shown as measured at v0.89.0; item-18 applier-latency fix landed post-benchmark, expected ~2× lift to ~5,000) | ~13,000 changes/s (NOTIFY + efficient delta replay) |
| **Single-change latency (default config)** | **~0.88 s** — poll-floor at ~1 s | **~0.95 s** — NOTIFY-kick |
| **Source-side write overhead (50k UPDATE)** | ~3,946 ms (~10.8× the no-trigger baseline) | ~742 ms (~2× baseline) |
| **Source residue after teardown** | **0 triggers, 0 capture tables** (`trigger teardown` cleans 100%) | `bucardo` schema (21 objects) + `bucardo_delta` / `bucardo_kick` triggers per table — **operator must drop manually** |

A few of these deserve their own sections — they're more interesting than
the numbers suggest at a glance.

### On the latency numbers

The original benchmark wrote a "~5.9 s sluice vs ~0.95 s Bucardo" headline.
**That figure was a config artifact** — it was measured with
`--apply-batch-size=auto`, which engaged the batched apply path, which had
a 5 s idle-flush timer that fired on partial batches. Under sluice's
*default* config (`--apply-batch-size=1`, the per-change apply path), the
real measurement is ~0.88 s — poll-floor, essentially tied with Bucardo's
~0.95 s.

Bucardo's NOTIFY-kick *is* genuinely lower-latency than a fixed poll at
equal poll cadence — that's a real architectural delta. But it's
sub-second tied at sluice's defaults, not the ~6× the original write-up
implied. Both batched-apply bugs (idle-flush + AIMD batch-latency miscount)
were fixed in PR #88 / `item-18-applier-latency`, which also lifts the
drain figure ~2× from the throttled measurement above.

### On the source overhead

This is a genuine design tradeoff, not a defect in either tool:

- **sluice's `sluice_capture` trigger** writes the full before/after row
  as JSONB into `sluice_change_log`. The CDC reader is then
  **self-contained** — it never re-reads the source row. ~10.8× write
  amplification on the source.
- **Bucardo's `bucardo_delta` trigger** writes only the changed primary
  key to a delta table; the daemon re-reads the live row at sync time.
  ~2× write amplification.

Bucardo's approach is faster on the source. sluice's approach is
*replay-safe* — a row that was subsequently deleted or further changed
between capture and apply still replays correctly because the captured
image is the truth. The cost is paid on the source's write path; the
benefit lands at the apply path's robustness.

For high-write-rate sources where source-side latency budget matters
(Bucardo's strongest case), sluice's `--capture-payload` modes
(`full` / `changed` / `minimal`, ADR-0068) let operators trade self-
containment for source overhead — `minimal` reaches toward Bucardo's
~2× overhead by storing the changed PK + columns and using the live row
at apply time. Default stays `full` (self-containment first; operators
opt in to leaner payloads with the head-to-head measurement plan in the
ADR).

### On the source residue

This is one of the few rows where the tools differ on a quality-of-life
property rather than a numeric tradeoff. Bucardo's own documentation
notes: *"table triggers are not automatically removed!"* After
`bucardo remove sync` the source still has every per-table delta /
track / kick trigger plus the `bucardo` schema (21 objects in our
measurement). Operators clean this up manually.

sluice's `sluice trigger teardown` removes 100% of source-side state.
Verified on Heroku standard-0 — zero residue. This is small in steady
state and noticeable when you're tearing things down for the second
or third time.

---

## Where Bucardo is strictly better

- **CDC drain throughput** — NOTIFY-driven, efficient delta replay. After
  sluice's item-18 fix the gap narrows from ~5× to ~2–3× but doesn't
  close to parity. If you're sustaining tens of thousands of changes per
  second and runtime drain is your bottleneck, Bucardo's the right pick.
- **Source-side write overhead** — ~2× vs sluice's ~10.8× at default
  payload. For write-amplification-sensitive sources, Bucardo's PK-only
  delta trigger wins by design. sluice's `--capture-payload=changed` or
  `=minimal` narrows it; Bucardo's default already does it.
- **Multi-master / multi-target topologies** — Bucardo's native model
  supports source-source replication and N-target fan-out cleanly. sluice
  is one-way source→target by design; fan-out is N processes, not one.
- **Maturity** — 23+ years in production. Large install base. Many
  battle-tested edge cases sluice hasn't encountered yet.

These are real wins; if your workload is dominated by them, Bucardo is
the answer.

---

## Where sluice is strictly better

- **Single-binary deploy, zero deps.** No Perl interpreter, no control
  database, no daemon to supervise. `sluice trigger setup` is one
  subcommand. The full Bucardo install on a stock `postgres:16` image
  required: (1) `apt-get install postgresql-plperl-16` on the *PG
  server image* (not just the bucardo client); (2) `apt-get install
  libdbi-perl libdbd-pg-perl` also on the PG server image (Bucardo's
  core functions are `plperlu` and `use DBI` inside the embedded Perl);
  (3) a `.bucardorc` carrying `dbpass=` plus auth wiring (`bucardo
  install` ignores `PGHOST`). None of these have a sluice analogue.
- **Cross-engine to MySQL / PlanetScale.** sluice's IR can target MySQL
  through the same `migrate` / `sync start` commands; the
  postgres-trigger → MySQL roadmap item lands the trigger flavor on the
  cross-engine path. **Bucardo is Postgres-only and has no path to
  MySQL.** This is the structural differentiator that no benchmark
  number captures: if your target *isn't* PG, Bucardo isn't an option.
- **Initial copy throughput** — ~2.7× faster in the head-to-head
  (208k vs 77k rows/s). sluice runs 8 parallel COPY chunks by default
  on tables above the threshold; Bucardo runs a single COPY.
- **Managed-PG support out of the box.** sluice is opinionated about
  refusing loudly when a managed source can't grant what's needed
  (e.g. event-trigger creation on RDS/Heroku) and offering the
  operator-actionable path (`--allow-polled-fingerprint`). Bucardo
  works on managed PG too, but the control-DB requirements push
  operators toward self-hosting the control DB, which is a separate
  operational stance.
- **Clean teardown.** Already covered above. `trigger teardown` →
  zero source residue. Verified on production-grade managed sources.

---

## When NOT to use sluice (and when Bucardo wins by default)

If **all** of the following apply, Bucardo is probably the right
choice:

1. Your source is PG and your target is PG, both on infrastructure you
   control (no managed-PG slot-less constraint).
2. You're comfortable with Perl-based operational tooling and the
   control-DB requirement.
3. CDC drain throughput / single-change latency is your binding
   constraint.
4. You don't need cross-engine targets (no MySQL, no PlanetScale-MySQL).
5. Multi-master or N-target fan-out is on your near-term roadmap.

If **any** of the following apply, sluice is probably the right
choice:

1. Your source is on a managed PG without superuser or replication-role
   (Heroku, RDS, Crunchy Bridge, Supabase, PlanetScale-Postgres) and
   you can't (or don't want to) self-host a control DB.
2. Your target is MySQL — Bucardo simply doesn't go there.
3. You want a single static binary you can drop into a container, a
   k8s job, or a systemd unit without provisioning side infrastructure.
4. Operational hygiene matters — you want clean teardown without manual
   `DROP TRIGGER` / `DROP SCHEMA` follow-up.
5. You care about loud failure on edge cases (passphrase rotation
   mid-chain, schema-history drift, slot loss on failover) more than
   raw throughput.

---

## Setup blockers we hit installing Bucardo (for the curious)

These aren't critiques — Bucardo *works* once they're sorted. They're
operational facts worth knowing before you start, because each is
~30-90 minutes of debugging if you've never seen it:

1. **Control DB needs `plperl` / `plperlu`.** Stock `postgres:16`
   doesn't ship it. `bucardo install` fails on
   `extension "plperl" is not available`. Fix: build a custom image
   with `apt-get install postgresql-plperl-16`.
2. **Control DB's *server-side* embedded Perl needs `DBI` + `DBD::Pg`.**
   Bucardo's core functions are `plperlu` and call `use DBI` from inside
   the database server. Without them, `bucardo install` fails at
   `bucardo.schema:854` with *"Can't locate DBI.pm … in PL/Perl function
   db_getconn"*. Fix: also `apt-get install libdbi-perl libdbd-pg-perl`
   **on the PG server image**, not the bucardo client image.
3. **`bucardo install` ignores `PGHOST`.** It needs explicit
   `--dbhost/--dbport/--dbuser` flags + a `.pgpass` + a password on
   the created `bucardo` role + a `.bucardorc` carrying `dbpass=`.

sluice's setup is `./sluice trigger setup --dsn=… --tables=bench` —
0.14 s, no dependencies to install on any side. This isn't a fairness
issue; it's a different operational model.

---

## How to decide

Read the [head-to-head report](https://github.com/sluicesync/sluice-testing/blob/main/session-reports/bucardo-vs-sluice-v0.89.0.md)
(public, single file, every number measured with command-line
reproduction notes) if you want the underlying data. Then:

1. **If you're already comfortable with Bucardo and it's working for
   you, this page isn't a reason to change.** Bucardo is mature and
   genuinely good at what it does.
2. **If you're evaluating CDC tooling for the first time and your
   source is managed PG or your target is MySQL,** start with sluice.
3. **If you need both** — sluice for the managed-PG / cross-engine
   migration, Bucardo for the long-running same-engine same-infra
   replication after the migration completes — they coexist. The
   triggers don't interfere with each other (different table names);
   the comparison just clarifies which one each piece of work calls
   for.

The honest framing: sluice is **not trying to displace Bucardo**.
sluice is filling the gap Bucardo doesn't cover — managed PG without
control DB, cross-engine to MySQL, single-binary deploy, opinionated
teardown — while staying competitive on raw correctness and on
default-config latency. If you've been on Bucardo for years and it's
not blocking you, stay. If you're starting fresh and one of sluice's
gap-filling properties matters, sluice.

See also:

- [`docs/comparison.md`](comparison.md) — the wider comparison vs Debezium / AWS DMS / Fivetran / pgcopydb / HVR / Striim / Qlik.
- [`docs/use-cases.md`](use-cases.md) — the operator-persona-by-persona breakdown.
- [`docs/postgres-source-prep.md`](postgres-source-prep.md) — what sluice needs from a PG source (very little; managed-PG-friendly).
