# `sluice verify` vs Vitess `vdiff` — design comparison

This doc compares sluice's verify approach to Vitess's `vdiff` workflow,
which operators familiar with PlanetScale or self-hosted Vitess will
have used or heard of. The goal is to give operators a clear mental
model of what sluice's verify does, where it's stronger, and where
it's weaker — so they know when to reach for sluice verify and when
to reach for vdiff (or both).

## TL;DR

Different tools for different jobs:

- **vdiff** is **full-fidelity row-by-row comparison** with collation-aware
  value comparison. Catches every mismatched cell. Heavy by design —
  streams every row from both sides. Built into the Vitess `MoveTables`
  workflow; runs during cutover validation. **Authoritative when it
  finishes.**
- **sluice verify** offers **count + statistical-sample** comparison
  with `MD5` (or `SHA-256`) row hashing. Catches rows that are wrong
  with adjustable confidence. Cheap to run continuously. **Statistical
  guarantee, not exhaustive coverage.**

For a one-shot post-migration "did everything land?" check, `sluice
verify --depth count` is the cheapest possible signal and works
across all four engines sluice supports. For "actually exercise the
data," `--depth sample` raises confidence; full-fidelity comparison
(coming in a future phase) will close the gap further.

For Vitess-only deployments cutting over a `MoveTables` workflow,
vdiff is the on-rails option and the right answer — it integrates
directly with `vtctldclient`'s workflow management.

## What vdiff does

Per the [Vitess vdiff docs](https://vitess.io/docs/archive/22.0/reference/vreplication/vdiff/)
and the [vdiff source](https://github.com/vitessio/vitess/blob/main/go/vt/vttablet/tabletmanager/vdiff/table_differ.go):

1. **Streams every row in PK order** from both source and target
   shards (via the `RowStreamer` API).
2. **Compares values field-by-field** via
   `evalengine.NullsafeCompare`, which is collation-aware — handles
   string collations, NULL semantics, and type coercion the way
   MySQL would on `WHERE a = b`.
3. **Reports missing / extra / unmatched rows** plus which columns
   differ for unmatched-but-PK-equal rows.
4. **Resumable** via `LastPK` checkpoints — interrupted vdiff runs
   can pick up where they left off.
5. **No hashing.** Direct value comparison.
6. **Heavy on tables without PK or PKE** — the docs explicitly call
   out the additional cost of full table scan + filesort.

Because vdiff compares every row, its runtime scales with table size —
a full read on both sides — so larger workflows take proportionally
longer. That cost is fundamental to any full-row verifier (sluice's own
deep-comparison modes included). Vitess reworked vdiff in v2 into a
managed, resumable VReplication workflow; see the [vdiff v2 announcement](https://vitess.io/blog/2022-11-22-vdiff-v2/)
for its current design and capabilities.

## What `sluice verify` does

Three depth modes (count is shipped, sample is shipped, full is
planned):

### `--depth count` (shipped v0.12.0)

```sql
-- single-shot fast path:
SELECT COUNT(*) FROM <table>;

-- chunked path on MySQL when the table has a single integer PK
-- (used to bypass PlanetScale's per-query row-read budget):
SELECT COUNT(*) FROM <table> WHERE pk >= ? AND pk < ?  -- × N chunks
```

One query per table per side. Catches "the table got truncated" or
"bulk copy lost half the rows." Misses every kind of value-level
drift. Cheapest possible probe — runs in seconds even on multi-TB
tables.

### `--depth sample` (shipped v0.14.0)

```sql
-- For each table, pick N rows deterministically (via MD5(pk||seed)),
-- compute MD5 of the row's column-values, compare hash sets.
SELECT
  <pk_expr> AS pk,
  MD5(CONCAT_WS('|', col1::text, col2::text, ...)) AS hash
FROM <table>
ORDER BY MD5(<pk_expr> || '<seed>')
LIMIT <N>;
```

Default N=100. Three drift shapes detected via merge-walk:

- PK on source only → target is missing the row.
- PK on target only → target has an extra row.
- PK on both, hashes differ → row content drift.

Server-side hashing means we ship one query per table per side;
no row data crosses the wire. Cheap. Statistical confidence
follows the binomial: with N=100 samples per table, ~99% chance
of detecting a 5% corruption rate; ~50% chance of detecting a
single bad row in a million-row table. Operators wanting higher
confidence raise N or use full mode.

### `--depth full` (planned)

Per the [verify proto-ADR](dev/design-sluice-verify.md), full mode
will compute a rolling content-hash over every row in PK order on
both sides, then bisect to find divergent ranges. Cost is one full
table scan per side — same fundamental cost as vdiff, just with
hashing instead of value comparison. Bisection narrows the
divergence to a small chunk for forensic drill-in.

## How they compare

| Dimension | vdiff | sluice verify |
|---|---|---|
| **Coverage** | Every row, every column | count: counts only • sample: N random rows • full (planned): every row |
| **Comparison method** | Direct value compare (collation-aware) | MD5 / SHA-256 row hash, merge-walk |
| **What it catches** | Every cell-level diff with the diffing column named | count: row-count mismatch • sample: PK or content drift; column-level granularity not surfaced |
| **Cost** | High — full table scan both sides; reputation: heavy on multi-TB | count: ~1 query × tables • sample: 1 query × tables × 2 sides • full (planned): same as vdiff |
| **Resumable** | Yes via LastPK checkpoints | No — one-shot CLI per run |
| **Cross-shard** | Yes — vdiff is shard-aware (Vitess-native) | sluice operates per-table; cross-shard handled by underlying engine |
| **Engine support** | Vitess-only (MySQL behind Vitess) | MySQL + Postgres + PlanetScale-MySQL + PlanetScale-PG |
| **Workflow integration** | Built into `MoveTables` cutover | Standalone CLI; cron-friendly exit codes |
| **Output** | Streamed report, often via `vtctldclient` | text / JSON; structured exit codes 0/1/2 |
| **Tables without PK** | Heavy (full scan + filesort) | count: works fine • sample: SKIPPED with reason |

## When to reach for which

**Reach for vdiff when:**
- You're using Vitess natively or PlanetScale, mid-cutover of a
  `MoveTables` workflow, and want vdiff's built-in workflow
  integration.
- You need to identify exactly which column drifted on which row
  (vdiff names the differing column; sluice sample identifies the
  drifting row's PK).
- You're willing to pay full-table-scan cost for full coverage and
  the cutover window allows it.

**Reach for sluice verify when:**
- You're running cron probes for sync-health monitoring (count mode,
  exit-code-driven alerting).
- Your migration is cross-engine (MySQL→PG or PG→MySQL — vdiff
  doesn't help; sluice does).
- You're on PlanetScale and want continuous probes that don't
  saturate the per-query row-read budget (chunked-count mode in
  sluice verify is built for this).
- You need a quick confidence check post-migration without paying
  full-scan cost.
- You're managing multi-engine sluice deployments and want the same
  CLI surface across all of them.

**Use both when:**
- Cutting over a high-stakes migration: vdiff for the authoritative
  pre-cutover validation, sluice verify on a cron post-cutover for
  ongoing drift detection.

## Hash-collision math: is MD5 sufficient?

Sample-mode hashes rows server-side via MD5 by default. The
operator-confidence question: can MD5 collisions cause sluice verify
to silently miss real drift?

**Math.** MD5 produces a 128-bit hash. The birthday paradox gives
the probability of any collision in N rows as approximately
`N² / (2 × 2^128)`. For sluice verify the relevant comparison is
"two rows in DIFFERENT positions on source and target produce the
same hash":

| Rows compared | P(collision) approx |
|---|---|
| 1,000,000 (1M) | ~2.9 × 10⁻²⁷ |
| 1,000,000,000 (1B) | ~2.9 × 10⁻²¹ |
| 1,000,000,000,000 (1T) | ~2.9 × 10⁻¹⁵ |
| 100,000,000,000,000 (100T) | ~3 × 10⁻¹¹ |

Effectively zero for honest-data scenarios at the row counts sluice
operators run. MD5's known cryptographic weakness — adversaries can
construct two messages with the same MD5 in O(2¹⁸) work — does NOT
apply to verify. There's no adversary; sluice's worry is "did the
data accidentally end up wrong," not "did someone deliberately
construct two rows that hash the same."

**`--strict-hash` opt-in (v0.14.2+)**: operators wanting an extra
margin (or matching a compliance posture that requires SHA-256) can
pass `--strict-hash` to switch to SHA-256 hashing. Same merge-walk;
just a wider hash space (2²⁵⁶ instead of 2¹²⁸). Cost: SHA-256 is
~2× slower than MD5 server-side, but at sample-mode's typical 100
rows × N tables, the difference is in milliseconds.

## Why MD5/SHA-256 and not xxhash?

Vitess uses [xxhash](https://github.com/cespare/xxhash) in its
`xxhash_vindex` for row-to-shard routing — fast (~10 GB/s in pure
CPU), well-distributed, non-cryptographic. A reasonable question is
whether sluice verify should use xxhash too.

**Short answer: not for the current server-side path; yes for a
future client-side path.**

**Why not server-side.** sluice verify's sample-mode runs the hash
inside MySQL or Postgres (`MD5(...)` / `SHA2(..., 256)` /
`SHA256(...)`) and ships only the hex-string hash over the wire.
Both engines have MD5 and SHA-256 in core. **xxhash is not in PG
or MySQL core** — it requires a third-party extension (`pgxxhash`
on PGXN; `xxhash_mysql_udf` for MySQL). PlanetScale doesn't allow
installing these. Forcing operators to install an extension just
to use a faster hash would defeat sluice's single-binary posture.

**Speed isn't the bottleneck for sample-mode anyway.** At the
default 100 rows per table × ~1 KB per row, MD5 takes well under
a millisecond; the wire round-trip (~10–100 ms) dominates.
Switching to xxhash would save microseconds of CPU but zero wall
time.

**Where xxhash would fit.** A future **client-side** hashing path
would stream raw row values from the server, canonicalize them
in Go (handling cross-engine value-rendering differences:
`TINYINT(1)=1` vs `BOOLEAN=t`, decimal precision, timestamp
formatting, etc.), then hash via `cespare/xxhash`. That's exactly
what's needed to unlock **cross-engine sample-mode** (the
verify proto-ADR's deferred open question). For that path:

- Hash function runs in the operator's process — no DB-side
  dependency, no extension footprint.
- Cross-engine canonical values mean source and target hashes
  align by construction.
- Speed of xxhash matters more on the per-row hot loop in a
  future full-mode (every-row) implementation.

When we tackle cross-engine sample (or full-mode if it goes
client-side), xxhash is the right choice for those code paths.
For the current server-side sample-mode, MD5 (default) and
SHA-256 (`--strict-hash`) remain correct.

## Complementary, not competing

vdiff and sluice verify aren't replacements for each other — they
target different operator workflows. vdiff is the right tool inside
a Vitess cutover; sluice verify is the right tool for cross-engine
migrations and continuous post-cutover probes. The fact that they
use different algorithms (direct value compare vs. row hashing) is a
design consequence of those different workflows, not a fundamental
disagreement about what verification means.

The full-mode roadmap in the verify proto-ADR will close the
fidelity gap when an operator wants vdiff-style every-row coverage
without leaving the sluice CLI. That's a future phase; sample-mode
plus the chunked-count fallback covers most operator-confidence
scenarios today.

## See also

- [Verify proto-ADR](dev/design-sluice-verify.md) — design rationale
  and phase plan.
- [Vitess vdiff documentation](https://vitess.io/docs/archive/22.0/reference/vreplication/vdiff/).
- [Sync-health monitoring proto-ADR](dev/design-sync-health-monitoring.md)
  — the liveness side of the "100% confidence" goal.
- [`docs/vitess-vstream-troubleshooting.md`](vitess-vstream-troubleshooting.md)
  — operator runbook for diagnosing VStream lag.
