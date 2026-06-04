# VStream COPY-phase row order and the source-side dedup silent-loss bug

Status: research findings (no production code changed). Authored for the
GitHub issue #14 follow-up after a ~70% silent-loss field report.

## TL;DR

- **Vitess does NOT order the COPY scan by the column sluice's dedup keys
  on.** sluice's `copyDedupTracker` keys on the columns carrying MySQL's
  `PRI_KEY_FLAG` in the FIELD event. Vitess orders the COPY `SELECT … ORDER
  BY` by `rs.pkColumns`, which for a table with **no explicit PRIMARY KEY**
  is a *primary-key-equivalent (PKE)* — the cheapest NON-NULL UNIQUE index
  by a column-type cost heuristic, **not necessarily the index sluice sees
  as "the PK."** When those two signals diverge, the dedup drops forward
  rows it mistakes for behind-the-scan re-emissions → silent loss.
- **Empirically reproduced** against `vitess/vttestserver:mysql80`: a table
  with a NOT-NULL `UNIQUE KEY id` plus a *cheaper* `UNIQUE KEY uk_tiny`
  (TINYINT) was scanned in `tiny` order; the dedup, keyed on `id`, **dropped
  9 of 12 rows (75%)** — the field report's signature.
- **The FIELD event the managed VStream delivers carries no scan-order PK
  signal** (`Pkfields` lives on the lower-level `VStreamRowsResponse`, which
  vtgate's managed VStream does not surface as a FieldEvent attribute). So
  sluice *cannot* fix this by reading a better field — the scan order is not
  observable on the stream sluice consumes.
- **Recommended fix:** delete the source-side dedup and make the VStream
  cold-start bulk-copy **idempotent on the target** via `INSERT … ON
  DUPLICATE KEY UPDATE` (PlanetScale / `BulkLoadBatchedInsert` path) — the
  exact upsert form the CDC applier already uses (ADR-0010). This absorbs
  *both* re-emissions and the forward rows that the order-mismatch would
  otherwise drop, at a modest, bounded throughput cost. The truly-keyless
  table (no PK and no NON-NULL UNIQUE) must **refuse loudly** — there is no
  key to make the copy idempotent on, and dedup was already a no-op there.

---

## 1. The bug

`internal/engines/mysql/vstream_copy_dedup.go` implements a COPY-phase dedup
that exists to absorb Vitess's "behind-the-scan re-emission" during COPY
mode (GitHub #14). Its invariant (file doc, lines 56–61):

> Vitess's COPY scan emits ROW events in PK-ascending order within a single
> (keyspace, shard, table) scope. Any ROW event with PK ≤ the maximum PK
> already seen for that scope is a behind-the-scan emission and must be
> dropped.

`shouldKeep` (lines 166–194) drops any row whose PK-tuple is `<=` the running
max-seen. The PK columns it keys on are derived in `recordFields` (lines
120–145) from the FIELD event's per-field `MySqlFlag_PRI_KEY_FLAG` bit.

**The invariant is false for tables without an explicit PRIMARY KEY.** A
field migration of a ~19M-row PlanetScale `connections` table — a `bigint
unsigned AUTO_INCREMENT id` with `UNIQUE KEY id (id)` but **no PRIMARY KEY**,
plus several other UNIQUE/secondary keys — silently dropped ~13.5M rows
(~70%). `min(id)`/`max(id)` were preserved; ~13.5M scattered ids were
missing. That is precisely the signature of "dedup keyed on `id`, but the
COPY emission was NOT `id`-ordered": the scan visited a high id early, the
running max jumped, and every later row with a smaller id was dropped as a
phantom re-emission.

---

## 2. Vitess source ground truth — what the COPY scan orders by

All citations are against a `--depth 1` clone of `vitessio/vitess` kept at
`C:\code\vitess` (main branch as of 2026-06-04).

### 2.1 The scan is ORDER BY `rs.pkColumns`

`go/vt/vttablet/tabletserver/vstreamer/rowstreamer.go`:

- `buildSelect` (lines 247–332) emits the COPY query. The trailing clause
  (lines 325–330) is an explicit `ORDER BY` over **`rs.pkColumns`**, in
  order. There is always an ORDER BY; the scan is never unordered.
- The same `rs.pkColumns` drive the `force index (…)` hint (lines 283–289,
  via `st.PKIndexName`) and the resume-`WHERE` for subsequent copy cycles
  (lines 291–320). So the emission order is exactly `rs.pkColumns` ascending.

So the question reduces to: **what is `rs.pkColumns` for cases (a) explicit
PK, (b) implicit/UNIQUE-only PK, (c) no key?**

### 2.2 `buildPKColumns` — the three cases

`rowstreamer.go` `buildPKColumns` (lines 218–245):

```
if len(rs.ukColumnNames) > 0 { return buildPKColumnsFromUniqueKey() }   // ukColumns directive override
if len(st.PKColumns) == 0 {
    pkColumns, err := rs.vse.mapPKEquivalentCols(ctx, rs.cp, st)        // (b) PKE fallback
    if err == nil && len(pkColumns) != 0 { return pkColumns, nil }
    // (c) no PK and no PKE → EVERY column, in ordinal order
    pkColumns = make([]int, len(st.Fields)); for i := range … { pkColumns[i] = i }
    return pkColumns
}
for _, pk := range st.PKColumns { … }                                  // (a) explicit PK
st.PKIndexName = "PRIMARY"
```

- **(a) Explicit PRIMARY KEY.** `st.PKColumns` is populated (see §2.3); the
  scan orders by the PK. `PKIndexName="PRIMARY"`. Matches sluice's
  assumption — `PRI_KEY_FLAG` and the scan order coincide.
- **(b) No explicit PRIMARY KEY.** `st.PKColumns` is empty, so Vitess calls
  `mapPKEquivalentCols` → `mysqlctl.GetPrimaryKeyEquivalentColumns`. The PKE
  is **the cheapest NON-NULL UNIQUE index**, chosen by a per-column-type
  cost heuristic. The scan orders by **whatever index that picks** — which
  is *not necessarily* `id`.
- **(c) No PK and no NON-NULL UNIQUE.** Falls back to ordering by **every
  column** in ordinal order. (sluice's dedup is already a no-op here — no
  `PRI_KEY_FLAG` columns — so this case never mis-dropped; but it also can't
  be made idempotent on a key, see §4.)

### 2.3 `st.PKColumns` only counts the index literally named `PRIMARY`

`go/vt/vttablet/tabletserver/schema/engine.go` `populatePrimaryKeys` (lines
856–876) runs `mysql.BaseShowPrimary`. That query
(`go/mysql/schema.go`, lines 30–34) is:

```sql
SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND LOWER(INDEX_NAME) = 'primary'
ORDER BY table_name, SEQ_IN_INDEX
```

It matches **only** an index named `PRIMARY`. A `UNIQUE KEY id` is named
`id`, not `PRIMARY`, so for the `connections` shape `st.PKColumns` is empty
→ the PKE fallback (b) fires. (InnoDB promotes a NOT-NULL unique to the
clustered index *physically*, but `information_schema.STATISTICS` still
reports its `INDEX_NAME` as the user-given name, not `PRIMARY` — confirmed
empirically in §3: case (b) ordered by `id` only because `id`'s unique key
was the *only/cheapest* PKE, not because it was reported as `PRIMARY`.)

### 2.4 The PKE chooses the cheapest NON-NULL UNIQUE by a type-cost heuristic

`go/vt/mysqlctl/schema.go` `GetPrimaryKeyEquivalentColumns` (lines 581–664).
The SQL (lines 598–636):

- considers only indexes that are **`NON_UNIQUE = 0 AND NULLABLE != 'YES'`**
  (NON-NULL UNIQUE), excluding any index with a nullable or non-unique
  column;
- ranks candidates by `SUM(type_cost) ASC, col_count ASC LIMIT 1`, where
  `type_cost` is a hardcoded per-data-type weight (enum=0, tinyint=1, …,
  int=7, bigint=10, …, varchar=61, …). **Smaller, cheaper-typed indexes
  win.**

So with two NON-NULL UNIQUE keys — `UNIQUE KEY id (bigint, cost 10)` and
`UNIQUE KEY uk_tiny (tinyint, cost 1)` — **`uk_tiny` wins** and the COPY
scan orders by `tiny`, **not `id`**. That is the exact divergence that
breaks sluice's dedup.

### 2.5 What sluice can actually see — the FIELD event has no scan-order PK

`rowstreamer.go` builds two independent things in `streamQuery`:

- `Pkfields` (lines 372–380) from `rs.pkColumns` — the **scan-order** PK,
  carrying the correct ordering signal — attached to the **`VStreamRowsResponse`**
  (line 389), the low-level rowstreamer response.
- `Fields` = `rs.plan.fields()` (line 388), whose per-column `Flags` come
  from the schema engine's column metadata (`getFields` →
  `field.CloneVT()`, `go/vt/vttablet/tabletserver/vstreamer/vstreamer.go`
  ~1122–1135, and `planbuilder.go` `fields()` 167–173). `PRI_KEY_FLAG`
  there reflects MySQL's column metadata for the actual PRIMARY/promoted
  index — **decoupled from `pkColumns`.**

sluice's snapshot reader consumes the **managed** VStream (vtgate
`client.VStream`), whose row payloads arrive as `VEventType_FIELD` +
`VEventType_ROW`. The FIELD event proto (`FieldEvent`,
`go/vt/proto/binlogdata/binlogdata.pb.go` lines 1492–1509) has **only**
`Fields []*query.Field` — **no `Pkfields`**. `Pkfields` exists solely on
`VStreamRowsResponse`/`VStreamTablesResponse` (pb.go 2495, 2669), which the
managed VStream does not relay to consumers as a FieldEvent attribute.

**Consequence:** the COPY scan order is *not observable* on the event stream
sluice reads. sluice cannot repair the dedup by keying on a better field;
the order assumption itself is the defect.

---

## 3. Empirical confirmation (run)

sluice has a local Vitess harness: `vitess/vttestserver:mysql80` (vtcombo)
via testcontainers, behind `//go:build integration && vstream`
(`internal/engines/mysql/cdc_vstream_integration_test.go`). It was quick to
stand up (~45s boot on the local Rancher Desktop), so a throwaway experiment
was run (then deleted — not committed).

**Setup.** Four tables, each seeded with 12 rows whose explicit `id` values
were inserted in a scrambled order (`50,10,90,30,70,20,110,40,80,60,100,5`).
A snapshot stream was opened via `Engine{Flavor: FlavorPlanetScale}.OpenSnapshotStream`
(the real COPY path, dedup live inline), each table drained via `ReadRows`,
and the post-dedup arrival order + `copyDedupTracker.dropCount` logged.

| table | shape | post-dedup arrival `id` order | dropped |
|---|---|---|---|
| `a_explicit_pk` | `PRIMARY KEY (id)` | `5,10,20,…,110` (all 12) | 0 |
| `b_implicit_uniq` | `UNIQUE KEY id (id)`, NOT NULL, no PK | `5,10,20,…,110` (all 12) | 0 |
| `c_implicit_uniq_plus_cheaper` | `UNIQUE KEY id` + cheaper `UNIQUE KEY uk_tiny (tinyint)` | **`5,100,110` (3 of 12)** | **9** |
| `d_nullable_uniq` | `UNIQUE KEY id (id)` **NULLABLE** | `5,10,20,…,110` (all 12) | 0 |

The dedup derived `pkColumns=[id]` for **all** of a/b/c (the `PRI_KEY_FLAG`
signal), confirming §2.5: it keys on `id` regardless of scan order.

**Reading the result:**

- (a) explicit PK and (b) `id`-only PKE both scan in `id` order → dedup
  matches → no loss. (This is why the bug hid: the common single-unique-key
  case is fine.)
- **(c) is the bug.** With a *cheaper* TINYINT unique key, Vitess ordered the
  COPY scan by `tiny` (cost 1) instead of `id` (cost 10) — §2.4. The dedup,
  keyed on `id`, saw a non-`id`-ascending arrival and **dropped 75% of the
  table** as phantom behind-the-scan rows. This is the field report's
  ~70%-loss mechanism, reproduced in miniature.
- (d) ordered by `id` here only because the nullable `id` unique key was the
  one MySQL promoted; the PKE query excludes nullable indexes, so on a real
  multi-key table a nullable `id` would *not* be the PKE and the scan would
  order by some other key — another route to the same divergence.

The `connections` field table had multiple UNIQUE/secondary keys, so it hit
the case-(c) class: Vitess picked a cheaper PKE than `id`, and the `id`-keyed
dedup shredded the copy.

---

## 4. Idempotent-writer fix — performance analysis

### 4.1 What the VStream cold-start bulk-copy writer is today

- The flavor capability `FlavorPlanetScale.BulkLoad = ir.BulkLoadBatchedInsert`
  (`flavor.go` 118–133). PlanetScale **cannot** use `LOAD DATA LOCAL INFILE`
  (documented; `flavor.go` line 97), so the VStream cold-start always lands
  on the batched-INSERT path.
- `RowWriter.writeBatched` (`row_writer.go` 372–416): buffers up to
  `defaultMaxRowsPerBatch = 500` rows (or `--max-buffer-bytes`, default
  64 MiB), then `buildBatchInsert` emits a single multi-row
  `INSERT INTO … (cols) VALUES (…),(…),…` via placeholders (423–456). No
  conflict clause — a duplicate PK **errors** (Error 1062), which is exactly
  why the dedup was bolted on in front of it.
- (Vanilla MySQL uses `BulkLoadLoadDataInfile` via `load_data_writer.go`, but
  that path is irrelevant to the VStream/PlanetScale bug — VStream cold-start
  is PlanetScale-only.)

### 4.2 The idempotent options, ranked

The target already has the index sluice needs (the source's PK/unique key is
recreated on the target before bulk-copy in the simple-mode order: create
tables → bulk-copy → indexes/constraints — but note the **deferred-index
caveat** in §4.4). Given a key, the candidates:

1. **`INSERT … ON DUPLICATE KEY UPDATE` (recommended).** On a duplicate of
   *any* unique/PK key, MySQL updates the named columns instead of erroring.
   This is the precise form the CDC `ChangeApplier` already builds
   (`change_applier.go` `buildInsertSQL` 1143–1201, the `AS new ON DUPLICATE
   KEY UPDATE` row-alias upsert, ADR-0010). Re-using it keeps one idempotency
   model across snapshot + CDC.
   - *Correctness:* absorbs both true re-emissions **and** the forward rows
     the order-mismatch would otherwise have dropped. A re-emitted row
     re-writes identical data (idempotent); a forward row inserts. Net: the
     full table lands, no row dropped.
   - *Throughput:* on a cold-start the target table starts empty, so for the
     dominant single-pass case **every row is an INSERT, not an UPDATE** —
     the `ON DUPLICATE KEY UPDATE` clause is dead weight that only fires on
     the (rare) genuine re-emission. The cost is (i) the upsert clause makes
     the statement text larger and (ii) MySQL must probe the unique index per
     row to detect a conflict. The index probe is the same one a plain INSERT
     already does to enforce uniqueness, so the marginal cost is small —
     in practice low-single-digit-percent on bulk INSERT, dominated by
     network + parse, not the conflict check. Multi-row batching (500/stmt)
     is preserved.

2. **`INSERT IGNORE` (faster, but lossy on a different axis — reject).**
   Skips rows that violate a unique constraint *without updating*. Faster
   than upsert (no UPDATE branch). **But** `INSERT IGNORE` also silently
   swallows *other* errors (data truncation, NOT-NULL violations, bad
   dates → warnings, row skipped) — it would convert a genuine
   type-translation bug into silent loss, violating the loud-failure tenet.
   For a re-emission whose payload is *identical*, IGNORE and UPSERT are
   equivalent; for a re-emission whose payload *changed mid-copy*, IGNORE
   keeps the stale first copy and UPSERT keeps the newer one — UPSERT is the
   safer choice and matches CDC semantics. Reject IGNORE.

3. **`REPLACE INTO` (reject).** DELETE-then-INSERT on conflict. Heavier
   (delete + insert, re-fires triggers/cascades, churns autoinc), and on a
   table with **multiple** unique keys a REPLACE can delete a *different*
   existing row that collides on a secondary unique key — silent collateral
   loss. The `connections` table is exactly the multi-unique-key shape that
   makes REPLACE dangerous. Reject.

4. **`LOAD DATA … REPLACE/IGNORE` (N/A on PlanetScale).** LOAD DATA is
   disabled on PlanetScale (the only engine that runs the VStream path), so
   its IGNORE/REPLACE modifiers don't apply here. (For vanilla MySQL targets
   reached via the binlog CDC path, the dedup bug doesn't occur, so no change
   is needed there.)

### 4.3 PlanetScale / Vitess support for the chosen form

`INSERT … ON DUPLICATE KEY UPDATE` already runs against PlanetScale today —
it is the CDC applier's insert form (`buildInsertSQL`) and is exercised by
the `psverify` suite. Vitess supports it for single-shard and within-shard
routing. (The `connections` migration that surfaced the bug was a
single-keyspace copy; multi-shard upsert routing is the same as the CDC
applier already relies on.) No new PlanetScale capability is required.

### 4.4 Conflicts with other sluice functionality

- **Deferred index creation (the one real gotcha).** The simple-mode
  orchestrator copies rows *before* creating secondary indexes/constraints
  (pscale-fork tactic). `ON DUPLICATE KEY UPDATE` only detects a conflict on
  a **unique index that exists at copy time**. If the target's PK/unique key
  is created *after* the bulk copy, upsert degrades to a plain INSERT and the
  re-emission would still collide later (or duplicate-row through). So the
  fix requires the **PK / the relevant NON-NULL UNIQUE key to exist on the
  target before the VStream bulk-copy runs** — secondary non-unique indexes
  can still be deferred. Verify the cold-start path's table-create step emits
  the primary/unique key up front (it does for the PK; confirm for the
  promoted-unique case). This is the load-bearing prerequisite and must be a
  pinned test.
- **CDC handoff unaffected.** The dedup's stated recovery story — "dropped
  rows are replayed by the post-COPY CDC tail" — is *not* actually what saves
  case (c): the CDC tail only replays rows *changed* during the scan window,
  not the forward rows the dedup wrongly dropped. Removing the dedup +
  idempotent copy fixes the real loss; the CDC applier is already idempotent
  (same upsert), so the snapshot→CDC overlap stays absorbed.
- **No-PK tables.** Dedup is already a no-op for them; an idempotent writer
  has nothing to key on (see §5). No regression — they were never deduped.

---

## 5. Recommended fix

**Primary recommendation (least-perf-impact correct fix):**

1. **Make the VStream cold-start bulk-copy idempotent on the target** using
   `INSERT … ON DUPLICATE KEY UPDATE` — reuse the CDC applier's
   `buildInsertSQL` upsert shape (ADR-0010) on the batched-INSERT writer for
   the VStream snapshot path. This absorbs re-emissions *and* the
   order-mismatch forward rows, with the conflict clause effectively free on
   an empty-target cold start (every row inserts; the upsert only matters for
   the genuine re-emission minority).
2. **Delete the source-side `copyDedupTracker`** and its wiring in
   `cdc_vstream_snapshot.go` (`recordFields`/`shouldKeep`/`dedup` field, the
   COPY_COMPLETED summary log). Its core invariant is unfixable (the scan
   order is not observable on the stream — §2.5) and it is *actively
   causing* the silent loss, not preventing it.
3. **Refuse loudly for the truly-keyless table** (case (c): no PRIMARY KEY
   and no NON-NULL UNIQUE key). There is no key to make the copy idempotent
   on, so re-emissions can't be absorbed and would duplicate-insert. This was
   already the dedup's no-op blind spot; surface it as an explicit
   capability/preflight refusal ("table X has no PRIMARY KEY or NON-NULL
   UNIQUE key; VStream COPY can re-emit rows and sluice cannot make the copy
   idempotent — add a key or migrate via the non-VStream path"). Matches the
   "contain complexity / refuse loudly" tenet.

**Prerequisite (must-pin):** the PK / relevant NON-NULL UNIQUE index must
exist on the target *before* the VStream bulk-copy, or the upsert silently
degrades to plain INSERT (§4.4). Add a test that asserts the cold-start
create-table step emits the key before copy for both the explicit-PK and the
implicit-unique-PK shapes.

### Test matrix (pin the class, not the representative)

Mirror the case (a)/(b)/(c)/(d) matrix from §3 — they exercise *different
Vitess scan-order code paths* (`buildPKColumns` branches), so one
representative is insufficient (the Bug-74 lesson). Specifically pin:

- (a) explicit PK — regression: still lands all rows, upsert no-op.
- (b) single NON-NULL UNIQUE, no PK — lands all rows (today's lucky-pass
  case; pin so it stays correct after dedup removal).
- (c) **NON-NULL UNIQUE `id` + a cheaper NON-NULL UNIQUE key** — the actual
  bug. Without the fix this drops ~75%; with it, lands all 12. This is the
  load-bearing regression pin.
- (d) nullable unique key present alongside another key — confirms the PKE
  exclusion of nullable indexes doesn't reintroduce loss.
- (e) **no PK and no NON-NULL UNIQUE** — assert the loud refusal, not a
  silent duplicate-laden copy.

Run them under the `integration && vstream` vttestserver harness (local, no
PlanetScale creds needed) and, for final sign-off, the `psverify` suite
against real PlanetScale.

### Risk to other paths

- **Vanilla-MySQL binlog CDC path:** unaffected — the dedup and this fix are
  VStream-only; binlog cold-start uses the SQL RowReader with deterministic
  PK order. No change there.
- **Throughput:** bounded, low-single-digit-percent on the PlanetScale
  batched path (§4.2); the conflict clause is dead weight on an empty target.
- **Multi-shard:** upsert routing is the same vtgate path the CDC applier
  already uses; the multi-shard fan-in buffer is orthogonal (ADR-0071).
- **Deferred-index ordering:** the one genuine coupling — the key must exist
  before copy (§4.4). Pinned above.

---

## Appendix — key source citations

| What | File | Lines |
|---|---|---|
| COPY `ORDER BY rs.pkColumns` | `vitess/go/vt/vttablet/tabletserver/vstreamer/rowstreamer.go` | 325–330 |
| `buildPKColumns` (a/b/c branches) | same | 218–245 |
| PKE fallback call | same | 224–228 |
| `Pkfields` from scan-order PK (on RowsResponse, not FieldEvent) | same | 372–389 |
| `mapPKEquivalentCols` | `vitess/go/vt/vttablet/tabletserver/vstreamer/engine.go` | 602–632 |
| PKE selection SQL (cheapest NON-NULL UNIQUE) | `vitess/go/vt/mysqlctl/schema.go` | 581–664 |
| `st.PKColumns` ← only `INDEX_NAME='primary'` | `vitess/go/vt/vttablet/tabletserver/schema/engine.go` | 856–876 |
| `BaseShowPrimary` query | `vitess/go/mysql/schema.go` | 30–34 |
| `FieldEvent` proto (no `Pkfields`) | `vitess/go/vt/proto/binlogdata/binlogdata.pb.go` | 1492–1509 |
| sluice dedup (`shouldKeep`/`recordFields`) | `sluice/internal/engines/mysql/vstream_copy_dedup.go` | 120–194 |
| sluice dedup wiring into COPY pump | `sluice/internal/engines/mysql/cdc_vstream_snapshot.go` | 433–559 |
| sluice batched-INSERT writer (no conflict clause) | `sluice/internal/engines/mysql/row_writer.go` | 372–456 |
| CDC applier upsert (`ON DUPLICATE KEY UPDATE`) | `sluice/internal/engines/mysql/change_applier.go` | 1143–1201 |
| PlanetScale `BulkLoadBatchedInsert` (no LOAD DATA) | `sluice/internal/engines/mysql/flavor.go` | 97, 118–133 |
