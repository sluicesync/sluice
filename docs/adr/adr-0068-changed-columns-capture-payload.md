# ADR-0068: changed-columns capture payload (cut postgres-trigger source-write overhead)

## Status

**Accepted (2026-05-29) — design signed off; implementation to follow.** Per
owner decision, sluice ships **all three** payload modes (`full` / `changed` /
`minimal`, §1) as selectable rather than picking the before-trim level up front
— so the overhead/behavior tradeoffs can be validated **head-to-head** under
different workloads before any default flip. Default stays `full`.
This is roadmap item 18 sub-item (b). Driven by the sluice-vs-Bucardo
head-to-head (`sluice-testing/session-reports/bucardo-vs-sluice-v0.89.0.md`):
the `postgres-trigger` capture trigger imposes ~10.8x source-write
amplification on a 50k-UPDATE microbench, versus Bucardo's ~2x, because it
writes the **full** before-image *and* the **full** after-image as JSONB into
`sluice_change_log` on every row change. Builds on ADR-0066 (the trigger
engine + change-log design) and ADR-0010 (idempotent applier). No code written
yet — this ADR locks the design first, per the "lay out the design before
touching the capture/replay contract" working agreement. Item 18 sub-item (a)
(the batched-apply latency + AIMD-throughput fix) already merged separately
(PR #88); the agreed release plan is to bundle (a) + (b) into one release.

## Context

### The overhead, and where it comes from

The capture trigger (`renderCaptureRowFunction`, ADR-0066 §3) does, per row:

| op | `pk_jsonb` | `before_jsonb` | `after_jsonb` |
|----|-----------|----------------|---------------|
| INSERT | PK | NULL | `to_jsonb(NEW)` (full) |
| UPDATE | PK | `to_jsonb(OLD)` (full) | `to_jsonb(NEW)` (full) |
| DELETE | PK | `to_jsonb(OLD)` (full) | NULL |

So an UPDATE writes **two full row images** (plus the PK) into the change-log
on every change — the dominant cost behind the ~10.8x (trigger `to_jsonb` CPU +
the change-log INSERT + WAL for ~2 rows + the BIGSERIAL index maintenance).
INSERT inherently needs the full new row (all-new data); DELETE needs only
enough to locate the row.

### The design tension (and why we are NOT adopting Bucardo's model)

sluice's full-image capture is **deliberately self-contained and replay-safe**
(ADR-0066): the poller reads the log and applies it, never re-reading the
source row, and a row updated N times produces N change rows each carrying the
values *as of that change* (point-in-time fidelity). Bucardo gets its ~2x by
storing only the changed **primary key** in a delta table and **re-reading the
live source row at sync time** — which is cheaper on write but (a) loses
point-in-time fidelity (rapid updates collapse to the latest value), (b) adds
source read-load at sync, and (c) makes the delta non-self-contained (the live
source is the source of truth). We reject that model as the path here.

**The insight that makes a middle ground possible:** the overhead is the *full*
images, not the *self-containment*. Storing the **changed columns** (+ PK) for
an UPDATE keeps the log fully self-contained and point-in-time-faithful — the
changed values at change time are still in the log — while writing far less for
the common case of a narrow update on a wide table.

### Empirically verified: the trigger sees the full OLD row regardless of REPLICA IDENTITY

A plpgsql row trigger's `OLD`/`NEW` are full row records; `REPLICA IDENTITY`
governs only the WAL old-tuple for *logical decoding* (the slot/pgoutput path),
not trigger variables. Verified on the local rig (PG 16, source 5442) against a
table left at `relreplident = 'd'` (DEFAULT, not FULL):

```
CREATE TABLE _ri_probe (id bigint PRIMARY KEY, a text, b int, c bool);  -- REPLICA IDENTITY DEFAULT
-- AFTER UPDATE trigger logging to_jsonb(OLD), to_jsonb(NEW); UPDATE SET a='y' WHERE id=1:
old_image = {"a": "x", "b": 10, "c": true, "id": 1}   -- FULL old row, not PK-only
new_image = {"a": "y", "b": 10, "c": true, "id": 1}
```

Consequence: **changed-columns diffing can be computed inside the trigger
unconditionally — no `REPLICA IDENTITY FULL` requirement.** (Note: the comment
at `internal/engines/pgtrigger/cdc_reader.go:318` — "When the source's REPLICA
IDENTITY is DEFAULT, OLD carries only the PK columns" — is **incorrect for the
trigger path** (it is true only for the slot/pgoutput reader). That stale
comment should be corrected as part of this work; it does not reflect runtime
behavior.)

### The reader and applier are already payload-shape-agnostic

This is the second insight that keeps the change small. On the apply side
(`internal/engines/postgres/change_applier.go`):

- `buildUpdateSQL` builds the `SET` clause from **whatever columns are in
  `After`** (`buildSetClause(after, …)`) and the `WHERE` predicate from
  **whatever columns are in `Before`** (`buildWhereClause(before, …)`). Its own
  comment already notes "SET uses every column in After (unchanged-column
  detection is a v1.5 optimization)."
- The reader (`cdc_reader.go`) decodes `before_jsonb`/`after_jsonb` into
  `ir.Row` maps verbatim — it does not assume completeness.

So a trigger that emits a *partial* `after` (PK + changed) yields a smaller
`SET`, and a trigger that emits a *partial* `before` (PK only) yields a
PK-scoped `WHERE` — **both correct and idempotent with no reader/applier code
change.** The mode is entirely a source-side property of the installed trigger.

## Decision (proposed)

### 1. A `--capture-payload` mode, baked into the trigger at setup time

Add `sluice trigger setup --capture-payload=full|changed|minimal` (default
`full`). The mode selects which trigger-function body is emitted; it is a
property of the installed capture function, not a runtime flag on `sync`. The
reader/applier are unaffected (they consume whatever the log holds, §0). The
three modes are points on a single axis — decreasing payload, decreasing
information retained — so they can be benchmarked head-to-head (owner decision):

| mode | UPDATE `before` | UPDATE `after` | DELETE `before` | INSERT `after` |
|------|-----------------|----------------|-----------------|----------------|
| **`full`** (default) | full old row | full new row | full old row | full new row |
| **`changed`** | full old row | PK + changed cols | full old row | full new row |
| **`minimal`** | PK only | PK + changed cols | PK only | full new row |

- **`full`** — today's behavior, byte-identical. Gap-free, self-contained,
  full-before optimistic-divergence `WHERE`. Conservative default per the
  "validate end-to-end / loud-failure" tenets.
- **`changed`** — trims only the `after` image to `PK ∪ {cols where
  to_jsonb(OLD)->col IS DISTINCT FROM to_jsonb(NEW)->col}`. Keeps the full
  `before` (so the apply `WHERE` still does optimistic divergence detection).
  Moderate saving; a wide-table UPDATE still writes one full image (the before).
- **`minimal`** — also trims `before` to the PK. The apply `WHERE` becomes a PK
  match (standard last-write-wins CDC); reaches toward Bucardo's ~2x. The one
  real semantic change (loss of the divergence-detecting `WHERE`), acceptable
  for one-way CDC (no concurrent target writers) and quantified by the
  head-to-head benchmark before any default flip.

INSERT is identical in all three modes (all-new data — nothing to trim).

### 2. Trigger emission for `changed` / `minimal`

Both leaner modes compute the same `after` in the UPDATE branch — the changed
set, by iterating the new-row keys and comparing the jsonb-extracted values
(`IS DISTINCT FROM` is NULL-safe and type-exact on jsonb), always unioning the
PK columns so the applier's `WHERE` and `SET` both have the key. They differ
only in `before`:

```sql
-- both `changed` and `minimal`:
v_after := (
  SELECT jsonb_object_agg(n.key, n.value)
  FROM jsonb_each(to_jsonb(NEW)) n
  WHERE  n.key = ANY(v_pk_cols)
     OR  (to_jsonb(OLD) -> n.key) IS DISTINCT FROM n.value
);
-- `changed`:  v_before := to_jsonb(OLD);   -- full old row (divergence WHERE)
-- `minimal`:  v_before := v_pk;            -- PK only (PK-scoped WHERE)
```

A zero-column UPDATE (`SET a = a`) yields `after = PK only`; the applier then
runs `UPDATE … SET <pk>=<pk> WHERE <pk>` — a harmless idempotent no-op. (We do
NOT suppress the change row, so per-stream change counts stay faithful.)

Emission is per-mode trigger-function bodies behind a `SetupOptions.CapturePayload`
field; the three render paths share the PK-discovery + INSERT-into-change-log
scaffolding and differ only in the `v_before` / `v_after` assignment block.

### 3. What is preserved, and what is traded

**Preserved (the load-bearing properties):**
- **Self-containment** — the changed values + PK are in the log; the poller
  never re-reads the source. (Strictly better than the rejected PK-only+re-read
  model.)
- **Point-in-time fidelity** — N updates → N change rows with their respective
  changed values, replayed in `id` order to the correct final state.
- **Idempotent replay (ADR-0010)** — a PK-scoped `WHERE` + a `SET` of the
  changed columns re-applied is a no-op-equivalent (re-sets the same values);
  the snapshot→CDC gapless handoff (ADR-0066) is position-based and unaffected.

**Traded — `minimal` only (`changed` keeps full `before`):**
- In `minimal`, the full-before-image `WHERE` (today: `WHERE id=1 AND a='x' AND
  b=10 …`) becomes a PK `WHERE` (`WHERE id=1`). The full-before `WHERE` provided
  *optimistic divergence detection* — an apply silently affects zero rows if the
  target row had already diverged. For sluice's one-way replica (no concurrent
  target writers) this is equivalent in the happy path; the PK `WHERE` is the
  standard last-write-wins-by-stream-order CDC semantics. `changed` retains the
  full-before `WHERE` (it trims only the `after` image), so it keeps divergence
  detection at the cost of a larger payload — which is exactly the tradeoff the
  head-to-head benchmark exists to quantify.

### 4. Decisions (owner-signed-off) + remaining knobs

1. **Before-trim: ship BOTH levels as selectable modes.** Rather than pick
   after-only vs both-trimmed up front, ship `changed` (full before) AND
   `minimal` (PK before) so they can be benchmarked head-to-head under
   different workloads (owner decision, 2026-05-29). The §Measurement plan runs
   `full` vs `changed` vs `minimal` and quantifies the overhead/behavior
   tradeoff; a default flip (if any) is a later, data-driven call.
2. **Default mode: `full`** (signed off). Opt-in the leaner modes; no default
   change until the head-to-head data justifies one.
3. **Granularity** — one mode per `trigger setup` invocation for v1 (simplest);
   per-table is a follow-up if an operator needs mixed modes on one source.
4. **Mode names: `full` / `changed` / `minimal`** (ordered by decreasing
   payload). `changed` = changed-`after`, full `before`; `minimal` = changed-
   `after`, PK `before`. (Reserve `lite` for any future PK-only+re-read mode —
   a different, non-self-contained replay contract.)

### 5. Refuse / edge boundaries (unchanged from ADR-0066 §14)

No new refusals. The no-PK refusal still applies (the changed set always unions
the PK, which requires a PK — already guaranteed by §14). Generated columns are
already excluded by the applier (`nonGeneratedRowKeys`); the changed set may
include a generated column's new value harmlessly since the applier filters it.

## Consequences

- **Source-write overhead drops** for UPDATE/DELETE-heavy streams on wide
  tables; the exact factor must be measured (see below). INSERT-heavy or
  narrow-table workloads see little change (INSERT is unchanged; a narrow table
  has little to trim).
- **Trigger CPU per row** rises modestly (the per-column diff) but the
  change-log INSERT + WAL shrink; on the write-bound workloads this targets,
  the net should favor the leaner payload. Confirm by measurement.
- **TOAST win:** large *unchanged* columns are excluded from the changed set,
  so they are not re-serialized into the log — a disproportionate saving for
  wide rows with large rarely-changed columns.
- **No reader/applier code change** (payload-shape-agnostic, §0); the one
  required non-trigger edit is fixing the stale `cdc_reader.go:318` comment.
- **Default behavior is byte-identical** to today (mode `full`), so existing
  installs and the gap-free contract are untouched.

### Measurement plan (gates the default + the before-trim choice)

Reproduce the Bucardo microbench *and* a realistic mixed-width workload on the
local rig, **head-to-head across all three modes** (`full` vs `changed` vs
`minimal`): source-write time per change (3 trials each), change-log table
size, trigger CPU, and end-to-end CDC correctness (ordered md5 src vs target)
on the standard rich-type table. Pin the class per CLAUDE.md: exercise
narrow-update-on-wide-table, all-columns-changed, zero-columns-changed,
NULL-transition, and large-TOAST-unchanged cases, plus the rich-type matrix
(numeric/jsonb/bytea/timestamptz/array) through the JSONB-round-trip path
(ADR-0066 §4 / Bug-74 matrix). The comparison data is what a later default-flip
decision (if any) rests on, and it validates `minimal`'s PK-`WHERE` semantics
against `changed`/`full` under each workload.

## Alternatives considered

- **PK-only + re-read at apply (Bucardo's model).** ~2x overhead but abandons
  self-containment + point-in-time fidelity + adds source read-load at sync.
  Rejected as the path; could be a *separate* future opt-in mode for operators
  who explicitly accept eventual-consistency, but it is a different replay
  contract and not this ADR.
- **Compacter serialization (positional array instead of keyed JSONB).**
  Saves the repeated key names but keeps full images; marginal next to dropping
  whole columns, and abandons ADR-0066 §4's native-jsonb decode. Rejected.
- **Require `REPLICA IDENTITY FULL`.** Unnecessary — the trigger already has
  full OLD/NEW (§Context, verified). Rejected (would be a pointless operator
  burden + WAL cost).
- **Do nothing / demand-gate harder.** Valid — this is an enhancement, not a
  correctness fix, and there is no current write-heavy-source operator pull.
  The owner may choose to hold (the roadmap entry stays demand-gated). This ADR
  exists so that when demand arrives, the design is ready.

## References

- ADR-0066 — postgres-trigger engine + change-log design (§2 schema, §3 trigger,
  §4 jsonb shape, §14 refuse boundaries).
- ADR-0010 — idempotent applier replay contract.
- ADR-0007 — position-and-data atomicity.
- `sluice-testing/session-reports/bucardo-vs-sluice-v0.89.0.md` — the ~10.8x
  source-write measurement + the latency-breakdown addendum that re-attributed
  item 18.
- Roadmap item 18 (`docs/dev/roadmap.md`) — (a) merged (PR #88); (b) is this ADR.
