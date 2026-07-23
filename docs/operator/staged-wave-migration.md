# Staged ("wave") migration — moving a database a few tables at a time

Not every migration wants to be one event. On a large database it is often safer to move the biggest or most self-contained tables first, cut the application over for *just those*, let them bake in production for a while, and then bring the rest across in later waves. Each wave is small enough to reason about, and a problem in wave 3 doesn't implicate the tables that have been running fine since wave 1.

sluice supports this today. This guide covers the two mechanisms and when each fits, the per-stream names a Postgres source needs, how foreign keys constrain your wave ordering, and the one thing sluice deliberately does *not* do: replicate writes back from the target to the source.

---

## The two mechanisms

| | **One growing stream** | **Several independent streams** |
|---|---|---|
| Shape | a single `--stream-id` whose table scope expands | one `--stream-id` per wave, running side by side |
| Add a wave with | `sluice schema add-table` | another `sluice sync start` |
| Cut over | all tables together, at the end | **per wave**, independently |
| Postgres source | **supported** | **supported** — needs a per-stream `--publication-name` ([why](#pg-publication-caveat)) |
| MySQL / PlanetScale / Vitess source | supported | supported |

The distinction that matters: **can you cut waves over independently?** If the point of staging is to de-risk the *copy* — you want everything to land eventually, in one cutover, but you'd rather not snapshot 4 TB in one run — use one growing stream. If the point is to de-risk the *cutover* — you want wave 1 serving production traffic from the target while wave 2 is still being copied — you need independent streams. Both source families support that; a Postgres source additionally needs a per-stream `--publication-name` (below).

---

## Mechanism A — one growing stream

Start a stream scoped to your first wave, then extend its scope as you go. The stream keeps one CDC position, one slot, one control-state row.

```bash
# Wave 1: the big tables.
sluice sync start \
    --source-driver postgres --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'orders,order_items' \
    --stream-id appdb
```

Later, bring `users` into the same stream. On Postgres this can be done **without draining** the stream:

```bash
sluice schema add-table users \
    --source-driver postgres --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --stream-id appdb --no-drain
```

`add-table` creates the table on the target, bulk-copies its rows from a consistent snapshot, extends the source publication, and hands the table to the running CDC stream — the same gapless snapshot→CDC boundary a cold start gets. It prompts for typed confirmation (the table name) unless you pass `--yes`.

`--no-drain` is Postgres-only in this release. On a **MySQL-family source** the drained workflow applies: `sluice sync stop --wait`, then `schema add-table`, then `sluice sync start` again (a re-run with the same `--stream-id` warm-resumes; it does not re-snapshot).

One table per invocation. Repeat it per table, or script the loop.

> **On a Postgres source, `add-table` is the mechanism that grows scope safely.** It extends the publication with `ALTER PUBLICATION … ADD TABLE` — purely additive, so it can't disturb tables already in scope.

---

## Mechanism B — several independent streams

Each wave gets its own `--stream-id`, its own CDC position, and — on Postgres — its own `--slot-name` **and** `--publication-name`. The waves are fully independent: you cut wave 1 over and stop its stream while wave 2 is still snapshotting.

On a **Postgres** source, the full per-wave shape is:

```bash
sluice sync start \
    --source-driver postgres --source "$SRC" \
    --target-driver postgres --target "$DST_WAVE1" \
    --include-table 'orders,order_items' \
    --stream-id wave1 --slot-name wave1 --publication-name wave1
```

On a **MySQL-family** source only `--stream-id` is needed — there is no slot and no publication:

```bash
# Wave 1 — cut over first, weeks before the rest.
sluice sync start \
    --source-driver mysql    --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'orders,order_items' \
    --stream-id wave1

# Wave 2 — started later, runs alongside wave 1.
sluice sync start \
    --source-driver mysql    --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'users,sessions' \
    --stream-id wave2
```

`--include-table` is comma-separated, repeatable, and glob-aware (`audit_*`). It scopes **both** legs — the cold-start snapshot and the live CDC apply.

CDC state is keyed per stream (`sluice_cdc_state` is `PRIMARY KEY (stream_id)`), so the waves never contend for position. On a MySQL-family source each stream opens its own binlog / VStream reader, and there is no shared server-side object to collide over.

<a id="pg-publication-caveat"></a>

### Why Postgres needs both per-stream names

`--slot-name` is the obvious one: without it every stream lands on the default `sluice_slot` and they collide immediately — a loud, hard-to-miss failure.

`--publication-name` is the one that matters more, because forgetting it used to fail *silently*. The replication **slot** is per-stream, but the **publication** — the table filter `pgoutput` applies — is a separate object, and streams that share one fight over it. Each cold start scopes the publication to *its own* table list with `ALTER PUBLICATION … SET TABLE …`, which replaces the member set atomically. Two waves sharing the default `sluice_pub` would therefore de-scope each other: the first wave's slot would stay healthy and keep advancing while receiving **nothing** for its tables.

sluice will not let that happen silently. A cold start that would **remove** tables from a publication refuses loudly with `SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT` whenever another sluice slot **exists** on the source — active *or* inactive, because a stopped wave's slot still holds a resumable position that expects its scope (since v0.99.289; earlier releases checked only *active* slots, which left a stopped-mid-migration wave unprotected). The refusal names the at-risk tables, labels each conflicting slot active/inactive, and fires **before** mutating anything — a refused attempt leaves every stream untouched. Pass `--publication-name` per stream and it proceeds; note that merely stopping a wave is no longer enough to clear the conflict — a finished wave's slot must be **dropped** (see the decommission step below).

Widening or equal-scope rescopes remove nothing and never trigger the refusal, so the fleet shape (several streams, identical scope) and `schema add-table` (purely additive) are unaffected.

Both flags share the `sluice_` prefix convention: `wave1` becomes `sluice_wave1`, so every sluice-owned source object is findable the same way — `pg_replication_slots WHERE slot_name LIKE 'sluice\_%'` and `pg_publication WHERE pubname LIKE 'sluice\_%'`.

MySQL, PlanetScale, and Vitess sources have no publication and ignore the flag. See [ADR-0175](../adr/adr-0175-postgres-publication-scope-isolation.md).

---

## Foreign keys decide your wave ordering

This is the constraint that shapes wave composition more than table size does.

A wave-1 table with a foreign key pointing at a wave-2 table cannot have that constraint created — the referenced table doesn't exist on the target yet. Two ways through:

**Order waves along FK dependency edges.** Parents before children. This is the clean answer when the dependency graph allows it, and it is worth drawing the graph before you pick waves.

**`--skip-foreign-keys` when it doesn't.** Cyclic dependencies, or a wave you can't reorder, need the escape hatch. It creates no FK constraints on the target and — importantly — **synthesizes a backing index on each skipped FK's referencing columns**, so the join performance that the FK's index was providing doesn't silently regress while you wait for the later wave. Every skipped constraint is named in the run's output. After the final wave lands, add the constraints back yourself.

Note this is a different situation from `--where` row filtering, which orphans children of *rows* that were filtered out and refuses loudly with `SLUICE-E-WHERE-FK-ORPHAN`. See [`filtered-subset-migration.md`](filtered-subset-migration.md) if you are staging by rows rather than by tables.

---

## Cutting a wave over

Per wave, the sequence is the standard cutover, scoped to that wave's tables:

```bash
# 1. Prime the target's identity columns past the source's high-water mark.
sluice cutover \
    --source-driver mysql    --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'orders,order_items' \
    --sequence-margin=1000

# 2. Flip application traffic for these tables. (Your step, not sluice's.)

# 3. Drain and stop this wave's stream.
sluice sync stop \
    --target-driver postgres --target "$DST" \
    --stream-id wave1 --wait

# 4. Prove it landed.
sluice verify \
    --source-driver mysql    --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'orders,order_items' --depth=sample

# 5. Decommission the wave's source-side objects (Postgres sources).
#    A finished wave's replication slot pins WAL on the source for the
#    REST of the migration — weeks, on a multi-wave plan — and (since
#    v0.99.289) blocks any later differently-scoped cold start, because
#    an existing slot claims its scope even when inactive. Drop the slot,
#    and the wave's publication if it had its own:
psql "$SRC" -c "SELECT pg_drop_replication_slot('sluice_wave1');"
psql "$SRC" -c "DROP PUBLICATION IF EXISTS sluice_wave1;"
```

`cutover` takes `--include-table` too, so a wave's sequences are primed without touching tables that are still source-authoritative. Skipping it is the classic staged-migration failure: application writes to the target start allocating IDs that collide with rows CDC is about to deliver.

Use `--wait` on `sync stop`. Without it the stop is asynchronous and late in-flight changes may not have landed.

Do not skip step 5 on a Postgres source: the WAL a forgotten slot retains has invalidated slots on small managed instances at a few hundred MB, and every later wave with a different scope will refuse until the leftover slot is gone.

---

## Split-brain is yours to prevent

sluice does not own the traffic switch, deliberately — the cutover moment is application-specific. The consequence is that sluice **cannot tell** whether a wave's tables are still being written on the source.

If writes keep landing on the source after you've cut a wave over, CDC will faithfully replicate them on top of the application's writes to the target. Per row, last writer wins, and nothing surfaces as an error — this is the one place in a staged migration where you can lose data quietly. The stream is doing exactly what you asked; the problem is upstream of sluice.

The only real protection is on the source side, at the moment of cutover:

- revoke `INSERT/UPDATE/DELETE` on the wave's tables from the application role, or
- install a rejecting trigger on them, or
- take the source's write path out of the application entirely for those tables.

"We updated the config and believe nothing writes there anymore" is not protection. Make the source refuse.

---

## What sluice does not do: write-back to the source

PlanetScale's MySQL import keeps a reverse stream running after cutover, so writes landing on the new database replicate *back* to the old one and you can fail back if the new database can't take the load. sluice has no equivalent today, and it isn't a small gap.

The blocker is that sluice's CDC apply path carries no origin marker — there is nothing that says "this change is one I applied, don't re-emit it." A reverse `sync start` running alongside a forward one is therefore a replication loop.

There is a narrower version that the wave design makes tractable, and it is worth understanding even though it is **not a supported feature**: once a wave is cut over and its forward stream is **stopped**, nothing is streaming those tables source→target any more. A reverse stream scoped to exactly that wave's tables is table-disjoint from every live forward stream, so it isn't a loop. What still stands in the way:

- The source's identity columns must be primed past the target's — a `cutover` run in the reverse direction.
- Cross-engine reverse (Postgres→MySQL after migrating MySQL→Postgres) needs a reverse-direction schema that round-trips, which the forward migration does not guarantee.
- It has never been tested. It is a plausible composition of shipped primitives, not a validated path.

If you need genuine fail-back today, the honest answer is to keep the source authoritative until you are confident: cut a wave's *reads* over first, leave writes on the source, and only move writes once the target has proven it under real read load. That gets you most of the risk reduction without a reverse stream.

---

## Choosing waves: a checklist

1. **Draw the FK graph.** It constrains ordering more than size does. Parents before children; note the cycles that will need `--skip-foreign-keys`.
2. **Prefer self-contained clusters.** A wave whose tables reference only each other cuts over cleanly and can be verified in isolation.
3. **Put the scariest table in wave 1, not last.** The point of staging is to learn early, on the table most likely to surprise you, while the blast radius is smallest and rollback is still just "keep using the source."
4. **On a Postgres source, set `--slot-name` AND `--publication-name` per wave.** Independent per-wave cutover needs Mechanism B, and on Postgres both per-stream names are required for it to be correct — sluice refuses loudly if you forget, but it is cheaper to pass them up front.
5. **Decide the write-fence per wave before you start**, not at the cutover window.
6. **Budget the stream count.** Each concurrent wave is a full CDC reader (and on Postgres, a slot). A handful is fine; dozens is not a design, it's a load test.

---

## See also

- [`docs/cookbook/recipe-bidirectional-cutover.md`](../cookbook/recipe-bidirectional-cutover.md) — the single-wave cutover flow this guide generalizes
- [`docs/operator/filtered-subset-migration.md`](filtered-subset-migration.md) — staging by **rows** (`--where`) rather than by tables
- [`docs/operator/multi-database-multi-schema.md`](multi-database-multi-schema.md) — staging by **namespace** (`--include-database` / `--include-schema`)
- [`docs/schema-change-runbook.md`](../schema-change-runbook.md) — `schema add-table` in the broader schema-evolution context
- [`docs/snapshot-cdc-handoff.md`](../snapshot-cdc-handoff.md) — why each wave's snapshot→CDC boundary is gapless
- [`docs/operator/error-codes.md`](error-codes.md) — every `SLUICE-E-*` code named here
