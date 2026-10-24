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

Later, bring `users` into the same stream. Stop the stream for the add, then start it again:

```bash
sluice sync stop --stream-id appdb --target-driver postgres --target "$DST" --wait
sluice schema add-table users \
    --source-driver postgres --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --stream-id appdb
sluice sync start \
    --source-driver postgres --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --include-table 'orders,order_items' \
    --stream-id appdb
```

`add-table` creates the table on the target, bulk-copies its rows from a consistent snapshot and, on Postgres, extends the source publication. The restart warm-resumes from the stream's position and replays what changed during the add after the copy, in order; it does not re-snapshot. On a terminal the add prompts for typed confirmation (the table name) unless you pass `--yes`; on a non-terminal stdin (a script, CI, an agent) it refuses with `SLUICE-E-CONFIRMATION-REQUIRED` instead of prompting.

**`--no-drain` is withdrawn on every source.** It refuses loudly before it reads or writes anything and names this drained workflow: a Postgres source (`NO-DRAIN-POSTGRES-SOURCE`, since v0.163.10: Bug 332 measured the running stream dropping the new table's changes until it admitted the table, or, on a stream without `--include-table`, applying them while the copy ran so the copy undid deletes and updates; the "strict zero-loss since v0.32.0" this page used to promise held for inserts alone), a MySQL or MariaDB binlog source (`NO-DRAIN-BINLOG-SOURCE`: its live path lost every change made to the new table between the add's snapshot and the running stream admitting it, which spans the whole copy), a PlanetScale/Vitess (VStream) source (`NO-DRAIN-VSTREAM-SOURCE`: its add-table copy also drops the changes made to the table while it runs), and a trigger-CDC source. For a `sluice sync run` fleet leg, restart it through the fleet, which starts the leg from its config. Keep the stream stopped until `schema add-table` returns, with no fleet reload and no supervisor or systemd restart in between: an add against a stream that is running would lose the same changes, so the drained add refuses one, as below.

**The drained flow checks that the stream recorded its stop (`DRAINED-ADD-TABLE-STREAM-NOT-STOPPED`).** A drained add is exact only because the restarted stream replays the add's window from its saved position, so it must not run while the stream applies changes: on a source without a publication the add records the table only after its copy, and a running stream drops every change to the table until then, which left 80 deleted and 10 updated rows stale on the target in the PlanetScale/Vitess measurement. A `sluice sync start` (or `sluice sync run` leg) keeps a liveness record on its control row by the target server's clock (`sluice_cdc_state.stream_alive_ms`, written only by what applies changes under that stream id): every position write stamps it running in the same statement, every attempt stamps it running as it starts, and the stream records that it stopped when it ends — `sync stop --wait`, Ctrl-C, a refusal or a failure alike. `schema add-table` without `--no-drain`, including `--dry-run`, passes only on that recorded stop: it refuses a running stamp however old, a record never written, and one it cannot read, naming `sluice sync stop --wait --stream-id <id>`. An idle running stream writes nothing new, so its record ages exactly like a crashed one's. A stream that crashed or was killed, or last ran under a release that did not keep the record, therefore refuses until it has been started once and stopped with `sync stop --wait`; a start that fails or is refused after it reaches the target records the stop as it exits too, and `sync stop` alone against a stream that is not running only leaves its stop request set until the next start. The check is made three times: before the add changes anything, right after it records the add as pending, and after the copy, before the table is handed to the stream, where the stream's persisted position must also be the one the add started from — a stream that applied anything during the add refuses, even one that then stopped cleanly (on a source without a publication the table is then on the target but not in the stream's scope: stop the stream, empty the table, add it again; on a Postgres source it already joined the stream's scope, and `sluice schema add-table --abandon <table>` takes it back out first). A stream started while the add copies (a supervisor restart, a `sync run` reload) defers with `INCLUDE-TABLE-ADD-IN-PROGRESS` until the add has finished, whether or not the added table is in its `--include-table` scope yet. A control table the stream's role cannot alter (a DML-only role, a PlanetScale branch with safe migrations) gets no liveness column: the stream keeps running and WARNs `STREAM-LIVENESS-UNRECORDED`, and the drained add refuses until the column is added, by a role that can: on Postgres the statements `sluice control-tables ddl --engine postgres` prints include it; on a MySQL-family target run `ALTER TABLE sluice_cdc_state ADD COLUMN stream_alive_ms BIGINT NULL` (with `sluice deploy-ddl` on a safe-migrations branch, where the WARN prints that statement).

After the add, `users` is part of the stream's scope: sluice records it in the stream's live-added set on the target (`sluice_cdc_state.live_added_tables`), so restarting the stream with the same `--include-table 'orders,order_items'` keeps streaming it, on every source. Only `sluice schema add-table --abandon users --yes` takes it back out (an `--exclude-table` that names it is refused, `LIVE-ADDED-TABLE-EXCLUDED`, rather than silently winning), and the next start then stops applying it and records it as dropped, so bringing it back later needs a fresh copy. Through v0.162.0 only the MySQL `--no-drain` path recorded the table, and a Postgres stream restarted with its original `--include-table` silently stopped applying `users`.

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

sluice will not let that happen silently. A cold start that would **remove** tables from a publication refuses loudly with `SLUICE-E-CDC-PUBLICATION-SCOPE-CONFLICT` whenever another sluice slot **exists** on the same database of the source (a slot on another database cannot read the publication) — active *or* inactive, because a stopped wave's slot still holds a resumable position that expects its scope (since v0.99.289; earlier releases checked only *active* slots, which left a stopped-mid-migration wave unprotected). The refusal names the at-risk tables, labels each conflicting slot active/inactive, and fires **before** mutating anything — a refused attempt leaves every stream untouched. Pass `--publication-name` per stream and it proceeds; note that merely stopping a wave is no longer enough to clear the conflict — a finished wave's slot must be **dropped** (see the decommission step below).

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

# 5. Decommission the finished wave. A finished wave's replication
#    slot pins WAL on the source for the REST of the migration —
#    weeks, on a multi-wave plan — and (since v0.99.289) blocks any
#    later differently-scoped cold start, because an existing slot
#    claims its scope even when inactive. This drops the wave's slot
#    and its RECORDED per-stream publication on the source (never the
#    shared sluice_pub) and clears its control row on the target:
sluice sync decommission \
    --source-driver mysql    --source "$SRC" \
    --target-driver postgres --target "$DST" \
    --stream-id wave1 --yes
```

`cutover` takes `--include-table` too, so a wave's sequences are primed without touching tables that are still source-authoritative. Skipping it is the classic staged-migration failure: application writes to the target start allocating IDs that collide with rows CDC is about to deliver.

Use `--wait` on `sync stop`. Without it the stop is asynchronous and late in-flight changes may not have landed.

`sync decommission` is engine-aware. On a **Postgres source** it drops the wave's replication slot and the per-stream publication recorded on the wave's control row — the shared `sluice_pub` is never dropped, and a stream whose slot is still active refuses loudly with `SLUICE-E-DECOMMISSION-STREAM-ACTIVE` (run `sync stop --wait` first). On a **MySQL-family source** there are no source-side objects (the binlog is the stream), so it just clears the control row and says so. On a **trigger-CDC source** the change-log table and triggers are shared across streams and are deliberately left alone — `sluice trigger prune` bounds the log's growth and `sluice trigger teardown` removes the machinery once no streams remain. `--dry-run` previews without touching anything; a partially-failed run reports exactly what was and wasn't removed, keeps the control row, and an idempotent re-run completes the rest. If you prefer raw SQL on a Postgres source (or are on an older sluice), the manual equivalent is `psql "$SRC" -c "SELECT pg_drop_replication_slot('sluice_wave1');"` followed by `psql "$SRC" -c "DROP PUBLICATION IF EXISTS sluice_wave1;"` — plus deleting the stream's row from the target's `sluice_cdc_state`.

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
