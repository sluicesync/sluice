# ADR-0101: Native-MySQL concurrent multi-table cold-copy (FTWRL-coordinated N consistent-snapshot readers)

## Status

Accepted. The native-MySQL (binlog-flavor) analogue of the VStream cross-table cold-copy levers [ADR-0099](adr-0099-cross-table-vstream-copy-concurrency.md) (K independent read streams over a disjoint table partition) + [ADR-0100](adr-0100-cross-table-vstream-write-concurrency.md) (K end-to-end read→write pipelines). It **reuses the ADR-0100 pipeline consumer verbatim** (`runConcurrentTableCopy` + the `ir.ConcurrentCopyPartitioner` surface) and **mirrors ADR-0099's deterministic disjoint partition** (`partitionTablesForStreams`); its only new core is the source-side **FTWRL-coordinated N-consistent-snapshot opener**. Builds on [ADR-0007](adr-0007-position-persistence.md) (position-then-data — the single recorded binlog position is the CDC anchor, read only after all pipelines complete), [ADR-0074](adr-0074-multi-database-mysql.md) §5 (the single-spanning-snapshot + single-position consistency crux this generalizes from 1 reader to N), and [ADR-0008](adr-0008-go-mysql.md) (the binlog dump CDC reader the recorded position seeds). Does not touch the VStream path, the Postgres cold-copy path, the ADR-0079 fast shareable-snapshot path, or `sluice migrate`.

## Context

### The measured problem (Track D, validated live)

Track D measured **vanilla MySQL → PS-MySQL at ~0.66 MB/s** vs **Vitess → PS-MySQL at ~26 MB/s** — a ~40× gap. The decisive datum: the native `sync` cold-start copies tables **serially**, ONE table written at a time, exactly the serial-consumer bottleneck ADR-0100 diagnosed for the VStream path (the target PROCESSLIST showed one table receiving rows). The cross-table concurrency that closes the Vitess gap (ADR-0099 K read streams × ADR-0100 K read→write pipelines) is driven by `vstream_copy_table_parallelism`, a **VStream-source-ONLY** knob. A native (non-Vitess) MySQL source had no equivalent — so the serial cold-start was the throughput floor for every self-managed-MySQL → PlanetScale/MySQL migration.

### Why native MySQL is harder than VStream or Postgres

The two existing concurrent-copy designs lean on a source-side feature native MySQL lacks:

- **Postgres (ADR-0079 fast path)** mints N independent snapshot-pinned readers via `SET TRANSACTION SNAPSHOT` against one **exported, shareable** snapshot — all N readers see the exact same MVCC view by construction. Native MySQL has **no exported/shareable snapshot**: you cannot share one InnoDB read-view across connections.
- **VStream (ADR-0099)** opens K independent vtgate gRPC streams, each its own COPY; the per-table snapshot positions are stitched with a parallelism-agnostic GTID-set-min. Native MySQL has **no VStream**.

So native MySQL needs its own mechanism to give N concurrent reader connections a **consistent** multi-table view at a **single** CDC-resume point. The proven pattern is mydumper / `mysqldump --single-transaction --master-data`: freeze writes globally for a few milliseconds with `FLUSH TABLES WITH READ LOCK` (FTWRL), open every reader transaction under the lock so they all pin the same consistent point, record ONE binlog position, unlock. sluice's existing single-reader binlog snapshot (`openBinlogSnapshotStreamShared`) **already does exactly this for one connection** (FTWRL → `START TRANSACTION WITH CONSISTENT SNAPSHOT` → `SHOW BINARY LOG STATUS` → UNLOCK). This ADR generalizes the *one* pinned transaction to *N*, all opened under the *one* held FTWRL.

## Decision

Make the native-MySQL binlog cold-copy open **N reader connections, each running its own `REPEATABLE READ` + `START TRANSACTION WITH CONSISTENT SNAPSHOT`, all under ONE held FTWRL**, record **ONE** binlog position, then UNLOCK. The in-scope tables are partitioned into N disjoint groups (the **same** deterministic `partitionTablesForStreams`), one group per reader. The engine surfaces that partition via the **existing** `ir.ConcurrentCopyPartitioner` surface, so the **existing** ADR-0100 pipeline consumer (`runConcurrentTableCopy`) drives **W = N concurrent read→write pipelines** — each pipeline reading its group's tables from its own pinned-snapshot connection and writing them through the per-table copy helper. In v1 the native (gap-free) cold-copy uses the plain-INSERT writer per table, so concurrency is **W × 1** (W tables in flight, one writer each) — this is the lever that lifts the Track-D serial-consumer ceiling. Composing the ADR-0097 D-way per-table write fan-out on top (true **W × D**, to approach the VStream PS-MySQL ceiling) is a documented follow-up: the plain-INSERT path does not currently fan out, and folding D in needs a parallel-plain-INSERT writer (or routing native through the idempotent+fan-out path).

```
                 ┌── FTWRL held ──┐
 conn 0: REPEATABLE READ; START TRANSACTION WITH CONSISTENT SNAPSHOT   ┐
 conn 1: REPEATABLE READ; START TRANSACTION WITH CONSISTENT SNAPSHOT   ├ all at the
   …                              …                                    │ SAME cut
 conn N-1: REPEATABLE READ; START TRANSACTION WITH CONSISTENT SNAPSHOT ┘
        record ONE binlog position (SHOW BINARY LOG STATUS)
                 └── UNLOCK TABLES ── (writes resume; N tx views frozen)

 group 0 (conn 0): [a, c] ── ReadRows(a)→write ─ ReadRows(c)→write   ┐
 group 1 (conn 1): [b, d] ── ReadRows(b)→write ─ ReadRows(d)→write   ├ W=N errgroup
   …                                                                  ┘
                          all W complete → CDC from the ONE recorded position
```

The user runs one `sluice sync start`; N is invisible above the snapshot contract. N is tied to a single new source-DSN knob (§1); the absent/zero value is byte-identical to today's serial path for every constructor.

The six design questions, resolved:

### 1. The concurrency knob — `copy_table_parallelism=N`, zero-value-safe, stripped before the wire

N is a **source-DSN parameter `copy_table_parallelism=N`** — deliberately named WITHOUT the `vstream_` prefix because it governs the native binlog cold-copy, not VStream. It mirrors the VStream `vstream_copy_table_parallelism` knob's shape (the established VStream-tuning surface) so an operator who knows one knows the other. It is parsed entirely inside the mysql engine; it is NOT a pipeline CLI flag (the partition is a binlog-snapshot mechanism property, not an engine-neutral orchestrator concern — same reasoning ADR-0099 §3 used).

**Stripped before the MySQL session (Bug 126).** `openDB` strips `vstream_*` DSN params so they never reach a MySQL session as `SET vstream_*=…`. `copy_table_parallelism` is NOT under that prefix, so `stripVStreamParams` is widened to also strip this exact key (an explicit allowlist entry, not a prefix). Without this, the go-sql-driver's session-init would emit `SET copy_table_parallelism=N` and MySQL would reject the unknown system variable — the same failure class Bug 126 fixed for `vstream_*`.

**Zero-value-safe (the v0.99.51 trap).** `resolveCopyTableParallelism(n, nTables)` (the SAME resolver ADR-0099 already ships) maps `n ≤ 1 → 1` (the serial single-snapshot path, byte-identical to today), `n > 1 → min(n, nTables, ceiling)`. The Go zero value / absent param / a one-table scope all resolve to N = 1 ⇒ the engine surfaces NO concurrent groups ⇒ the pipeline takes the serial table loop byte-identically. There is no value that produces "0 readers = copies nothing". Defaulting to 1 (not >1) is deliberate: each reader is a full pinned connection holding a long transaction, so cross-table concurrency is an explicit throughput opt-in for a known-large copy, never a surprise multiplier on every cold-start.

### 2. The FTWRL-coordinated N-snapshot opener — the new core

`openBinlogSnapshotStreamConcurrent` (the multi-reader analogue of `openBinlogSnapshotStreamShared`) does, in order:

1. Open the writer-side pool; **pin connection 0** and, on it, `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` then **`FLUSH TABLES WITH READ LOCK`** — freezing writes globally.
2. **Open N-1 additional pinned connections** (conn 1…N-1). On EACH (including conn 0), `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ; START TRANSACTION WITH CONSISTENT SNAPSHOT`. Because the FTWRL is held the whole time, **all N consistent snapshots pin the same logical cut**. (FTWRL blocks commits, so no transaction can interleave a commit between the first and the Nth `START TRANSACTION`.)
3. **Record ONE binlog position** (`SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS`) — the single CDC-resume anchor — on conn 0, inside the lock.
4. **`UNLOCK TABLES`** on conn 0 — writes resume; the N reader transactions keep their frozen snapshots (an open InnoDB REPEATABLE-READ tx's view survives the unlock).
5. Partition the in-scope tables into N disjoint groups (`partitionTablesForStreams(tables, N, nil)`) and build a **multi-snapshot RowReader** (§6) that owns the N connections and routes each table's `ReadRows` to the connection owning that table's group. Surface the partition via `ConcurrentCopyGroups()`.
6. The paired CDC reader is built from the same DSN exactly as the single-reader path; the recorded position is the handoff anchor.

The FTWRL is held only across steps 1–4 (opening N transactions + reading the position) — millisecond-scale, independent of table sizes, because no data is read under the lock. This is the mydumper/Debezium consistent-snapshot pattern, scaled from 1 to N pinned transactions.

### 3. Why ONE global position, not per-reader independent snapshots (the chosen-vs-rejected design)

**Chosen: one held FTWRL → N transactions at one cut → ONE recorded position → no stitch.** **Rejected alternative: N independent `START TRANSACTION WITH CONSISTENT SNAPSHOT` (no global lock), each capturing its own binlog position, then a position-min stitch (the ADR-0095/0099 VStream approach).**

The chosen design is *obviously correct* and far simpler:

- **Gaplessness is by construction, not by argument.** All N readers are at the identical cut = the one recorded position. CDC resumes from that one position; every change committed after it is in the binlog tail, every change before it is in every reader's snapshot. There is no per-reader window to reconcile — the seam is a single point, so there is nothing to stitch and no "is set A ⊆ set B" obligation.
- **It reuses sluice's already-proven single-reader consistency crux** (ADR-0074 §5: one spanning snapshot, one position, FTWRL closing the snapshot↔position gap). This ADR changes only the *count* of pinned transactions opened under that one lock — the consistency argument is identical.
- **No GTID-set / position-min stitch code, no loud-refusal-on-divergence path.** The VStream design needs the set-min stitch precisely because its K streams are K independent causally-unordered vtgate sessions with no shared lock; the FTWRL sidesteps that entire surface.

The rejected per-reader-independent-snapshot design *avoids the global lock* (attractive where FTWRL is unavailable — see §4), but it needs each reader to capture its own position and a min-stitch to pick the gapless anchor, plus the loud-refusal when no captured position is ≤ all others — the exact hard part the FTWRL removes. **v1 chooses the lock + single position**; the lock is ms-scale and FTWRL is available on the self-managed target source. The per-reader-independent design is documented as the future enhancement for FTWRL-denied environments (it would lift the §4 refusal into a working concurrent path).

### 4. FTWRL unavailable — loud refuse or documented serial fallback, NEVER a silent inconsistent snapshot

FTWRL needs the RELOAD / FLUSH_TABLES privilege and is **blocked on some managed MySQL** (RDS, Aurora, etc.). Producing N independent consistent-snapshot transactions WITHOUT the global lock would let a transaction commit between the 1st and Nth `START TRANSACTION` — that commit would land in some readers' snapshots but not others, and the single recorded position could not name a cut consistent with all N → a **silently inconsistent multi-table snapshot** (the worst silent-loss class).

So when FTWRL fails AND N > 1 was requested, sluice does NOT proceed with N independent snapshots. It takes the **serial fallback**: the existing single-reader `openBinlogSnapshotStreamShared` path (one connection, one transaction, one position — consistent by construction, no concurrency), with a **loud WARN** naming the reason ("`copy_table_parallelism=N` requested but FLUSH TABLES WITH READ LOCK failed (needs RELOAD privilege); falling back to the SERIAL single-snapshot cold-copy — concurrency disabled, consistency preserved"). The serial path is byte-identical to today and consistent; the operator loses the throughput opt-in, never correctness. (Contrast the *single-reader* path's existing behaviour, which warns and proceeds lock-free because one transaction is internally consistent regardless of the position-capture window's tiny gap; the *N-reader* path has no such safe lock-free variant, so it must fall back to serial rather than proceed.)

This is detected by the FTWRL `ExecContext` returning an error (privilege or unsupported); the fallback is pinned by a unit/integration test simulating a restricted user. The single recorded-position consistency is never weakened — either FTWRL holds and N readers share the cut, or N collapses to 1 and the one transaction is the cut.

### 5. Connection budget — N readers + W writers, bounded, honest WARN

During the concurrent copy the **source** connection count is N (the N pinned reader connections + transactions) plus the one binlog-dump CDC connection opened at handoff; the **target** connection count is W (= N group consumers, one plain-INSERT writer each in v1; W × D once the per-table fan-out follow-up lands). N is clamped to `[1, min(len(tables), maxCopyTableParallelism=32)]` so a typo can't open a thousand connections.

Consistent with ADR-0099/0100 §3's honesty fix, **N is NOT folded into the connection-budget preflight in v1.** That preflight resolves only D (the target write fan-out); N is a source-DSN knob parsed inside the engine, invisible to the pipeline-layer preflight. The engine emits a **WARN naming N** when the concurrent copy starts, stating the operator contract: `N × D ≤ --max-target-connections` AND `N ≤ source max_connections` are the operator's responsibility (no false auto-clamp claimed that isn't implemented). The existing D-axis preflight still fires its loud refusal exactly as before. A future enhancement folds N into the preflight for a real product-clamp.

### 6. Composition with the ADR-0100 consumer — the multi-snapshot RowReader is the only new reader code

The ADR-0100 consumer calls `rows.ReadRows(table)` on ONE `ir.RowReader` from W concurrent goroutines. The existing binlog `RowReader` holds ONE `querier` (one pinned `*sql.Conn`) — a single InnoDB connection cannot run N concurrent `SELECT`s. So the concurrent path returns a **multi-snapshot RowReader**: it holds the N pinned connections + the table→connection map (derived from the partition), and its `ReadRows(table)` dispatches the table to **its group's** connection. Because the partition is disjoint, each connection serves exactly one group's tables, and the ADR-0100 consumer drains each group serially within one goroutine ⇒ each connection runs at most ONE `SELECT` at a time (no intra-connection concurrency). The N goroutines run N `SELECT`s concurrently across the N **distinct** connections — safe.

This reader implements `ir.ConcurrentCopyPartitioner` (returns the N groups) and does NOT implement `ir.IdempotentCopyReader` — the binlog snapshot is gap-free and overlap-free by construction (each table read exactly once from a frozen REPEATABLE-READ view; plain INSERT, no upsert). The ADR-0100 consumer's existing guard refuses a concurrent partition unless the reader is idempotent (it was written for the VStream re-emitting COPY). That guard is **widened**: a concurrent partition from a NON-idempotent reader is allowed onto the plain-INSERT concurrent path, because a non-idempotent gap-free reader is exactly the native-MySQL snapshot — its disjoint partition means each table is plain-INSERTed by exactly one pipeline, no re-emission to absorb. The guard's original intent (never concurrently plain-INSERT a *re-emitting* stream) is preserved: an idempotent reader still takes the upsert path; only the gap-free non-idempotent reader gains the plain concurrent path.

### 7. CDC handoff — the single recorded position, read only after all pipelines complete

The CDC anchor is the ONE binlog position recorded inside the FTWRL window (§2 step 3), stored on `stream.Position` at open and **never mutated during copy** (unlike VStream, which stitches its position after copy). So position-after-all is structural and needs no new barrier: the streamer reads `stream.Position` only after `coldStartRunCopy` returns, which returns only after `runConcurrentTableCopy`'s W-way errgroup joins every reader+writer (ADR-0100 §4). No `WaitCopyCompleteFn` is set on this path — the position is already correct and immutable at open, so the handoff's nil-hook default (drain-then-read) is correct. The recorded position is a cut at-or-before every reader's snapshot, and CDC replays everything after it; the snapshot→CDC seam is the same single-position seam the serial binlog path already uses, just fed by N readers instead of one. There is no per-table overlap (gap-free snapshot), so the seam is exactly-once for committed rows at the cut and at-least-once only for the unavoidable [position, first-CDC-event] binlog tail the idempotent applier already absorbs.

### 8. Lifecycle — N transactions committed/closed, ctx-cancel releases the lock + all connections, no leak

The multi-snapshot RowReader / SnapshotStream owns all N connections + the writer pool. `ReleaseRows` / `Close` COMMITs and closes **all N** pinned connections (idempotent, first-error-wins, same shape as the single-reader path's `releaseRows`). If the opener fails partway (e.g. the 3rd `START TRANSACTION` errors), it unwinds every already-opened connection AND releases the FTWRL before returning — no half-open lock, no leaked connection. ctx-cancel during copy cancels the W-way errgroup (every reader goroutine's `ReadRows` ctx is the errgroup's derived ctx), and `Close` then commits+closes the N connections; the FTWRL is already released by then (it is held only during open). A goroutine-count-delta + no-leak `-race` test pins it.

## Alternatives considered

- **N independent `START TRANSACTION WITH CONSISTENT SNAPSHOT` + per-reader position + position-min stitch (no FTWRL).** Avoids the global lock, but reintroduces the per-reader-position-alignment hard part the FTWRL sidesteps, plus a loud-refusal-on-divergence path. The FTWRL is ms-scale and available on the self-managed source. **Rejected for v1; documented as the FTWRL-denied future enhancement** (§3/§4).
- **Proceed with N independent snapshots when FTWRL is denied (no fallback).** Silently inconsistent multi-table snapshot — the worst silent-loss class. **Rejected**; FTWRL-denied falls back to serial (consistent) or refuses, never proceeds inconsistently (§4).
- **A new pipeline-level CLI flag instead of a source-DSN knob.** Couples the engine-neutral orchestrator to a binlog-snapshot-specific concurrency mechanism. **Rejected** for the DSN knob (mirrors ADR-0099 §3).
- **One shared writer pool draining any table (not W pipelines).** ADR-0100 §1 rejected this for the VStream path (breaks the 1:1 producer↔consumer-per-table coupling, needs a new scheduler). It is even less attractive here: the per-group connection ownership means a table can only be read from ITS group's connection, so a pool would still need the partition. **Rejected**; reuse the ADR-0100 W-pipeline consumer verbatim.
- **Re-derive a separate partition for the native path.** The IR-first tenet + ADR-0099's already-unit-pinned `partitionTablesForStreams` make reuse strictly better. **Reused verbatim** (a pure function of (sorted tables, N)).

## Consequences

- **Native vanilla-MySQL → PS-MySQL / MySQL cold-copy throughput rises with N**, lifting the Track-D serial-consumer ceiling (~0.66 MB/s serial, one table at a time) toward ~N× that rate — the target PROCESSLIST should now show up to W = N tables receiving rows concurrently. Note this is **W × 1** in v1 (one writer per concurrent table): it removes the *serial-consumer* bottleneck but each table's writer is still a single cross-region INSERT stream, so it does not by itself reach the VStream PS-MySQL ceiling (~26 MB/s, which is W × D). Full parity needs the per-table D fan-out follow-up. Bounded by source/target/network capacity and the N (→ N × D) connection budget, not by sluice's consumer.
- **Reuses the ADR-0100 pipeline consumer verbatim** (`runConcurrentTableCopy` + `ir.ConcurrentCopyPartitioner`) and **ADR-0099's deterministic disjoint partition** — the only new core is the source-side FTWRL-coordinated N-snapshot opener + the multi-snapshot RowReader.
- **The snapshot→CDC seam is a SINGLE recorded position** (no stitch) — simpler and more obviously gapless than the VStream set-min seam, because all N readers share one consistent cut under the FTWRL.
- **FTWRL-unavailable falls back to the serial single-snapshot path with a loud WARN** — consistency preserved, concurrency disabled; never a silent inconsistent snapshot.
- **N = 1 (no surfaced groups) is byte-identical to today** — the serial table loop, one pinned transaction, no FTWRL-N-open, no errgroup. The zero value and absent DSN param both resolve to N = 1.
- **Concurrency chunk → `-race`-before-tag.** This adds N concurrent reader goroutines (the W-way errgroup), N pinned connections, the FTWRL open/unlock ordering, and the ctx-cancel/cleanup path. Per the project rule, the integration **`-race`** gate MUST pass **before** any tag is cut (push-first, tag-after, or `scripts/race-integration.ps1`). CGO is off on the dev box, so `-race` is CI-only here.

## Testing

- **Concurrency knob parse + resolve (unit):** `copy_table_parallelism` parses (absent → 1, valid → value, malformed → LOUD error), `resolveCopyTableParallelism` clamps (reused from ADR-0099, re-pinned for the native caller). `stripVStreamParams` strips `copy_table_parallelism` so it never reaches the MySQL session.
- **Partition coverage + disjointness + determinism (unit):** inherited from ADR-0099's `partitionTablesForStreams` pin (every table in exactly one group, shuffle-invariant). An additional pin asserts the native opener surfaces exactly that partition via `ConcurrentCopyGroups()`.
- **Multi-snapshot RowReader dispatch (unit):** a `ReadRows(table)` routes each table to its group's connection; a table not in any group is refused loudly (never silently read from a wrong/zero connection).
- **Concurrent-partition guard widened (unit, pipeline):** a NON-idempotent reader surfacing ≥2 groups now drives the plain-INSERT concurrent path (not refused); an idempotent reader still takes the upsert path; a reader with no partition takes the serial loop byte-identically.
- **Zero-value-safe N = 1 (unit):** absent param / N = 1 / one-table scope surface NO concurrent groups ⇒ the serial single-snapshot opener runs (byte-identical), no FTWRL-N-open path taken.
- **FTWRL-unavailable → serial fallback (unit + integration):** an opener whose FTWRL fails AND N > 1 falls back to the serial single-snapshot path with a loud WARN, NEVER opening N independent snapshots (the silent-inconsistency guard). Integration simulates a restricted (no-RELOAD) user.
- **Open-failure unwind (unit):** a forced error on the k-th `START TRANSACTION` releases the FTWRL and closes the already-opened connections — no leaked lock, no leaked connection.
- **ctx-cancel leaks nothing (unit, `-race`):** cancel mid-copy with W = N consumers running → goroutine-count delta == 0, all N connections closed, copy reports the cancel (not success).
- **Position after all pipelines (unit):** the recorded position is read only after the W-way errgroup joins (structural — the streamer reads `stream.Position` after `coldStartRunCopy` returns); no per-pipeline position write exists to advance mid-copy.
- **Integration (`integration` tag, real MySQL with RELOAD):** cold-copy a multi-table DB with `copy_table_parallelism=N>1` through the FULL pipeline → (a) assert MULTIPLE tables receive rows in an overlapping window (poll per-table target counts / wall-clock << serial sum); (b) target `COUNT(*)` + content checksum == source per table (no gap/dup); (c) a clean CDC handoff from the ONE recorded position — apply post-snapshot writes, confirm they replicate exactly-once. Plus the FTWRL-denied serial-fallback case.

## Silent-loss surfaces for value-fidelity review

Four invariants, each a silent-loss class if broken, called out explicitly:

1. **All N readers pin the SAME consistent cut as the recorded position.** The FTWRL must be held across ALL N `START TRANSACTION WITH CONSISTENT SNAPSHOT` calls AND the `SHOW BINARY LOG STATUS` read, then released. A reader opened after UNLOCK (or the position read outside the lock) could pin a different cut → a multi-table snapshot whose CDC anchor names no consistent point → silent gap/dup. Guard: the open/record/unlock ORDERING is pinned (FTWRL before the first `START TRANSACTION`, all N + the position read inside, UNLOCK after); the FTWRL-denied case falls back to serial, never proceeds with N.
2. **Every in-scope table is read by exactly one reader (disjoint partition).** Inherited from ADR-0099's `partitionTablesForStreams` coverage/disjointness pin — a table in zero groups is silently never copied; a table in two groups is read from two connections into one queue's drain. The multi-snapshot reader's table→connection map IS the partition.
3. **The CDC position == the FTWRL-recorded position, read only after all pipelines complete.** The position is recorded once inside the lock and never mutated; the streamer reads it after the W-way errgroup joins (ADR-0100 §4). No per-pipeline position write exists. A position read mid-copy or a re-recorded position would risk a gap.
4. **FTWRL-unavailable NEVER produces a silently inconsistent N-snapshot.** It falls back to the serial single-snapshot path (consistent) with a loud WARN, or refuses — pinned, not best-effort.
