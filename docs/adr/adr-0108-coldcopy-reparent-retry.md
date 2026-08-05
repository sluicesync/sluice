# ADR-0108: Cold-copy reparent-retry

## Status

**Accepted (2026-06-21).** The copy-phase analog of ADR-0038's
apply-phase retry. Scoped to the MySQL cold-copy write path (the
demonstrated Track-D failure). Roadmap item 33.

## Context

A bulk cold-copy of a large table runs for minutes. During that window a
**transient target primary reparent** can occur — most concretely, a
PlanetScale **non-Metal** branch hitting a storage **auto-grow** at the
~39 GB boundary, which triggers a primary reparent of the underlying
Vitess shard. The in-flight `INSERT` connection dies and the bulk write
returns the raw driver error.

The CDC **apply** path already rides this out: ADR-0038's classifier
(`internal/engines/mysql/applier_errors.go` `classifyApplierError`) wraps
the transient shapes (Error 1105 `code = Unavailable` — how "tablet not
serving" surfaces — plus `driver.ErrBadConn` / `gomysql.ErrInvalidConn` /
`io.EOF` / `context.DeadlineExceeded` / "connection reset by peer" /
"broken pipe" / "i/o timeout") as `ir.RetriableError`, and the
pipeline's streamer retry loop (`internal/pipeline/streamer_retry.go`)
retries with bounded ADR-0038 backoff.

The **cold-copy** path did **not**. The MySQL `RowWriter` batch flush
(`row_writer_batch.go` for the idempotent UPSERT path,
`row_writer.go`/`writeBatchedConn` for the plain INSERT path, and their
`WriteRowsParallel` / `WriteRowsIdempotentParallel` fan-out callers)
returned the raw driver error unwrapped, with no retry. Confirmed live on
Track-D: on the reparent the cold-copy process **EXITED** and the
watchdog **crash-looped (9 relaunches)** — each relaunch immediately
re-hit the still-in-progress reparent. The supervisor could not make
progress; the copy itself had to ride it out.

## Decision

Add a **bounded, observable reparent-retry around the cold-copy batch
flush** in the MySQL RowWriter — the copy-phase analog of ADR-0038.

### Where the retry lives

Unlike ADR-0038 (which lives on the pipeline side because the pipeline
owns the apply batch loop), the cold-copy retry lives **in the engine**,
in one helper — `flushWithReparentRetry`
(`row_writer_reparent_retry.go`) — wrapped around the per-batch flush
that both the plain and idempotent flush closures call. Reasons:

- The pipeline's ADR-0038 loop is apply-phase only; the cold-copy bulk
  write is engine-internal (the orchestrator hands the writer a row
  channel and waits). There is no pipeline-side batch loop to wrap.
- The load-bearing recovery step — **re-acquire a FRESH connection** —
  is inherently engine-local (`w.db` is the engine's pool).

The helper deliberately does **not** import `internal/pipeline` (an
engine package must not). The backoff shape is re-derived in a small
self-contained function (`coldCopyReparentBackoff`), mirroring
`pipeline.computeRetryBackoff` minus the `RetryHint` plumbing (cold-copy
has no hint source).

### Classification reuse

A flush error is routed through the SAME `classifyApplierError` the CDC
apply path uses. The loop retries **only** when the classified error
satisfies `ir.RetriableError`; otherwise it returns the error unchanged
(terminal, exactly as before this ADR). No new retry class is introduced
for the copy path beyond what the apply path already trusts.

### The re-acquire-fresh-conn requirement (load-bearing)

The cold-copy connection is **pinned** for the whole table write (so the
post-flush session-scoped `SHOW WARNINGS` Vector-B probe reads on the
same session that ran the INSERT). **After a reparent the pinned conn is
DEAD.** A retry MUST re-acquire a FRESH connection from `w.db` — the pool
reconnects to the new primary on the next `db.Conn()` — and re-run BOTH
the flush exec AND the warning check on it. It must **never** reuse the
dead conn. `flushWithReparentRetry` enforces this structurally: the first
attempt runs on the caller's pinned conn; every retry calls
`w.db.Conn(ctx)` for a fresh one, runs the attempt, and closes it.

### Bounds

Package constants (vars, so the tests can shrink them — production never
mutates them, so there is no zero-value-default trap; they are baked at
package init, not config fields):

| Bound | Value |
|---|---|
| attempts | 12 |
| base backoff | 100ms |
| per-attempt cap | 30s |

Exponential doubling from base, capped: `100ms → 200ms → … → 25.6s → 30s
→ 30s → …`. 12 × max(30s) ≈ up to ~4 min tolerated before a **LOUD
terminal error** — long enough to ride a 30–60s (up to ~2–3 min)
reparent / storage-grow, short enough that a genuinely-wedged target
surfaces rather than hiding for hours. Stretched slightly past ADR-0038's
8×30s because a storage-auto-grow reparent can run longer than a
tx-killer blip.

Each retry logs a loud `WARN` (table, attempt, max, backoff, err). On
exhaustion the helper returns a loud terminal error naming the table +
row count + attempt count and wrapping the most recent transient (`%w`,
so the underlying `*MySQLError` stays reachable). The terminal error does
**not** implement `ir.RetriableError` — once the budget is spent, the
copy aborts. The backoff honors `ctx.Done()` for prompt cancellation.
**Never infinite, never silent.**

### Classifier extension (belt-and-suspenders)

`classifyApplierError`'s text fallback is extended to also match the
substrings **"not serving"** and **"reparent"** (case-insensitive,
pinned in `reparentRetriableSubstrings` with a change-detector test in
the same discipline as `vitessRetriableSubstrings`). The vttablet-framed
shape (Error 1105 `code = Unavailable`) is already caught, but a
PlanetScale/vtgate reparent can surface **without** that framing; this
fallback catches it. It also benefits the CDC apply path (ADR-0038) for
free.

> **This paragraph described an unreachable belt from 2026-07-23 to
> 2026-07-28.** See the implementation note at the end of this ADR — the
> terminal-code shield returns before the text fallback for every
> structured `*MySQLError`, so the un-framed reparent it names was
> classified TERMINAL. Fixed inside the `case 1105:` arm.

### Plain-path 1062-on-retry tolerance (the wart)

A plain cold-copy batch is a SINGLE atomic multi-row `INSERT`. On a
classified transient the prior attempt either:

1. **fully rolled back** → the retry re-applies cleanly; or
2. **committed but the ack was lost** (the reparent dropped the
   connection between the server's commit and the client's
   acknowledgement) → the retry re-applies the BYTE-IDENTICAL batch and
   collides with the rows it already landed → **Error 1062** (duplicate
   key).

Because the batch is byte-identical and **cold-copy is the SOLE writer
onto a fresh target**, a 1062 **on the retry of the same batch** PROVES
those exact rows are already durable. So the plain path **tolerates** it
(treats the batch as done, continues — no silent loss; the data is
there). This is implemented as a named, commented wart in
`writeBatchedConn` ("tolerate-1062-on-retry"), with a loud WARN when it
fires.

### The keyless carve-out — where the argument above does NOT hold

> **Added 2026-08-05 (audit finding B-9).** The paragraph above was
> written as if it were universal. It is not, and the exception is a
> silent-duplication hole rather than a corner case.

The entire proof is about a **collision**, and a collision needs
something to collide with. On a table with **no PRIMARY KEY and no
all-NOT-NULL UNIQUE index**, branch (2) produces **no 1062 at all** — the
re-sent batch simply inserts a second copy of every row, the flush
reports success, and the copy exits 0 with the table silently doubled.
Nothing in the original design noticed, because the design reasoned about
the collision and never about its absence.

This mattered more after v0.111.1 widened the vtgate 1105 classification:
that widening ARMED the retry on exactly the production PlanetScale path
the first field report came from.

**What sluice does now.** `flushWithReparentRetry` — the one place the
retry policy lives — takes the `*ir.Table` and, before re-sending
anything, refuses when `irbackup.TableReplayIdempotent` reports false.
The refusal is coded `SLUICE-E-COPY-RETRY-AMBIGUOUS-KEYLESS`, names the
table and the batch size, and points at the durable remedy (add a key).
The grow gate is still tripped and the reparent still recorded before the
refusal: the transient was real, so the sibling lanes should still
quiesce and the restore reconciler should still hear about the table.

**Refuse, not reconcile — and why.** The audit's alternative was a
post-table `COUNT(*)` reconciliation, which detects instead of prevents
and would keep a keyless table riding a reparent. It was rejected because
it cannot be made honest at this layer: there is no trustworthy baseline
to compare against (`WriteRowsParallel` runs N workers over one table, and
the resume/restore paths write into a deliberately non-empty target), the
comparison would be a full scan issued at the moment the target is least
able to serve it, and it would only fire after the whole table had copied
— at which point the remedy is the same restart the refusal names
immediately. Comparing against "rows I sent" is the writer's own
bookkeeping, which is the evidence-sharing shape the project's
name-the-independent-expected-value rule exists to reject.

**The cost, stated plainly.** A keyless table whose cold copy hits a
genuine transient now fails where it would usually have succeeded (the
rolled-back branch is the common one). That is a real regression in
resilience for that table shape, accepted because the alternative is a
silent double-insert.

**Where else this shape lives.** The gate sits in the shared helper, so
every MySQL write core that retries inherits it. The Postgres chunked-COPY
retry (`copyChunkWithRetry`) carried the same hole with a narrower
rationale — its comment argued only the rolled-back branch — and now
carries the same gate and the same code. `raw_copy` and Postgres's plain
multi-row INSERT core never re-send at all and are exempt for that reason
— the plain INSERT core deliberately so, and it takes the Await/Trip
halves of the grow gate without the replay (`quiesceAndReportTransient`).

**`LOAD DATA` was on that exempt list, and as of item 114 it is not.**
When this ADR was written, MySQL's `LOAD DATA LOCAL INFILE` core streamed
one statement per table and had nothing buffered to re-drive, so it could
not replay and the exemption was accurate. Item 114 segmented it into
bounded statements and drove **each segment through this same
`flushWithReparentRetry`** — deliberately the shared helper, so the retry
policy, the ADR-0110 grow-gate Await/Trip and the wall-clock bound are not
re-derived per core. That means the `LOAD DATA` core now re-sends, and it
**inherits this keyless carve-out rather than the exemption**: a keyless
table hitting a classified transient there refuses with the same
`SLUICE-E-COPY-RETRY-AMBIGUOUS-KEYLESS`. On a KEYED table the replay is
convergent for a reason specific to this core — `LOAD DATA` *LOCAL*
downgrades a duplicate-key error to a warning and skips the offending row,
so a byte-identical replay inserts exactly the rows that are missing
whether the prior attempt rolled back, committed, or committed a prefix
(verified against a real MySQL 8.0, not assumed). See
`internal/engines/mysql/load_data_writer.go`'s header for that argument and
for the post-load warning-probe wart it costs.

**What this does NOT change.** The at-least-once CDC apply accounting for
keyless tables (ADR-0089) is unaffected; that is a separate, documented
window with its own reasoning.

**The tolerance is scoped to `isRetry` ONLY.** A FIRST-attempt 1062 stays
**terminal** — a real non-PK uniqueness violation or a dirty target must
fail loudly (unchanged ADR-0038 policy: 1062 is non-retriable). The
`flushWithReparentRetry` helper threads an `isRetry bool` into the
attempt closure precisely so the plain path can make this distinction;
the helper itself never tolerates anything (it only retries classified
transients).

The **idempotent path needs no such wart** — its `ON DUPLICATE KEY
UPDATE` absorbs the ambiguous-commit replay natively. It, too, needs a
key for that to be true, and it already refuses a keyless table upfront
(`errKeylessIdempotent`); `TestReplayKeyPredicatesAgree` pins that its
predicate and the carve-out's classify every table shape identically, so
routing it through the shared gate cannot refuse a table it used to
accept.

### Fan-out composition

The retry is local to a worker/batch. Under `WriteRowsParallel` /
`WriteRowsIdempotentParallel`, a transient on one table/worker now
retries **locally** (re-acquiring its own fresh conn) instead of aborting
its siblings. The existing errgroup behavior — first **terminal** error
cancels the shared child ctx and unwinds peers — is preserved: only an
**exhausted** or **non-retriable** flush returns terminal and aborts
siblings. The loud-on-genuine-error abort is unchanged.

## Consequences

- A storage-auto-grow / planned-reparent during a large MySQL cold-copy
  is ridden out in-process instead of crash-looping the supervisor.
- The recovery is bounded and observable (≤ ~4 min, WARN per retry, loud
  terminal on exhaustion).
- The plain-path 1062 wart is the one subtlety. It is safe for a table
  that HAS a key (atomic single-statement batch + sole writer onto a
  fresh target) and scoped to retry-after-classified-transient only —
  **but not for a keyless table**, where the collision it rests on cannot
  occur. That case is now refused rather than retried; see "The keyless
  carve-out" above. The earlier wording here read "provably safe" without
  qualification, which is the overclaim the carve-out corrects.
- A keyless table's cold copy no longer rides a transient at all. It
  fails loudly and restartably instead of possibly duplicating.

## Scope / non-goals

- **MySQL cold-copy only** (the demonstrated Track-D path) *as originally
  written*. **Superseded 2026-08-05:** the PG chunked-COPY retry shipped
  since (roadmap item 38 / `postgres/row_writer_grow_gate.go`), and audit
  B-9 found it carrying the same committed-but-unacked hole behind a
  narrower rationale, so the keyless gate landed on both engines in one
  change. This bullet is kept rather than deleted because "NOT addressed
  here — noted as a follow-up" is exactly the kind of line that stops the
  next reader from checking.
- No new CLI flag or config field — the bounds are baked constants (the
  envelope is sized for the managed-Vitess reparent window; the apply
  path's `--apply-retry-*` knobs remain apply-phase only). If a future
  operator needs to tune the copy envelope, promote the constants to
  config with zero-value-safe defaults at that time.

## Concurrency note

This touches the cold-copy write path including the parallel-worker
fan-out (`WriteRowsParallel` / `WriteRowsIdempotentParallel`) — a
`-race` integration gate is required before any tag (the per-worker
retry, fresh-conn re-acquire, and shared `workerCtx` cancel are the
concurrency-sensitive surface).

## Implementation note (2026-07-28): the belt was unreachable — the canonical vtgate reparent error classified TERMINAL

Found in the wild against real PlanetScale (PS-160, a 122 GB copy of a
153M-row table). In one reparent window vtgate returned three errors: the
vttablet-framed `QueryList.TerminateAll()` (retried, 1105 + vttablet), a
`1205 Lock wait timeout` (retried, code-only), and

```
Error 1105 (HY000): target: scaletest-mysql.-.primary: primary is not serving, there may be a reparent operation in progress
```

which **aborted the migration after 38 seconds** with
`SLUICE-E-BULKCOPY-TABLE-FAILED` — neither the 30m wall-clock bound nor
the 100000-attempt backstop reached (confirmed by the absence of the
budget-exhaustion wording).

**Why.** That sentence is emitted by **vtgate itself**
(`vitess.io/vitess` `go/vt/vtgate/buffer` `ClusterEventReparentInProgress`,
raised in `tabletgateway.go` as `vterrors.Errorf(Code_CLUSTER_EVENT, …)`
when a PRIMARY target has no serving tablet), so it carries **no
`vttablet` tag** — and `classifyVitessMessage` requires that tag by
design ("a bare HY000 without it is a non-Vitess generic error and stays
terminal"). The `case 1105:` arm therefore fell out of the switch into
the **terminal-code shield** (audit 2026-07-23 D0-3), which returns
verbatim *above* the text-fallback legs. The
"belt-and-suspenders" fallback this ADR describes has been unreachable
for every structured `*MySQLError` since that shield landed. The shield
fix was correct — it closed a 1062 silent-batch-skip — and it silently
orphaned this belt: a comment asserting coverage with no test that fails
when the coverage stops existing.

**Fix.** The canonical vtgate availability sentences are now an
explicit structured-code + message AND-gate **inside** `case 1105:`
(`vtgateTransientSubstrings` / `isVtgateTransientMessage`),
alongside the existing vttablet gate. The shield is untouched: 1062 and
every other structurally-terminal code still return verbatim.

**Deliberately NARROW, not the loose fallback set.** The in-switch gate
matches the *full* vtgate sentences, never the loose `"reparent"` /
`"not serving"` tokens, because a 1105 message can echo the offending
statement (`… : Sql: "insert into reparent_history …"`) — and a
false-positive transient **arms the tolerate-1062-on-retry wart** on the
next attempt, which is the D0-3 silent-batch-skip chain. The loose set
still applies below the shield, where no server response exists.
`TestClassifyApplierError_TerminalCodeShield` now includes 1105 in its
cross-product so widening that gate fails a test.

**Gate.** `TestClassifyApplierError_UnframedVtgateReparent` pins the
verbatim wire strings plus the other errors from the same windows, the
terminal codes that must not move, and the echo negatives;
`TestVtgateTransientSubstrings_PinDown` pins the literals against
vtgate's raise sites. Mutation-checked both directions (neuter the gate →
FAIL; widen it to the loose set → the shield's 1105 rows FAIL).

**Same defect, both paths.** Cold copy and the CDC apply path share one
classifier, so ADR-0038's apply retry had the identical hole; both
wrapper frames are pinned in the gate. The `ir.TransientClassifier`
DDL-phase verdict (ADR-0114) rides the same fix.

### The three-sentence set was incomplete, and the field found it in HOURS

The first cut of this fix derived its match set from
`buffer.ClusterEvents` — vtgate's own exported list of cluster-event
sentences. That is a principled source, it is upstream's own
enumeration, and **it was wrong for our purpose.** A rebuild of the
branch was deployed to the scale rig and re-run against a clean PS-160.
It rode **18 transients** that would previously have aborted, and then
died at ~7.1M rows on a *fourth* sentence:

```
Error 1105 (HY000): target: scaletest-my3.-.primary: inconsistent state detected, primary is serving but initially found no available tablet
```

raised at `tabletgateway.go:400` — **a few lines below the reparent
raise, in the same function** — as `Code_UNAVAILABLE`, **not**
`Code_CLUSTER_EVENT`. That is exactly why it is absent from
`buffer.ClusterEvents`: upstream's constant list is scoped to the
*buffering* feature, not to "errors a client should retry."

**The generalizable lesson: enumerating from an upstream constant list
has a blind spot wherever upstream raises the same class outside its own
list.** The list is authoritative for what upstream put IN it and says
nothing about what it left out. The correct derivation is the *raise
sites* — walk the function, read every `vterrors.Errorf`/`New` on the
path, and classify each one — with the constant list as one input rather
than the boundary. The set is now derived that way, from BOTH
`buffer.ClusterEvents` AND the `Code_UNAVAILABLE` raises in
`tabletgateway.go`'s `withRetry`, with file:line for each source recorded
on `vtgateTransientSubstrings` so the next reader re-derives instead of
trusting.

Corroboration that these are one class, not two: upstream's own
`tabletgateway_flaky_test.go:349` accepts **either** the inconsistent-state
sentence **or** `no healthy tablet available for '…'`, commenting
"depending on whether the health check ticks before or after the buffering
code, we might get different errors." Two faces of one race — so matching
one and not the other would have been arbitrary, and the second field
abort was in some sense scheduled the moment the first fix shipped.

**Sentences matched** (see the slice comment for the per-site citation):
the three `buffer.ClusterEvents` cluster events, plus
inconsistent-state (`:400`), no-healthy-tablet (`:406`),
no-connection-for-tablet (`VT14003`, `:422`), and the buffer's
`Code_UNAVAILABLE` sentinels — primary-buffer-full, entry-evicted,
destination-shard-missing (`buffer.go:47-49`), which reach the client
through the `WaitForFailoverEnd` wrap at `:373`.

**Sentences REJECTED, deliberately** — the boundary was considered, not
guessed. `Code_INTERNAL` non-transactional-replica-query (`:337`) and
`Code_FAILED_PRECONDITION` disallowed-tablet-type (`:349`) are semantic /
configuration faults that never self-heal. `VT14002` "no available
connection" (`:414`) *is* availability-class but is three generic English
words a migrated log row can legitimately contain, so it fails the
echo-safety rule this set is built on — and it is raised only when
`err == nil`, so a real availability failure in the same pass preserves
the error we *do* match. `contextCanceledError` "context was canceled
before failover finished" (`buffer.go:50`) is cancel-flavored and stays
terminal for the same reason a bare `code = Canceled` is excluded
(v0.99.94) — a client-side shutdown must not retry. The
`WaitForFailoverEnd` wrap sentence itself is **not** matched even though
it denotes a real failover, because it wraps all four buffer sentinels
*including* the canceled one; matching it would silently defeat that
exclusion. `ClusterEventMoveTables` ("disallowed due to rule") stays
rejected: too generic, and a routing-rule denial does not self-heal the
way a failover window does. Each rejection has a negative test row.

## See also

- ADR-0038 — applier retry on transient errors (the apply-phase analog;
  the classifier this reuses)
- ADR-0102 — native-MySQL per-table write fan-out (`WriteRowsParallel`,
  the plain-INSERT fan-out caller)
- ADR-0097 — write-side fan-out (`WriteRowsIdempotentParallel`)
- ADR-0007 — position persistence (the whole-table join is the durability
  guarantee; the retry is invisible to it)
- roadmap item 30 — the PS-10 non-Metal storage-resize resilience runs
  that surfaced this
