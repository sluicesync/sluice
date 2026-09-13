# sluice v0.152.1

**Error classification, fixed in four places — and three of them were found by the checks that run before a tag, not by the work that prompted them.** The headline is a real migration that died three minutes into a storage-grow window it was otherwise handling perfectly. The rest are what fell out when the pre-release reviews asked the two questions this project's conventions require: *did you enumerate the siblings*, and *is the fix actually reaching its consumers*. The answers were no and no.

Nothing here loses or alters data. Every item turns a working recovery into a dead run or a half-hour stall, which is the next-worst thing.

## A full volume killed the copy, and the retry machinery was innocent

A fresh PlanetScale Neki branch starts with a 10 GiB volume that grows on demand. Copying 13 GB into one, everything sluice is supposed to do happened: the grow gate closed and reopened, chunks retried, the shard reported itself read-only while the platform worked. Then the run died at **3m40s** after four chunk retries — nowhere near any budget.

The retry budget was never the limit. **One error arrived without a verdict attached.**

`afterConnectRegisterGeometry` is an AfterConnect hook, so it runs on *every* connection the engine opens — including every per-chunk writer connection a parallel copy mints. When the volume filled, the shard's sidecars went unhealthy and that hook's spatial-OID probe came back `NK205 no healthy sidecars available`. NK205 is classified transient on purpose — exactly so a copy can wait out this window — but the hook returned the error **bare**. By the time it reached the chunk-open retry, which decides by asking whether an error carries an engine verdict, there was nothing to read. A condition that clears the moment the platform finishes growing the volume was treated as fatal.

It is classified now. Re-measured on a fresh PS-10 with the identical 13 GB workload:

| | before | after |
|---|---|---|
| outcome | died at 3m40s | **completed, 3 tables in 17m23s** |
| `NK205`s | 1, fatal | **34, all ridden out** |
| volume | filled, run dead | **grew 10 GiB → 39 GiB mid-copy** |
| data | — | **12,000,512 rows, counts and content hashes exact vs source** |

The 39 GiB figure is from the platform's own metrics, not from sluice's logs, and the verification is against the source database rather than against anything the run wrote about itself. **After a storage-grow operation completes, the copy now continues automatically** — no operator action, no `--resume`, no restart.

This is worth stating plainly because the diagnosis was wrong twice before it was right: the entry tier was never the problem, and neither was pacing. A single unclassified `return err` in a connection hook was the whole difference.

### …and it had two siblings, one of them three lines away

The fix above was written, measured, and very nearly tagged covering exactly one function. The pre-tag perf-parity review asked which other connection-setup returns land on the same path, and found two:

- **`detectPostGIS`** runs in the same two functions, three lines after the pool opens — a `pg_extension` catalog query of exactly the fixed probe's shape. On the same sick shard, `NK205` arrives here just as readily. Which of the two probes a failing shard happened to hit first would have decided the run: a coin flip between fixed and not.
- **`afterConnectSessionPins`** is the *other* AfterConnect hook, and it is the wider one — the default on every pool, where the geometry hook is on four — and hook composition runs it **first**. Until it was classified, the fix above was only ever reachable on connections where this one had already succeeded.

Both classify now. The durable half is `TestConnectionSetupReturnsAreClassified`, an AST roster over the connection-setup functions that fails on any bare `fmt.Errorf` return, with a floor that fails if a rostered function is renamed out from under it rather than passing by finding nothing. The existing classification gate could not have caught any of this: it walks errors *parked* via `setErr`, and a hook that **returns** its error is invisible to it.

MySQL has the same shape at two sites and is **not** fixed here — it has no equivalent classifier to wrap with, and creating one means making the schema-drift decision below on that engine first. Filed rather than implied.

## A missing table is not something to wait for (Bug 285, a v0.152.0 regression)

`42P01 undefined_table` is classified **retriable**, and for the CDC applier that is correct: a long-running stream can meet a table the target does not have yet, sluice does not auto-apply DDL, and an operator creating it is the documented recovery. Riding it out beats exiting into a supervisor restart loop.

v0.152.0 gave the raw-copy lane a retry and routed its failures through that same classifier — so the lane inherited a verdict written for a different problem. On a cold copy the premise is false in both halves: the relation is one *this run* created minutes earlier, and nobody is adding it mid-migrate.

The cost was measured: v0.151.1 answered in **67 ms** with `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` and the right question — *"did the schema-apply phase fail or apply to a different schema?"*. v0.152.0 retried for **30m24s across 69 attempts**, then reported the same code behind a headline about storage growth. Loud and lossless throughout, but half an hour of activity where there had been a one-line diagnosis, and the first sentence an operator read was a misdiagnosis.

Fixed as a class rather than on the lane that changed. The regression cycle graded against the control before filing, which is what showed the typed lane had the same stall all along — so the copy and schema paths now share `classifyCopyError`, which reverses exactly this one verdict, and the CDC applier keeps the retriable behaviour it needs.

### The reversal was reaching one consumer in six

That fix worked by *wrapping*: take the already-classified retriable error and put a terminal wrapper around it. `Unwrap` is preserved deliberately, so `errors.Is`/`errors.As` against the underlying driver error keep working — and that is exactly what defeated it. **`errors.As` does not stop at a type that fails to implement the interface it is looking for; it unwraps straight through and matches what is inside.** The terminal wrapper implemented only `Terminal()`, so the retriable verdict underneath stayed reachable.

Measured on the real types before the fix: `ir.IsTerminal(err)` returned **true** and `re.Retriable()` returned **true**, for the same error, at the same time.

One consumer of six tests `ir.IsTerminal` first and saw the verdict — the chunk-open retry, which is the lane Bug 285 was reported on, so the reported symptom was genuinely fixed. The other five ask the ordinary `errors.As(...) && re.Retriable()` question and did not:

- the typed COPY core and the idempotent batch core rode a missing relation to the ~30-minute reparent wall — and on a **keyless** table reported it as an ambiguous-replay refusal, *"a replay could double rows"*, about a table that does not exist;
- `quiesceAndReportTransient` **tripped the run-wide grow gate**, parking every sibling cold-copy lane to wait out a storage-grow window that was not happening — which is, precisely, the misleading storage-growth headline Bug 285 was filed about;
- the index and constraint DDL phases retried a `CREATE INDEX` against a missing column.

The fix is two methods: the wrapper implements the interface it reverses, so the match happens *at* the wrapper and stops. That also repairs v0.152.0's in-doubt-COMMIT refusal and the dead-pinned-connection refusal, which are terminal producers feeding the same five predicates and were shadowed the same way.

**Why the pins did not see it, which is the part worth keeping:** the Bug 285 test asserted `ir.IsTerminal` — the one question that already worked. Its neighbour in the same file grades the other verdicts with the `errors.As && Retriable()` idiom and would have passed for a missing table too. Both were green against code where the fix was inert. This is the evidence-sharing shape: the check and the thing checked derived their answer the same way, so there was no independent expected value. The replacement asks **both** questions of **every** terminal producer and requires them to agree.

### A CDC classifier changed verdict by accident, in the commit that promised it would not

The Bug 285 commit is titled *"schema drift is retriable for CDC and TERMINAL for a copy"*. It also switched the PG CDC **reader** to the copy classifier — a one-word edit that made schema drift terminal across the CDC pump's five walreceiver call sites, contradicting the commit's own stated scope, with no test in either direction to notice.

Reverted, and pinned. The pin compares the two classifiers against each other on the same input, so it cannot pass if the split collapses in *either* direction. Whether the CDC reader should eventually take the terminal verdict is a real question — its errors arrive from the *source*, where "an operator is about to create the missing relation" is a much weaker premise — but that is a change owed a measurement, not a patch-release side effect.

## Fixed

- **A transient platform fault during connection setup is retried rather than failing the table** — at all three Postgres connection-setup returns, not the one that produced the bug report. Reaches every per-chunk connection, so it is the difference between surviving a volume grow and dying in it.
- **A terminal verdict is now visible to every consumer, not just the one that asks the right way.** Applies to all three of the engine's terminal producers, including two that shipped in v0.152.0.
- **A missing target relation fails a copy immediately again**, with the code and the remedy, instead of being retried to a wall it can never clear — and without tripping the run-wide grow gate on the way. The CDC applier and the CDC reader both keep the retriable verdict.
- **The MySQL version matrix is green again.** `SHOW MASTER STATUS` was removed in MySQL 8.4, and a test issued it directly under a comment asserting "MySQL 8.0+ uses SHOW MASTER STATUS" — so the whole engine suite went red on both legs while the production path, which has walked all three spellings for months, was fine. The test now delegates to that production path instead of restating one of its branches. Test-only.

## Compatibility

No flags, no config, no state-table or on-disk format changes. No resume or backup compatibility impact. Every behaviour difference is a retry verdict, and each one moves a run either toward completing or toward failing in milliseconds instead of half an hour.

## Who needs this

- **Anyone migrating into a fresh managed Postgres volume that grows on demand** — PlanetScale Neki most directly. Before this, a copy larger than the starting volume could die during the grow it was meant to survive.
- **Anyone on v0.152.0.** Two of that release's own fixes were partly inert for the same reason, so this is worth taking even if you never hit the storage-grow case.
- **Anyone whose schema-apply phase has ever targeted the wrong schema:** you were waiting half an hour, with the copy's sibling lanes parked behind a grow gate that had nothing to wait for, for an answer available in 67 ms.
