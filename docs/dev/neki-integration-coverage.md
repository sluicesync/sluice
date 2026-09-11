# Neki integration coverage — design

**Status:** designed, not built. The fast-follow the operator chose alongside the MoveTables work when v0.150.0 shipped.

**The constraint that shapes everything here:** Neki is not open source. There is no image to boot in CI, so the testcontainers pattern every other engine uses is unavailable — and the operator flagged the consequence explicitly: *"the integration coverage is likely only going to be possible by using actual PlanetScale Neki databases … so there would be potential long-term costs to consider."* A design that quietly makes a live cluster a prerequisite for every PR would impose a recurring bill for the life of the project.

So the coverage splits in two, and the split is the whole design: **everything that can be proven without a cluster runs on every PR at zero cost; the residue that genuinely needs a real router is opt-in and never gates a merge.**

## What actually needs a live Neki, and what does not

The Neki-specific surface is already mostly pure. Taking it honestly, function by function:

| surface | needs a router? | why |
| --- | --- | --- |
| `isNekiVersion`, `probeIsNeki` | **no** | a string predicate over `version()`; the wire behaviour is the `SELECT`, which plain PG shares |
| `shardKeyFor`, `columnsForGroup`, `planShardKeyUpsert` | **no** | pure functions over a parsed topology document |
| `buildBatchUpsert` | **no** | renders SQL text; pinned on the string |
| `isNekiTableBlocked`, `annotateNekiBlockedTable` | **no** | classifies a `*pgconn.PgError` by SQLSTATE |
| `opclassExtensionFor`, `annotateMissingOpclass` | **no** | regex + mapping over an error message |
| `dropUnchangedShardKeys`, `refuseShardKeyOutsideConflictKey` | **no** | operates on before/after images already in memory |
| `createAndPrimeSequence` taking the no-transaction branch | **partly** | the *branch choice* is unit-testable; that Neki rejects DDL in a transaction is not |
| `readSequencePositionFromCatalog` fallback | **partly** | the fallback trigger is `isNekiRelationReadRefusal`, pure; the refusal itself is router behaviour |
| routing, `ON CONFLICT` per-shard semantics, NK013/NK306/NK213 | **yes** | these ARE the router |
| MoveTables / reshard underneath a live stream | **yes** | workflow machinery has no local analogue |

Most of the tree is already in the first bucket and already pinned. The gap is not "no coverage" — it is that **nothing holds the wiring together**, and nothing re-checks the router behaviours when the platform moves under us. Neki is a platform preview; the measurements in `neki-readiness.md` are true on a build from 2026-09-10 and will rot.

## Tier 1 — the free half, on every PR

**A flavor-forced run of the existing Postgres integration suite.** The engine decides it is talking to Neki from one probe. Force that decision true against an ordinary Postgres container and the whole Neki code path executes — the no-transaction DDL branch, the catalog sequence read, the shard-key planner's no-op path on an unsharded target, the raw-copy decline — against a real server, in the existing shard matrix, for no extra cost.

It cannot prove Neki *behaves* the way the adaptations assume. It proves something narrower and still valuable: **the adaptations do not break on a server that does not need them, and every one of them is reachable.** That second half is what a wiring gate buys, and it is exactly the class `TestEveryNekiFlagIsWiredAtConstruction` already guards statically.

The honest name for this is `TestPostgresSuite_NekiFlavorForced`, not `TestNeki…`, so nobody reads a green run as evidence about Neki.

**Plus the pure-function matrix the table above makes possible** — already largely present; the work is filling the gaps it exposes (notably `planShardKeyUpsert` across the multi-shard × guard × key-shape grid) rather than inventing a harness.

## Tier 2 — the opt-in half, gated and never merge-blocking

Mirror `psverify` exactly; it is the same problem solved once already for live PlanetScale MySQL.

- **Build tag `nekiverify`**, so the files are excluded from every ordinary build and from `go test ./...`.
- **Credentials from the environment** — `NEKI_DSN` (unsharded) and `NEKI_TORTURE_DSN` (multi-shard), matching the local env files.
- **A dedicated workflow**, `workflow_dispatch` by default with the `schedule:` block present but commented out, exactly as `psverify.yml` ships it. Weekly can be switched on later if the platform proves to move fast enough to warrant it; the cost decision stays the operator's and stays reversible in one line.
- **A fail-on-skip belt.** This is the load-bearing part and the reason to copy `psverify` rather than improvise: a missing secret must not green-skip. The psverify audit finding (2026-07-15 MED-T1) was precisely that a dispatch greened while silently skipping four of six suites. A live suite that skips is the dishonest outcome, because "green" then means "we did not look".

**What the opt-in suite should cover, in priority order** — each one is a behaviour already measured by hand, which is the argument for mechanising it:

1. The three refusals that defend against silent duplication: `NK013` on a shard key in a `SET` list, the per-shard `ON CONFLICT` gap, and per-shard `UNIQUE`/`EXCLUDE`. These are the ones where a platform change turning a loud refusal into silent acceptance would be a data-loss regression we would not otherwise notice.
2. CDC into a sharded target end to end, with the ordered-content checksum rather than a row count.
3. `NK213` classification against a real MoveTables cutover.
4. DDL-in-transaction and the sequence catalog fallback.

Reshard and MoveTables mid-stream stay **manual**. They need a writer, a workflow and several minutes of wall clock; automating them would be the most expensive cell in the matrix and the one least likely to regress silently, since both fail loudly today.

## What this deliberately does not do

- **No `neki` engine, no new driver name.** ADR-0186 settled that; coverage follows the code rather than inventing a surface to test.
- **No standing cluster.** The gated suite runs against whatever DSNs the operator provides, so the cost is a decision per run rather than a subscription. The four databases currently up are kept at the operator's request and are not a dependency of this design.
- **No merge gate.** A required check that depends on a paid external cluster makes every PR hostage to someone else's uptime and billing. The Vitess cluster gate is the precedent for the opposite arrangement — it is a tag-time publish gate, not a PR check, for exactly this reason.

## Decided: weekly, with the cost watched

**Operator decision, 2026-09-11: the gated suite runs weekly**, on the understanding that if it proves expensive it drops to dispatch-only or a longer interval. So the `schedule:` block ships **live**, not commented out, and the cadence is a knob rather than a commitment. That is the right default for a platform in preview: the whole value of the suite is noticing when Neki moves under us, and a suite nobody dispatches notices nothing.

**Enabling the schedule pulls in one more requirement, and CI enforces it.** `scripts/check-schedule-consumers.sh` (Lint) fails the build when a new `schedule:` trigger has neither a red-consumer nor a documented exemption — the TESTCI-1 finding that a scheduled workflow whose red nothing consumes is indistinguishable from one that is green. So `nekiverify.yml` must be added to `report-red.yml`'s `workflows:` list (today: Extended suites, DuckDB parquet compat, Fuzz roundtrip, Postgres version matrix, Vitess version matrix) **in the same PR that adds the schedule**, so a scheduled red files a standing issue and closes it on the next green.

Note the interaction with the fail-on-skip belt, because together they are what makes the weekly run honest: the belt turns a missing-secret skip into a red, and the consumer turns that red into an issue. Without both, a weekly run that quietly stops exercising anything looks exactly like a weekly run that passes.

**What to watch before deciding it is too costly.** The run is two short-lived connections against existing databases — it provisions nothing. The cost is therefore whatever the databases cost to keep alive, which is already an operator decision independent of this suite, plus negligible compute. If the databases are ever torn down, the belt turns the weekly run red on missing secrets rather than green-skipping, which is the correct signal and also the prompt to switch the schedule off.
