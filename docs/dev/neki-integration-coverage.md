# Neki integration coverage — design

**Status: BUILT AND RUNNING (2026-09-11).** Tier 1 is green per-PR. Tier 2's fixture harness provisions a sharded Neki cluster from nothing and the three refusal-premise checks pass against it; `nekiverify.yml` runs weekly with the fail-on-skip belt and a `report-red.yml` consumer.

**The standing `neki-test` and `neki-torture` databases were deleted the same day**, which is what this design was for: their fixture — shard groups, key ranges, topology document — existed only as ad-hoc SQL in a session scratchpad, so they could not be torn down without losing the ability to reproduce them. `provisionShardedNeki` is now that recipe, executable and proven. The local credential files `NEKI_SLUICESYNC.env` and `NEKI_TORTURE.env` are correspondingly dead and were renamed `.DELETED-2026-09-11`.

Still open: the CDC-into-a-sharded-target arm, the NK213/MoveTables arm (now cheap — a stuck one-year block on a per-run database costs nothing), and the DDL-in-transaction / sequence-catalog-fallback arm.

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

**Status, 2026-09-11 — ALL FOUR ITEMS ARE BUILT.** Item 1 is BUILT and has run green on a live cluster (`TestNekiverify_ShardedRefusalPremises`). Items 2, 3 and 4 are BUILT but **have not yet run against a live cluster** — they are written and type-checked under the tag and wait on the first scheduled run; grade them on that run rather than assuming they pass. All three ride item 1's fixture rather than provisioning their own database.

**Added alongside them, not on the list and worth being explicit about:** a check that every `__neki` function sluice's own remedies tell operators to run still EXISTS on the router (`nekiRemedyFunctionsExist`). A remedy that cannot run is a Tier-2 harm by the standing work loop's own ranking — read mid-incident, by someone whose stream has already stopped — and the `__neki` surface is a preview API that can be renamed between previews at no cost to sluice's execution and total cost to the operator following the hint. The universe is derived by scanning the three packages that mention the surface (`engines/postgres`, `pipeline/migcore`, `sluicecode`) rather than listed, with a floor at the eight names derived on 2026-09-11 and a fabricated-name control. It checks existence, not behaviour: proving `move_tables_reverse_traffic` still reverses traffic needs item 3's cutover.

**Item 3's cost was over-estimated here, and the correction matters.** This section previously said item 3 "needs a second Neki database provisioned in the same run" and treated it as the expensive cell. That is wrong: Neki's MoveTables moves tables between **databases on the same cluster**, and a second database is `CREATE DATABASE` through the router — as the readiness doc's own measured run records. So item 3 costs extra **wall clock** on the database the suite already provisions, not a second provisioned cluster. The per-phase timing table it logs exists to turn that remaining cost into a measured number: if the arm dominates the run, moving it to a dispatch-only input is a one-line change. Operator decision 2026-09-11: build it into the weekly run and revisit with the measurement.

**What each item cost as built** (the original estimates, kept because the gap between them and the outcome is the useful part):

- **Item 2 (CDC into a sharded target, ordered-content checksum).** Needs a SOURCE as well as the Neki target. The cheap shape is a testcontainers PostgreSQL source on the runner (Docker is available there) feeding a sync into the sharded fixture, with the checksum taken as an ordered digest of the full row content read back through the router — not a row count, because a router that duplicated a row onto two shards and dropped another would show the same count. It costs no extra Neki database: it can ride the existing fixture. The honest prerequisite is that the ROUTER-side premise underneath it be separated out first — that a scatter-gather read returns each row exactly once under concurrent writes — because otherwise a checksum mismatch has two candidate causes and the test cannot say which.
- **Item 3 (NK213 against a real MoveTables cutover).** The expensive one, and the reason it is still open rather than merely unwritten: MoveTables moves tables BETWEEN databases, so it needs a second Neki database provisioned in the same run, plus a workflow that takes minutes, and the cutover leaves a one-year block on the source table. Per-run provisioning defuses the block (it dies with the database) but not the cost or the wall clock. The classifier half is already pinned statically, and `nekiRemedyFunctionsExist` now covers the likeliest live regression (a renamed remedy function), so what item 3 would add is specifically: that the block still arrives as SQLSTATE NK213, and that `move_tables_reverse_traffic` still hands the table back. Worth doing, worth costing deliberately rather than sliding into the weekly run.

Item 4 split in two along the line the table at the top of this file draws, which is worth recording as the pattern for the rest: the half that is really a claim about **PostgreSQL** (what `pg_sequences` reports for each sequence state, which `readSequencePositionFromCatalog` depends on) went to the ordinary integration shard, where it runs per-PR and sweeps every major in the version matrix for free — and immediately found that the mapping is **not** lossless, contrary to the comment asserting it was (see `docs/dev/audit-backlog.md`, 2026-09-11). Only the half that genuinely needs a **router** — DDL not surviving a transaction, and the NK013 relation-read refusal the fallback diverts on — costs a live cluster. Asking "which server is this premise about?" before writing the test moved most of item 4 out of the paid tier and made it stronger.

Reshard and MoveTables mid-stream stay **manual**. They need a writer, a workflow and several minutes of wall clock; automating them would be the most expensive cell in the matrix and the one least likely to regress silently, since both fail loudly today.

## What this deliberately does not do

- **No `neki` engine, no new driver name.** ADR-0186 settled that; coverage follows the code rather than inventing a surface to test.
- **No standing cluster.** The gated suite runs against whatever DSNs the operator provides, so the cost is a decision per run rather than a subscription. The four databases currently up are kept at the operator's request and are not a dependency of this design.
- **No merge gate.** A required check that depends on a paid external cluster makes every PR hostage to someone else's uptime and billing. The Vitess cluster gate is the precedent for the opposite arrangement — it is a tag-time publish gate, not a PR check, for exactly this reason.

## Decided: weekly, with the cost watched

**Operator decision, 2026-09-11: the gated suite runs weekly**, on the understanding that if it proves expensive it drops to dispatch-only or a longer interval. So the `schedule:` block ships **live**, not commented out, and the cadence is a knob rather than a commitment. That is the right default for a platform in preview: the whole value of the suite is noticing when Neki moves under us, and a suite nobody dispatches notices nothing.

**Enabling the schedule pulls in one more requirement, and CI enforces it.** `scripts/check-schedule-consumers.sh` (Lint) fails the build when a new `schedule:` trigger has neither a red-consumer nor a documented exemption — the TESTCI-1 finding that a scheduled workflow whose red nothing consumes is indistinguishable from one that is green. So `nekiverify.yml` must be added to `report-red.yml`'s `workflows:` list (the list today, all seven: Extended suites, DuckDB parquet compat, Fuzz roundtrip, Postgres version matrix, MySQL version matrix, Vitess version matrix, and Neki Verify itself — this sentence was written while the last two were still being added, and a parenthetical naming the pre-change state is exactly how a doc starts lying) **in the same PR that adds the schedule**, so a scheduled red files a standing issue and closes it on the next green.

Note the interaction with the fail-on-skip belt, because together they are what makes the weekly run honest: the belt turns a missing-secret skip into a red, and the consumer turns that red into an issue. Without both, a weekly run that quietly stops exercising anything looks exactly like a weekly run that passes.

## Databases are PROVISIONED PER RUN, not kept standing

Superseding the earlier assumption that the suite reads two long-lived DSNs. **PlanetScale prorates** (operator-confirmed, 2026-09-11), so a database that exists for twenty minutes costs twenty minutes — which makes on-demand strictly better than a standing pair: no idle spend, and no fixture quietly drifting between runs because somebody tested against it by hand.

`pscale database create --engine neki --region us-east --cluster-size PS-10-AWS-ARM-NEKI --replicas 0 --wait` creates one; deleting is a single call. A single-node PS-10 Neki is $10/month list, so a weekly ~20-minute run is a rounding error.

### Measured end to end on a throwaway database, 2026-09-11

| step | how | measured |
| --- | --- | --- |
| create database | `pscale database create --engine neki --region us-east --cluster-size PS-10-AWS-ARM-NEKI --replicas 2 --wait` | **418 s** to ready *at `--replicas 0`; the HA shape is untimed* |
| add a shard | `POST /v1/organizations/{org}/databases/{db}/branches/{branch}/configuration-profiles/default/shards` → 201 | **31 s** to `ready:true` |
| distribute data | `set_data_topology` → `reshard_create` → `workflow_switch_traffic` | minutes; not separately timed here |
| delete database | `pscale database delete --force` | **1 s** to return |

So setup is roughly **8–10 minutes** before the first assertion runs. Fine for a weekly job; far too slow for a per-PR one, which is another reason Tier 1 exists.

**The shard-creation endpoint is not discoverable from either interface you would naturally reach for**, and it was re-derived here after being found and used earlier the same day -- the knowledge was lost to a context compaction, and re-finding it cost an hour. That is the argument for writing the path down rather than for treating it as a discovery. `pscale` has no shard subcommand at all. The `__neki` SQL surface has `delete_shard` and **no create** — and `validate_data_topology` refuses a group naming an absent shard with *"not a shard the cluster has created … create the shard first"*, which states the requirement without saying where to satisfy it. The REST path only surfaced because `POST …/branches/main/shards` answers **308** with a `location:` header pointing at the `configuration-profiles/default/` form. Follow the redirect and it works.

Written down because the wrong conclusion was reachable and nearly reached: that a multi-shard Neki cannot be provisioned programmatically, and therefore that the sharded arm needs a standing database. It does not.

**A fresh database has exactly one shard** (`sh1`, sole member of one shard group, `authoritative: true`). Every finding worth regression-testing — per-shard `UNIQUE`/`EXCLUDE`, the `ON CONFLICT` duplication gap, `NK013` on a shard-key `SET` — needs a MULTI-shard database, so adding a shard and resharding onto it is mandatory setup, not an optional extra. Treat the reshard as a setup step that can fail independently of any assertion, so a reshard failure is not reported as a product regression.

### Provision `--replicas 2`, and test `--replicas 0` occasionally on purpose

**Operator decision, 2026-09-11: the default is `--replicas 2`** — the high-availability shape, which is what the console offers and therefore what customers actually run. The fixture should match the thing being defended, not the cheapest thing that boots.

That matters because the cheap shape is of contested legality. The console refuses single-node Neki; `pscale size cluster list --engine neki` advertises and prices it; `pscale database create --replicas 0` creates one that works (see a reported Neki platform finding). A shape that works today and may not be meant to exist is a bad thing to build a weekly regression suite on — if it were withdrawn, the suite would start failing for a reason that has nothing to do with sluice.

**But keep exercising `--replicas 0` deliberately**, on the operator's read that it will likely become a supported option once PlanetScale settles the details. An occasional single-node run — dispatch-only, not on the weekly schedule — is how we find out early whether sluice cares about the difference. It should not: nothing in the Neki adaptations reads replica count. That is a claim worth testing rather than assuming, and it is cheap to test.

Cost is not the deciding factor either way. PS-10 Neki is $30/month HA versus $10/month single-node, and prorated over a ~20-minute run both are rounding errors. **The 418 s create time was measured at `--replicas 0`; the HA shape is untimed and may be slower** — worth measuring before the job timeout is fixed.

### The orphan sweep is not optional

A workflow that creates and destroys billable infrastructure ~52 times a year **will** eventually die between the two — a cancelled run, a runner timeout, an assertion that panics before teardown. That is precisely the unattended-billable-infra failure this project's operating rules exist to prevent, and an `if: always()` teardown does not cover a runner that vanishes.

So the job does two things, and the second is what actually saves you:

1. **Teardown in `if: always()`**, deleting the databases this run created.
2. **A sweep at the START of every run** that lists `nekiverify-*` databases and deletes any older than a couple of hours. The previous run's orphan is cleaned by the next run, so a single missed teardown costs hours of a $10/month database rather than accumulating forever.

Name every provisioned database with the `nekiverify-` prefix and a run identifier so the sweep can recognise its own litter and never touch an operator's database.

**And the sweep must be loud.** If it finds an orphan, that is evidence the previous run did not complete — worth a WARN in the log and worth noticing, not a silent cleanup that hides a recurring teardown failure.
