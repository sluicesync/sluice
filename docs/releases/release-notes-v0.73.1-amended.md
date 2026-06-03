# sluice v0.73.1 — Bug 83 hotfix: ADR-0054 Shape A Phase 2 now functional end-to-end

> **📝 Follow-up notice (2026-05-22) — PG happy path was still blocked on v0.73.1.** The post-v0.73.1 cycle caught Bug 84: a PG-specific classifier false-positive (`unrecognized multi-shape combo delta`) caused by the cold-start seed (rich PG SchemaReader projection) not matching the pgoutput RelationMessage projection on existing columns (notably `Integer.AutoIncrement` for IDENTITY columns).
>
> **FIXED in [v0.73.2](https://github.com/sluicesync/sluice/releases/tag/v0.73.2) — upgrade strongly recommended.** MySQL → MySQL Shape A on v0.73.1 was working end-to-end and is unchanged.


**Headline:** Closes the Bug 83 chain that made v0.73.0's headline feature non-functional. The autonomous post-release cycle caught the regression within hours of v0.73.0 publish; v0.73.1 lands the cold-start intercept seed mechanism + two paired second-iteration fixes surfaced by the new end-to-end integration pin. Shape A operators can now drop the `--no-coordinate-live-ddl` workaround that the v0.73.0 correction banner mandated.

## Fixed

- **`fix(adr-0054): Bug 83 first iteration — Phase 2 intercept cold-start seed`** — The intercept's table cache started empty at CDC startup and treated the first CDC `SchemaSnapshot` per table as the cold-start anchor. Cold-start doesn't emit SchemaSnapshot through the same channel (only CDC readers do), so the "first-seen" was whatever pgoutput / binlog emitted first — which, if source DDL had happened between cold-start completion and the first CDC row event, was the POST-DDL schema. The intercept cached the post-DDL schema as the anchor and never routed the boundary; the next CDC row event crashed the applier with `column "<new>" does not exist` / `Unknown column '<new>'`. v0.73.1 captures the pre-Shape-A-rewrite source IR per filtered table at cold-start completion and feeds it to the intercept as a synthetic SchemaSnapshot seed, restoring the correct (pre, post) boundary classification on the first real CDC SchemaSnapshot.

- **`fix(adr-0054): Bug 83 second iteration — ADD COLUMN emits nullable`** — The first-iteration seed fix correctly engaged the lease + classifier on PG, but the apply itself failed with SQLSTATE 23502: pgoutput's `RelationMessage` does NOT carry `pg_attribute.attnotnull`, so every column in the CDC-projected IR had `Nullable=false` (zero-value default). The shape applier's `ADD COLUMN ... NOT NULL` then violated the not-null constraint on the non-empty target. v0.73.1 overrides `Nullable=true` on `AlterAddColumn`'s emit in BOTH engines (PG + MySQL) for behaviour symmetry. **v1 limitation:** target columns added via Phase 2 live coordination land nullable. Operators who need NOT NULL on the target apply `ALTER COLUMN SET NOT NULL` post-apply once existing rows have a backfilled value. Documented in [ADR-0054 Phase 2 v1 known follow-ups](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md).

- **`fix(adr-0054): Bug 83 second iteration — MySQL seed key alignment`** — Even after the seed-and-nullability fixes, the MySQL integration pin still failed: the MySQL source schema reader doesn't populate `ir.Table.Schema` (it reads `information_schema` for a single bound DB), so the cold-start seed's `QualifiedName()` was the bare table name. The MySQL CDC reader, however, sets `Schema` to the DSN's DB name on its emitted `SchemaSnapshot`, so the first CDC snapshot's `QualifiedName` was `"<db>.<table>"` — a key MISS against the bare-name seed, which made the intercept fall back to the "no pre" branch (the exact regression the seed was supposed to fix). The intercept's seed-cache lookup now falls back to the bare table-name key when the qualified-name lookup misses, then promotes the entry to the qualified key for stable subsequent lookups. Engine-agnostic — PG (which always populates `Schema`) sees no change.

## Tests

- **`test(pipeline): shard_consolidation_bug83_{pg,mysql}_integration_test.go`** — end-to-end pin reproducing the Bug 83 failure path against real PG and MySQL containers: cold-start completes → source DDL → first CDC row event → assert lease APPLIED + post-DDL row replicated to target. The "Validate end-to-end before building more" tenet was violated by v0.73.0's consumer-side-only integration tests (the lease/router/intercept were unit + integration-tested in isolation; nothing exercised the full source-DDL → CDC-reader → intercept → apply path). This pin closes the gap and would have caught Bug 83 pre-release. **Both engines green.**

- **`test(pipeline): shard_consolidation_intercept_test.go`** — new unit tests for the seed parameter (seed-then-CDC-snapshot routes; seed-only no-CDC doesn't route; seed multi-table dispatch) and the bare-name fallback (`TestIntercept_SeededFromColdStart_BareNameKeyAlignment`).

## Compatibility

- **Drop-in upgrade from v0.73.0.** No CLI surface change, no storage shape change.
- **Shape A operators on v0.73.0 can drop the `--no-coordinate-live-ddl` workaround** the v0.73.0 correction banner mandated; v0.73.1's coordination engages and applies recognized-shape DDLs end-to-end on both engines.
- **v1 nullable-emit trade-off** (see Fixed entry above): target columns added via Phase 2 land nullable regardless of source nullability. Operators who need NOT NULL on the target follow up with `ALTER COLUMN SET NOT NULL` after backfilling existing rows.
- **Non-Shape-A streams are unaffected.** The fix is entirely scoped to the intercept seed mechanism + `AlterAddColumn` emit path.

## Who needs this

- **Anyone who pulled v0.73.0** and is running Shape A consolidation. The headline feature was non-functional on v0.73.0; v0.73.1 restores it.
- **Sharded source operators consolidating into one target** (PlanetScale Vitess shards → MySQL or PG consolidated, application-level sharding into analytics warehouses, hash-partitioned topologies) finally get the drain-window-proportional-to-slowest-shard elimination the v0.73.0 release notes promised.
- **Anyone NOT running Shape A** sees no observable change — the fix is entirely scoped to `--inject-shard-column`-engaged streams.

## What v0.73.0 was supposed to deliver (now actually delivered)

ADR-0054 closes the deferred Phase 2 surface from ADR-0048 §4. All five decision points (DP-A through DP-E) signed off by the owner via design dialogue; resolutions recorded inline in the ADR. Highlights — hybrid TTL + heartbeat-extend lease (K8s leader-election shape), recorded-version + DDL-text-checksum idempotence, probe-and-record crash recovery (uniform across PG transactional DDL and MySQL non-transactional DDL), always-on-by-default with `--no-coordinate-live-ddl` opt-out, recognized-shape catalog via IR-delta classifier covering ADD/DROP COLUMN, CREATE/DROP INDEX, ALTER COLUMN type+nullability. See the [v0.73.0 release notes](https://github.com/sluicesync/sluice/releases/tag/v0.73.0) (with correction banner) for the full design surface.

## Cross-references

- [ADR-0054 — Shape A Phase 2: live cross-shard DDL coordination](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0054-shape-a-phase-2-live-cross-shard-ddl-coordination.md)
- [v0.73.0 release notes with correction banner](https://github.com/sluicesync/sluice/releases/tag/v0.73.0)

