# sluice v0.72.1 — Shape A hotfix (Bugs 80 + 81)

**Paired hotfix for the ADR-0048 Shape A regressions surfaced by the v0.72.0 post-release regression cycle.** Both fixed together: fixing Bug 80 alone (loud crash) would have swapped a loud crash for a silent cross-shard collision via Bug 81 (preflight no-op). The cycle subagent's recommendation was unambiguous — pair-the-class.

**Drop-in upgrade from v0.72.0.** No storage shape change, no CLI surface change. Anyone who attempted `--inject-shard-column` on v0.72.0 hit Bug 80 immediately; v0.72.1 makes the flag actually usable end-to-end. Operators who weren't using Shape A see no observable change.

## Fixed

- **`fix(engines): Bug 80 — Shape A reader projection now filters SluiceInjected columns`.** Pre-fix, the row reader built its SELECT projection from the schema-mutated `*ir.Table` — which includes the `SluiceInjected` discriminator column. The source database doesn't have that column, so every Shape A bulk-copy crashed mid-read with `SQLSTATE 42703 "column does not exist"` on PG / `Error 1054 "Unknown column ... in field list"` on MySQL, *after* the target tables were already created. New `sourceReadableColumns` helper filters BOTH generated columns AND `SluiceInjected` columns; consumed by `buildSelect`, the streaming-scan path, and the batched-read path on both engines. The writer-side `nonGeneratedColumns` helper is deliberately left unchanged — the discriminator column MUST land on the target; the orchestrator's `shardStampRows` wrap stamps the discriminator value onto each row between read and write, and the writer's projection picks it up. The two helpers are intentionally asymmetric, and the asymmetry is now compile-pinned by per-engine unit tests.

- **`fix(engines): Bug 81 — shardPreflightProber implemented on PG + MySQL RowWriter`.** Pre-fix, the `shardPreflightProber` interface was defined and unit-tested in `internal/pipeline` but **no engine implemented it**. The type-assertion `rw.(shardPreflightProber)` at `shard_preflight.go:136` silently failed on every shipping engine, and the ADR-0048 DP-2 populated-target three-point preflight was a complete no-op — operators with a populated target re-running a shard with a colliding VALUE would have had their bulk-copy collide silently. v0.72.1 implements three read-only catalog probes per engine (`HasNullShardColumn`, `ShardValuePresent`, `CompositePKLeadsWith`); the preflight now refuses loudly with the existing `errShardConsolidationRefused` sentinel + operator-actionable messages naming the offending table, column, and recovery path.

## Tests

- **`test(integration): Bug 80 + Bug 81 end-to-end pins`** — `internal/pipeline/migrate_shape_a_e2e_integration_test.go` exercises the full Shape A migrate path against real PG and MySQL containers with non-zero row counts on the target (the load-bearing assertion that catches both bugs); covers Bug 80 happy path, Bug 81 preflight-refuse-on-collision, and Bug 81 preflight-pass-on-fresh-shard-VALUE (regression guard against an over-refusal). `bug81_prober_witness_integration_test.go` compile-pins the prober interface assertion on both engines' RowWriter — pre-v0.72.1 the assertion silently failed; post-v0.72.1 it succeeds. Per-engine unit pins (`bug80_source_readable_test.go` on both PG and MySQL) lock the helper-asymmetry. This is the "validate end-to-end before building more" floor that should have caught both bugs pre-v0.72.0.

## Known follow-ups (not blockers)

- **Shape A on MySQL with `BIGINT AUTO_INCREMENT` PK still fails at CREATE TABLE** with `Error 1075 "Incorrect table definition; there can be only one auto column and it must be defined as a key"` because the composite PK rewrite places the discriminator first, demoting the AUTO_INCREMENT column. This is a separate Shape A design issue (separate from Bugs 80/81); a fix-or-refuse-loudly path will land in a follow-up release after an ADR-0048 amendment. Workaround for v0.72.1: use a non-AUTO_INCREMENT PK on MySQL Shape A sources, or migrate to PG. Already filed.

## Who needs this

- **Anyone who tried `--inject-shard-column` on v0.72.0** and saw the SQLSTATE 42703 / Error 1054 crash — v0.72.1 makes the flag actually usable.

- **Anyone who plans to use `--inject-shard-column` on a populated target** (subsequent-shard load, operator re-runs, etc.) — v0.72.1 is the first release where the DP-2 three-point preflight actually engages. Without this fix, a colliding shard VALUE would have proceeded silently to bulk-copy.

- **Operators who weren't using Shape A on v0.72.0** see no observable change. Safe to upgrade either way.
