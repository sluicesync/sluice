# sluice v0.127.1

**A fast-follow patch closing what v0.127.0's own audit found in itself.** The v0.127.0 release closed the 2026-08-14 audit; the post-publish blind audit and regression cycle then caught three residuals of those very fixes — a silent-loss edge in the M-2 halt, a coverage hole in an H-6 guard, and a UX regression from the H-2 door move. All three are fixed here. **Drop-in from v0.127.0 — no schema, format, or flag change.**

## Fixed

**A schema-existence probe error now halts instead of silently skipping every routed event (audit SL-1, silent-loss).** v0.127.0's M-2 fix turned a misrouted-namespace whole-stream drop into a loud halt by probing whether the target database/schema exists — but its condition halted only when the probe SUCCEEDED and found the schema absent. When the probe ITSELF errored (a transient deadline or connection reset on an overloaded or sharded target), the applier fell through to the unknown-table skip sentinel, dropping the event and advancing the stream position past it — a silent loss re-opening the exact window M-2 closed, on both the MySQL and PostgreSQL appliers, and contradicting the fix's own comment ("a probe error refuses to guess"). The classification is now a pure helper on each engine: a probe error HALTS with the probe error and is never the skip sentinel; a confirmed-absent schema is the loud routing fault; only a confirmed-present schema yields the recoverable skip. Pinned by a four-branch unit test (the probe-error branch asserts the skip sentinel is never returned), mutation-verified red on both lanes. The audit flagged the PostgreSQL C-11 apply lane as the prior audit's lightly-reviewed sibling, so it gets the same gate.

**The sharded-target refusal's shard-naming message is restored on the sync cold-start / `add-table` path (Bug 253).** v0.127.0's H-2 fix moved the sharded-target door later — behind the cold-start emptiness pre-flight probe — so adding a new (absent) table to a sharded Vitess/PlanetScale keyspace via sync cold-start or `schema add-table` refused with a generic vtgate `Error 1105 (table not found)` instead of the actionable `SLUICE-E-SCHEMA-TARGET-KEYSPACE-SHARDED` that names the shards and the specific new table(s). Loud and safe throughout (the table was never materialized, no data lost); the cost was operator guidance. The emptiness probe now disambiguates an unclassified error via the reliable `information_schema` existence check — vtgate answers that correctly, and only a direct SELECT of the missing table `1105`s — so a confirmed-absent table defers to the create-phase door (which emits the coded refusal), while a present table or an errored existence probe surfaces the original error. MySQL-flavor (Vitess/PlanetScale) targets only; the `migrate` path, whose state-store door fires earlier, was unaffected.

## Changed

**The vacuous-green guard now covers the fuzz-roundtrip workflow (audit CI-1).** H-6's meta-guard scanned three of the scheduled correctness workflows but omitted `fuzz-roundtrip.yml`, whose anchored `TestMigrate_FuzzRoundtrip` leg — the project's highest-value silent-loss catcher — could green on ZERO iterations if the test were renamed (`go test -run` exits 0 on no match). The leg now asserts it ran and passed, and the guard's scanned-file list covers it; mutation-verified.

## Compatibility

Drop-in from v0.127.0. No schema, backup-format, or flag change. Both behavioral fixes touch only error/edge paths: a schema-probe error now halts loudly instead of silently skipping, and an absent table added to a sharded keyspace now gets the actionable refusal message instead of an opaque `Error 1105`.

## Who needs this

- **Anyone running continuous CDC sync with a namespace mapping** (`--target` / `--map-database` / `--map-schema`) — SL-1's silent-drop-on-probe-error is closed on both engines.
- **Vitess / PlanetScale operators adding tables to a sharded keyspace** — Bug 253 restores the actionable "design the vschema/vindexes platform-side" refusal on the sync cold-start and `add-table` paths.
- Everyone else: a drop-in upgrade with no behavior change on the happy path.

## Install

```
# Homebrew
brew upgrade sluice
# Scoop (Windows)
scoop update sluice
# Docker
docker pull ghcr.io/sluicesync/sluice:0.127.1
```
