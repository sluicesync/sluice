# ADR-0148: PlanetScale deploy-request index build (auto-adjust for the deferred-index statement-time wall)

- **Status:** Proposed (design; prototype-validated end-to-end, not yet implemented)
- **Date:** 2026-07-02
- **Related:** the deferred-index errno-3024 roadmap item; `--upfront-indexes` (`ef07c1da`, the shipped opt-in escape hatch); ADR-0147 (OLAP count — the count-side sibling of the same errno-3024 wall); the existing thin PlanetScale HTTP client in `internal/planetscale/telemetry` (the no-SDK precedent to mirror).

## Context

sluice defers secondary-index creation to a post-copy `ALTER … ADD INDEX`. On a large PlanetScale-MySQL target that ALTER runs synchronously and is killed by PlanetScale's max-statement-execution-time limit (**errno 3024**, ~900 s; live-proven: a 49 GB table's `ADD INDEX` failed at ~901 s), leaving the data copied but indexes uncreated.

Two workarounds exist or were considered:
- **`--upfront-indexes` (SHIPPED)** — build indexes before the copy so the INSERTs maintain them; no post-copy ALTER, so the wall is never reached. But it's an opt-in **reliability escape hatch, not a speed option**: benchmarked ~11× slower load (deferred 29 s vs upfront 333 s on 3.2 M rows / 4 indexes), because per-row B-tree maintenance is far costlier than a post-copy sort-based build. And it requires the operator to *know* to set it (a too-low auto-threshold would pay the 11× needlessly; a too-high one would still fail — the tier-dependent guessing problem).
- **A size heuristic** to auto-select upfront — set aside: the threshold is tier-dependent and a wrong guess is expensive both ways.

The only approach that **auto-adjusts in a single run with no guessing and no re-copy** — keeping the *fast deferred copy* AND avoiding the wall — is to build the deferred index through PlanetScale's **deploy-request workflow**, which applies the schema change to the production branch's data via VReplication (async, not bound by the statement-time limit). Note `SET @@ddl_strategy='vitess'` does NOT work directly on PlanetScale (the direct ALTER runs synchronously and hits errno 3024); the deploy-request workflow is the supported async path.

## Prototype (validated 2026-07-02 on a real PS-10)

The full chain was driven end-to-end via the PlanetScale API (raw HTTP + service-token auth, no `planetscale-go` SDK — mirroring the telemetry client) and **confirmed working**: an index added on a dev branch was deployed onto main's data (index present, data intact, planner uses it). Endpoints exercised: `POST …/branches`, branch password create, `ALTER … ADD INDEX` on the branch DSN, `POST …/deploy-requests` (branch→main), `GET …/deploy-requests/{n}` (poll `deployable`/`deployment_state`), `GET …/deploy-requests/{n}/diff`, `POST …/deploy-requests/{n}/deploy`, `POST …/deploy-requests/{n}/skip-revert`.

**Findings that shape the design:**
1. **Safe-migrations must be enabled on the target (production) branch** — deploy-request creation fails otherwise (`"…must be a branch with safe migrations enabled"`). Enabling it is a **behavior change**: with safe-migrations on, *direct* DDL is blocked, so every schema change must go through a deploy request. This is the biggest design consideration (auto-enable vs require, and the side effect on the operator's DB).
2. **No human approval was required for the service token** — the request became `deployable` (~8 s, once the diff computed) and deployed without a review step. Good for automation; some orgs may require review.
3. **Deploy lifecycle:** `open`/`pending` → `ready` (`deployable=true`) → `deploy` → `queued` → `complete_pending_revert`. Terminal success is `complete_pending_revert` (applied, with a revert window).
4. **The revert window holds the deployment "in progress"** — PS refuses to delete the DB (and presumably other lifecycle ops) until `skip-revert` finalizes it (or the window closes). sluice must `skip-revert` to finalize.
5. **Two extra credentials** beyond the data-plane DSN: a PlanetScale **API service token** (control-plane, deploy-request + branch + password scopes) and a **branch password** for the ALTER-on-branch step.
6. Small-table deploy was near-instant; a large table's deploy runs the index build via VReplication — real wall-clock but **async and not subject to errno 3024**.

## Decision (proposed)

Make the deploy-request workflow the **automatic fallback** when a deferred `ADD INDEX` fails with errno 3024 on a PlanetScale target — not merely an opt-in mode. This is possible, and strictly better than a re-run, precisely because the deploy-request path builds the index **post-copy, on the already-migrated data** (via VReplication) — *exactly like the direct `ALTER` it replaces*. So when the direct deferred `ALTER` hits the wall, the data is already copied; sluice transparently rebuilds that same index via a deploy request on the existing data, with **no re-copy, no operator action, and no `--upfront-indexes` recommendation.** (This is the key asymmetry with `--upfront-indexes`, a *pre*-copy strategy that cannot be applied retroactively to an already-copied table.)

**The flow:** fast deferred bulk copy (unchanged default) → deferred `ADD INDEX` (unchanged default — fast for small/moderate tables) → **on errno 3024, per failed index/table:** create a dev branch off the target branch, apply the `ADD INDEX` to it, open a deploy request into the target branch, deploy, poll to `complete_pending_revert`, `skip-revert`, delete the dev branch → proceed. Implemented as a **thin HTTP client** in a new `internal/planetscale/deploy` package (no `planetscale-go` dependency — the telemetry client is the precedent).

`--upfront-indexes` remains an explicit opt-in (it avoids even *attempting* the doomed direct `ALTER`). The auto-fallback makes it optional, not required; the fast deferred `ALTER` stays the default everywhere and is only replaced when it actually fails. No size heuristic / guessing is needed for correctness — try-then-fallback beats a tier-dependent threshold.

**Cost of the auto-fallback — the burned attempt.** The direct deferred `ALTER` runs for ~900 s and fails before the fallback engages, so a large table pays that ~900 s once. This is the price of not-guessing. An optional future optimization — a cheap `information_schema.DATA_LENGTH` probe — could let sluice skip the doomed direct `ALTER` for *clearly*-huge tables (well past the wall on any tier) and go straight to the deploy request, avoiding the ~900 s waste, while still using try-then-fallback for the ambiguous middle. Not required for correctness; a perf refinement, and it pairs with (not replaces) the fallback.

## Open design questions

- **Safe-migrations: auto-enable vs require.** Auto-enabling changes the operator's production branch (direct DDL becomes blocked thereafter). Safer to *require* it be enabled and fail loud with a clear message if not? Or auto-enable with an explicit warning? Needs an operator-facing decision.
- **Credential UX.** sluice's migrate takes only a data-plane DSN today. This mode needs a PS API token + branch-password creation. How is the API token supplied (flag/env, mirroring the metrics token), and scoped?
- **Per-table vs whole-schema.** One deploy request per table, or batch all tables' indexes into one deploy request (fewer branches/deploys, but a larger single migration)?
- **Failure & cleanup.** Orphaned dev branches / open deploy requests on mid-workflow failure must be cleaned up (or `auto_delete_branch` on create); the revert window must be finalized. Idempotent re-run / `--resume` semantics.
- **Gating.** Only worth it above some size (below it the direct ALTER is faster and simpler) — pairs with the size-estimate work.
- **Batching the copy.** The dev branch needs the target schema to ALTER; confirm the branch-off-production semantics for a freshly-migrated (large) table and the deploy's VReplication timing at scale (the prototype used a tiny table).

## Consequences

- The true one-run auto-adjust: fast deferred copy + (on failure) async index build on the already-copied data, no errno-3024 wall, no re-copy, no size guessing, no operator action.
- **Subsumes the "(B) error-guided" idea** — no need to fail loud and tell the operator to re-run with `--upfront-indexes`; the fallback recovers automatically because the deploy-request build is post-copy.
- New control-plane surface in sluice (HTTP client + async workflow + branch/credential lifecycle) — the largest PlanetScale-specific integration to date, flavor-gated and engaged only on an actual errno-3024 failure (or via explicit `--upfront-indexes` to skip the attempt).
- Depends on an additional PlanetScale API credential and the safe-migrations prerequisite — the main UX costs.
