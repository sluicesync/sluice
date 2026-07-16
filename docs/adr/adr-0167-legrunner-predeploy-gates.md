# ADR-0167: legRunner pre-deploy gates — DR-diff blast-radius assertion + post-wait freshness recheck

- **Status:** Accepted (implemented; audit 2026-07-15 task M1.9 / finding MED-D0-7)
- **Date:** 2026-07-15
- **Related:** ADR-0162 (the stale-base freshness gate this closes the TOCTOU window of; its finding 3 is the empirical basis), ADR-0165 (the shared `legRunner` all three consumers compose), ADR-0148 (the deploy-request ground truth; its live prototype exercised the diff endpoint).

## Context

The ADR-0162 stale-base freshness gate is **point-in-time**: `provisionFreshBranch` compares the dev branch's schema to production once, before any DDL. The leg then applies DDL, opens a deploy request, and can sit in the deployable/review wait for up to `--deploy-timeout` (default 1h — orgs with mandatory DR review routinely use the whole window) before calling `Deploy`. Audit finding MED-D0-7 named the two gaps:

1. sluice never looks at the deploy request's **computed diff** before deploying — so a stale branch base (ADR-0162 finding 3 empirically proved PlanetScale will deploy a phantom revert from one) or an out-of-band edit to the sluice-owned dev branch ships whatever the DR carries, sight unseen;
2. production can move **during** the wait, and whether PlanetScale re-diffs a DR at deploy time is derived-not-verified — the conservative assumption must be that it does not.

## Decision

Two pre-deploy gates in the shared `legRunner`, running between "the diff is computed / review finished" (`waitDeployable` returning) and the `Deploy` call — the last moment sluice can refuse with nothing shipped; the existing always-cleanup contract tears the dev branch down on both refusals.

### 1. DR-diff blast-radius assertion

A new thin-client verb `api.GetDeployRequestDiff` (`GET /deploy-requests/{number}/diff` — the endpoint the ADR-0148 live prototype exercised on a real PS-10; response envelope `{"data":[{"name",…}]}`, the live-verified branch-schema shape family) fetches the DR's computed per-object diff. The runner asserts every diff object's `name` is inside the leg's **intended table set** (`expectedDiffTables`, a new runner field); any stranger refuses via the existing `drFailure` shape (`SLUICE-E-PS-DEPLOY-REQUEST-FAILED`, DR URL attached) naming the unexpected object(s) and the intended set. Subset-only: production already carrying part of the intent legitimately shrinks the diff (the `no_changes` terminal case is already handled by the poller).

Per consumer:

- **expand-contract** — expand leg: `--table` + the two staged migrate-state control tables (`sluice_migrate_state`, `sluice_migrate_table_progress`, which ship inside the expand DR by design); contract leg: `--table` alone.
- **index fallback (ADR-0148)** — the one table whose pending index DDL the DR carries.
- **deploy-ddl** — deliberately EMPTY, which skips the fetch: its `--ddl` is an arbitrary operator statement sluice refuses to parse (no regex over DDL — the tenet), so there is no intended set to assert against. Its guards remain the provisioning freshness gate and the recheck below.

A fetch error fails the leg loudly (the gate is a safety check; proceeding unverified would hollow it out). The 429 handling rides the client's existing retry.

### 2. Post-wait production freshness recheck

`provisionFreshBranch` now returns production's rendered schema at gate-pass time (captured from the GET the staleness compare already made — no extra call), and the runner records the wall-clock. After `waitDeployable`, when elapsed exceeds **2 minutes** (`legFreshnessRecheckAfter`), one more `GetBranchSchema(production)` compares against the baseline: a changed render refuses with the ADR-0162 code (`SLUICE-E-PS-BRANCH-STALE-BASE` — the same deploying-would-silently-revert class, message naming the mid-wait movement, remedy = re-run, which re-provisions from current production). Sub-threshold waits skip the extra GET so the fast path (~seconds to deployable on an auto-approving org) stays at its current call count. The clock is injectable (`now` field) so the pins spend no wall time.

Gate order: diff assertion first (it catches the stale base that the freshness recheck's *same-table* revert case would miss only when the phantom revert touches an intended table — the two checks are complementary, not redundant: a stale base reverting a column on the *intended* table produces no stranger object, and only the ADR-0162 provisioning gate or a human reviewing the DR catches that today; production moving mid-wait on any table is the recheck's half).

## Consequences

- A stale-base phantom revert (or an out-of-band branch edit) touching any table outside the leg's intent is now refused at the last gate before deploy, with the DR URL for inspection; production movement during a long review wait is refused likewise. Cleanup behavior is unchanged.
- Cost: one diff GET per gated leg (skipped for deploy-ddl) + one schema GET only after a >2-min wait.
- **Derived-not-verified, carried explicitly:** (a) the live diff-freeze semantics — whether PlanetScale re-diffs at deploy time — remain unverified; the gates assume it does not (conservative). (b) The diff endpoint's exact response field set is derived from the pscale tooling, not yet a sanitized live capture; the client pin (`TestClient_GetDeployRequestDiff_ResponseShape`) says so in its comment. **The next live psverify dispatch should verbatim-capture a real `/diff` response and exercise one gated leg end to end.**
- The `expectedDiffTables` names must match PlanetScale's diff-object names exactly (bare table names, as the branch-schema endpoint renders them); a casing/qualification divergence would surface as a loud false refusal on the first real run — one more reason for the psverify leg.

## Alternatives considered

- **Parsing the leg's DDL to derive the intended set for deploy-ddl too.** Rejected: regex over DDL strings is a named anti-tenet, and a wrong parse converts the safety gate into a false-refusal generator on exactly the arbitrary statements it can't understand.
- **Always re-checking freshness (no threshold).** One extra GET per leg is cheap, but the sub-threshold window adds nothing (the provisioning gate just ran) and the threshold keeps the fake-driven pins honest about WHY the recheck exists (the review wait).
- **Warning instead of refusing on a stranger diff object.** The stranger is the empirically-deployed phantom-revert signature; a WARN before an irreversible production deploy is the silent-loss posture the tenet forbids.
