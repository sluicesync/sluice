# PlanetScale pre-release validation checklist (roadmap item 10, Path A)

Operator-run, credential-gated. This codifies the validation ladder that closed the v0.99.283 filtered-sync arc — a local real-Vitess cluster leg, a real-PlanetScale end-to-end leg, and the CI `-race` gate, run as one pre-release pass — so that a one-off confidence event is a repeatable checklist instead of tribal memory.

**Honesty up front: this is NOT CI.** Layers 0–2 run without credentials and mostly run automatically; layers 3–4 need a PlanetScale credential window, cost real money (PS-10 instances, ~dollars/hour), and are dispatched by the operator on demand. Cron for the credentialed legs stays OFF (quota predictability). What the credentialed legs buy that CI structurally cannot: vendor realism — TLS fronting, the pgwire proxy, the real tablet throttler, real deploy-request control-plane behavior, real reparent/storage-grow windows.

## When to run which layer

Run `scripts/prerelease-triggers.sh` from the release base first. A delta that touches VStream / PlanetScale / Vitess surfaces (readers, the filtered-sync classifier, deploy-request paths, telemetry) warrants layers 2–3; a docs-only or non-PS delta warrants nothing beyond layer 1's always-on gates. Layer 4 is for arcs that change filtered-sync semantics or the cold-start COPY path — the classes where only scale + real infrastructure have caught bugs.

## Layer 1 — always-on, no credentials (verify green, don't re-run)

- Per-PR: the `vstream` Integration job (vttestserver, `-race`) and the pipeline shards' VStream tests — required checks, so green `main` already implies them.
- Weekly: `vitess-version-matrix.yml` (multi-version cluster suite).
- Tag-time: the **`Filtered move-OUT gate (tag)`** job (publish gate #6) boots a real multi-process Vitess cluster and runs the `TestVitessClusterFilteredSync` crux family — cold-start move-OUT, warm resume, and the PAD-SPACE A0 client-side-COPY fallback — with a vacuous-green guard. Every tag gets this regardless of the checklist; budget ~60–85 min.

## Layer 2 — local real cluster, Docker only (run for PS-touching deltas)

- The `vitesscluster` / `vitessreshard` build-tag suites (or the `vitess-cluster-validator` agent) against the multi-process local cluster: filtered sync crux tests, reshard exactly-once oracles, chaos helpers as relevant to the delta.
- Ground-truth any *premise* the delta rests on against the real cluster before shipping a guard or a fallback on it — the v0.99.283 arc's examples: the vindex move-OUT question was resolved as not-a-hazard by demonstrating Vitess refuses an in-place primary-vindex UPDATE (VT12001) on a real 2-shard cluster, and the PAD-SPACE fallback's server-filter behavior was proven on the cluster before the client-side fallback shipped.

## Layer 3 — live PlanetScale, credentialed (operator dispatch)

- **psverify dispatch** — the standing workflow (12 live tests across 5 packages, `-race`, fail-on-skip). Full provisioning/secrets/teardown recipe: [`docs/dev/psverify-dispatch.md`](../psverify-dispatch.md). Cost: 4 × PS-10 for ~1 hour.
- **Telemetry smoke while it runs:** point `sluice metrics-watch` (ADR-0107; a `read_metrics_endpoints` service token) at one of the live databases so the control-plane telemetry path is exercised against real metrics, not fixtures.
- **Teardown is part of the checklist, not an afterthought:** delete every provisioned database, verify by listing, and reset the infra-pointing secrets — a leftover PS database bills until someone notices.

## Layer 4 — deep filtered-sync validation at scale (the v0.99.283 seed)

For deltas that change filtered-sync semantics, the COPY fallback, or the cold-start path. The canonical shape, as run for v0.99.283 against real PlanetScale at 5M rows:

1. Provision a PS MySQL database; load a multi-million-row table whose filter column uses a **PAD-SPACE collation** (e.g. `utf8mb4_general_ci`) with planted trailing-space boundary rows, plus an unfiltered bystander table. Note the platform's tier-determined storage floors when sizing (a PS-10 starts around a ~12 GB Vitess volume floor; `--min-storage` is PG-only).
2. `sync start --where` on the padded column → assert the client-side COPY fallback engages (the run says so at INFO), the trailing-space row survives the cold start, and the out-of-scope row is absent.
3. Live row-moves: UPDATE a row out of predicate scope (target DELETE), into scope (target INSERT), and a non-predicate-column UPDATE (stays an UPDATE).
4. Kill and warm-resume the stream; assert the filter still holds on the resumed CDC leg and convergence is byte-identical (`sluice verify --depth sample`).
5. Tear down; verify deletion.

Record the run (versions, database names, what passed, teardown confirmation) in the session/release notes — the record is what lets the next release skip re-deriving confidence the last one already bought.

## Provenance

Seeded from the v0.99.283 triple validation (2026-07-19): local Vitess cluster crux tests + real-PlanetScale 5M-row end-to-end (provision→validate→teardown verified) + CI `-race`, which together lifted the v0.99.282 PAD-SPACE refusal into a working fallback. Path B (CI-integrated PlanetScale credentials, i.e. making layer 3 automatic) remains deliberately deferred — see roadmap item 10.
