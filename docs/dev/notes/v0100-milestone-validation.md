# sluice v0.100.0 — milestone validation record

**Date assembled:** 2026-07-24 · **Base tag:** v0.99.292 · **Milestone:** v0.100.0 (confidence + polish, no stability guarantee)

This is the evidence record for the v0.100.0 cut, per the readiness plan's B2 ("assemble, don't re-run — the delta since the last audit is small; a fresh from-scratch blind audit is not due"). Every line points at a green artifact or a ground-truthed result; nothing here is a fresh heavyweight re-audit.

## Standing gates (per-PR, every commit that reached v0.100.0)

- **Six required real-DB integration checks** on every PR/push (testcontainers MySQL/PG/Vitess, `-race`, sharded with a can't-rot coverage guard): cross-engine migrate + sync + backup. The VStream Bug-125 silent-loss class has its own required check.
- **Tag-time publish gate:** the filtered move-OUT Vitess cluster gate (real cluster, the `TestVitessClusterFilteredSync` crux family) is the required 6th publish check.
- All v0.100.0 code is CI-green at `89b89862` (C2/C3 WARNs + Bugs 204/205 fixes + the cold-start/headroom preflights), with the cloud-KMS doc/wording flip at `696dc076` on top.

## Milestone confidence pass

- **Extended-suites `suite=all` dispatch** (the classes NOT in per-PR gates): run 30067317185 — NOBLOB belt, pipeline vstream, real-world corpus, KMS localstack, Vitess chaos, MySQL DDL-fixture corpus all **green**; the Vitess reshard-correctness leg went green on rerun after one infra-shaped `tablet down` flake on the first attempt (the core exactly-once oracles passed). The two known quarantines (RelaxSkew A/B skew harness; chaos RollingUpgrade infra) are non-default-mode / never-reach-a-sluice-assertion and documented in `docs/production-readiness.md`.
- **`scripts/prerelease-triggers.sh v0.99.292`** → two expected hits: a docs-drift advisory (ADR sidecar edits; the site is in sync) and the `-race`-before-tag rule for the retry-loop/cold-start files — satisfied by the CI runs above before this tag.

## Live cloud validation (2026-07-24)

Full log: `workspace/v0100-live-cloud-validation.md`.

- **Azure Key Vault (real AKV) — ALL CELLS PASS**, including the N-9 crux: a chain wrapped under key version N restores clean with an *unversioned* operator URL after the key is rotated to N+1 (rebind targets the recorded wrap-time version); the negative control (forcing N+1) fails loudly. Signed+encrypted `backup full` → `verify` → tamper-refusal → restore all green against the live vault. Throwaway vault + RG deleted and purged; verified zero remaining resources; cost < $0.10. One doc-level finding (fixed): real AKV returns `BadParameter`, not the "auth error" the WARN wording predicted.
- **GCP Cloud KMS — ALL CELLS PASS**: real `AsymmetricSign` + `GetPublicKey` verify (the DER→raw signature conversion holds against the live service), tamper-refusal, `restore --require-signature`. Key version DESTROY_SCHEDULED.
- ADR-0152/0154 "real-cloud validation deferred (N-9)" notes and the production-readiness AKV entry flipped to validated (`696dc076`).
- **psverify (live PlanetScale, `-race`) — GREEN, zero skips.** The live PS-MySQL/PS-Postgres verification suite (`go test -tags=psverify -race`) passed every test with the vacuous-green guard reporting `fail-on-skip: no skipped tests` — so every check ran live under the race detector. This run surfaced and closed a real managed-PG finding: PlanetScale upgraded PS-PG to **Postgres 18**, whose walsender-release latency (10+ min after disconnect, vs <40s pre-PG18) exceeded both sluice's 55006 slot-active retry budget and the psverify suite's own timeouts. Two fixes landed for it — the product's 55006 retry became a generous 5-minute wall-clock budget (a genuine managed-PG resume-robustness improvement, shipped in this release), and the psverify suite was corrected to use per-test unique slot/publication names (removing an artificial cross-test collision that PG18's slow reap exposed). Post-fix latencies are healthy: `CDCReader_FailoverFlag` 3.5s (was 720s), and `StreamerPGToPG`'s full cold-start→bulk-copy→CDC-round-trip completed in 42.9s (live CDC delivery on PS-PG-PG18 is seconds, not minutes). One transient `conn closed` mid-copy cleared on rerun (a PS-PG proxy connection drop; the migrate tests on the same copy path passed).

## Field-durability evidence

- **Live PlanetScale 5M-row filtered-sync validation** (this release line's filtered-sync/A0 path — the surface that stalled the earlier "not ready" call), end-to-end against real PS.
- **Multi-week soak fleet** on the current release binary (v0.99.292): PS-MySQL→PS-PG, two Cloudflare-D1→PS legs, and a 5M-row filtered PS-MySQL→PS-MySQL soak — all warm-resuming clean across restarts.

## Audit posture

Three blind audits in the ten days before the milestone (2026-07-15, 07-19, 07-23) are fully remediated; every finding became a permanent deterministic gate (the Tier-1 ratchet). No new silent-loss class is open. The demand-gated tail (index-DDL forward, trigger carry, warehouse targets, the five translator rules, etc.) is documented with workarounds in `docs/production-readiness.md` — the correct shape for a milestone that makes no stability guarantee.

## Residuals explicitly carried (not blockers)

- The multi-database (`--all-databases`) sync cold-start path still creates tables ungated (the single-DB path and `migrate` are gated) — filed, documented.
- Same-engine faithful carry of UNIQUE constraint attributes (the v0.100.0 line WARNs; carry is the follow-up); the plain-unique-index `NULLS NOT DISTINCT` sibling gap.
- GCP/Azure KMS live legs are operator-dispatched, not per-PR (no free local emulator); AWS `kmsverify` is the standing per-CI leg.
