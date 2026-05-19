# Local Vitess (vttestserver) vs real PlanetScale — equivalence & on-demand harness

> **Purpose.** Decide what daily Vitess regression signal *must* come
> from real PlanetScale vs what local vttestserver already proves, so
> PlanetScale databases can move from always-on to **on-demand /
> time-boxed** (cost reduction) without losing the signal that
> actually catches sluice regressions. Same philosophy as the
> 2026-05-19 Vultr decommission: validate-on-demand, not idle-always-on.
> Authored 2026-05-19.

## TL;DR

Local Vitess (`integration vstream`, `vitess/vttestserver:mysql80`) is
**sufficient for the code-mechanics / correctness regression surface**
— the bulk of what breaks when sluice code is edited. It is
**structurally insufficient for the latency- and operational-event
class**, which is not hypothetical: findings **#14 / #18 / #21** plus
the entire reason **ADR-0038** exists are real, PlanetScale-only
behaviors. Recommendation: local Vitess = daily default; PlanetScale =
narrow, scripted, on-demand periodic pass scoped to *only* the PS-only
rows below.

## Coverage matrix

### Local vttestserver covers FAITHFULLY (no real PS needed)

| Surface | Evidence in-tree |
|---|---|
| VStream wire mechanics — FIELD/ROW events, VGTID positioning | `TestVStream_VTTestServer_BasicChangeStream`, `_Truncate`, `_SnapshotStream` |
| FIELD-delta schema-evolution boundary (ADD/DROP/MODIFY) | `cdc_vstream_schema_evolution_integration_test.go` (ADR-0049 Phase-1c, loud-floor proven) |
| Multi-shard **snapshot** mechanics | `TestVStream_VTTestServer_MultiShardSnapshot` |
| Composite-PK change streaming | `TestVStream_VTTestServer_CompositePK` |
| Vitess-flavored error **classification code path** | ADR-0038 `vitessRetriableSubstrings` + `applier_errors_test.go` (the *decision* logic; not the *triggering*) |
| Vitess DDL idioms — no-FK, sequence/reference tables, vindex backing | corpus iter-4 Option-A slice (planned) |
| Snapshot→CDC handoff, position persistence, resume-after-DDL mechanics | engine/wire-level; vttestserver reproduces |
| `planetscale` engine registration / Capabilities-delta read+plan path | pure code path (`engineNameVStream="planetscale"`); needs no server at all |

### PlanetScale-ONLY — vttestserver structurally cannot reproduce

| Surface | Why local can't; evidence it's real |
|---|---|
| **Latency-driven batch sizing** | **#18**: the Vitess ~20s tx-killer fires only when a batch's commit window crosses that threshold — a function of real cross-region latency, not present locally |
| **Cold-start dedup under sustained write rate** | **#14**: VStream COPY-dedup race surfaced under sustained write load during cold-start; latency affects retry rate |
| **PS-MySQL TCP-reset cascades** | **#21**: operator-observed on the real PS connection path; no local analogue |
| **Operational events actually firing** — tx-killer / throttler / vttablet failover under managed load | vttestserver has a tx lifetime but does not *operationally* trigger these; **ADR-0038 exists because these fire on real managed PS** (GitHub #13: `1105 (HY000) … code = Aborted … tx killer rollback`) |
| **PlanetScale online DDL (deploy requests / cutover)** | a different schema-application lifecycle than a plain `ALTER`; vttestserver re-emits FIELD on `ALTER` but does not model the deploy-request/cutover flow that is the operator-reported failure ADR-0049 targets |
| **Resharding events** | already a documented deliberate gap — vttestserver reshard is not scriptable in-test |
| **PS connection/session layer** — `set workload=olap`, boost/replica reads, auth path | PS control-plane specific |

## Synthesis / decision

- **Daily default = local Vitess.** It covers the regression-prone
  mechanics surface and is faster (no physical-distance latency). Edits
  to sluice code regress *here* far more often than in the PS-only class.
- **PlanetScale = on-demand, time-boxed, narrowly scoped.** Do **not**
  re-run what vttestserver already proves. The PS pass exercises *only*
  the PS-only rows: the #14/#18/#21 class, tx-killer/throttler under
  real load, and the online-DDL (deploy-request) lifecycle vs ADR-0049
  boundary detection.
- This keeps the corpus/CDC story **honest** about what it does and
  does not prove (loud-failure / validate-end-to-end tenets): we are
  not pretending local coverage subsumes managed-Vitess operational
  reality — we are scoping the expensive signal to exactly the delta.

## On-demand PlanetScale harness (ephemeral-branch, near-zero idle cost)

PlanetScale **branches** spin up/tear down fast; the cost driver is
*databases kept online*, not branch lifetime. The pscale service token
is already provisioned (see auto-memory `planetscale-creds`;
`PLANETSCALE_SERVICE_TOKEN.env` in `sluice-testing`). Pattern (mirrors
the Vultr validate-then-teardown discipline):

1. `pscale branch create <db> ps-validate-<date>` (or create the DB if
   none kept) — only now does PS cost accrue.
2. Wait ready; capture the connection DSN into the run env (never echo
   credentials — `planetscale-creds` standing rule).
3. Run **only the PS-only-scoped suite** (a narrow `psverify`-tagged
   target — *not* the full integration matrix): latency/batch-sizing
   (#18), cold-start dedup under write load (#14), TCP-reset resilience
   (#21), tx-killer/throttler-under-load, online-DDL-vs-ADR-0049.
4. Capture verdict + logs to the validation track artifact
   (`sluice-validation` rig / Track-1b — *not* `sluice-testing`; see
   `planetscale-validation-track`).
5. `pscale branch delete …` (and the DB, if ephemeral) — **cost stops.**
6. Cadence: per-release or on-demand when touching VStream/applier
   /CDC-recovery code, **not** 24/7. "Every so often, as-needed" is the
   explicit operator intent (2026-05-19).

**Open follow-up (not blocking this note):** the `psverify` tag today
is broad; tightening it to a PS-only-scoped subset (so step 3 is
genuinely narrow and the online window is minutes, not an hour) is the
concrete next implementation step when the operator moves PS to
on-demand. Tracked alongside corpus iter-4 / Track-1b.

## References

- `docs/dev/notes/prep-continuous-validation-on-vultr.md` §"The one
  concern worth re-evaluating" — origin of the #14/#18/#21 latency list.
- `docs/adr/adr-0038-applier-retry-on-transient-errors.md` — the
  managed-Vitess transient class (GitHub #13) that is PS-real.
- `docs/adr/adr-0049-cdc-schema-history.md` + Phase-1c — what
  vttestserver *does* prove about schema-evolution.
- auto-memory: `planetscale-validation-track`, `planetscale-creds`,
  `vultr-retired-local-validation-vm` (the precedent decommission).
