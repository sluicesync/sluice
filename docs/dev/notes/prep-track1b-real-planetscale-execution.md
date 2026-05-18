# Prep — Track 1b: real-PlanetScale execution design

Design contract for the **PS-managed product envelope** that local
Vitess cannot represent. **Stop after the design for review.** No
PlanetScale calls are made by this doc — design only; execution is
explicitly gated on per-scope operator go-ahead (real billable
service).

## What Track 1b is (and is not)

From `prep-planetscale-vitess-readiness.md`: local Vitess (Track 1a,
**done**) is authoritative for reshard/CDC *mechanics* — identical
Vitess code path, scriptable, free. Track 1b covers only what real
PlanetScale exposes that local Vitess cannot:

- `pscale keyspace resize` is **cluster-size, not shard-count** —
  there is **no CLI-automatable shard-count reshard** on real PS.
  ⇒ dynamic-reshard correctness stays on local Vitess; on real PS it
  is a **documented manual/periodic check only**, not in this scope.
- In scope (the PS product envelope): static already-sharded source,
  the no-`LOCAL INFILE` batched-INSERT copy path at scale, branching/
  deploy-request survival, vtgate `information_schema` fidelity +
  latency, connection-cap behavior.

## Credential path — proven this session

`pscale` authenticates **non-interactively via service token** (no
browser `auth login`). Confirmed working 2026-05-17:

- Token file: `C:\code\sluice-testing\PLANETSCALE_SERVICE_TOKEN.env`
  (keys `PLANETSCALE_SERVICE_TOKEN`, `PLANETSCALE_SERVICE_TOKEN_ID`;
  gitignored). Source into env in the *same* shell as the pscale
  command (Bash tool doesn't persist env): `set -a; . <file>; set +a`.
- Org `regions-metrics`; 16 DBs visible incl.
  `sluice-validation-mysql-source/-destination`,
  `sluice-validation-postgres-destination`, the `sluice-mysql-*` /
  `sluice-postgres-*` pairs, `schema-example-01/02(-postgres)`, and
  `example` (the interesting-CREATE-TABLE corpus).
- DSNs for sluice itself: `PLANETSCALE_CREDENTIALS.env` at the sluice
  repo root (`psverify` build tag consumes it). **Never echo token or
  DSN values** anywhere (chat/logs/commits/PR/CI) — see memory
  `planetscale-creds`.

This means Track 1b is autonomously *credential-capable*, but the
runs are **real billable PS** → every scenario needs explicit operator
go-ahead per-scope (not blanket). The standing autonomous-PS
authorization (2026-05-07) covers the *test-cycle loop* against the
known DBs, not arbitrary new keyspace creation / scale tests.

## Scenarios (design)

### 1b.1 — static sharded source migration
`pscale keyspace create --shards N` + `vschema update` on a dedicated
DB → sluice `migrate` a real already-sharded keyspace through vtgate
(real scatter/edge/auth). Oracle: src==dst via the existing
count+sample+full verify (`sluice verify`), reading both sides
directly. Automatable end-to-end; needs operator OK to create the
keyspace (cost).

### 1b.2 — no-`LOCAL INFILE` batched-INSERT at scale
PS's *default* copy path is batched INSERT (the #18 LOAD-DATA
hardening does **not** apply to PS). Seed 10M+ / wide rows into a PS
source, time + correctness-check the copy. This is the throughput
make-or-break. Oracle: row count + sampled value equality + wall-time
recorded to `sluice-validation/session-reports/`.

### 1b.3 — branching / deploy-request survival
`pscale branch` → migrate into branch → `pscale deploy-request` →
promote. Question: does sluice migrate/CDC state survive a branch
promotion (schema-version, cdc position, slot equivalents)? Oracle:
CDC stream continuity across the promotion, no silent gap (the
loud-failure bar from Track 1c).

### 1b.4 — vtgate `information_schema` fidelity + latency
sluice's schema reader depends on `information_schema`; vtgate
aggregates differently than vanilla MySQL and the rig already flagged
~30s serial introspection for 329 tables. Measure + assert schema
read correctness vs a vanilla baseline; record latency.

### 1b.5 — connection-cap behavior
Drive sluice against PS's aggressive connection limits; assert
graceful degradation (loud, retried, not silent corruption).

## Placement / harness

- Lives behind the existing `psverify` build tag
  (`internal/engines/{postgres,mysql}/planetscale_verify_test.go`,
  `internal/pipeline/planetscale_verify_test.go`,
  `.github/workflows/psverify.yml` — `workflow_dispatch` only, never
  default CI: quota + security boundary).
- Results/log narrative go to the **validation rig**
  (`C:\code\sluice-validation`, RUNBOOK-governed, own BUG-CATALOG /
  session-reports) — *not* sluice-testing (post-release regression) and
  *not* committed to sluice. See memory `planetscale-validation-track`.
- Reuse, don't rebuild: the `psverify` scaffolding + the validation
  rig's existing PS harness. Phase-A mandate: study
  `planetscale_verify_test.go` + the rig RUNBOOK before writing.

## Open questions (resolve before any PS run)

1. Which scenarios get a go-ahead now vs deferred? (1b.4/1b.5 are
   cheap + read-mostly; 1b.1/1b.2 create keyspaces / large data =
   cost — likely operator-supervised.)
2. Dedicated throwaway DBs for 1b.1/1b.2, or the existing
   `sluice-validation-*`? (Recommend dedicated, named
   `t1b-*`, dropped after — avoid polluting the validation baseline.)
3. Scale target for 1b.2 (10M? 50M?) and the acceptable copy
   wall-time bar (define the pass/fail before running, not after).
4. Is branch-promotion (1b.3) in scope now or is it the riskiest /
   most-manual → defer to operator-driven?

## Suggested first-cut prompt

> Read CLAUDE.md, this doc, the readiness prep, and the
> sluice-validation RUNBOOK. Do NOT call PlanetScale until the
> operator green-lights specific scenarios. Start with 1b.4 (schema
> fidelity/latency — cheap, read-only) and 1b.5 (conn caps) as the
> low-cost first cut; design 1b.1/1b.2 fixtures but hold execution.
