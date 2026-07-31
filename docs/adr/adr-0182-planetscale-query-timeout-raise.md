# ADR-0182: Opt-in raise of PlanetScale's keyspace query-timeout for a migration

- **Status:** Accepted — **IMPLEMENTED (2026-07-31, roadmap item 110)**; main-session review + land pending (not yet released). Opt-in, tightly gated, off by default.
- **Date:** 2026-07-31
- **Related:** roadmap item 110; ADR-0148 (the deploy-request index-build fallback whose `internal/planetscale/api` client, credential plumbing, and CLI-composer posture this reuses); ADR-0162 (the shared no-SDK control-plane client); roadmap item 109 / `SLUICE-E-CONSTRAINT-STATEMENT-TIME-LIMIT` (the constraints-phase half of the same errno-3024 wall); `--upfront-indexes` (the pre-copy alternative this composes with); ADR-0082 (`sluice_migrate_state`, the crash-safe store this records the raise in).

## Context

PlanetScale's ~900 s `queryserver-config-query-timeout` (errno 3024) is the root of the whole statement-time-wall family: a deferred `ALTER … ADD INDEX`, an `ADD FOREIGN KEY` (item 109), and — measured 2026-07-31 — even a single `--upfront-indexes` bulk-copy insert on network-attached storage all die at the wall on a large table.

The wall is a *configurable keyspace setting* (`queryserver-config-query-timeout`, default 900, max 3600). Measured head-to-head 2026-07-31: on a Metal (local-NVMe) target the deferred 4-index build on 153 M rows completes in ~35 m — which fits 3600 s but not 900 s — so *raising the timeout makes the direct path viable there* without a deploy request or the ~11× `--upfront-indexes` load penalty.

This is NOT reachable through the documented `update_keyspace` PATCH (that endpoint does nothing for these options). It goes through a stage → submit → roll-out workflow, fully reverse-engineered and live-verified 2026-07-31 (see §API flow). Two wire quirks bit during probing and are load-bearing: the config-change body field is **`options`** (not `vttablet_options`) and each value is a **JSON string** (`"3600"`, not the number `3600`) — the number form and the `vttablet_options` field name both 422.

## Honest scope (what this is and is NOT)

This is a **useful option that composes with the existing mechanisms, not a silver bullet.** Measured evidence:

- On a **Metal** target `--upfront-indexes` is already the cleaner path and needs no timeout tuning; the raise's real value there is the *deferred* path, which stays fast-copy AND avoids the wall when the build fits under 3600 s.
- On **network-attached (Scaler) storage** a large index build exceeds even 3600 s, so the raise does **not** help that case.
- Whether raising to 3600 s rescues the Scaler **upfront-copy insert wall** (the my5 insert that walled at 900 s) is a hypothesis, **not yet verified** — the ADR does not claim it.

So the raise gives *headroom so the direct path succeeds where it would otherwise wall on fast-enough tiers*; the `--upfront-indexes`, ADR-0148 deploy-request, and item-109 metadata-only-FK fallbacks all remain, orthogonal.

## API flow (live-verified 2026-07-31)

All paths under `/v1/organizations/{org}/databases/{db}/branches/{branch}`:

1. **Resolve keyspace:** `GET …/keyspaces` → `{"data":[{"name":…}]}`.
2. **Read current:** `GET …/keyspaces/{ks}` → `{"vttablet_options":{…}, "config_change_in_progress":bool}` (an empty `vttablet_options` ⇒ the keyspace is at the 900 default).
3. **Stage:** `POST …/keyspaces/{ks}/config-changes` `{"change_type":"vttablet","options":{"queryserver-config-query-timeout":"3600"}}` → `{"id":…,"state":"draft","previous_options":{…}}`.
4. **Submit (branch-level, NOT keyspace-level):** `POST …/config-changes/submit` `{"ids":["<id>"]}` → `state:"applying"`.
5. **Poll:** `GET …/keyspaces/{ks}/rollout-status` to `state:"complete"`, paired with `GET …/keyspaces/{ks}` reporting `config_change_in_progress:false`. The rollout is a rolling tablet restart, ~2–6 m.
6. **Revert:** repeat 3–5 with the recorded previous value.

`change_type` at the keyspace level accepts `vttablet` and `mysqld`; `vtgate` config is branch-level and not relevant here.

## Decision

A **single opt-in flag**, `--planetscale-raise-query-timeout` (bool, off by default), raises the target keyspace's `queryserver-config-query-timeout` to 3600 for the duration of a `migrate`, then reverts it to the value it found.

### Gating

The raise fires **only** when the flag is set AND the target is PlanetScale/Vitess AND `--planetscale-org` + the service token are present (the ADR-0148 plumbing — one flag set, reused). Unlike the index fallback's *opportunistic* arming (a missing credential WARNs and stays off, because the credentials routinely arrive from ambient env vars), the query-timeout raise is *explicitly requested*, so a flag set on a target/credential combination that cannot honor it is a **loud refusal**, not a silent no-op — the operator asked for it and should learn why it cannot happen. The refusal is checked at the CLI (naming the flag) before the migrate runs.

### Size gate

If the migration's largest table is below a floor (`queryTimeoutRaiseMinRows = 1,000,000` estimated rows, via the source `ir.RowCountEstimator` the parallel-copy chunk decision already uses), the raise is **skipped with an INFO line** — two rolling restarts (~5–12 m total, disruptive to everyone else on the keyspace) cost more than the ~900 s wall a small migration would never approach. The floor is deliberately conservative: the measured wall-hitting scale is ~153 M rows, so a 1 M floor never skips a migration that could plausibly wall while sparing a genuinely small one. Row count is an imperfect proxy (a narrow-but-huge or wide-but-short table is judged only by rows) but it is the estimate the migrate path already has pre-copy — noted, not hidden.

### Revert to the recorded PREVIOUS value, never a hardcoded 900

The stage response's `previous_options` (cross-checked against the pre-raise `GET keyspace`) is recorded and restored exactly — an operator who set 1200 gets 1200 back. An empty `vttablet_options` (the keyspace was at its default) reverts by setting the documented default **900** — the API accepts the explicit value, which restores the exact prior *observable* state; sluice does not rely on a "clear the option" verb it did not verify.

### Crash-safe: record-and-auto-revert via `sluice_migrate_state`

The "leave it as you found it" guarantee is the bulk of the work and is crash-safe:

- The previous value is **persisted to migrate-state BEFORE the raise takes effect** (the record callback gates the apply — the raise proceeds only if the record landed), so a crash during the raise rollout is still recoverable.
- On **any run start** (fresh, `--resume`, or a bare re-run), a dangling raise recorded in migrate-state is **reverted first, before anything else touches the target** — and before the `--reset-target-data` path clears the state row, so a destructive re-run still restores the keyspace.
- The revert runs on **success AND on every abort path** (a deferred closure registered the instant the raise completes), on an *uncancellable, timeout-bounded* context — the reason a migration is aborting must not be the reason its keyspace is left raised. The record is cleared only on a *successful* revert, so a failed revert stays recoverable by the next run.
- A dangling raise found with **no controller to revert it** (credentials absent on this run) is a **loud refusal**, not a silent skip — the keyspace is in a modified state and sluice must restore it (or the operator must supply the credentials).

**Persistence shape.** A new nullable `ps_query_timeout_raise` TEXT column on the MySQL `sluice_migrate_state` header row (added additively, detect-then-ALTER, exactly like `state_format`), holding a small JSON envelope `{"previous":"…"}`. NULL means "no raise recorded"; a non-NULL value carries the previous (which may itself be empty — "the keyspace was at its default"). It is written through its **own** statements (`RecordQueryTimeoutRaise` / `ClearQueryTimeoutRaise`), **never the generic header upsert**, so ordinary phase-transition writes cannot clobber a live raise record — the load-bearing property, pinned by an integration test that records a raise, does a phase-transition `Write`, and asserts the record survived. MySQL-only by construction (the raise only ever targets a PlanetScale/Vitess keyspace), exposed via the optional `ir.QueryTimeoutRaiseRecorder` interface the pipeline type-asserts; Postgres (never a PlanetScale target) is untouched.

### Sequencing

Raise + wait-for-rollout runs **before the copy phase** (the rolling restart must not hit an in-flight copy). Revert + wait-for-rollout runs **after all phases complete**, and on any failure/abort. Pinned by a Run-level ordering test asserting `raise < copy < revert` on both the success and the injected-failure paths.

### `config_change_in_progress` at raise time

sluice does not stack config changes: if one is already rolling out (including the stream's own crashed raise), the workflow **waits for it to settle**, bounded by a rollout timeout — a wedged or someone-else's-never-settling change becomes a loud refusal rather than a hang.

## Architecture

Mirrors ADR-0148 exactly:

- **`internal/planetscale/api/config_change.go`** — the config-change verbs (`ListKeyspaces`, `GetKeyspace`, `CreateKeyspaceConfigChange`, `SubmitConfigChanges`, `GetKeyspaceRolloutStatus`) on the shared `api.Client`, shape-faithful to the live-verified wire (`options` field, string values).
- **`internal/planetscale/querytimeout`** — `Raiser` implements the engine-neutral `ir.QueryTimeoutController` (`Raise(ctx, record)` / `Revert(ctx, previous)`), owning keyspace resolution, the settle wait, staging/submitting, and rollout polling.
- **`internal/pipeline/query_timeout_raise.go`** — the orchestration WORKFLOW (size gate, record-before-apply, the defer-style revert, the crash-recovery auto-revert). The pipeline never imports the PlanetScale API; the controller is injected opaquely.
- **CLI (`cmd/sluice/migrate_query_timeout.go`)** — composes the controller from the shared `--planetscale-*` flags (available for the auto-revert even without the flag) and produces the loud refusal when the flag is set but nothing can honor it.

The controller is composed whenever the credentials resolve, so a run that carries the credentials but NOT the flag can still auto-revert a dangling raise; the flag only ARMS the raise.

## Alternatives considered / deferred

- **A dedicated control table** for the raise record was rejected in favor of a column on the existing `sluice_migrate_state` header — the record is per-`migration_id` and rides the same crash-safe, resumable store the migration already uses; a separate table would duplicate its lifecycle.
- **Storing the raise under a synthetic `migration_id`** (reusing the store with zero schema change) was rejected — it would either pollute the resume-facing `TableProgress` map or overload `last_error`; a named, purpose-built column reads cleaner and cannot be clobbered by the generic upsert.
- **A `SLUICE-E-PS-QUERY-TIMEOUT-*` error code** for the arming / dangling-raise / config-change-in-progress refusals is **deliberately deferred**. The refusals are loud plain errors that name the row and the remedy; a machine-parsable code is worth minting if an automation later needs to branch on this class (the registry grows organically as remedies earn a code). Filed as a follow-up rather than expanding the strict error-code doc-sync surface in this chunk.
- **Bounding the rollout wait with a flag** (à la `--planetscale-deploy-timeout`) was not added — a config-change rollout is minutes, not hours, so an internal default bound (20 m, and 25 m for the uncancellable teardown revert) is sufficient; a flag can be added if the field shows a rollout that legitimately runs longer.

## Perf-parity note

This is a reliability/headroom option, not a throughput technique, and it reaches exactly one cell: `migrate` on the PlanetScale/Vitess MySQL flavor. It does not touch sync cold-start, backup, restore, chain-replay, broker, or CDC apply, and makes no claim on `docs/dev/perf-parity-matrix.md`.

## Concurrency note

The `Raiser` polling loops and the pipeline's deferred revert run on their own goroutine-free control flow (no shared mutable state across goroutines; the teardown revert uses `context.WithoutCancel` + a timeout, the established sluice teardown pattern). No new goroutines or channels are introduced, so there is no `-race`-specific exposure beyond what the existing migrate path already carries.
