# ADR-0157: Schema-drift notifications (alert an operator when a source DDL stalls a sync)

- Status: Accepted (implemented; shipping)
- Date: 2026-07-14
- Deciders: sluice maintainers
- Related: ADR-0058 (online schema-change forwarding), ADR-0091 (default-on schema-change forwarding), ADR-0060 (CDC schema-drift diff), ADR-0107 item 36 (target-metrics threshold alerter + the `internal/notify` sink layer), ADR-0126 (per-sync notification wiring)

## Context

A continuous sync has two main ways to stall: a **resource** problem on the target (storage full, replica lag spiking) and a **schema** problem on the source (a DDL sluice cannot auto-forward). ADR-0107 item 36 built an alerter for the first class: a sync-scoped sidecar polls the target's telemetry snapshot and fires a `notify.Notification` to the configured sinks (webhook / Slack / SMTP) on a threshold breach, with edge-trigger + cooldown semantics and strict failure isolation.

The second class has **no notification path**. When the schema-forward intercept refuses a source DDL — RENAME COLUMN on MySQL (unprovable), an ADD COLUMN with a volatile DEFAULT, a multi-shape combo, or a target DDL apply that fails — it stores the refusal into `Streamer.schemaSnapshotErr` (an `atomic.Pointer[error]`), which the streamer surfaces to **stall the stream** (the CDC position is not advanced past the boundary, so no data is lost — the loud-failure tenet). The refusal is *logged* with a structured drift diff (ADR-0060) and a recovery hint (`forwardRecoveryHint`: "run `sluice sync stop --wait`, then schema migrate, then `sluice sync start --resume`"). But for an operator running a long-lived, unattended sync, a stall is invisible until someone reads the logs — exactly the "set it and forget it" case where a silent stall is most costly.

This ADR closes that gap: fire a notification, through the **existing** sink layer, when a schema change stalls the sync — carrying the drift detail and the recovery steps so the alert is actionable, not just "something is wrong."

## Decision

Emit a `notify.Notification` on a **schema-drift refusal** (the stall), delivered to the same sinks the metrics alerter uses, gated by a `--notify-schema-drift` toggle that defaults **on** whenever any notify sink is configured.

Key differences from the metrics alerter, and how they shape the design:

1. **Event-driven, not polled.** A metrics alert is evaluated on a tick against a cached telemetry snapshot. A schema-drift stall is a discrete event with no numeric threshold. So it fires **at the refusal**, not from a tick loop, and it needs **no telemetry provider** — it is available on every engine pair, PlanetScale or not.

2. **Non-threshold notification shape.** `notify.Notification` today is metric-shaped (`Metric`, `Value`, `Threshold`), and the Slack/webhook/SMTP sinks unconditionally render a `Metric V ≥ T` line. A schema-drift event has no `V ≥ T`. Add a `Category` field to `Notification` (`CategoryThreshold` — the default, byte-identical to today — and `CategorySchemaDrift`); each sink renders the `V ≥ T` line **only** for `CategoryThreshold`, and renders `Title` + `Body` for the event category. This keeps the sink layer generic and the existing metrics alerts byte-for-byte unchanged.

3. **Edge-once per stall.** The streamer surfaces the same pending intercept error while the stream is stalled, and a retry loop may re-observe it. Fire **once per distinct refusal** via a latch on the streamer (keyed on the refusal error identity / message), re-armed only after the stall clears (a successful resume). A stalled sync cannot spam by construction, but the latch prevents a retry from re-firing the same drift.

4. **Reuse the sink assembly + failure isolation verbatim.** The notifier is built by the existing `buildMetricsNotifierFrom(webhookURL, slackWebhookURL, smtp)` — one definition of the sink set, so schema-drift and metrics alerts can never target different sinks. Delivery is failure-isolated exactly as the metrics path: a dead sink is logged at WARN and **swallowed**; a notification failure must never stall or crash the sync (it is already stalled on the drift — the notification is advisory).

### Notification content

- `Level`: `LevelCritical` — a stalled sync is the urgent class.
- `Category`: `CategorySchemaDrift`.
- `StreamID`: the stream.
- `Title`: e.g. `Schema change stalled sync "app-prod" — manual recovery needed`.
- `Body`: the refusal's structured detail (the shape + the offending table/column, from the ADR-0060 drift diff) **plus** the `forwardRecoveryHint` recovery steps — the same text logged today, so the alert tells the operator exactly what drifted and how to recover.
- `At`: the refusal time.

### Gating

- `--notify-schema-drift` (bool, default **true**), on `sync start` and per-sync in a `sync run` fleet spec (mirroring the other `--notify-*` flags). When true **and** at least one sink is configured, schema-drift notifications fire. When no sink is configured, it is inert (no-op) — the same "opt in by configuring a sink" model as the metrics alerts. `--notify-schema-drift=false` disables it while keeping metrics alerts.
- **Zero-value-safe (the v0.99.51 trap):** because the desired default is *on*, the field is named for the **opt-out** it isn't — it is a plain `NotifySchemaDrift bool` **defaulting true via the CLI/kong default**, but every non-CLI construction (tests, fleet paths) gets the Go zero value `false`. To keep the zero value safe, the *runtime* guard is "fire when a sink is configured **and** not explicitly disabled": the streamer stores `SuppressSchemaDriftNotify bool` (zero value false ⇒ enabled), and the CLI sets it from `!NotifySchemaDrift`. So the on-by-default behavior holds for every construction, not just the CLI one.

## Alternatives considered

- **Poll for drift like a metric.** Rejected: drift is a discrete event, not a sampled value; polling would add latency and a redundant detection path when the intercept already knows the exact moment and detail.
- **Log only (status quo).** The gap this ADR closes — invisible to an unattended operator.
- **Also notify on a successful *forward*.** An informational "sluice forwarded ADD COLUMN x" audit notification is useful but a different (info, not urgent) class and potentially chatty on a busy schema. Deferred; the default here is the **stall** case the operator flagged. A future `--notify-schema-forward` (default off) can add the audit stream without changing this design.
- **A new sink type / message channel.** Rejected: reuse the existing `MultiNotifier` and its failure-isolation contract; a schema-drift alert is just another `Notification`.

## Consequences

- An operator running a long-lived sync with a notify sink configured is paged (Slack/email/webhook) the moment a source DDL stalls the stream, with the offending object and the drained-recovery steps in the body — closing the "silently stalled sync" operational risk.
- The `internal/notify` sink layer gains a `Category` field; the metrics alerts are byte-for-byte unchanged (they set `CategoryThreshold`, the default).
- No telemetry dependency: schema-drift notifications work on every engine pair, unlike the PlanetScale-telemetry-gated metrics alerts.
- Failure-isolated and off the value path: a notification failure is logged and swallowed; the sync's stall/no-loss behavior is unchanged whether or not a sink is configured or reachable.
- Test surface: the pure "build the schema-drift `Notification` from a refusal error" mapping is unit-testable; the edge-once latch is unit-testable; each sink's `Category`-branch rendering is unit-testable; an integration test drives a real MySQL-source RENAME COLUMN refusal and asserts exactly one notification with the recovery hint in the body.
