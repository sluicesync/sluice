# sluice v0.87.0 — connection-stability hardening + a clearer managed-PG failure

**Headline:** Two operator-facing improvements for managed-Postgres / cloud-NAT environments, plus CI hardening. Long-lived connections (CDC streams + the trigger poller) now carry TCP keep-alives so a cloud NAT or load balancer can't silently stall a quiet stream; and a slot-based `postgres` source whose role lacks the `REPLICATION` attribute is now refused **upfront** with a pointer to the slot-less `postgres-trigger` engine, instead of failing opaquely mid-cold-start. Drop-in from v0.86.1 — no config, schema, or IR changes.

## Features

- **Heroku-permission refuse-loudly preflight for slot-based Postgres CDC (task #61).** Slot-based `postgres` CDC creates a logical replication slot at cold start, which needs the connecting role to be a superuser or carry `REPLICATION`. On managed tiers that forbid it (Heroku Postgres Essential, Render Basic, Supabase free), `sluice sync start --source-driver=postgres` used to fail **mid-cold-start** with a raw `ERROR: permission denied to create replication slot` (SQLSTATE 42501). A new source-side preflight now detects the missing capability **before** any slot work and refuses loudly — naming the role and pointing at `--source-driver=postgres-trigger`, sluice's slot-less trigger-capture engine built for exactly this tier. The refusal fires **only** for the slot-based `postgres` engine: it never trips for `postgres-trigger`, MySQL, or a pure bulk `migrate` (which needs only `SELECT` and works fine on Heroku).

## Changed

- **TCP keep-alives on all long-lived database connections (task #77).** CDC streams (Postgres pgoutput, MySQL binlog) and the `postgres-trigger` poller hold a connection open across quiet periods; cloud NAT gateways and L4 load balancers silently evict idle connections, after which the next read/write stalls for minutes instead of failing fast. A new `internal/netkeepalive` policy (enable + idle 30s + interval 10s + count 3 — mirroring PlanetScale's heroku-migrator TCP settings) is now wired into **all four** long-lived dial paths: the Postgres query pool + pgoutput replication connection, the `postgres-trigger` poller/snapshot/setup pools, the MySQL query pool, and the MySQL binlog syncer. It is the transport-level complement to sluice's existing application-level keepalives (pgoutput standby-status updates, binlog `HeartbeatPeriod`): it keeps the NAT mapping warm and bounds dead-peer detection to seconds. Values are fixed (no new config surface).

## Internal

- **CI: bounded-retry GHCR login + image pull.** The pre-baked-image pull step now retries through `scripts/ci-ghcr-pull.sh` to self-heal transient `docker login ghcr.io: context deadline exceeded` / pull-timeout flakes on the self-hosted runner pool (still fails loudly after the retry budget). CI-only — no effect on the shipped binary.

## Compatibility

- **Minor version bump (v0.87.0)** — additive, drop-in from v0.86.1. No config / schema / IR changes.
- **Two behavior changes, both improvements:** (1) long-lived connections now carry TCP keep-alive probes (transparent stability hardening); (2) a slot-based `postgres` source whose role lacks `REPLICATION` is refused **upfront** (with the `postgres-trigger` pointer) rather than mid-cold-start — a clearer, earlier failure for a role that genuinely cannot create a slot. Pure bulk `migrate`, the `postgres-trigger` engine, and all MySQL directions are unaffected.

## Who needs this

- **Operators running CDC / continuous sync against managed Postgres or behind cloud NAT / load balancers** (Heroku, Render, Supabase, RDS/Cloud SQL, PlanetScale) — the keep-alive hardening prevents silent mid-stream stalls on idle connections.
- **Anyone who tried `--source-driver=postgres` on a managed tier that forbids replication slots** — you now get an immediate, actionable refusal pointing you to `--source-driver=postgres-trigger`, instead of a confusing mid-run permission error.
