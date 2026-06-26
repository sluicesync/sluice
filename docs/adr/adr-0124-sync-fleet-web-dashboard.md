# ADR-0124: Sync fleet web dashboard — a read-only HTML view over `sync run`

## Status

**Accepted (2026-06-26).** Roadmap item 47 deferred-polish, staged MINIMAL-FIRST.
Implements the "TUI / web dashboard" layer that ADR-0122 §Status explicitly deferred
out of the command-center v1 ("A TUI / web dashboard is explicitly a LATER layer over
the already-exported `/metrics` + the aggregated status; it is NOT v1.").

One deliverable: an opt-in `--dashboard-listen ADDR` flag on `sluice sync run` that
serves a self-contained, auto-refreshing HTML page rendering the live fleet's health
(per-sync state, restarts, consecutive failures, last error, uptime) plus a JSON API
(`GET /api/fleet`) backing it. Read-only; no data path; off by default.

## Context

`sluice sync run` (ADR-0122) supervises N failure-isolated syncs in one process and
already exposes their live health in-process via `Supervisor.Snapshot() []SyncStatusSnapshot`
(state ∈ {starting, running, backoff, failed, stopped}, consecutive-failure count,
restart count, last error, last-start / state-since timestamps). The same data is
rendered to the terminal by `sync status --all`, but an operator keeping a fleet alive
wants a glance-able live view they can leave open in a browser — the natural companion
to the command center, and the layer ADR-0122 deferred.

The primitives are all present: the supervisor snapshot is the data, the project
already runs read-only stdlib `net/http` servers for `/metrics` + `/healthz` + `/readyz`
(`internal/pipeline/metrics.go`) with a clean "wire handlers, `Start()`, failure-isolated
from the pipeline" pattern, and kong subcommand/flag registration is mechanical
(`cmd/sluice/cli.go`). Nothing in the apply path changes.

The tenet pressure is the same loud-failure / user-trust one as the supervisor itself:
the dashboard is an *observability* surface and must NEVER be able to affect a running
sync. So it is strictly read-only (only `GET`, only `Supervisor.Snapshot()` reads — no
mutation, no stop/restart controls in v1), its HTTP server is failure-isolated from the
fleet exactly as the metrics server is (a dashboard bind/serve error is logged and the
fleet runs on), and it is opt-in (empty `--dashboard-listen` ⇒ no server, no goroutine,
the zero-value-safe default).

## Decision

1. **Surface: `--dashboard-listen ADDR` on `sync run` only.** The richest fleet data
   (`Supervisor.Snapshot()`) is in-process in the supervisor; a standalone
   `sync dashboard` against a remote target would only have the thinner `sync status`
   data and a separate connection budget. v1 binds the dashboard to the in-process
   supervisor. Empty/unset ⇒ off. The flag mirrors `--metrics-listen`'s shape and bind
   semantics.

2. **Two endpoints, one tiny stdlib server.**
   - `GET /` → the dashboard HTML (a single `go:embed`-ed page; auto-refreshes by
     polling `/api/fleet` on a client-side timer — no websockets, no build step).
   - `GET /api/fleet` → JSON: `{ "generated_at": <rfc3339>, "syncs": [ {id, state,
     consecutive_failures, restarts, last_error, last_start, since, seconds_in_state} ] }`,
     derived purely from `Supervisor.Snapshot()`. Stable, documented shape so the page
     (or any external poller) can consume it. (`seconds_in_state` is the wall-clock
     seconds the sync has been in its current state — `now − since`.)
   - `GET /healthz` → `200 "ok"` (liveness; mirrors the metrics server) so the dashboard
     port is probe-able.

3. **Read-only, no controls in v1.** No stop / restart / reload buttons — those would
   make the dashboard a mutation surface and require auth/CSRF thinking the project has
   deliberately not taken on. The page is a live status board. (A future ADR may add
   authenticated controls; explicitly out of scope here.)

4. **Failure-isolated, like the metrics server.** The dashboard server is constructed
   and `Start()`ed alongside the metrics server in the `sync run` wiring; a listen/serve
   error is logged loudly and does not abort the supervisor. Shutdown is tied to the
   fleet context (graceful `Shutdown` on drain).

5. **No new dependencies.** Hand-rendered JSON (`encoding/json`) + a single embedded
   HTML/CSS/JS file via `go:embed`. No client_golang, no JS framework, no templating
   engine beyond optional `html/template` for escaping the (operator-controlled) bind
   metadata. Mirrors the metrics server's "stdlib only" precedent.

6. **Security posture: documented, not enforced.** Like `/metrics`, the dashboard has
   no auth and should be bound to localhost or a trusted network; the flag help and the
   operator doc say so explicitly. It exposes only what `sync status --all` already does
   (stream-ids, states, error strings) — no DSNs, no secrets, no row data. Error strings
   are HTML-escaped on render.

## Consequences

- An operator runs `sluice sync run --config syncs.yaml --dashboard-listen :9300` and
  opens `http://localhost:9300/` to watch the fleet live — the deferred ADR-0122 layer,
  delivered. External tooling can poll `/api/fleet` for the same data as JSON.
- Purely additive and opt-in: no behavior change to `sync run` without the flag, no
  apply-path change, the `SyncRunner` interface is untouched (the dashboard reads the
  supervisor, not the runners). Zero-value-safe (empty addr ⇒ off).
- v1 shows supervisor fleet-health (the "is my fleet up / restarting / failed" view).
  Per-sync lag / throughput (`sluice_sync_lag_seconds` et al.) stay on the per-sync
  `/metrics` endpoints for now; surfacing them in the dashboard is a documented
  follow-up that would either scrape the per-sync metrics or widen the runner surface —
  deliberately deferred to keep v1 minimal and the runner interface clean.

## Alternatives considered

- **A full TUI (terminal UI).** Heavier (a TUI lib dependency, terminal-state handling)
  and not remotely viewable; the operator asked for something openable in a browser. A
  web page over a stdlib server is lighter and matches the existing `/metrics` precedent.
- **Embed the dashboard in the existing metrics server / mux.** Rejected for v1: the
  metrics server is per-`Streamer`, but the dashboard is a *fleet* (supervisor-level)
  view, so it belongs on the `sync run` supervisor, not on any one sync's metrics port.
  Keeping it a separate listener also keeps the failure-isolation boundary crisp.
- **Mutation controls (stop/restart/reload from the UI).** Rejected for v1 — turns a
  read-only observability surface into an unauthenticated control plane. Deferred.
