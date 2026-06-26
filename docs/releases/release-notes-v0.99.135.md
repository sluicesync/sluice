# sluice v0.99.135

**New: a read-only WEB DASHBOARD for the sync command center — `sluice sync run --dashboard-listen :9300` serves a self-contained, auto-refreshing HTML page (plus a `/api/fleet` JSON API) showing your live fleet's health at a glance (ADR-0124, roadmap item 47). This is the "TUI / web dashboard" layer ADR-0122 deferred. Opt-in, read-only, no new dependencies; fully drop-in over v0.99.134.**

## Features

**Read-only fleet web dashboard on `sync run` (`--dashboard-listen ADDR`, ADR-0124).** The command center (`sync run`, shipped v0.99.132) supervises N failure-isolated syncs in one process; until now you watched the fleet through `sync status --all` (a terminal snapshot) or the per-sync `/metrics` endpoints. This adds the layer ADR-0122 explicitly deferred: a live, glance-able fleet view you leave open in a browser. Pass `--dashboard-listen :9300` to `sync run` and open `http://localhost:9300/` — a single self-contained HTML page (inline CSS + vanilla JS, no external/CDN assets, no build step) polls the fleet every few seconds and renders one row per sync: stream-id, state (color-coded — running green, backoff amber, starting blue, failed red, stopped grey), restart count, consecutive-failure count, how long it has been in its current state, and its last error. A header shows live total / running / failed counts and a "last updated" stamp; if the API momentarily can't be reached the page raises an "unreachable" banner instead of blanking the last-known state.

**Backed purely by the supervisor's existing snapshot — nothing in the apply path changes.** The dashboard reads only `Supervisor.Snapshot()` (the same in-process fleet state `sync status --all` already renders), so it is strictly an observability surface: read-only, GET-only, with no stop / restart / reload controls (those would make it a mutation surface and are deliberately out of scope for v1). It runs on its own tiny stdlib HTTP server, constructed and started alongside the supervisor and **failure-isolated exactly like the `/metrics` server** — once it is serving, a dashboard error can never affect a running sync. A backing JSON API is exposed at `GET /api/fleet` (a stable, documented shape: `generated_at` plus a `syncs` array of `{id, state, consecutive_failures, restarts, last_error, last_start, since, seconds_in_state}`) so external tooling can poll the same data the page renders, and `GET /healthz` makes the dashboard port probe-able. Error strings and stream-ids are rendered XSS-safely (set as text, never as HTML).

## Compatibility

Purely additive and opt-in. With no `--dashboard-listen` flag (the default), `sync run` behaves exactly as in v0.99.134 — no server, no goroutine, the zero value is off. No behavior change to any existing flag or command, and **no data / read / write / CDC path change** (the dashboard reads the supervisor snapshot only). A new HTTP dependency-free server (`go:embed` + stdlib; no client_golang, no JS framework). One operational note, stated in the flag help and ADR-0124: the dashboard has **no authentication** — like `/metrics`, bind it to localhost or a trusted network. It exposes only what `sync status --all` already does (stream-ids, states, error strings) — no DSNs, no secrets, no row data. A bind-time failure is loud (the fleet refuses to start rather than silently run without the dashboard the operator asked for); once bound, serve errors are swallowed so they can't disturb the fleet. Fully drop-in over v0.99.134.

## Who needs this

Operators running a `sync run` fleet who want a live, browser-viewable health board for their syncs without wiring up Prometheus + Grafana — or who want a simple JSON endpoint (`/api/fleet`) to poll fleet health from their own tooling. Everyone else is unaffected (the dashboard is inert unless `--dashboard-listen` is set). Per-sync lag / throughput remain on the per-sync `/metrics` endpoints for now; surfacing them in the dashboard is a documented follow-up.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.135 · **Container:** ghcr.io/sluicesync/sluice:0.99.135
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
