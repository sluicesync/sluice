# ADR-0125: Sync fleet TUI — a terminal client over the dashboard API

## Status

**Accepted (2026-06-26).** Roadmap item 47 deferred-polish. Completes the other half
of the "TUI / web dashboard" layer ADR-0122 deferred (ADR-0124 shipped the web
dashboard in v0.99.135; this adds the terminal UI).

One deliverable: `sluice sync tui --connect ADDR` — a full-screen, auto-refreshing
terminal UI (bubbletea + lipgloss) that renders a running fleet's health by polling
the `/api/fleet` JSON the dashboard server already exposes. Read-only; opt-in (it's a
separate subcommand); requires a running `sync run --dashboard-listen`.

## Context

ADR-0124 shipped the read-only web dashboard (`sync run --dashboard-listen`) over the
supervisor's `Snapshot()`, exposing the fleet at `GET /api/fleet` as a stable JSON
shape (`generated_at` + per-sync `id`/`state`/`consecutive_failures`/`restarts`/
`last_error`/`last_start`/`since`/`seconds_in_state`). The operator asked for a
terminal equivalent — a TUI showing the same per-sync detail, refreshing live.

The question is where the TUI gets its data. Two models: (a) **in-process** — run the
supervisor and the TUI in one process (`sync run --tui`), reading `Snapshot()`
directly; or (b) **client-of-API** — a separate `sync tui` that polls the dashboard's
`/api/fleet` over HTTP. (a) is one command but a full-screen bubbletea program and the
supervisor's `slog` terminal logging fight over the same TTY (the TUI owns the screen),
forcing log redirection. (b) reuses the existing JSON contract, has no logging
conflict, is trivially testable (mock the fetch), composes with the already-shipped
dashboard, and — as a bonus — works against a *remote* fleet over an SSH tunnel to the
dashboard port. The k9s / lazygit pattern (a TUI client over a backend) is the
established shape.

## Decision

1. **`sluice sync tui --connect ADDR` — a client of `/api/fleet`.** `ADDR` is a
   `host:port` or full URL of a running `sync run --dashboard-listen`. The TUI polls
   `GET {addr}/api/fleet` on a ticker (`--refresh`, default 2s) and renders the fleet.
   It is a pure read-only client — no mutation, mirroring the dashboard's posture.

2. **bubbletea + lipgloss** (the operator's choice over a zero-dep ANSI renderer) for a
   polished view: a bordered, lipgloss-styled fleet table in the sluice brand palette
   (primary `#F35815`), a header with live total / running / failed counts, per-sync
   rows color-coded by state, ↑/↓ row selection, `enter` to open a detail pane (full
   `last_error` + timestamps that the table truncates), and `q` / `Ctrl-C` to quit.
   A fetch failure raises an "unreachable" banner and keeps the last-known fleet on
   screen (mirroring the web dashboard's behavior) rather than blanking.

3. **The bubbletea model lives in a testable package** (`internal/fleettui`), with
   `cmd/sluice/sync_tui.go` wiring the kong subcommand. The model defines its own small
   structs to unmarshal `/api/fleet` (decoupled from `internal/pipeline`'s unexported
   `fleetReport`), so the JSON shape is the contract between them. `Update` is pure
   (msg → model + cmd), so state transitions (data refresh, selection, quit, error
   banner) are unit-tested without a terminal, and `View` output is asserted against a
   fixed model.

4. **No data-path or supervisor change.** The TUI reads the same JSON the dashboard
   already serves; `Supervisor`, the apply path, and the web dashboard are untouched.

## Consequences

- An operator runs `sync run --dashboard-listen :9310` and, in another terminal (local
  or over an SSH tunnel), `sluice sync tui --connect :9310` for a live terminal fleet
  view — the deferred TUI layer, delivered, without disturbing the fleet process.
- Adds the bubbletea / lipgloss dependency family. This is a deliberate, operator-chosen
  exception to the project's stdlib-minimal default (ADR-0124 took the zero-dep path for
  the web page); the dependency is confined to the TUI command and its package, so the
  engine/pipeline/IR core stays dependency-light.
- Purely additive and opt-in: a new subcommand, no behavior change to anything else.
- v1 is read-only (no stop/restart/reload controls), matching the dashboard. The detail
  pane shows the full per-sync record; controls remain explicitly out of scope.

## Alternatives considered

- **In-process `sync run --tui`** (read `Snapshot()` directly). Rejected for v1: the
  full-screen TUI and the supervisor's terminal logging contend for the TTY, forcing log
  redirection, and it couldn't observe a remote fleet. The client-of-API model is
  cleaner and reuses the shipped contract. (A future convenience `--tui` could layer on
  top by pointing the same model at an in-process data source.)
- **Zero-dependency ANSI renderer.** Viable and matches the project ethos, but the
  operator chose the richer bubbletea view (selectable rows + detail pane) over the
  lean redraw.
