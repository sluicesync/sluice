# sluice v0.99.137

**New: a terminal UI for the sync command center — `sluice sync tui --connect ADDR` renders a running fleet's health live in your terminal (bubbletea/lipgloss), completing the "TUI / web dashboard" layer ADR-0122 deferred (the web dashboard shipped in v0.99.135). Opt-in, read-only; fully drop-in over v0.99.136.**

## Features

**Sync fleet TUI: `sluice sync tui --connect ADDR` (ADR-0125).** Where v0.99.135 added the browser dashboard, this adds the terminal equivalent for operators who live in a shell. Run `sluice sync run --dashboard-listen :9300` in one terminal, then `sluice sync tui --connect :9300` in another, and watch the fleet live: a bordered, brand-styled table of every supervised sync — stream-id, state (color-coded: running green, backoff amber, starting orange, failed red, stopped grey), restart count (↻N), consecutive failures, time-in-state (humanized, e.g. `3m12s`), and a truncated last error — with live total / running / failed counts and the dashboard's generated-at stamp. Arrow keys (or `k`/`j`) move the selection, `enter` opens a detail pane with the selected sync's full un-truncated last error plus its last-start / since / seconds-in-state, `esc` closes it, and `q` / `Ctrl-C` quits.

**A pure client of the dashboard API — no new coupling.** The TUI is a read-only HTTP client of the `/api/fleet` JSON the dashboard already serves (ADR-0124): no supervisor coupling, no mutation, no stop/restart controls. That means it composes with the shipped dashboard and works against a **remote** fleet over an SSH tunnel to the dashboard port (the k9s / lazygit pattern). A fetch failure raises a `⚠ <addr> unreachable — showing last known state` banner and keeps the last-known fleet on screen rather than blanking it, mirroring the web dashboard. `--connect` accepts `host:port`, `http(s)://host:port`, or a full `.../api/fleet` URL; `--refresh` (default 2s) sets the poll cadence. The TUI needs an interactive terminal — without a TTY it exits with a clean message pointing you at the web dashboard or `sync status --all`.

## Compatibility

Purely additive and opt-in: a new `sync tui` subcommand, no behavior change to anything else, no data / read / write / CDC path change. It is built on bubbletea + lipgloss — a deliberate dependency choice confined entirely to the new `internal/fleettui` package and its wiring command, so the engine / pipeline / IR core stays dependency-light. Requires a running `sync run --dashboard-listen` to connect to. Fully drop-in over v0.99.136.

## Who needs this

Operators running a `sync run` fleet who prefer a terminal view to a browser — or who want to watch a remote fleet over SSH without port-forwarding to a browser. Everyone else is unaffected (it's a separate opt-in command). Per-sync lag / throughput remain on the per-sync `/metrics` endpoints; the TUI shows the same supervisor health the web dashboard and `sync status --all` do.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.137 · **Container:** ghcr.io/sluicesync/sluice:0.99.137
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
