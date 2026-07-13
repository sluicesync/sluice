# ADR-0156: TTY-aware live status panel for continuous commands

- Status: Proposed
- Date: 2026-07-12
- Deciders: sluice maintainers
- Related: ADR-0155 (TTY-aware pretty output for one-shot commands)

## Context

ADR-0155 gives sluice's **one-shot** commands (`migrate`, and the rollout to
`verify`/`restore`/`backup`/`trigger`/…) a TTY-aware pretty view: a phase
checklist that fills in, a progress bar, and a final summary panel — while every
non-TTY / `--log-format=json` / `--no-progress` invocation keeps the exact
structured `slog` stream, byte-for-byte.

That model assumes a run that **completes**. sluice also has **continuous**
commands that never complete on their own:

- `sync start` — a single continuous-sync stream (initial copy → CDC → ongoing).
- `sync from-backup run` — the broker (poll a chain, replay incrementals).
- `backup stream run` — rolling incrementals at a cadence.
- `metrics-watch` — poll a PlanetScale control-plane and alert.

For these, the checklist-then-summary shape is wrong: there is no terminal
summary, the interesting information (position, lag, rows applied, throttle
WARNs) evolves over the process lifetime, and a run can last days. The current
interactive experience for `sync start` is the raw `slog` stream — legible to a
log aggregator, but a wall of lines for an operator watching a terminal, and it
undersells the tool (this is the same motivation as ADR-0155, and `sync start`
is the homepage demo command).

There is already a *fleet* dashboard — `sync run --dashboard-listen` + `sync
tui` — but that is a multi-stream, out-of-process, poll-an-endpoint design. It
does not cover a single foreground `sync start`, and a single stream has room
for far more detail (position, lag trend, throughput, recent events) than one
fleet row.

## Decision

Add a **TTY-aware live status panel** for the continuous commands, reusing
ADR-0155's gating and the `internal/progress` TTY-detection + brand styling, but
with a distinct **continuous** presentation contract.

### Gating — identical to ADR-0155

The live panel renders only when stdout is a TTY **and** `--log-format=text`
**and** `--no-progress` is unset. Otherwise (piped / CI / `--log-format=json` /
`--no-progress`) the command emits today's **byte-identical** structured `slog`
stream. `--no-progress` is the same global escape hatch; the observability /
Loki-ingestion contract is untouched. `sync health` and `sync status` (cron /
scripted, exit-code-oriented) are explicitly out of scope and never get a panel.

### The panel (for `sync start`)

A persistent, in-place-updating view for the process lifetime:

- **Header** — `source → target`, stream-id, and the current **mode**: `initial
  copy` (with a per-table progress bar, reusing ADR-0155's bar + the
  est-exceeded clamp) → `CDC` once the snapshot hands off.
- **Live body (CDC mode)** — last-applied position (source-relative), **freshness**
  (`seconds since last apply`, the load-bearing lag signal — `lag_bytes` only
  where both sides are Postgres, per the soak runbook), cumulative **rows
  applied**, a short **throughput** readout, and stream health (connected /
  reconnecting / restarts).
- **Recent events** — a bounded (last-N) region showing WARN/ERROR as they
  occur (throttle WARNs, reconnects). This is the key departure from ADR-0155:
  a days-long run **cannot buffer warnings until a summary**, so warnings surface
  **live in the panel**, and a running counter keeps the total visible.
- **Footer** — keybindings: `q` / ctrl+c to **drain and stop** (wired to the
  existing graceful `sync stop` path so an abort in the panel actually drains
  in-flight changes), and room for later `p` pause-scroll / detail toggles.

`sync from-backup run` (broker) and `backup stream run` reuse the same panel with
mode-appropriate fields (incrementals replayed / rolled, chunk counts, position).
`metrics-watch` adopts the panel for its sample/alert stream.

### Log handling — surface, don't buffer

ADR-0155's `silenceSlogForTTY` buffers WARN/ERROR and flushes them into the
final summary. That is safe only because a one-shot run is short. For a
continuous run the panel installs a slog gate that **forwards** WARN/ERROR into
the panel's live "recent events" region (bounded ring buffer) instead of
buffering unboundedly, and drops INFO/DEBUG on the TTY (they still exist on the
non-TTY / json path). The panel is the only writer to the TTY for the run's
duration.

### Rollout order

1. **`sync start`** first — it is the homepage sync-CDC demo and exercises both
   the initial-copy bar and the CDC live body.
2. Then `sync from-backup run` (broker) and `backup stream run` — same panel,
   mode-specific fields.
3. `metrics-watch` last — it already has bespoke live output; migrate it onto
   the shared panel for consistency.

## Consequences

**Positive:** a single continuous stream reads as a live, legible status view
on-brand with the fleet dashboard; the homepage sync-CDC demo becomes far more
compelling; the structured-log / json contract is unchanged (default whenever
non-TTY / `--log-format=json` / `--no-progress`); warnings are visible live
rather than lost in a log wall.

**Negative / risks:** a continuous bubbletea program owns the terminal for a
long-lived process — the graceful-stop wiring (`q`/ctrl+c → drain) must be
exact so an operator can always stop cleanly, and a panic in the renderer must
not take down the stream (the sync goroutine and the renderer are separate; a
renderer failure should fall back to structured logging, not abort the sync).
The "recent events" ring must be bounded so memory is flat over days. `lag_bytes`
being PG-pair-only means the panel leads with `seconds_since_last_apply` for
cross-engine, and must not imply a byte-lag it cannot compute.

**Testing:** the panel model is a pure `Update` (msg→model) like ADR-0155's, so
phase/CDC/event transitions are teatest-covered without a terminal; a
TTY-detection test pins that a non-terminal stdout keeps the structured stream
under `--log-format=text`; an integration check confirms `q`/ctrl+c in the panel
drains and stops the stream (no dropped in-flight changes).

## Alternatives considered

- **Reuse the fleet TUI (`sync tui`) for a single stream.** Rejected as the
  primary: `sync tui` polls an out-of-process `--dashboard-listen` endpoint and
  renders a compact multi-stream table; a foreground `sync start` has no such
  endpoint and warrants a richer single-stream view. They will **share lipgloss
  styles and components**, and a later unification (the fleet row rendered from
  the same panel model) is open, but coupling a foreground single-stream panel
  to the poll-an-endpoint fleet design now is the wrong dependency.
- **Extend ADR-0155's one-shot sink with a "never completes" flag.** Rejected:
  the buffer-until-summary log model and the checklist-then-summary shape are
  fundamentally one-shot; a continuous view needs live warning surfacing and a
  persistent (not terminal) body. It reuses ADR-0155's package (TTY detection,
  styles, the initial-copy bar) but is its own presentation contract.
- **Do nothing (keep the raw log stream interactively).** Rejected for the same
  reason as ADR-0155: it undersells the tool and buries the load-bearing signals
  (position, lag, throttle) in a log wall — while the structured logs are
  preserved anyway as the non-TTY default, so the observability path pays no cost.
- **Prettify `sync status` / `sync health` too.** Rejected: those are
  snapshot/cron, exit-code- and `--format json`-oriented; a TUI would break their
  scripted use. They stay machine-first.
