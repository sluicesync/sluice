# ADR-0155: TTY-aware pretty output for one-shot commands

- Status: Accepted (rolled out across the one-shot commands, v0.99.232–v0.99.241 — `migrate` first, then `verify`/`restore`/`backup*`, `cutover`/`matview`/`trigger*`, `slot list`; pre-panel INFO-leak sweep completed in v0.99.241)
- Date: 2026-07-12
- Deciders: sluice maintainers

## Context

sluice's one-shot commands (`migrate`, `verify`, `backup`, `restore`) emit their
progress as structured `slog` records — e.g.
`time=… level=INFO msg="migration: phase complete" phase=bulk_copy`. This is
**deliberate and load-bearing**: the same stream is what `--log-format=json`
serialises for Loki/Datadog/CloudWatch ingestion of long-running syncs, and what
CI/k8s/piped invocations consume. It must not change.

But for an operator running a command **interactively at a terminal**, that
line-oriented, timestamped, `level=INFO msg=` output reads as a wall of logs
rather than a legible progress view — and it undersells the tool. sluice already
owns a terminal-UI toolkit (bubbletea + lipgloss, used by the fleet dashboard
`sync tui` and the `internal/fleettui` package), so a nicer interactive view is
within reach without new heavy dependencies.

The trigger was concrete: a homepage demo of `migrate` showed the raw log wall,
and the question arose whether interactive commands should present more like the
fleet TUI. This ADR decides how, without regressing the structured-log contract.

## Decision

Introduce a **TTY-aware presentation layer**. Commands emit their progress to a
small `progress.Sink` abstraction instead of calling `slog` directly at each
phase; the concrete sink is chosen once, at startup, by the environment:

- **Interactive (stdout is a TTY **and** `--log-format=text` **and** not
  `--no-progress`):** a bubbletea/lipgloss **pretty sink** renders a live view —
  a phase checklist that fills in as phases complete, a per-table progress bar
  during bulk copy, and a final summary panel (tables, rows, duration, any
  degraded-FK / dropped-collation warnings). On completion the live view is
  replaced by a compact static summary so scrollback stays clean.
- **Non-interactive (piped / redirected / CI / `--log-format=json` /
  `--no-progress`):** a **log sink** that emits the **exact same `slog` records
  sluice emits today** — byte-for-byte unchanged. This is the default whenever
  stdout is not a terminal, so every existing automation, the JSON ingestion
  path, and `sluice ... | tee` keep working identically.

TTY detection uses `github.com/mattn/go-isatty` (already an indirect dependency;
promote to direct) on **stdout**. `--log-format=json` always forces the log sink
regardless of TTY (structured wins when explicitly requested). A new global
`--no-progress` flag forces the log sink for operators who want plain logs at a
terminal (and is the escape hatch if the pretty view ever misbehaves).

### The `progress.Sink` interface (initial shape)

```go
package progress

type Sink interface {
    PhaseStarted(phase ir.MigrationPhase)
    PhaseCompleted(phase ir.MigrationPhase)
    TableProgress(table string, done, total int64) // bulk-copy ticker feeds this
    Warn(msg string, attrs ...any)                 // degraded FKs, dropped collations
    Summary(s Result)                              // final, terminal render
}
```

- `logSink` wraps the current `slog` calls one-for-one — so the migration's
  existing log lines (`"migration: phase complete" phase=…`, `"migration
  complete" tables=N`, the 2s bulk-copy progress ticker, the constraint-degrade
  WARN) are produced verbatim.
- `ttySink` drives a bubbletea model reusing `internal/fleettui`'s lipgloss
  styles for brand consistency with the fleet dashboard.
- **ASCII-only glyphs** (the ADR-0155 sibling of the v0.99.232 `↻`-tofu fix):
  no glyph the default Windows terminal font lacks. Progress bars use `#`/`-` or
  lipgloss's block runes only after confirming they render on the Windows console
  (fall back to ASCII if not); status marks use `[ok]`/`[..]`, never `✓`/`↻`.

The `Migrator` (and later `verify`/`backup`/`restore`) gains an optional
`Progress progress.Sink` field; **nil defaults to the log sink**, so any caller
that doesn't set it (tests, library embedders, the fleet/broker paths) behaves
exactly as today.

### Rollout is per-command, not automatic

The sink framework is built **once** and shared, but each command must be
**wired** to it (its phase/progress call-sites converted from direct `slog` to
`sink` calls). This is intentional:

1. `migrate` first — it has the richest phase structure and is the homepage
   demo. Ship it, render the before/after, confirm the look.
2. Then `verify` (a result table — a natural fit), then `backup`/`restore`
   (chunk/segment progress), reusing the same sink + styles.

So it does **not** upgrade every command at once; it upgrades them one at a time
onto a shared, consistent presentation, which lets us validate the look on
`migrate` before rolling forward and keeps each wiring change small and
reviewable.

## Consequences

**Positive:** interactive UX is dramatically nicer and on-brand with the fleet
dashboard; demos look polished without faking output; the structured-log /
JSON-ingestion contract is untouched (default whenever non-TTY or `--log-format
json`); ASCII-only keeps it correct on every terminal.

**Negative / risks:** a real presentation refactor (each command's output
call-sites move behind the sink); the pretty renderer must be carefully no-op on
non-TTY (guarded by the default-log-sink rule + `--no-progress`); bubbletea owns
the terminal while active, so any concurrent direct `slog`/`fmt` writes to
stdout during a pretty render would corrupt it — the pretty sink must be the
*only* writer to the TTY for the command's duration (errors still surface via the
final summary + a non-zero exit + the coded-error machinery, which is unchanged).

**Testing:** `logSink` is asserted to reproduce today's exact records (golden
log lines) so the JSON/CI contract can't silently drift; `ttySink` is exercised
via bubbletea's test harness (teatest) for the phase/summary transitions; a
TTY-detection unit test pins that a non-terminal stdout selects the log sink even
under `--log-format=text`.

## Alternatives considered

- **A custom `slog.Handler` that pretty-prints records.** Rejected: slog is
  line-oriented with no live-redraw loop; progress bars + in-place phase
  checklists need a bubbletea event loop, which a handler can't cleanly own.
- **A separate `--pretty` opt-in (default off).** Rejected as the primary
  trigger: the good default for an interactive terminal is the pretty view;
  opt-*out* (`--no-progress`) plus automatic non-TTY fallback serves both
  audiences without operators needing to know a flag. (`--no-progress` still
  exists as the escape hatch.)
- **Do nothing / keep raw logs.** Rejected: it undersells the tool interactively
  and was the concrete motivation; the structured logs are preserved anyway as
  the non-TTY default, so there is no cost to the observability path.
