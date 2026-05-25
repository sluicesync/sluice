# sluice v0.83.0 — sluice cutover subcommand for sequence priming (F10)

**Headline:** Minor release closing the **PK-collision-on-first-INSERT-after-cutover** class. Between snapshot and cutover, the source advances sequences as new rows are inserted. CDC ships those rows to the target, but the target's sequence value still lags the source by the catch-up window. Operators' first post-cutover INSERT then collides with an existing replicated row's PK. Pre-v0.83.0, operators ran `SELECT setval(...)` per table by hand; v0.83.0 makes it one command.

## Added

- **`feat(cli/engines): F10 — sluice cutover subcommand for sequence priming (#49 / ADR-0062)`**

  ### CLI surface

  - `sluice cutover --config sluice.yaml` — reads source sequences, applies them to target with safety margin
  - `--cutover-sequence-margin=N` (default `1000`) — buffer added to each `setval` / `AUTO_INCREMENT`
  - Exit code: 0 on clean run; non-zero when any table refuses loudly

  ### Idempotency

  Target-side-read-guarded. The decision tree:

  - **target ≥ applyValue + margin** → "refused" (operator post-cutover INSERT class)
  - **target ≥ applyValue** → "noop" (idempotent re-run, no work)
  - **otherwise** → "primed" via `setval(seq, source+margin, true)` (PG) / `ALTER TABLE … AUTO_INCREMENT = N` (MySQL)

  Running cutover twice does not regress sequence values. Operators who experienced a partial network failure mid-run can simply re-invoke.

  ### Refuse-loudly class

  When the target's current value exceeds source + 2× margin, sluice refuses with operator-action: "manual re-snapshot recommended". This catches the situation where the operator ran post-cutover INSERTs *before* calling `sluice cutover` — target has already advanced past where the priming pass would land it.

  ### Composite-PK / UUID-PK / identifier-only skipping

  Tables without an owning sequence (composite PK, UUID PK) are skipped with a clear reason in the report. No false-positive refusal on identifier-only tables.

## Architecture

- `ir.SequencePrimer` interface + `ir.SequenceState` / `ir.SequencePrimeReport` / `ir.ErrCutoverSequenceTargetAhead` (`internal/ir/cutover.go`)
- PG: `internal/engines/postgres/cutover_sequence.go` — `pg_get_serial_sequence` for column→sequence resolution; `pg_sequences.last_value` (NULL = never called) on source; `setval('<target_seq>', N+margin, true)` on target
- MySQL: `internal/engines/mysql/cutover_sequence.go` — `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` with `information_schema_stats_expiry = 0` to bypass catalog cache; `ALTER TABLE … AUTO_INCREMENT = N` on target
- Pipeline orchestrator: `internal/pipeline/cutover.go` — single-goroutine, table-by-table, target-side-read-guarded
- CLI: `cmd/sluice/cutover.go` — mirrors the `matview` subcommand shape (render-once + exit-code-on-report-state)

## Hotfix (during PR CI)

- **`fix(engines/postgres): F10 — pg_sequences has no is_called column`** — the original implementation queried `pg_sequences.is_called` which doesn't exist (the `is_called` field is on the underlying sequence object, not the view). PG 16 CI surfaced this as `ERROR: column "is_called" does not exist (SQLSTATE 42703)`. Fixed by querying `last_value` only — NULL signals "never called" (treat as 0), non-NULL is the most-recently-issued value. Caught before merge; both `SchemaReader.ReadSequenceState` and `SchemaWriter.PrimeSequences` updated.

## Tests

- **11 unit tests**: orchestrator dispatch / refuse-loudly / margin (7), IR-shape (2), PG sequence-name parser — 4 shape variants: bare, fully-quoted, mixed-quoted, dot-in-quoted (2). Bug 74 class-pin discipline.
- **3 PG integration tests**: prime + idempotency, refuse-loudly when target ahead, composite-PK skip
- **3 MySQL integration tests**: same matrix against `AUTO_INCREMENT`
- **1 cross-engine integration test**: PG → MySQL full handoff + idempotency (PG `pg_sequences` → MySQL `AUTO_INCREMENT`)
- **`-race` gate**: orchestrator is single-goroutine (no race surface); integration tests exercise the engine plumbing on CI's Linux `-race` Integration job.

## Docs

- **ADR-0062 — Cutover sequence priming** (`docs/adr/adr-0062-cutover-sequence-priming.md`). Covers motivation (Reddit-research F10), two-phase model (snapshot sequence sync + cutover re-pass), safety-margin rationale, idempotency contract, refuse-loudly class, and relationship to chain-restore + bulk-copy phase.

## Compatibility

- **Drop-in upgrade from v0.82.0.** New subcommand only — operators who don't run `sluice cutover` see no behavior change. The defaults are conservative (margin=1000) so a first invocation against any healthy migration target is safe.
- **Minor version bump (v0.83.0)** because of the new subcommand.
- **Severity a** — closes the PK-collision-on-first-post-cutover-INSERT class. Operators migrating to a new system who hit this used to surface "data corruption" tickets that were actually sequence-priming gaps.

## Who needs this

- **Anyone migrating to a new PG or MySQL system** where the source receives writes during the CDC catch-up window. Run `sluice cutover` after stopping source writes and before flipping application traffic to the new system. First post-cutover INSERT lands cleanly above the catch-up window's max sequence value.
- **Operators NOT using sluice for cutover** — no observable change.

## Cross-references

- [ADR-0062 — Cutover sequence priming](https://github.com/orware/sluice/blob/main/docs/adr/adr-0062-cutover-sequence-priming.md)
- [Reddit research F10 catalogue entry](https://github.com/orware/sluice/blob/main/docs/research/) — operator-pain source
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative* — the 11-unit-test matrix follows this discipline
