# ADR-0118: CLI flag-naming consistency (aliases over renames)

- Status: Accepted (implemented in v0.99.127)
- Date: 2026-06-24
- Deciders: maintainer
- Related: [ADR-0091](adr-0091-default-on-schema-change-forwarding.md) (the `--forward-schema-add-column` deprecation pattern this ADR reuses), [ADR-0076](adr-0076-cross-table-copy-worker-pool.md) / [ADR-0079](adr-0079-fast-cold-start-for-sync-path.md) (the two-axis copy parallelism flags), [ADR-0084](adr-0084-backup-restore-cross-table-parallelism.md) / [ADR-0088](adr-0088-mysql-coordinated-parallel-backup-snapshot.md) (backup/restore parallelism), [ADR-0097](adr-0097-parallel-writer-fanout-vstream-snapshot-copy.md) (`--copy-fanout-degree`), [ADR-0038](adr-0038-applier-retry-on-transient-errors.md) (apply/rollover retry knobs), [ADR-0104](adr-0104-mysql-pipelined-cdc-apply.md) (`--apply-concurrency`)

## Context

A CLI audit of `cmd/sluice` (`cli.go`, `backup.go`, `trigger.go`, `cutover.go`, `sync_health.go`) surfaced four pockets of flag-naming drift. None of them is a correctness bug — every flag does what its help text says — but each is a discoverability / least-surprise hazard for an operator who reads one command's flags and reasonably assumes the same spelling means the same thing under another command. The whole tool's credibility rests on operators trusting that the surface is coherent; an inert-but-accepted flag or a needlessly divergent spelling erodes that trust quietly.

The four findings, each ground-truthed against the source:

### 1. Parallelism-flag name overlap (the worst hazard)

`--bulk-parallelism`, `--table-parallelism`, and `--bulk-parallel-min-rows` are spelled identically across `migrate`, `sync start`, `backup full`, and `restore`, but mean materially different things per command — and on `sync start` they are **silently inert for MySQL/VStream sources**:

- **`migrate`** (`cli.go` `BulkParallelism` ~194, `TableParallelism` ~196, `BulkParallelMinRows` ~202): within-table PK-range readers / cross-table copy jobs / small-table threshold, for every source. The general-purpose meaning.
- **`sync start`** (`cli.go` `BulkParallelism` ~697, `TableParallelism` ~699, `BulkParallelMinRows` ~701): the help strings already say "FAST cold-start (ADR-0079, PG source) only … Inert on MySQL/VStream sources (serial cold-start)." So a `sync start` against a **MySQL or Vitess/PlanetScale source** accepts these flags, parses them, and **does nothing** with them — no effect, no error. The operator who set `--bulk-parallelism=8` to speed up a Vitess cold-copy gets a serial copy and no signal. (The flag that *does* govern VStream cold-copy write concurrency is `--copy-fanout-degree`; see finding 4.)
- **`backup full`** (`backup.go` `TableParallelism` ~395): tables read concurrently during the backup sweep (read-side analog).
- **`restore`** (`backup.go` `TableParallelism` ~1179, `BulkParallelism` ~1181): tables bulk-applied concurrently / a single table's chunks applied concurrently (write-side analog).

So one spelling carries four related-but-distinct meanings, and one of those is a no-op for the most common high-throughput source class on that command. That is exactly the shape that makes an operator mis-tune and mis-trust.

### 2. `apply-retry-*` vs `retry-*` — same concept, two spellings

`sync start` exposes the retriable-failure backoff knobs as `--apply-retry-attempts` / `--apply-retry-backoff-base` / `--apply-retry-backoff-cap` (`cli.go` `ApplyRetryAttempts`/`ApplyRetryBackoffBase`/`ApplyRetryBackoffCap` ~717–719). `backup full` (the backup-stream rollover path) exposes the identical concept as `--retry-attempts` / `--retry-backoff-base` / `--retry-backoff-cap` (`backup.go` `RetryAttempts`/`RetryBackoffBase`/`RetryBackoffCap` ~684–686). The two are conceptually the same retry loop with **identical defaults** (8 attempts / 100ms base / 30s cap), and `backup.go`'s own help even says "Mirrors the sync-stream's `--apply-retry-attempts`." Two spellings for one concept is a memorisation tax.

### 3. `--cutover-sequence-margin` — command name redundantly prefixed

`cutover`'s `SequenceMargin` field carries `name:"cutover-sequence-margin"` (`cutover.go` ~59). It is the **only** flag in the whole CLI that prefixes its own command name into the flag name. Under the `cutover` subcommand the `cutover-` prefix is pure redundancy — every other subcommand names its flags relative to the command, not absolutely.

### 4. VStream cold-copy concurrency is split across two surfaces

The two axes of VStream / native-MySQL cold-copy concurrency are exposed inconsistently:

- **Write axis** — a first-class CLI flag: `--copy-fanout-degree` (`cli.go` `CopyFanoutDegree` ~705, ADR-0097): PK-hash fan-out of the snapshot row stream to N concurrent INSERT writers.
- **Read axis (VStream)** — **DSN-only**: `vstream_copy_table_parallelism`, read solely from the source DSN params (`internal/engines/mysql/cdc_vstream_copy_concurrency.go` `vstreamCopyTableParallelismFromDSN` ~60). No CLI flag exists.
- **Read axis (native MySQL)** — also **DSN-only**: `copy_table_parallelism` (`internal/engines/mysql/cdc_snapshot_concurrent.go` `nativeCopyTableParallelismFromDSN` ~68).

So an operator tuning a VStream cold-copy sets the write axis on the command line and the read axis buried in the DSN query-string — two different surfaces for one logical concern, and the read axis is undiscoverable via `--help`.

### Mechanism available: kong aliases

kong v1.15.0 (`go.mod`) parses an `aliases:` struct tag (`tag.go` ~300–302) and matches a flag by any of its aliases at parse time (`build.go` ~345, `context.go` ~748). A single Go field can therefore accept multiple flag spellings, with one canonical name in `--help` and the alias(es) honored silently. This is the load-bearing primitive for findings 2 and 3: we can add the missing/canonical spelling as an alias **without** breaking the existing one. The tool already ships a deprecate-don't-remove pattern for a superseded flag — `--forward-schema-add-column` (`cli.go` ~783) keeps working, is honored, and emits a one-time `slog.WarnContext` deprecation line at engage time (`internal/pipeline/schema_forward_engage.go` ~75–81). This ADR reuses both mechanisms.

## Decision

A **staged, additive, back-compat-first** plan. The ordering principle is **aliases > renames; deprecate, don't remove; document before you touch a name.** No flag is ever removed or repurposed in a way that breaks an existing command line. The four findings are addressed at the lowest risk that fixes them.

### Finding 1 — overlap: clarify + signal, do NOT rename (lowest risk)

Renaming `--bulk-parallelism` / `--table-parallelism` / `--bulk-parallel-min-rows` per command is **rejected**: they are long-established, documented across releases, and any rename — even aliased — multiplies the surface an operator must learn rather than shrinking it. The names are also genuinely the *same concept* (degree of copy parallelism); the divergence is in *applicability*, not meaning. So:

- **(a) Lead each flag's `--help` with the per-command / per-source applicability.** The `sync start` variants already say "FAST cold-start (ADR-0079, PG source) only … Inert on MySQL/VStream sources" — keep that, but **front-load** it as the first clause so it is the first thing a `--help` reader sees, before the mechanism prose. Same for the `backup`/`restore` read-vs-write-side framing.
- **(b) Emit a one-time runtime WARN when an inert flag is explicitly set.** On `sync start` against a MySQL/VStream source, if the operator explicitly set `--bulk-parallelism`, `--table-parallelism`, or `--bulk-parallel-min-rows` (i.e. the value is non-default *because the operator passed it*, distinguished by kong's set-tracking, not by the resolved value), log a WARN: "`--bulk-parallelism` has no effect for a MySQL/VStream source on `sync start` (it governs the ADR-0079 PG-source fast cold-start only); use `--copy-fanout-degree` to tune VStream cold-copy concurrency." This converts a silent no-op into a loud one — the tenet posture — without changing any behavior.
- **(c) Document the canonical mental model** in `docs/` (the flag-reference page): one table mapping {command} × {flag} → {axis, applicability}, so the overlap is explained in one place rather than inferred from four help strings.

Per-command-distinct aliases (e.g. `--cold-copy-table-jobs` on `sync start`) are **considered and not recommended** in this stage: adding a second spelling for a flag that is *inert anyway on the common source* would add surface without fixing the real issue, which is the silent no-op. The WARN (b) is the targeted fix.

### Finding 2 — `apply-retry-*` ↔ `retry-*`: unify via additive aliases

Pick **`--apply-retry-*` as canonical on `sync start`** and **`--retry-*` as canonical on `backup full`** — i.e. leave both commands' *current* primary spellings as-is — and cross-add each as an alias on the other so an operator's muscle memory works on either command:

- `backup full`: add `aliases:"apply-retry-attempts"` to `RetryAttempts` (and the `-base`/`-cap` pair), so `--apply-retry-attempts` works there too.
- `sync start`: add `aliases:"retry-attempts"` to `ApplyRetryAttempts` (and pair), so `--retry-attempts` works there too.

Both spellings then work on both commands; the primary (shown in `--help`) stays each command's existing one, so no existing script breaks and no deprecation warning is needed (neither spelling is being retired — they are being made mutually accepted). Defaults are already identical (8 / 100ms / 30s), so the alias is purely a name-acceptance change. This is the minimal, fully back-compatible unification.

### Finding 3 — `--cutover-sequence-margin`: canonical `--sequence-margin`, keep the old as a deprecated alias

- Change `cutover`'s `SequenceMargin` to `name:"sequence-margin"` (the clean canonical) **with** `aliases:"cutover-sequence-margin"` so every existing command line and script keeps working unchanged.
- Emit the `--forward-schema-add-column`-style one-time deprecation WARN **only when the operator passed the `--cutover-sequence-margin` alias specifically** (detected via kong's per-flag set-tracking of which name was used, not merely a non-default value): "`--cutover-sequence-margin` is deprecated; use `--sequence-margin`. The old name still works and will be removed in a future release." This mirrors the established ADR-0091 deprecation posture exactly.

### Finding 4 — promote the DSN read-axis params to first-class CLI flags (additive)

Add two CLI flags on `sync start` that mirror the existing DSN params, **without** removing the DSN form:

- `--vstream-copy-table-parallelism` → the VStream read axis (today `vstream_copy_table_parallelism` DSN-only).
- `--copy-table-parallelism` → the native-MySQL read axis (today `copy_table_parallelism` DSN-only).

Precedence is **explicit CLI flag wins; absent CLI flag falls back to the DSN param; absent both falls back to the engine default** (1 = serial, matching `vstreamCopyTableParallelismFromDSN` / `nativeCopyTableParallelismFromDSN` today). The DSN form is retained verbatim (existing setups keep working), but both VStream concurrency axes become CLI-discoverable and sit beside their write-axis sibling `--copy-fanout-degree`. The help text names which source class each engages on and that the DSN form remains accepted, so the split-surface problem is closed additively.

## Back-compatibility plan

Every change in this ADR is back-compatible by construction:

- **Findings 2 and 4 break nothing** — they only *add* accepted spellings (aliases) / *add* a flag that defers to the existing DSN param. No existing command line changes behavior.
- **Finding 3** keeps the old spelling working as an alias; only the `--help` canonical name and a soft deprecation WARN change. The WARN follows the `--forward-schema-add-column` template (`internal/pipeline/schema_forward_engage.go` ~75): accepted-and-honored, one-time WARN, "removed in a future release," no behavior change in this release.
- **Finding 1** changes only help-text ordering and adds a WARN on an *already*-inert flag — no flag is renamed or removed; the no-op simply becomes loud.
- **Deprecation lifecycle:** any name slated for eventual removal (only finding 3's alias is, and only eventually) goes Proposed → accepted-with-WARN for ≥ N releases → removal as a separate, separately-announced change. Nothing here removes a name now.

## Zero-value-safety note

The v0.99.51 trap (a `bool` that must default *on* needing opt-out semantics) **does not apply to any change here**: none of the four findings introduces a defaulting `bool`. The new flags in finding 4 are `int` (degree, 0 = auto/serial-default, matching the existing DSN-param resolvers' zero handling); the aliases in findings 2/3 attach to existing `int` / `time.Duration` / `int64` fields whose defaults are unchanged; finding 1 adds no field at all (help text + a WARN gated on kong's "was this flag explicitly set" tracking, which is the correct signal — *not* a value comparison, so a default-equal explicit value is still detected). The "is it set" detection for the WARNs must read kong's per-flag set state, never infer intent from the resolved value, precisely so the zero-value of an unset flag is never mistaken for an operator choice.

## Consequences

**Positive.** The CLI reads coherently: identical concepts share spellings (or accept each other's), the one command-name-prefixed flag is normalised, both VStream concurrency axes are CLI-discoverable, and the one silent no-op (parallelism flags on a VStream `sync start`) becomes a loud WARN — the loud-failure tenet applied to a UX hazard. All of it lands without breaking a single existing command line or script.

**Negative / trade-offs.** Aliases grow the per-flag surface in the model slightly (two accepted spellings for some flags) — accepted, because the cost is borne by the tool, not the operator, and `--help` still shows one canonical name each. Finding 1 is deliberately *not* fully resolved by renaming (the names stay overlapping); the bet is that a clear WARN + a documented mental model beats a proliferation of per-command aliases for the same concept. If field evidence later shows operators still mis-tune despite the WARN, a follow-up ADR can revisit per-command-distinct canonical names — but that is a bigger break and should be demand-gated, not done speculatively.

## Alternatives considered

- **Rename the overlapping parallelism flags per command (finding 1).** Rejected: even aliased, it multiplies the names an operator must learn and breaks nothing the WARN+docs approach leaves unsolved. The real defect is the silent no-op, which the WARN fixes directly.
- **Hard-deprecate one spelling of the retry flags (finding 2) and force migration.** Rejected: both spellings are shipped and documented; mutual aliasing costs nothing and avoids a forced script edit. There is no canonical-name win large enough to justify a deprecation cycle here.
- **Make the DSN read-axis params CLI-only and drop the DSN form (finding 4).** Rejected: existing setups put it in the DSN; removing that form is a break for no benefit. CLI flag + DSN fallback gives discoverability without cost.
- **Leave everything as-is and only fix the docs.** Rejected for finding 1's silent no-op specifically — a flag that is accepted and does nothing on the common source is a trust hazard a doc note alone doesn't close; the runtime WARN is the tenet-aligned minimum.

## Testing

This is a design proposal; the testing plan below is what an implementing change would carry.

- **Alias acceptance (findings 2, 3, 4):** unit tests parse a command line using each alias and assert it binds the same field as the canonical name (e.g. `--apply-retry-attempts` and `--retry-attempts` both set `RetryAttempts` on `backup full`; `--cutover-sequence-margin` and `--sequence-margin` both set `SequenceMargin`). kong's alias matching (`context.go` ~748) is the mechanism; the test pins that we wired the tag, not kong itself.
- **Deprecation WARN (finding 3) and inert-flag WARN (finding 1):** assert the WARN fires **only** when the deprecated/inert flag name was *explicitly passed* (kong set-tracking), and does **not** fire when the field holds its default via non-specification — the zero-value-safety property. A table test over {passed-default-value, passed-non-default, not-passed} guards against inferring intent from the value.
- **Precedence (finding 4):** unit test the resolver order CLI-flag > DSN-param > engine-default for both `--vstream-copy-table-parallelism` and `--copy-table-parallelism`, including the "CLI flag absent, DSN param present" fallback and "both absent → serial" default.
- **No-behavior-change pins:** a golden test that a representative real command line for each touched command produces an identical resolved config before and after (aliases and help-text changes must not move any default or resolved value).
- **Docs (finding 1c):** the flag-reference table is verified by the existing `docs-drift-detector` sweep so the documented mental model can't silently drift from the flag surface.
