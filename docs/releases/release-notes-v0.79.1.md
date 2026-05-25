# sluice v0.79.1 — Bug 90 hotfix: refuse loudly on ADD COLUMN with computed DEFAULT

**Headline:** Patch release closing Bug 90 — a severity-a silent-divergence class on v0.79.0's marquee F12+F16 ADD COLUMN forwarding path. v0.79.0 documented in ADR-0058 §2a that volatile / stateful DEFAULT expressions (`NOW()`, `CURRENT_TIMESTAMP`, `nextval(...)`, `random()`, `gen_random_uuid()`, MySQL `UUID()` / `RAND()` / `LAST_INSERT_ID()`, session-state functions) MUST refuse loudly — but the guard was dead code in production. v0.79.1 enforces the contract.

## Fixed

- **`fix(pipeline): Bug 90 — refuse loudly on ADD COLUMN with computed DEFAULT (ADR-0058 §2a) (#54)`**

  ### What was broken

  v0.79.0's intercept (`internal/pipeline/schema_forward_intercept.go`) gated the refuse-loudly check on `ir.Column.Default` being a `DefaultExpression`. But the CDC reader's projection (`internal/engines/postgres/cdc_relations.go:projectRelation` and `internal/engines/mysql/cdc_reader.go:maybeSnapshotSchemaB1`) drops the DEFAULT field entirely — **pgoutput's RelationMessage carries no `attdefault` slot, and MySQL's TableMapEvent carries no `COLUMN_DEFAULT`**. Every production SchemaSnapshot reached the intercept with `Default == nil`, making the §2a guard dead code.

  Operator-visible symptom: turning on `--forward-schema-add-column` for a routine `created_at TIMESTAMPTZ DEFAULT NOW()` ALTER produced a happy-path log line, the target ALTER landed, and the source's pre-existing rows had per-row materialized timestamps (from the source's table rewrite at ALTER time) while the target's pre-existing rows were NULL (or a single target-session-evaluated value). Silent source/target divergence.

  ### The fix (two-locus)

  1. **`internal/pipeline/schema_forward_volatility.go`** (new, ~340 LOC) — text-based volatility classifier. Explicit deny-list:
     - Time-volatile: `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_TIME`, `CURRENT_DATE`, `LOCALTIME`, `LOCALTIMESTAMP`, `TRANSACTION_TIMESTAMP()`, `STATEMENT_TIMESTAMP()`, `CLOCK_TIMESTAMP()`, MySQL `UTC_TIMESTAMP()`, `UNIX_TIMESTAMP()`, `SYSDATE()`
     - Sequence-stateful: `nextval(...)`, `currval(...)`, `setval(...)`, MySQL `LAST_INSERT_ID()`
     - Random / non-deterministic: `random()`, `gen_random_uuid()`, `uuid_generate_v4()`, MySQL `RAND()`, `UUID()`, `UUID_SHORT()`
     - Session-state: `current_user`, `current_schema`, `current_database`, `inet_client_addr()`, etc.

     Small deterministic allow-list: `ABS`, `COALESCE`, `CAST`, `LOWER`, `UPPER`, etc. Anything else triggers **refuse-on-uncertainty** — better to over-refuse than silently corrupt.

  2. **Intercept integration** (`schema_forward_intercept.go` + `schema_forward_engage.go` + `streamer.go`) — `schemaForwardDeps` gains a `defaultProberFunc`. When the in-band `ir.Column.Default` is nil (the production hot-path), the intercept invokes the prober to fetch the source's authoritative DEFAULT text via `SchemaReader.ReadSchema()`, then classifies. Probe error → refuse-on-uncertainty.

  ### Refuse-message shape

  ```
  pipeline: apply changes: pipeline: forward schema add-column: ADD COLUMN
  "created_at" on "public.widgets" has a computed DEFAULT expression "now()"
  which references volatile/stateful identifier "now" — target-session
  evaluation diverges from source per-row insert (ADR-0058 §2a). recovery:
  drained model — run 'sluice sync stop --wait', then run schema migrate
  (manual or 'sluice schema migrate') against "public.widgets", then resume
  via 'sluice sync start --resume'. Drop --forward-schema-add-column to keep
  the drained model as the default for any subsequent source DDL.
  ```

  Names the column, table, expression text, the specific volatile identifier within it, cites ADR-0058 §2a, and offers two recovery paths.

## Tests

- **`test(pipeline): schema_forward_volatility_test.go`** (new) — 53-cell class matrix exercising the volatility deny-list across PG and MySQL function names; 8-cell `TestClassifyDefaultValueVolatility_IRTypes` covering literal IR DefaultValue cases (proves the safe pass-through). Per the Bug 74 "pin the class, not the representative" discipline.
- **`test(pipeline): schema_forward_intercept_test.go` additions** — three new tests:
  - `TestForwardAddColumn_ProbedVolatileDefault_Refuse` (6-cell class pin: NOW / CURRENT_TIMESTAMP / nextval / random / gen_random_uuid / UUID)
  - `TestForwardAddColumn_ProbedLiteralDefault_Forwards` (5-cell safe-pass control: prevents over-refusal on literals)
  - `TestForwardAddColumn_ProberError_Refuse` (refuse-on-uncertainty when source probe fails)
- **Integration**: `TestAddColumnForward_PG_RefusesComputedDefault` (NOW + nextval end-to-end) + `TestAddColumnForward_MySQL_RefusesComputedDefault` (CURRENT_TIMESTAMP + UUID end-to-end).
- **All 11 pre-existing ADR-0058 forwarding tests remain green** — literal DEFAULTs (`DEFAULT 'pending'`, `DEFAULT 0`, `DEFAULT FALSE`, `DEFAULT NULL`) continue to forward; no over-correction.

## Docs

- **ADR-0058 amendment** — Bug 90 closure subsection appended under §2a documenting:
  - The structural cause (CDC reader projection drops DEFAULT, making v0.79.0's guard dead code)
  - The volatility-classifier surface (deny-list + allow-list + refuse-on-uncertainty)
  - The source-prober design + the rationale for re-using `SchemaReader.ReadSchema()` rather than introducing a new per-column probe interface (rare event; the wasteful-at-scale concern is acknowledged for future refinement)
  - The per-engine integration pin matrix

## Compatibility

- **Drop-in upgrade from v0.79.0.** Pure bugfix; no flag surface change. Default behavior (without `--forward-schema-add-column`) unchanged.
- **Severity a.** Closes a silent-divergence class on the marquee F12+F16 feature shipped in v0.79.0. Operators who opted into forwarding got happy-path log lines while their pre-existing target rows silently diverged from source — exactly the silent-loss class the project's tenets exist to prevent.

## Who needs this

- **Any operator on v0.79.0 running `--forward-schema-add-column`** against any source where ADD COLUMN with a computed DEFAULT is plausible (e.g. `created_at`, `updated_at` audit columns with `DEFAULT NOW()`; UUID PKs with `DEFAULT gen_random_uuid()`; sequence-keyed columns; randomized tokens). v0.79.0 silently diverged target from source; v0.79.1 refuses loudly with the drained-model recovery hint.
- **Operators not using `--forward-schema-add-column`** see no observable change.

## The Bug 74 lesson value, again

This is the **fifth** silent-loss-class hotfix in this overnight session arc — Bug 86 → 87 → 88 → 89 → **90**, each discovered by a family-matrix test doing exactly what the Bug 74 "pin the class, not the representative" discipline asks. The v0.79.0 cycle subagent intentionally tested both `NOW()` AND `nextval()` variants because the failure dispatches on the *volatility class* of the DEFAULT expression, not on a specific function. The class pin caught a contract gap the production code documented but didn't enforce.

The cumulative cost: each hotfix release is ~1-2 hours of subagent work + a CI roundtrip + a regression cycle. The win: every release this session caught the *next* silent-loss class before it reached operators. The matrix discipline is paying for itself release after release.

## Cross-references

- [v0.79.0 release notes](https://github.com/orware/sluice/releases/tag/v0.79.0) — the marquee F12+F16 release this hotfix completes
- [ADR-0058 — Online schema-change forwarding](https://github.com/orware/sluice/blob/main/docs/adr/adr-0058-online-schema-change-forwarding.md) (Bug 90 closure subsection appended)
- Bug 74 lesson: see `CLAUDE.md` § *Pin the class, not the representative*
- Three-phase protocol: `CLAUDE.md` § *Debugging non-obvious failures* (Phase A was non-negotiable here — the v0.79.0 guard's text suggested it was working; Phase A logs proved it was dead code)
