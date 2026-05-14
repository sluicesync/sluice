# Prep — PII redaction Phase 1 (roadmap item 15a)

Implementation plan for the first phase of `--redact TABLE.COLUMN=STRATEGY[:options]`. Targets v0.55.0 or later; this doc is the design pass before code lands.

Reference: GitHub issue #24, roadmap item 15 (full motivation + comparable-products analysis), and ADR-0032's optional-engine-surface pattern (which Phase 1 mirrors for the per-engine value-shaping path).

## Goals (Phase 1 scope)

Four strategies covering ~70% of operator asks per the issue:

- **`null`** — replace the value with NULL on the target. Column must be NULLABLE on the target schema; refuse loudly otherwise.
- **`static:<value>`** — replace with a literal constant. Type-coerced to the target column's IR type via the existing prepareValue path.
- **`hash:<algo>`** — one-way hash. v1 supports `sha256` and `hmac-sha256` (latter requires a keyset, see `--redact-key-source`). Hex-encoded output; works for string and []byte columns.
- **`truncate:<n>`** — keep first N characters, drop the rest. String columns only.

Out of scope (deferred):
- `mask:<pattern>` (Phase 2 — format-preserving)
- `tokenize:<format>` (Phase 2 — deterministic surrogate)
- `randomize:<shape>` (Phase 2 — random same-shape value)
- `jsonpath` (Phase 3 — in-JSON walker)
- Cross-stream keyset persistence (Phase 4 — multi-destination determinism)

## Where redaction sits in the pipeline

```
                    SOURCE
                       |
                   Read row
                       |
                       v
            +-----------------------+
            |  IR row (typed values)|
            +-----------------------+
                       |
            +-----------------------+
            |   REDACTION LAYER     |  <-- new in Phase 1
            +-----------------------+
                       |
            +-----------------------+
            |  prepareValue (per    |
            |  target engine)       |
            +-----------------------+
                       |
                       v
                   Write row
                       |
                    TARGET
```

Single typed function composed BEFORE the existing per-engine `prepareValue` step. Adds no new I/O.

The same layer handles all four codepaths that emit row values:

1. **Bulk-copy** (`Migrator` simple-mode, `pipeline.bulkCopyTable`): wraps the per-table row channel.
2. **CDC apply** (per-change, `Apply`): wraps each `ir.Change`'s row before dispatch.
3. **CDC apply** (batched, `ApplyBatch`): same wrap, just batched.
4. **Backup-stream** (`backup full` + `backup stream run`): wraps rows on the write path so backups are PII-clean too.

The wrap point is conceptually `for each (column, value) in row { value = redactor.Redact(col, value) }`. When no redactions match the column, the redactor returns the value verbatim — zero-cost passthrough.

## IR-side API

New package `internal/redact`:

```go
package redact

// Strategy is a per-column redaction policy. Implementations are
// stateless (or carry their own state — see hmac-sha256 keyset).
type Strategy interface {
    // Name returns a stable identifier for the strategy ("null",
    // "static:foo", "hash:sha256", "truncate:8"). Used by schema
    // preview and the audit log.
    Name() string

    // Redact returns the redacted value for the given input. col
    // describes the target column's IR type so the strategy can
    // coerce (e.g., truncate:8 returns first 8 chars of a string).
    Redact(col ir.Column, val ir.Value) (ir.Value, error)
}

// Registry maps "schema.table.column" → Strategy. nil/empty Registry
// is a no-op (every Redact returns the input verbatim).
//
// Lookup is O(1) via a flat map keyed by the lowercase
// "schema.table.column" string. Schema and table comparisons are
// case-folded to match operator declarations (`users.email` matches
// `Users.Email` on case-insensitive engines like MySQL default).
//
// PG case-sensitive: the operator can quote the identifier as
// declared (`"Schema"."Users"."Email"`) and the registry preserves
// case for that key only. Phase 1: simple lowercase; document the
// limitation for case-sensitive PG schemas.
type Registry struct {
    rules map[string]Strategy
}

// New creates an empty Registry. Add rules via Set.
func New() *Registry { ... }

// Set registers a Strategy for "schema.table.column". Last-write-
// wins; operator-supplied duplicates produce a single WARN at
// startup but the last-declared strategy applies.
func (r *Registry) Set(schema, table, column string, s Strategy) { ... }

// Get returns the Strategy for the column or nil if no rule.
func (r *Registry) Get(schema, table, column string) Strategy { ... }

// Empty returns true when no rules are registered (zero-cost
// passthrough flag — readers/writers skip the redaction wrap when
// Empty is true).
func (r *Registry) Empty() bool { ... }
```

## Strategy implementations

`internal/redact/strategies.go`:

```go
// Null returns NULL regardless of input. Refuses at parse time if
// the target column is NOT NULL; the refusal text names the
// column and suggests --redact-allow-not-null-override-via-static.
type Null struct{}

func (Null) Name() string { return "null" }
func (Null) Redact(col ir.Column, _ ir.Value) (ir.Value, error) {
    if !col.Nullable {
        return nil, fmt.Errorf("redact: column %s.%s is NOT NULL; refusing to redact via 'null' (use static:<empty-equivalent> instead)", col.Schema, col.Name)
    }
    return nil, nil
}

// Static returns a constant value coerced to the column's IR type.
// The coercion table mirrors prepareValue's: string → ir.Text,
// number-string → ir.Integer when col.Type is Integer, etc.
type Static struct {
    Value string  // operator-supplied literal (always parsed as string at CLI/YAML layer)
}

func (s Static) Name() string                                { return "static:" + s.Value }
func (s Static) Redact(col ir.Column, _ ir.Value) (ir.Value, error) { ... }

// Hash applies a one-way hash to string/[]byte values. SHA-256 is
// deterministic and stateless; HMAC-SHA256 requires a keyset from
// --redact-key-source (default: derived from --stream-id + a static
// salt for Phase 1; Phase 4 lands proper key management).
type Hash struct {
    Algo string  // "sha256" | "hmac-sha256"
    Key  []byte  // empty for sha256; non-empty for hmac-sha256
}

func (h Hash) Name() string                                  { return "hash:" + h.Algo }
func (h Hash) Redact(col ir.Column, val ir.Value) (ir.Value, error) { ... }

// Truncate keeps the first N runes (not bytes) of a string. Returns
// the input verbatim if shorter than N. Refuses non-string columns
// at parse time.
type Truncate struct {
    N int
}

func (t Truncate) Name() string                                  { return fmt.Sprintf("truncate:%d", t.N) }
func (t Truncate) Redact(col ir.Column, val ir.Value) (ir.Value, error) { ... }
```

## CLI surface

`cmd/sluice/cli.go` extensions (added to `MigrateCmd`, `SyncStartCmd`, `SchemaPreviewCmd`, `SchemaDiffCmd`, and the backup-stream commands):

```
--redact TABLE.COLUMN=STRATEGY[:options]
    Redact the named column. Repeatable. Strategy and options:
      null                       — replace with NULL (column must be NULLABLE)
      static:<value>             — replace with literal value
      hash:sha256                — SHA-256 hex (stateless, deterministic per input)
      hash:hmac-sha256           — HMAC-SHA256 hex (requires --redact-key-source)
      truncate:<n>               — keep first N runes (strings only)
    Examples:
      --redact users.email=hash:sha256
      --redact users.phone=truncate:4
      --redact accounts.ssn=static:'REDACTED'
      --redact billing.credit_card=null

--redact-key-source SOURCE
    Source for the HMAC keyset. Phase 1 supports:
      env:VARNAME      — read key from env var
      file:PATH        — read key from file (one line, trimmed)
      derive:<salt>    — derive from --stream-id + supplied salt (Phase 1 default)
    Default: derive:sluice-redact-v1

--require-redactions
    Refuse to start if no --redact rules declared (safety-conscious
    operators who want loud failure on misconfiguration).
```

YAML config parity at `~/.config/sluice/config.yaml`:

```yaml
redactions:
  - table: users.email
    strategy: hash
    algo: sha256
  - table: users.phone
    strategy: truncate
    length: 4
  - table: accounts.ssn
    strategy: static
    value: "REDACTED"
  - table: billing.credit_card
    strategy: null  # YAML's null sentinel works as "null" strategy
redact_key_source: derive:sluice-redact-v1
require_redactions: true
```

CLI flag override semantics match other repeatable flags: CLI rules append to YAML rules (no replacement). Duplicates on the same column emit one WARN and last-wins.

## Schema preview annotation

`sluice schema preview` and `sluice schema diff` get a new column annotation: every redacted column's emit line is suffixed with `-- REDACTED via <strategy-name>`. Mirrors the existing generated-column annotation pattern.

Example:

```sql
CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,    -- REDACTED via hash:sha256
    phone VARCHAR(20) NULL,         -- REDACTED via truncate:4
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);
```

Implementation: the schema-print path inspects the Migrator's `Redactor` (when set) and prepends the annotation. Same hook as the existing comment-emit on generated columns.

## Pipeline integration

`internal/pipeline.Migrator` gains:

```go
type Migrator struct {
    // ... existing fields ...

    // Redactor is the operator-supplied redaction registry. nil or
    // empty means no redaction (passthrough). When non-nil and
    // non-empty, every row's values are passed through Redactor.Get
    // → Strategy.Redact before reaching the per-engine prepareValue.
    Redactor *redact.Registry
}
```

`internal/pipeline.Streamer` gains the same field, threaded the same way.

The wrap point lives in a new helper `pipeline.redactRow(redactor, schema, table, row) (ir.Row, error)` that's called by:

- `bulkCopyTable` after row read, before chanCopy.
- `applyOne` after IR conversion, before `dispatch`.
- `applyOneBatch` per change in the batch loop.
- `backup_stream`'s row-emit path.

The helper is a no-op when `redactor == nil || redactor.Empty()`. The cost when active is one map lookup per (table, column) pair, then the Strategy's Redact call.

## CLI / YAML parsing

`cmd/sluice/redact_flag.go` (new): parses the `--redact TABLE.COLUMN=STRATEGY[:options]` arg into a `redact.Strategy` via a strategy registry that maps strategy names to factories.

```go
type strategyFactory func(opts string) (redact.Strategy, error)

var strategyFactories = map[string]strategyFactory{
    "null":     func(opts string) (redact.Strategy, error) { ... },
    "static":   func(opts string) (redact.Strategy, error) { ... },
    "hash":     func(opts string) (redact.Strategy, error) { ... },
    "truncate": func(opts string) (redact.Strategy, error) { ... },
}
```

Adding new strategies in Phase 2+ is one-line registration; the IR/pipeline surfaces don't change.

## Refusal modes

Loud refusals at startup (before any data moves):

1. **Strategy parse failure** — unknown strategy name, malformed options. Names the offending `--redact` value.
2. **null strategy on NOT NULL column** — names the column; suggests `static:<value>` alternative.
3. **truncate strategy on non-string column** — names the column + its IR type.
4. **hmac-sha256 without --redact-key-source** — names the column requiring a key.
5. **--require-redactions with no rules declared** — names the flag; suggests removing the flag if redactions aren't needed.

All refusals carry actionable hints (consistent with the rest of sluice's CLI UX per ADR-0030's "operator-actionable refusal" convention).

## Audit logging

Single INFO line at stream / migrate start:

```
sluice: redaction configured columns=12 strategies=[hash:sha256, truncate:4, static:..., null]
```

Lists distinct strategy names + column count. Doesn't log per-column rules (could be sensitive on its own — `--redact billing.credit_card=truncate:4` reveals which columns hold cards).

Per-row redaction events are NOT logged. (DEBUG-level audit hook is an opt-in extension for Phase 4+ if compliance audits demand it.)

## Backup-chain integration

Backups inherit the redactions when the same `--redact` rules are passed to `backup full` / `backup stream run`. The redaction layer sits at the same point in the backup path's IR flow (before chunk encoding), so backups stored on disk are PII-clean.

Restore-from-backup: redactions ARE NOT re-applied — the backup already redacted the source rows; the restored data is whatever the backup contains. The operator's responsibility to ensure the original backup was created with the right redactions.

## Cross-engine + redaction

Same-column redactions work whether the target is same-engine or cross-engine. The redaction layer operates on IR values, which are engine-neutral; composes cleanly with the existing cross-engine translation. PG `users.email` → MySQL `users.email` with `--redact users.email=hash:sha256` produces the same hash on both targets (deterministic).

## Test plan

`internal/redact/redact_test.go`:

- Strategy unit tests (one per strategy): null on NULLABLE / NOT NULL, static type-coercion, hash determinism, truncate rune-vs-byte correctness, truncate on non-string refusal.
- Registry tests: Set/Get round-trip, case-insensitive lookup, Empty propagation.

`cmd/sluice/redact_flag_test.go`:

- Parse `--redact TABLE.COL=STRAT[:OPTS]` for each strategy.
- Refusal paths for malformed input.
- YAML config integration (load `redactions:` block).

`internal/pipeline/redact_integration_test.go` (build-tag: `integration`):

- End-to-end MySQL→MySQL with `--redact users.email=hash:sha256`: source has plaintext emails, target has hex hashes, hashes match for identical inputs across runs.
- End-to-end PG→PG with `--redact users.phone=truncate:4`: target has 4-char phones.
- CDC apply: insert into source with PII column, verify CDC event lands redacted on target.
- Cross-engine MySQL→PG with redactions: verify same hash across both engines.

`internal/pipeline/schema_preview_redact_test.go`:

- `sluice schema preview` annotates redacted columns.
- `sluice schema diff` annotates redacted columns.

## Size estimate

| Component | LOC | Tests |
|---|---|---|
| `internal/redact` package (4 strategies + registry) | ~250 | ~250 |
| Pipeline `redactRow` helper + wiring | ~80 | (covered by integration) |
| `cmd/sluice/redact_flag.go` parser | ~150 | ~150 |
| Streamer/Migrator field + YAML config | ~50 | ~50 |
| Schema preview annotation | ~30 | ~30 |
| Integration tests | — | ~400 |
| **Total** | **~560** | **~880** |

Per the roadmap entry's estimate (~500-800 LOC + tests); within range.

## Open questions

1. **Case-folding policy for table/column names** in the registry. PG's case-sensitive identifiers vs MySQL's case-insensitive defaults — Phase 1 simplest is "lowercase all keys" with documented limitation for case-sensitive PG schemas using mixed-case identifiers.

2. **Hash output encoding** — hex (current plan) vs base64 vs binary. Hex is most readable + width-stable (SHA-256 → 64 hex chars). Base64 is shorter but variable-width. Sticking with hex for Phase 1.

3. **Verify-mode interaction** — `sluice verify --depth=sample` computes row hashes; redacted columns would naturally differ. Plan: `verify` skips redacted columns from the hash automatically (uses the same Redactor reference). Documented in the verify flag's help text.

4. **Backup-chain incompatibility** — a backup created without redactions then restored to a target with redactions configured: the redactions apply at restore-time (since restore goes through the same IR pipeline). This means backups are NOT redaction-aware historically; operators who want PII-clean backups need to re-create them with `--redact` rules. Phase 1 documents this; Phase 4+ explores "lazy" redaction stored alongside the chain.

5. **Determinism across restarts** — for `hash:sha256` (stateless), automatic. For `hmac-sha256`, the keyset must be stable across stream restarts. Phase 1 derives from `--stream-id + static-salt` so restarts of the same stream produce the same surrogate. Phase 4 lands the proper key-management story.

## Pre-implementation checklist

Before writing code:

- [ ] Confirm `internal/redact` package name acceptable (consider `internal/redaction` for clarity, or just `redact` for brevity — convention check vs other package names).
- [ ] Confirm `--redact TABLE.COLUMN=STRATEGY` flag syntax. Alternative: `--redact-column=TABLE.COL --redact-strategy=STRAT` (longer but less ambiguous on shell parsing). Sticking with single-flag for ergonomics.
- [ ] Decide on Phase 1's hmac-sha256 inclusion. Could defer to Phase 2 with format-preserving stuff; Phase 1 launches with just sha256 + null + static + truncate (3-strategy minimum). Recommendation: ship hmac-sha256 in Phase 1 since the only added complexity is the key-source flag.
- [ ] Wire schema preview annotation early — operators want to SEE what would be redacted before committing.
- [ ] Add ADR-0039 (or next available number) draft alongside the prep doc capturing the design decisions.

## Sequencing

Phase 1 ships standalone (no dependency on Phase 2-4). Targets ~v0.55.0 per the roadmap entry.

Suggested implementation order:
1. `internal/redact` package + 4 strategies + registry (LOC budget: ~250 + 250 tests)
2. `--redact` flag parser (LOC budget: ~150 + 150 tests)
3. YAML config integration (LOC budget: ~30)
4. Pipeline `redactRow` helper + Migrator/Streamer fields (LOC budget: ~80)
5. Schema preview / diff annotation (LOC budget: ~30 + 30 tests)
6. Integration tests (LOC budget: ~400 — same-engine + cross-engine + CDC)
7. Operator docs: new `docs/pii-redaction.md`
8. ADR-0039 (or whatever number is next) capturing the design

Could ship in two PRs if the chunk feels too big: PR 1 = redact package + strategies + tests + docs (no integration with pipeline). PR 2 = pipeline integration + flag wiring + integration tests + schema preview annotation. Operator-facing surface lands in PR 2.
