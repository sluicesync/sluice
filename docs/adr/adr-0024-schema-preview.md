# ADR-0024: `sluice schema preview` — operator-facing translation inspection

## Status

Accepted. Implemented in v0.6.0 (initial subcommand) with subsequent extensions across v0.8.x (output format polish), v0.11.2 (translation-notes coverage), v0.26.0 (advisory hints registry), v0.32.0 (multi-source / `--target-schema` awareness), and v0.39.0 (translator-gap preflight scan — see `internal/translate/gaps.go` and the dedicated section in this document).

## Context

Cross-engine migrations apply translation policies that are *invisible to the operator until the migration runs* — and for some types, the default choice has visible consequences operators may want to override:

- **PG `uuid` → MySQL.** No native MySQL UUID type. Sluice's default emits `CHAR(36)` (human-readable, 36 bytes). Storage-optimal alternative is `BINARY(16)` via `--type-override users.id=binary_uuid` (16 bytes; ~2.25× compression on UUID-heavy tables). Operators today only learn this exists by reading `docs/type-mapping.md` end-to-end.
- **MySQL `ENUM` → PG.** Default emits `TEXT` plus a `CHECK (value IN ('a','b',...))` constraint. Alternative: a real PG `CREATE TYPE ... AS ENUM` + `column my_enum_type`, which gets index-friendly comparison + a clean schema. The CHECK form is conservative; the type form is closer to "what PG would do natively".
- **PG `text` → MySQL.** Default emits `LONGTEXT` (4GB cap, large per-row overhead). Often `MEDIUMTEXT` (16MB) or `VARCHAR(N)` is the right call when the operator knows the column's actual length distribution.
- **MySQL `JSON` → PG.** Default `JSONB` (binary, indexable, fast). Override `json` (text, preserves key order, slower) is rare but exists.
- **MySQL `DATETIME(6)` → PG.** Default `TIMESTAMP(6)` (no timezone). Override to `TIMESTAMPTZ` may be intended depending on whether the source treats values as UTC.

Operators encounter these surprises during real-world testing — the v0.4.0 testing report's catalog is partly a record of "the default landed and we didn't realise we'd want a different choice". The trial-and-error cycle (run migrate, see output, drop dest, override, re-run) is expensive against production-sized data.

The fix is operator visibility *before* the migration runs.

## Decision

Add a new top-level subcommand: **`sluice schema preview`**. Reads the source schema, runs it through the translation pipeline (mappings + cross-engine type policy), and emits:

1. **Target DDL.** The exact `CREATE TABLE` / `CREATE INDEX` / `ALTER TABLE ... ADD CONSTRAINT` statements that would run on the target. Format matches the engine's native dialect.

2. **Translation notes.** Per-column inline comments on cross-engine-translated columns: `-- source: uuid (16 bytes)  →  target: char(36) (36 bytes); 2.25× expansion`. Notes are only emitted on cross-engine pairs (same-engine prints DDL with no notes — there's nothing to compare).

3. **Advisory hints.** When sluice's default choice has a known operator-preferable alternative, emit a one-line hint with the exact `--type-override` invocation:

   ```
   -- hint: PG uuid columns expand 2.25× as CHAR(36). For binary storage,
   --       add `--type-override users.id=binary_uuid` (CLI) or
   --       `mappings: { users.id: binary_uuid }` (sluice.yaml).
   ```

   Hints come from a registry seeded with the highest-traffic surprises (UUID, ENUM, large-TEXT, JSON-vs-JSONB). Engines without a "common surprise" pattern stay quiet.

### CLI shape

```bash
sluice schema preview \
  --source-driver postgres --source 'postgres://...' \
  --target-driver mysql    --target 'mysql://...'
```

Optional flags:
- `--config sluice.yaml` — apply existing mappings before computing the preview, so the operator sees the effect of overrides they've already configured.
- `--include-table` / `--exclude-table` — filter the preview to specific tables (mirrors `migrate`).
- `--type-override TABLE.COLUMN=TYPE` — try out an override without committing it to YAML.
- `--format text|json` — `text` (default, human-readable DDL with inline comments); `json` for tooling (schema diff scripts, CI gates that flag bad translations).
- `--output FILE` (alias `-o`) — write to FILE instead of stdout. Convenient for piping into version control, attaching to PRs, or feeding downstream tools without shell redirection. Works with both formats; the file extension is operator-chosen and not validated against `--format` (no `.json` enforcement when `--format json` — the operator's choice).

### Output structure (text format)

```
-- sluice schema preview
-- source: postgres (5 tables, 23 columns)
-- target: mysql
-- mappings applied: 0
-- advisory hints: 2

-- ──────────── users ────────────
-- 4 columns; 1 cross-engine translation; 1 hint

CREATE TABLE `app`.`users` (
  `id`         CHAR(36)     NOT NULL,    -- source: uuid (16 bytes) → 36 bytes; 2.25× expansion
  `email`      VARCHAR(255) NOT NULL,
  `created_at` DATETIME(6)  NOT NULL,    -- source: timestamptz → DATETIME(6); offset/tz dropped (UTC assumed)
  `data`       JSON         NOT NULL,    -- source: jsonb (binary) → mysql JSON (also binary internally; semantics preserved)
  PRIMARY KEY (`id`)
) ENGINE=InnoDB CHARSET=utf8mb4;

-- hint: PG uuid columns expand 2.25× as CHAR(36). For binary storage:
--       --type-override users.id=binary_uuid

-- ──────────── orders ────────────
...
```

### Engine surface

The schema-write phase already produces DDL strings — it just executes them today. The preview path needs a "give me the DDL but don't execute" entry point:

```go
// In ir.SchemaWriter
type DDLPreviewer interface {
    PreviewDDL(ctx context.Context, s *Schema) ([]DDLStatement, error)
}

type DDLStatement struct {
    Table   string  // qualified name; for grouping in output
    Kind    string  // "CREATE TABLE", "CREATE INDEX", "ALTER TABLE"
    SQL     string  // the statement itself
}
```

Both engines implement it. The schema-write phase can optionally route through PreviewDDL for a dry-run mode (refactor opportunity: today's `--dry-run` on `migrate` doesn't show DDL).

### Translation-notes registry

A per-engine-pair `notesFor(sourceCol, targetCol)` returns 0 or 1 note string. Lives in `internal/translate/notes.go` (new). Stays small and tabular; growing it is cheap as new surprises are reported.

### Advisory-hints registry

Same shape: `hintsFor(sourceCol, targetCol)` returns 0 or 1 hint with the exact override string. Also in `internal/translate/notes.go`. Each entry is a struct: `{sourcePattern, targetPattern, message, suggestedOverride}`.

Initial v0.6.0 entries (high-traffic surprises from real-world testing reports):

| Source                           | Default target              | Hint                                                                                    |
| -------------------------------- | --------------------------- | --------------------------------------------------------------------------------------- |
| PG `uuid`                        | MySQL `CHAR(36)`            | "PG uuid expands 2.25× as CHAR(36); for binary storage, `--type-override TABLE.COL=binary_uuid`" |
| ~~MySQL `ENUM` → PG~~            | ~~`TEXT` + CHECK~~          | ~~Override to `pg_enum`~~ — **removed during implementation review**: sluice's PG writer already emits a real `CREATE TYPE … AS ENUM` by default; no `pg_enum` alias exists; the hint as drafted would have pointed operators at a non-existent override. |
| PG `text` (no length hint)       | MySQL `LONGTEXT`            | "PG text → MySQL LONGTEXT (4GB cap, large overhead); if column is bounded, `--type-override TABLE.COL=varchar:length=N` or `mediumtext`" |
| MySQL `JSON`                     | PG `JSONB`                  | (no hint — JSONB is the right default; emit a note that the operator can downgrade to `json` text if key-order preservation is required) |
| MySQL `DATETIME(6)` UTC-intended | PG `TIMESTAMP(6)` (no tz)   | "MySQL DATETIME has no timezone; if the source values are UTC-encoded, `--type-override TABLE.COL=timestamptz` is closer to the original semantics" |
| PG `numeric` (unbounded)         | MySQL `DECIMAL(65,30)` max  | "PG unbounded numeric → MySQL DECIMAL(65,30) (max precision); for narrower storage, `--type-override TABLE.COL=decimal:precision=N,scale=M`" |

The UUID hint is the headline example from the v0.6.0 testing discussion that motivated this ADR. The registry stays small (~6 entries) so each one carries weight; growing it is cheap when new operator pain points surface.

## Consequences

- **Operators see the target schema before any data moves.** The "trial migrate to find out the column type was wrong" loop disappears.
- **Discovery of `--type-override`.** Today the flag is documented but not advertised in-flow. The hints registry surfaces it at the moment it's actionable.
- **Tooling integration.** `--format json` lets CI scripts gate "no advisory hints in production schema preview" or diff previews across config branches.
- **Same-engine paths get a free DDL dump too.** Operators running PG→PG can use the preview to check what their mappings actually produce, even though no cross-engine translation is happening.

## v0.39.0 extension — translator-gap preflight scan

The advisory-hints registry covers cross-engine **type** surprises. v0.39.0 adds a parallel surface for **expression** surprises: a preflight scan over every `DefaultExpression`, `Column.GeneratedExpr`, and `CheckConstraint.Expr` whose dialect tag is `mysql`, run during `sluice schema preview` against MySQL → PG pairs. Patterns that match sluice's deliberately-deferred translator catalog rules surface in the output before any data moves.

### What the scan catches

Seven catalog rules sluice does NOT auto-rewrite (`internal/translate/gaps.go`):

| Pattern | Catalog rule | Severity | Why deferred |
|---|---|---|---|
| `GREATEST` / `LEAST` | #11 | silent | NULL semantics differ — PG ignores NULL args, MySQL propagates. Output divergence is silent (no parse error). |
| `REGEXP_LIKE` | #13 | silent (PG 15+) | POSIX (PG) vs ICU (MySQL) regex flavours. Patterns with lookaheads / named groups / Unicode-property classes diverge silently. |
| `FIND_IN_SET` | #21 | loud | No portable PG equivalent in CHECK / GENERATED contexts. |
| `CONVERT_TZ` | #23 | loud | No `CONVERT_TZ` in PG core. |
| `INET_ATON` / `INET_NTOA` | #29 | loud | No portable equivalent without a custom function. |
| `SHA1` / `SHA2` | #10 | loud | Requires pgcrypto. **Suppressed when `--enable-pg-extension pgcrypto` is set** since the v0.38.0 rewrite ships under that flag. |

- **`loud`** means PG rejects the DDL at parse time — the migration would fail visibly, but late. The preflight surfaces it cheaply.
- **`silent`** means PG accepts the DDL but evaluates the expression differently than MySQL. The preflight surfaces it before the divergence ships into row data.

Detection is case-insensitive, with word-boundary matching (`IS_GREATEST_HIT(...)` does not trigger a false positive). The scan returns `nil` for non-MySQL-to-PG engine pairs; same-engine paths see no behaviour change.

### Operator-facing output (text format)

When the scan finds ≥ 1 gap, the preview header gains a count line and a dedicated section before the per-table DDL:

```
-- sluice schema preview
-- source: mysql (5 tables, 23 columns)
-- target: postgres
-- mappings applied: 0
-- advisory hints: 2
-- translator gaps: 3 (see section below)

-- ──────────── Translator gaps (MySQL → Postgres) ────────────
-- These MySQL expression patterns are NOT auto-translated to PostgreSQL.
-- Address each one before migrating, or PG will either reject the DDL (loud)
-- or evaluate the expression differently than MySQL (silent).

[loud]   users.email_hash (GENERATED)
         expression: SHA2(email, 256)
         catalog rule #10 (SHA family)
         note: enable pgcrypto on the target and pass --enable-pg-extension pgcrypto;
               sluice will then rewrite to encode(digest(email, 'sha256'), 'hex').

[loud]   orders.ip_int (DEFAULT)
         expression: INET_ATON(client_ip)
         catalog rule #29
         note: no portable PG equivalent. Either redefine the column shape
               (store the text address as inet) or use --expr-override
               orders.ip_int='(your-PG-expression)' to supply the PG form.

[silent] settings.max_retries (CHECK)
         expression: max_retries >= GREATEST(1, default_retries)
         catalog rule #11
         note: PG's GREATEST ignores NULL args; MySQL's propagates NULL.
               If either column is nullable, results diverge silently. Either
               wrap with COALESCE or use --expr-override.

-- ──────────── users ────────────
...
```

### JSON shape

```json
{
  "translator_gaps": [
    {
      "table": "users",
      "column": "email_hash",
      "constraint": "",
      "field": "generated",
      "pattern": "SHA2",
      "rule": 10,
      "severity": "loud",
      "expression": "SHA2(email, 256)",
      "note": "enable pgcrypto on the target..."
    }
  ]
}
```

The `translator_gaps` field is **omitted entirely when no gaps are detected** (additive; existing parsers ignore unknown fields). CI gates can fail the migration plan on any `"severity": "loud"` entry:

```bash
jq '.translator_gaps | map(select(.severity == "loud")) | length == 0' preview.json
```

Returns `true` when no loud gaps are present.

### SQLite-target affinity advisories (Bug 162)

When the **target** is `sqlite`, preview also surfaces an affinity-notes
list: columns whose IR type maps to a SQLite storage affinity that
differs from the nominal type — most importantly `decimal → TEXT`
(byte-exact; SQLite's NUMERIC affinity would silently coerce a value like
`19.99` to a binary float), plus `json` / `uuid` / `enum` / `set` → TEXT
and `char` / `varchar` → TEXT. These are **advisory** (the migration
proceeds; the decimal → TEXT note is the value-fidelity one — TEXT
preserves the exact decimal). In `--format text` they print as advisory
lines; in `--format json` they appear under a `sqlite_affinity_notes`
array (omitted for non-SQLite targets and when no such column is present):

```json
{
  "sqlite_affinity_notes": [
    {
      "table": "orders",
      "column": "price",
      "source_type": "numeric(10,2)",
      "target_type": "TEXT",
      "note": "decimal stored as TEXT affinity to preserve the exact value (reads back as text)"
    }
  ]
}
```

See [type-mapping.md](../type-mapping.md#sqlite--cloudflare-d1--ir) and
[ADR-0134](adr-0134-sqlite-target-engine.md) §2 for the affinity map.

### Why a separate scan and not the advisory-hints registry

The advisory-hints registry is keyed on `(sourceCol, targetCol)` type pairs — the source-of-truth is the column's declared type. Expression-body gaps are keyed on **what the expression *contains***, not the column type. Encoding "this expression body uses `INET_ATON`" in the hints registry's type-pair structure would mean a per-expression-body re-evaluation pass that the registry isn't shaped for. Splitting into a separate `internal/translate/gaps.go` scanner keeps each surface focused.

### Why not auto-rewrite the deferred rules

Each deferred rule has a load-bearing reason (NULL semantics, regex flavour, missing PG equivalent, or extension dependency); auto-rewriting any of them silently would change application semantics in subtle ways. The deferral is the conservative choice; the preflight is how operators discover the deferral before the migrate runs.

## Why a new subcommand and not `migrate --dry-run --show-target-ddl`

`--dry-run` is currently shaped around "what would the orchestration do" (cold-start vs warm-resume, table count, position token). Adding DDL output to it bloats the existing flag's mental model.

A new subcommand is also independently valuable: an operator might want to inspect the schema preview without intent to migrate immediately — e.g., during type-override iteration, or as part of a CI gate against the source schema. Coupling it to `migrate` ties the inspection to the act of migration.

That said, `migrate --dry-run` *should* eventually print "for full target DDL, run `sluice schema preview`" so operators land on the new tool when they're already in the dry-run mindset. Trivial follow-up after the subcommand ships.

## Why not extend `sluice engines` output

`sluice engines` lists registered engines and their capability declarations — it's a runtime-discovery tool, not a schema tool. The two concerns are different.

## Why not just better defaults

Some defaults (UUID → CHAR(36)) are arguably wrong for storage but right for human-readability/debuggability. Different operators reasonably want different defaults for the same source type. The right answer is making the choice visible and easy to change, not picking a "better" default that's wrong for the other half of users.

(Where defaults *are* unambiguously wrong — Bug 14's PG array → MySQL JSON conversion shape — we fix the default. The hints registry covers the cases where there's no universally-right answer.)

## Verification

- Unit tests on the notes/hints registry: per-engine-pair lookups return the expected strings.
- Unit test on the JSON output format: shape stable across engine versions.
- Integration test: boot a PG container, run `schema preview` against a fixture schema with a UUID column, assert the CHAR(36) note + the binary_uuid hint fire.
- Integration test: boot MySQL, fixture with ENUM, assert the PG enum-type hint fires.
- Snapshot test on the text output (golden file) so format regressions surface.

## Out of scope (future work)

- **Auto-applying recommended overrides.** The preview surfaces hints; it does not modify YAML or apply overrides. Operators copy/paste the suggested invocation. Auto-apply is a different design — opinionated, requires interactive UX, easy to get wrong.
- **Schema-diff against an existing target.** This ADR's preview is "what would sluice produce on a fresh target". A separate `sluice schema diff --against-target ...` could compare against an existing target's `information_schema` and highlight drift; useful for re-migration safety. Worth a separate ADR.
- **Multi-source aggregation.** Some users have multiple source DBs going to one target. The preview could merge — also a separate design.
