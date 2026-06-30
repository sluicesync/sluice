# ADR-0144: Opt-in validated rich-type inference for SQLite/D1 sources (`--infer-types`)

- Status: Accepted
- Date: 2026-06-29
- Deciders: sluice maintainers
- Relates: ADR-0128/0129 (SQLite/D1 source engine + date/bool policy), ADR-0133/0134 (SQLite schema-feature carry / target), Bug 161 (`--type-override` decode for SQLite sources)

## Context

SQLite/D1 has dynamic storage classes, so sluice maps a SQLite source conservatively and losslessly: `INTEGER`→`bigint`, `TEXT`→`text`, etc. That is the correct **default** — it never fails on type and never loses data. (Real-user feedback, 2026-06-29: pscale's aggressive *name-only* heuristics caused a total-data-loss when a `TEXT id` held a non-UUID value like `cus_abc123`.) But users migrating clean, well-formed data often want native Postgres types (`boolean`, `timestamptz`, `jsonb`, `uuid`) and otherwise have to `ALTER` everything afterward.

## Decision

Add an **opt-in, data-validated** rich-type inference for **SQLite/D1 sources only**: `--infer-types` (off by default). It promotes a small set of conservatively-typed columns to richer Postgres types **only after exhaustively validating that every value conforms**, and falls back to the safe type otherwise. The combination — *safe by default, rich on request, validated before coercion, loud on anything ambiguous* — gives native-type ergonomics without the name-only data-loss risk.

### Why this is safe (two independent nets)
The inference does **not** add new value-conversion code. It computes **validated `--type-override` entries** and applies them through the existing override path (`translate.ApplyMappings` → the SQLite reader's Bug-161 override decode). That decode **refuses loudly** on any value it cannot faithfully interpret (`decodeBoolean`/`decodeTemporal`/the string-family carry in `value_decode.go`). So:
1. **Up-front exhaustive validation** rejects non-conforming columns before promotion.
2. **The decoder's loud-refuse** is a second net for *parse-validity* — a value the decoder cannot parse aborts the migrate loudly, never coerces.

**Important boundary (the v0.99.166 value-fidelity review):** the decoder's loud-refuse is a *parse-validity* net, NOT a *target-capacity* net — a value that parses fine but the resolved target type cannot faithfully hold would be silently coerced. So the up-front validation MUST also reject *target-capacity* mismatches the decoder can't catch. Two such cases exist in the temporal family and are refused at validation (column kept `text`): (a) a **mixed** column with some offset-bearing and some naive values — promoting to naive `timestamp` would parse the offset values and store them UTC-shifted (silent wall-clock change), and promoting to `timestamptz` would invent a zone for the naive ones; (b) a value with **sub-microsecond** fractional seconds (>6 digits) that `timestamp(6)`/`timestamptz(6)` would silently round. With these refusals, claim (2) holds: nothing reaches the decoder that it would silently mis-hold.

### Candidate selection (name-hint) + validation (data)
A column is a *candidate* only if its **name hints** the target type AND its current conservative type is the right source family; it is *promoted* only if **exhaustive data validation** passes:

| Target | Name hint (case-insensitive) | Source family | Exhaustive validation (one aggregate per column, pushed to the source) |
|---|---|---|---|
| `boolean` | `is_*`, `has_*`, `*_flag` | INTEGER | every non-NULL value ∈ {0,1}: `COUNT(*) WHERE col NOT IN (0,1) AND col IS NOT NULL` = 0 |
| `timestamptz`/`timestamp` | `*_at`, `*_time`, `created`, `updated` | TEXT | every non-NULL value matches an ISO-8601 `GLOB`; **timestamptz iff ALL carry an explicit offset/`Z`, all-naive ⇒ `timestamp`** (tz-aware, never invent a zone); a **MIXED** offset/naive column and a **sub-µs** (>6 frac-digit) column are REFUSED (kept text) — see the target-capacity boundary above |
| `jsonb` | `*_json`, `metadata`, `payload`, `settings`, `attributes` | TEXT | every non-NULL value `json_valid(col)=1` AND is an object/array (not a bare number/string — guards `'123'`/`'free'` false positives) |
| `uuid` | `*_id`, `*_uuid`, `uuid`, `guid` | TEXT | every non-NULL value matches the 8-4-4-4-12 hex UUID `GLOB` |

Name-hints narrow candidates so a plain `status` INTEGER that happens to be 0/1 in a tiny table is not promoted; the data-validation is the safety gate. The `cus_abc123` failure case is handled by construction: `*_id` selects it as a UUID candidate, the UUID `GLOB` finds a non-match, it stays `text`. No loss, ever.

Validation is **exhaustive, not sampled**: a one-shot migration can't tolerate a single stray non-conformer, and the validation is a cheap aggregate `COUNT` (no row transfer). For D1, the same queries run over the HTTP query API.

### Temporal encoding (ADR-0129)
The SQLite date encoding is source-level. A promoted ISO-temporal column decodes through the source's encoding; the ISO `GLOB` validation guarantees the values are ISO-8601 text, and the decoder loud-refuses on any encoding mismatch (the safety net). The implementation aligns the promoted temporal columns' decode with ISO parsing so the common (default-encoding) case is seamless.

### jsonb normalization (called out in the report)
`text`→`jsonb` is the queryable/indexable type users want, but jsonb normalizes the document (whitespace, key order). The promotion report states this explicitly so the operator knows the stored bytes differ from the source text while the JSON value is equal. (A `json` byte-preserving alternative was considered and rejected as rarely the intent of "rich types.")

### Reporting
A loud, structured report of every promotion (`col text→timestamptz, N rows validated`) and every considered-but-kept-safe column (`col: *_id hint but 3 non-UUID values → kept text`). The operator opted in; they are told exactly what happened.

## Scope / consequences

- **SQLite/D1 source only.** MySQL/PG sources already carry rich static types — inference there is moot and only adds risk; `--infer-types` against a non-SQLite/D1 source is a loud no-op/refusal.
- **Opt-in.** Default behavior (no flag) is byte-for-byte unchanged conservative mapping.
- Composes with `--type-override`: an explicit operator override always wins over an inferred one (the operator is authoritative).
- Pinned per **type family** (the Bug-74 discipline): boolean, timestamptz, timestamp(naive), jsonb, uuid — each with conforming (promoted) AND non-conforming (kept-safe + reported) cases, validated on a real target.

## Alternatives considered

- **Name-only inference (pscale-style):** rejected — it caused the documented total-data-loss; data-validation is the whole point.
- **Sampled validation:** rejected — a one-shot migration needs exhaustive correctness, and the aggregate is cheap.
- **Default-on:** rejected — conservative-and-lossless must stay the default; rich is opt-in.
- **All source engines:** rejected — only dynamically-typed SQLite/D1 lacks the type info; elsewhere it's risk with no benefit.
