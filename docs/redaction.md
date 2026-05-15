# PII redaction in sluice

Operator guide to the `--redact` surface — 27 strategies across 5
phases (Phase 1 / 1.5 / 2.a / 2.b / 2.c / 3). Covers every
strategy operators can configure today, the determinism contracts
that govern their output, and the wiring patterns (CLI flag vs.
YAML config + dictionary loader).

If you're new here, jump to [Quick start](#quick-start). If you
already use `--redact` and want the full catalog, skip to
[Strategy reference](#strategy-reference).

---

## Quick start

Pick the strategy that matches the column's shape:

```bash
# Hash an email column with a deterministic SHA-256 surrogate.
sluice migrate \
  --source-driver=postgres --source=$SRC \
  --target-driver=postgres --target=$DST \
  --redact users.email=hash:sha256
```

Same strategy via YAML:

```yaml
# sluice.yaml
redactions:
  - table: users.email
    strategy: hash
    algo: sha256
```

```bash
sluice migrate -c sluice.yaml --source-driver=postgres --source=$SRC --target-driver=postgres --target=$DST
```

Both forms work on `migrate`, `sync start`, `backup full`, and
`schema preview`. Repeatable: pass `--redact` multiple times to
configure many columns.

---

## How it works

`--redact` rules are applied **between** sluice's source reader and
target writer, in the bulk-copy hot path (cold start) and the CDC
apply path (live sync). Rows the source emits flow through the
configured Strategy before they hit the target. The strategy's
output replaces the source value verbatim at the named column.

There is **zero overhead when no `--redact` is configured** — the
pipeline short-circuits on an empty Registry before any per-row
work. Operators not using redaction pay nothing for the feature.

### Coverage

| Surface | Bulk-copy | CDC apply | Notes |
|---|---|---|---|
| `sluice migrate` | ✅ | n/a | One-shot bulk copy |
| `sluice sync start` | ✅ cold start | ✅ live CDC | Both phases honour `--redact` |
| `sluice backup full` | ✅ | n/a | Backup chunks are PII-clean on disk; restore copies them through unchanged |
| `sluice schema preview` | n/a | n/a | Annotates `CREATE TABLE` with `-- REDACTED via <strategy>` comments; DDL itself unchanged |

---

## Strategy reference

### Phase 1: foundational strategies (v0.53.0)

| Strategy | Form | Behaviour | Refusal |
|---|---|---|---|
| `null` | `null` | Replace with `NULL` | Refuses on `NOT NULL` columns (use `static:` instead) |
| `static:<v>` | `static:<value>` | Replace with literal constant | None |
| `hash:sha256` | `hash:sha256` | SHA-256 hex digest (64 chars, deterministic) | Refuses on non-string / non-bytea input |
| `hash:hmac-sha256` | `hash:hmac-sha256` | HMAC-SHA256 hex digest; requires `--redact-key-source` | Refuses without keyset; refuses non-string input |
| `truncate:<n>` | `truncate:4` | First N runes (rune-counted; UTF-8 + emoji safe) | Refuses non-string input |

### Phase 2.a: generic format-preserving masks (v0.56.0)

| Strategy | Form | Behaviour |
|---|---|---|
| `mask:inner` | `mask:inner:<m1>,<m2>[,<char>]` | Keep first M1 + last M2 runes; mask middle |
| `mask:outer` | `mask:outer:<m1>,<m2>[,<char>]` | Mask first M1 + last M2; keep middle |

Default mask char is `X`. Examples:
- `mask:inner:4,4` on `4111111111111111` → `4111XXXXXXXX1111`
- `mask:inner:0,4,*` on `123456789` → `*****6789`
- `mask:outer:1,1` on `abcdef` → `Xbcde X`

### Phase 2.b: country/format-specific mask presets (v0.57.0 + v0.58.0)

| Preset | Input shape | Output | Validation |
|---|---|---|---|
| `mask:ssn` | `XXX-XX-XXXX` | `XXX-XX-NNNN` (preserve last 4) | Strict dash positions; digits only |
| `mask:pan` | 12-19 digits (with spaces/hyphens OK) | Preserve first 6 + last 4 | Luhn checksum required |
| `mask:pan-relaxed` | 12-19 digits | Same as `mask:pan` | NO Luhn check |
| `mask:email` | `local@domain` | First char of local + mask middle + entire `@domain` | Requires `@`; non-empty local |
| `mask:ca-sin` | `XXX-XXX-XXX` or `XXXXXXXXX` | Preserve last 3 | Luhn checksum required |
| `mask:uk-nin` | `AA999999A` | Preserve prefix letters + suffix; mask 6 digits | Suffix ∈ {A,B,C,D} |
| `mask:iban` | 15-34 chars per ISO 13616 | Preserve country code + check digits + 2 BBAN + last 4 | Country letters + check digits validated |
| `mask:uuid` | 8-4-4-4-12 hyphenated UUID | Preserve hyphens + first 4 + last 4 hex chars | Strict shape; hex chars |

**`mask:uuid` caveat (Bug 60 / v0.58.1)**: the masked output contains `X` characters which aren't valid hex. On a target column typed as native `uuid` (PostgreSQL), the migration will refuse at startup unless you also pass `--type-override=table.col=text`. The preflight catches this before any data movement.

### Phase 2.c: randomize generators (v0.59.0 + v0.60.0)

Random outputs that are **replay-stable per source row**. Same
source PK + same column always produces the same target value
across CDC resume, cold-start re-apply, and backup → restore. See
[ADR-0039](adr/adr-0039-randomize-strategy-determinism.md) for the
contract.

| Generator | Form | Behaviour |
|---|---|---|
| `randomize:int` | `randomize:int:<min>,<max>` | Integer in `[min, max]` inclusive |
| `randomize:email` | `randomize:email` | `<rand-local>@<rand-domain>.test` (IETF-reserved TLD) |
| `randomize:us-phone` | `randomize:us-phone` | NANP-valid `XXX-XXX-XXXX` (avoids reserved area codes) |
| `randomize:uuid` | `randomize:uuid` | RFC 4122 UUIDv4 (passes strict UUID column validation) |
| `randomize:ssn` | `randomize:ssn` | US SSN avoiding reserved ranges (no 000-XX-XXXX, no XXX-00-XXXX, no XXX-XX-0000) |
| `randomize:pan` | `randomize:pan[:<brand>]` | Luhn-valid PAN; optional `visa` / `mastercard` / `amex` |
| `randomize:ca-sin` | `randomize:ca-sin` | Luhn-valid CA SIN; first digit ∈ {1-7, 9} |
| `randomize:uk-nin` | `randomize:uk-nin` | UK NIN matching HMRC prefix alphabet; suffix ∈ {A,B,C,D} |
| `randomize:iban` | `randomize:iban[:<country-code>]` | IBAN with mod-97 check digits; optional `DE` / `GB` / `FR` |

**No-PK preflight**: every `randomize:*` strategy requires a
primary key on the source table (the seed is derived from the
row's PK values). The pipeline refuses at startup with an
operator-actionable error if any `randomize:*` rule targets a
heap (no-PK) table. Workaround: add a PK on the source, or pick a
non-random strategy (`hash:sha256`, `mask:*`, `static:`).

### Phase 3: dictionary strategies (v0.61.0)

Map source values into named lookup tables. Two strategies, two
different determinism contracts (see
[ADR-0040](adr/adr-0040-dictionary-strategy-determinism.md)):

| Strategy | Form | Keyed by | Use case |
|---|---|---|---|
| `randomize:dict` | `randomize:dict:<name>` | Source PK (replay-stable; inherits v0.59.0 contract) | Per-row random selection with controlled cardinality |
| `tokenize:dict` | `tokenize:dict:<name>` | Source value (HMAC) | Stable per-value surrogates; cross-table consistency |

The defining difference:

- **`randomize:dict`**: two source rows with the same source value
  but different PKs can map to DIFFERENT dict entries (PK-keyed).
- **`tokenize:dict`**: every occurrence of the same source value
  (in any table, in any column with the same dict) maps to the
  SAME dict entry (value-keyed via HMAC).

**Dictionaries must be declared in YAML config** — the
operator-readable form lives there, not on the CLI. CLI references
to undeclared dict names refuse at parse time.

Dictionary declarations support two forms:

```yaml
dictionaries:
  first_names:
    # Inline form: small dicts (typically < 100 entries).
    entries: [Alpha, Bravo, Charlie, Delta, Echo]

  city_names:
    # File form: large dicts. One entry per line; `#`-comments + blank lines ignored.
    file: ./fixtures/cities.txt
```

Either inline or file — not both. Empty dicts (0 entries) refuse
at config-load. Missing file paths refuse with the OS error.

**`tokenize:dict` does NOT require a PK** — it's the first sluice
strategy whose output depends on the input value, not the row's
identity. Heap tables and tables without PKs both work.

---

## CLI vs YAML

CLI flags and YAML config can be mixed. CLI rules are processed
first; YAML rules append. Duplicates on the same column
(`schema.table.column`) last-write-wins with a WARN.

### CLI-only form

```bash
sluice migrate \
  --redact users.email=hash:sha256 \
  --redact users.pan=mask:pan \
  --redact users.phone=randomize:us-phone
```

Useful for: ad-hoc / one-shot runs, testing strategies before
committing them to config.

### YAML-only form

```yaml
redactions:
  - table: users.email
    strategy: hash
    algo: sha256
  - table: users.pan
    strategy: mask
    form: pan
  - table: users.phone
    strategy: randomize
    form: us-phone

dictionaries:
  cities:
    file: /etc/sluice/cities.txt

redact_key_source: env:HMAC_SECRET
```

Useful for: production deployments (version-controllable,
reviewable, audit-friendly).

### Hybrid

CLI flags override / extend the YAML. Recommended pattern: keep
the bulk in YAML; use CLI for per-environment overrides
(`--redact=users.email=null` in staging).

---

## Determinism contracts

Three different determinism semantics across the strategy set:

| Semantics | Strategies | Guarantee |
|---|---|---|
| **Stateless deterministic** | `hash:sha256`, `truncate:`, `mask:*` (all forms), `static:`, `null` | Same input → same output across any sluice run anywhere |
| **Keyed deterministic** | `hash:hmac-sha256` | Same input + same key → same output (operator controls the key via `--redact-key-source`) |
| **PK-keyed replay-stable** | `randomize:*` (including `randomize:dict`) | Same source row (table + column + PK) → same output across re-runs |
| **Input-keyed cross-stream** | `tokenize:dict` | Same input value → same output across tables + columns (uses fixed HMAC key in v0.61.0; Phase 4 will lift to operator keyset) |

For CDC resume and backup → restore round-trips: every strategy
above is idempotent on the SAME data. Operators relying on stable
target values across reruns can use any of them.

For target-data correlation across tables (joining redacted
columns): use `tokenize:dict` or `hash:hmac-sha256`. The other
strategies don't carry cross-table consistency on the same source
value.

---

## HMAC key sources

Required by `hash:hmac-sha256`. Three forms:

```bash
# Environment variable (production: KMS-managed secret)
--redact-key-source=env:HMAC_SECRET

# File (operator-managed key on disk; one-line file)
--redact-key-source=file:/etc/sluice/hmac.key

# Derived from stream-id + salt (phase 1 placeholder; phase 4 replaces this)
--redact-key-source=derive:my-app-salt
```

**`tokenize:dict` uses its own fixed key** (`sluice-tokenize-dict-v1`) in v0.61.0.
The security model is "stable hashing", not secrecy — operators relying
on tokenize output as a non-reversible surrogate should NOT assume
cryptographic strength against a targeted attacker. Phase 4 will
introduce a proper operator-keyset story; the `v1` suffix on the
current key reserves room for migration.

---

## Preflight refusals

sluice runs two preflight checks before any data movement:

1. **`mask:uuid` on a UUID-typed column** (Bug 60 / v0.58.1):
   refuses unless `--type-override=col=text` short-circuits the
   target column type.
2. **`randomize:*` on a no-PK table** (v0.59.0): refuses; suggests
   either adding a PK to the source or picking a non-random
   strategy.

When both fire in the same run, the preflight aggregates into a
single combined error so you see the full picture in one pass.

---

## Audit log

At command start, sluice emits one INFO line summarising the
configured redaction surface:

```
sluice: redaction configured scope=migrate columns=5 strategies=[hash:sha256 mask:pan randomize:email tokenize:dict:first_names truncate:4]
```

Per-column rules are NOT logged (the rule itself can be
sensitive — `--redact billing.credit_card=truncate:4` reveals
which column holds card numbers). The strategy NAME is logged;
the configured options (mask char, dict entries, etc.) are not.

---

## Examples

### A: hash emails, mask phones, randomize SSNs

```bash
sluice migrate \
  --redact users.email=hash:sha256 \
  --redact users.phone=mask:inner:3,4 \
  --redact users.ssn=randomize:ssn
```

### B: cross-table stable surrogates

```yaml
dictionaries:
  first_names:
    entries: [Alpha, Bravo, Charlie, Delta, Echo, Foxtrot]

redactions:
  - table: users.first_name
    strategy: tokenize
    dict: first_names
  - table: orders.customer_first_name
    strategy: tokenize
    dict: first_names
  - table: leads.contact_first_name
    strategy: tokenize
    dict: first_names
```

Every `'Alice'` source value across all three tables maps to the
same dict entry (e.g. `'Alpha'`). Analytics joins on the redacted
column stay coherent.

### C: realistic synthetic PII for staging

```yaml
redactions:
  - table: users.email
    strategy: randomize
    form: email
  - table: users.phone
    strategy: randomize
    form: us-phone
  - table: users.ssn
    strategy: randomize
    form: ssn
  - table: customers.pan
    strategy: randomize
    form: pan
    brand: visa
  - table: customers.iban
    strategy: randomize
    form: iban
    country_code: DE
```

Every row gets realistic-shape values. CDC resume reproduces the
same synthetic values byte-for-byte.

### D: schema preview before committing

```bash
sluice schema preview \
  --source-driver=postgres --source=$SRC \
  --target-driver=postgres --target=$DST \
  --redact users.email=hash:sha256 \
  --redact users.ssn=mask:ssn
```

Each redacted column's CREATE TABLE line gets a trailing comment:

```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  email TEXT NOT NULL,    -- REDACTED via hash:sha256
  ssn TEXT,               -- REDACTED via mask:ssn
  ...
);
```

The DDL itself is unchanged — the annotation is comment-only — so
the output stays drop-in compatible if you copy it to apply
manually.

---

## Known limitations

- **Tokenize HMAC key is fixed** in v0.61.0 (`sluice-tokenize-dict-v1`).
  Operators wanting a per-tenant or per-environment-keyed
  tokenization must wait for Phase 4. The fixed key is stable
  enough for "we want consistent surrogates" but not strong enough
  for "we need to defeat an attacker who knows the key derivation".
- **Dictionary file form caches at parser-time.** If you edit the
  file mid-run, sluice doesn't reload. Restart for changes to
  take effect. Documented as a "changes to dictionaries between
  runs cause resume divergence; reset target data" semantic.
- **No country-aware structural generation** for `randomize:iban`
  beyond the check digits — the BBAN is random digits, not a
  valid country-specific bank/branch code. The output passes
  mod-97 validation but won't match a real account at any bank.
- **PAN brand catalog is intentionally narrow** (Visa /
  Mastercard / AmEx). Add Discover / JCB / UnionPay on operator
  demand via GitHub issues.
- **IBAN country catalog** ships DE / GB / FR. Other countries
  (ES / IT / NL / etc.) follow the same pattern; add on demand.

---

## ADRs

- [ADR-0039](adr/adr-0039-randomize-strategy-determinism.md) —
  Replay-stable per-row seeding for `randomize:*` strategies.
- [ADR-0040](adr/adr-0040-dictionary-strategy-determinism.md) —
  The two dictionary determinism contracts (PK-keyed vs.
  input-value-keyed).

## Prep docs (for sluice maintainers)

- `docs/dev/notes/prep-pii-redaction-phase-1.md` — Phase 1 / 1.5 design.
- `docs/dev/notes/prep-pii-redaction-phase-2-strategy-catalog.md` — Phase 2 + 3 catalog mapping to MySQL Enterprise.

## Release history

| Phase | Release(s) | Strategies introduced |
|---|---|---|
| 1 | v0.53.0 | `null`, `static:`, `hash:sha256`, `hash:hmac-sha256`, `truncate:` |
| 1.5 | v0.54.0 → v0.55.1 | CDC apply-path + schema-preview annotation + backup-stream + YAML config + `restore --target-schema` |
| 2.a | v0.56.0 → v0.56.1 | `mask:inner`, `mask:outer`; Bug 59 fix (`--redact` kong `sep:"none"`) |
| 2.b | v0.57.0 → v0.58.1 | `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`, `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`; Bug 60 fix (`mask:uuid` preflight) |
| 2.c | v0.59.0 → v0.60.0 | `randomize:int`, `randomize:email`, `randomize:us-phone`, `randomize:uuid`, `randomize:ssn`, `randomize:pan`, `randomize:ca-sin`, `randomize:uk-nin`, `randomize:iban`; replay-stable seeding contract; no-PK preflight |
| 3 | v0.61.0 | `randomize:dict`, `tokenize:dict`; YAML `dictionaries:` block (inline + file forms) |

**27 strategies, 5 phases, 9 minor releases.** Phase 4 (proper
operator-keyset story replacing the fixed `tokenize:dict` HMAC key)
is the next major PII chunk.
