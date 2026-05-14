# Prep — PII redaction Phase 2 strategy catalog

Reference for the format-preserving + generation + dictionary
strategies planned for v0.55.0+ Phase 2 / Phase 3 releases.
Inspired by [MySQL Enterprise's data-masking-component functions](https://dev.mysql.com/doc/refman/8.4/en/data-masking-component-function-reference.html)
— the operator suggested mapping each one to a sluice-native
strategy on 2026-05-14 because the MySQL list is the canonical
catalog of "what real-world PII redaction needs".

This doc is a planning artefact. The Phase 1 prep doc
(`prep-pii-redaction-phase-1.md`) covered the four foundational
strategies (`null` / `static` / `hash` / `truncate`). This doc
extends with the format-preserving + randomized + dictionary
strategies that round out the operator-facing catalog.

## Why mirror the MySQL Enterprise list

The MySQL Enterprise data-masking component (closed-source,
Enterprise tier only) is what the operator community already knows.
Mapping sluice strategies to the same conceptual names + behaviours
gives operators a familiar surface: "we want what MySQL Enterprise
gives us, but as a sluice-native open-source component". This
matches sluice's broader "operator-friendly, IR-first" positioning
— we're not wrapping the MySQL functions (they require Enterprise
licensing and are MySQL-only), we're providing equivalent
behaviours that work on every sluice-supported source/target pair.

## Strategy catalog mapping

### Already shipped in Phase 1 (v0.53.0)

| MySQL Enterprise function | sluice strategy | Phase |
|---|---|---|
| (no direct equivalent — "replace with NULL") | `null` | 1 |
| (no direct equivalent — "replace with literal") | `static:<v>` | 1 |
| (no direct equivalent — "SHA-256 hex") | `hash:sha256` | 1 |
| (no direct equivalent — "HMAC-keyed surrogate") | `hash:hmac-sha256` | 1 |
| (similar to `mask_inner` with margin=N,0) | `truncate:<n>` | 1 |

### Phase 2 — format-preserving masking

| MySQL Enterprise function | sluice strategy | Notes |
|---|---|---|
| `mask_inner(s, m1, m2, char)` | `mask:inner:<m1>,<m2>[,<char>]` | Generic format-preserving; keep first m1 + last m2 chars; mask middle with char (default `X`). |
| `mask_outer(s, m1, m2, char)` | `mask:outer:<m1>,<m2>[,<char>]` | Generic format-preserving; mask first m1 + last m2 chars; keep middle. |
| `mask_ssn(s)` | `mask:ssn` | US SSN preset: input `XXX-XX-XXXX` → `XXX-XX-NNNN` (keep last 4). Validates the dash format; refuses malformed. |
| `mask_pan(s)` | `mask:pan` | Strict payment-card PAN: validates Luhn checksum; preserves first 6 + last 4; masks middle. |
| `mask_pan_relaxed(s)` | `mask:pan-relaxed` | Lenient PAN: skips Luhn validation; preserves first 6 + last 4. Useful for synthetic test data that doesn't have valid checksums. |
| `mask_canada_sin(s)` | `mask:ca-sin` | Canadian SIN: input `XXX-XXX-XXX` → preserve last 3; mask first 6. |
| `mask_uk_nin(s)` | `mask:uk-nin` | UK NIN: input `AB123456C` → preserve first 2 + last 1; mask 6 digits. |
| `mask_iban(s)` | `mask:iban` | IBAN: preserves country code (first 2) + check digits (next 2) + first 2 of BBAN + last 4; masks middle. |
| `mask_uuid(s)` | `mask:uuid` | UUID: preserves group separators (hyphens) + outer 4 chars; masks middle. |

### Phase 2 — randomized generation

| MySQL Enterprise function | sluice strategy | Notes |
|---|---|---|
| `gen_range(min, max)` | `randomize:int:<min>,<max>` | Crypto-random integer in range. Determinism: NONE (every row gets a different value). |
| `gen_rnd_email()` | `randomize:email` | Random valid email: `<rand-alpha>@<rand-domain>.test` (uses .test TLD to ensure no collision with real domains). |
| `gen_rnd_ssn()` | `randomize:ssn` | Random US SSN: avoids reserved ranges (no 000-XX-XXXX, no XXX-00-XXXX, no XXX-XX-0000). |
| `gen_rnd_canada_sin()` | `randomize:ca-sin` | Random Canadian SIN with valid Luhn checksum. |
| `gen_rnd_uk_nin()` | `randomize:uk-nin` | Random UK NIN: `AB123456C` format. |
| `gen_rnd_pan()` | `randomize:pan[:<brand>]` | Random PAN with valid Luhn. Optional brand: `visa` (4XXX), `mastercard` (5XXX), `amex` (3XXX). |
| `gen_rnd_iban()` | `randomize:iban[:<country-code>]` | Random IBAN with valid country-specific check digits. |
| `gen_rnd_us_phone()` | `randomize:us-phone` | Random US phone: `XXX-XXX-XXXX` avoiding reserved area codes (no 555-01XX etc.). |
| `gen_rnd_uuid()` | `randomize:uuid` | Random UUIDv4. |

### Phase 3 — deterministic dictionary

| MySQL Enterprise function | sluice strategy | Notes |
|---|---|---|
| `gen_dictionary(dict)` | `randomize:dict:<name>` | Random term from named dictionary. Determinism: per-row random selection. |
| `gen_blocklist(input, dict)` | `tokenize:dict:<name>` | Deterministic: same input → same output (stable surrogate). Maps input via HMAC + modulo to dictionary slot. |
| `masking_dictionary_term_add()` | YAML-driven | sluice dictionaries live in YAML config (`dictionaries:` block), NOT in a DB table. Operator edits YAML; sluice reloads on next run. The MySQL Enterprise function exists because that component stores dictionaries in `mysql.masking_dictionaries`; sluice's YAML approach is simpler + version-controllable. |
| `masking_dictionary_term_remove()` | YAML-driven | Same — edit YAML to remove. |
| `masking_dictionary_remove()` | YAML-driven | Same — delete the dictionary block. |
| `masking_dictionaries_flush()` | N/A | sluice loads dictionaries at startup; no flush needed. |

## Proposed YAML for dictionaries (Phase 3)

```yaml
redactions:
  - table: users.first_name
    strategy: tokenize
    dict: first_names

dictionaries:
  first_names:
    - Alice
    - Bob
    - Carol
    - Dave
    # ...
  city_names:
    file: /etc/sluice/dictionaries/cities.txt  # alternative: load from file
```

The two forms (inline list + file pointer) keep small dictionaries
ergonomic while supporting large lists (thousands of entries) via
external files. Dictionary changes between runs are documented as
"causes resume divergence" — operators who change a dictionary
mid-stream should reset target data.

## Sizing estimate

| Phase 2 component | LOC (impl) | LOC (tests) |
|---|---|---|
| Generic `mask:inner` + `mask:outer` | ~80 | ~120 |
| 7 country/format-specific `mask:*` presets | ~280 (~40 each) | ~350 |
| `randomize:int` + `randomize:email/ssn/etc.` (9 generators) | ~400 | ~400 |
| Luhn validator helper (shared by pan + ca-sin) | ~40 | ~60 |
| **Phase 2 total** | **~800** | **~930** |

| Phase 3 component | LOC (impl) | LOC (tests) |
|---|---|---|
| Dictionary loader (YAML + file) | ~120 | ~150 |
| `randomize:dict` + `tokenize:dict` strategies | ~80 | ~100 |
| YAML config schema additions | ~30 | ~40 |
| **Phase 3 total** | **~230** | **~290** |

Phase 2 is the bulk; Phase 3 is a smaller follow-on.

## Sequencing

1. **Phase 1.5** (immediate next release): CDC apply-path redaction, schema-preview annotation, YAML config for the four Phase 1 strategies, backup-stream redaction. These complete what Phase 1 left undone before adding more strategies.
2. **Phase 2.a** (after 1.5): generic `mask:inner` + `mask:outer` + Luhn helper. ~120 LOC + ~180 tests.
3. **Phase 2.b** (after 2.a): country/format-specific `mask:*` presets in two waves:
   - First wave (highest volume): `mask:ssn`, `mask:pan`, `mask:pan-relaxed`, `mask:email`
   - Second wave: `mask:ca-sin`, `mask:uk-nin`, `mask:iban`, `mask:uuid`
4. **Phase 2.c** (after 2.b): `randomize:*` generators in two waves:
   - First wave: `randomize:int`, `randomize:email`, `randomize:us-phone`, `randomize:uuid`
   - Second wave: `randomize:ssn`, `randomize:pan`, `randomize:ca-sin`, `randomize:uk-nin`, `randomize:iban`
5. **Phase 3** (after 2.c): dictionary loader + `tokenize:dict` + `randomize:dict`.
6. **Phase 4** (after 3): cross-stream keyset persistence (was already on the roadmap; orthogonal to the strategy catalog).

Each "Phase 2.a / 2.b / 2.c" lands as its own minor release. The
strategy catalog is plumbing-light (the framework is already in
place from Phase 1); each new strategy is a self-contained Strategy
implementation + tests + a one-line factory registration in
`cmd/sluice/redact_flag.go`.

## Open questions for operator review

1. **YAML inline vs CLI for dictionaries.** YAML is the natural fit
   (lists don't compress well into a single CLI flag), but operators
   running ad-hoc `sluice migrate` invocations may want a small
   inline `--dictionary` flag. Recommendation: defer CLI form;
   YAML-only for Phase 3, add CLI if real-world usage demands.

2. **Country-specific scope.** MySQL Enterprise ships US + CA + UK
   + international. Real-world sluice operators may want EU
   (German Personalausweis, French INSEE), India (Aadhaar / PAN),
   Australia (TFN), etc. Recommendation: ship the MySQL-Enterprise
   set in Phase 2.b; add others on operator demand via GitHub issues.

3. **Email randomization realism.** `gen_rnd_email()` returns a
   plausible-looking address but uses `.test` TLD (no collision
   with real domains). Should sluice match that exactly, or
   support an operator-supplied domain list for more realistic
   shapes? Recommendation: `.test` default; `randomize:email:<domain>`
   form for operator-specified domain.

4. **PAN brand specificity.** MySQL Enterprise has a single
   `gen_rnd_pan()` that picks a brand randomly. Operators wanting
   "all rows have visa PANs for test consistency" need
   `randomize:pan:visa` etc. Recommendation: ship the brand-suffix
   form alongside the no-suffix random-brand form.

5. **Determinism contract for `randomize:*`.** MySQL Enterprise
   says "non-deterministic" — same value would be different on
   each call. sluice's runs are deterministic-leaning (CDC resume
   needs stable surrogates for idempotency). Recommendation: seed
   the random source with `--stream-id + column-name + row-PK`
   so the SAME row always gets the SAME random value, but
   different rows get different values. This preserves replay-on-
   crash safety. Document this as the sluice-native deviation from
   MySQL Enterprise's pure-random semantics.

## Pre-implementation checklist

Before writing code for Phase 2.a (`mask:inner` + `mask:outer`):

- [ ] Operator review of this catalog doc + the open questions.
- [ ] Decide on the `mask:<form>:<args>` option syntax: comma-separated
      positional args (matches MySQL Enterprise) vs key=value
      (`mask:inner:start=4,end=4,char=X`). Recommendation:
      positional with sensible defaults; key=value if confusion
      surfaces in real-world usage.
- [ ] Add an ADR documenting the sluice strategy catalog + the
      determinism contract for `randomize:*`.
- [ ] Reference back to `prep-pii-redaction-phase-1.md` so the
      sequencing across Phase 1 / 1.5 / 2 / 3 / 4 is captured in
      one breadcrumb trail.

## Pointers

- Phase 1 prep doc: `docs/dev/notes/prep-pii-redaction-phase-1.md`
- Phase 1 implementation: `internal/redact/` (commit 51ca278), `internal/pipeline/redact*` (commits 7dca394, 24720b1), `cmd/sluice/redact_flag*` (v0.53.0)
- Roadmap item 15: `docs/dev/roadmap.md`
- GitHub issue #24: PII redaction request
- MySQL Enterprise reference: https://dev.mysql.com/doc/refman/8.4/en/data-masking-component-function-reference.html
