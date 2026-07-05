---
name: redaction-setup
description: Use to configure and confirm PII redaction during a migrate or sync — masking / hashing / tokenizing sensitive columns as data flows to the target. Drives `sluice migrate`/`sync start` with `--redact` (+ `--keyset-source` for keyed strategies). Gated — state-changing (writes redacted data to the target). Trigger when the user asks to redact / mask PII / anonymize / pseudonymize columns during a migration or sync.
---

# redaction-setup

Configure PII redaction on the migrate/sync path and confirm it took effect on the target. State-changing (the target receives redacted data); honors the standard migrate/sync approval rules.

## When to use
The user wants a copy of production with PII masked/hashed/tokenized — for staging, analytics, or a vendor handoff — with the schema and non-PII data intact. If deterministic-across-runs surrogates matter (CDC determinism), use a keyed strategy (step 2b).

## Inputs you need
- Source + target DSNs (env: `SLUICE_SOURCE` / `SLUICE_TARGET`) and drivers.
- The columns to redact and the strategy per column.
- For **keyed** (deterministic) strategies (`hash:hmac-sha256`, `tokenize:dict`): a keyset via `--keyset-source` (see step 2b). **Note the flag is `--keyset-source`, not `--redact-key-source`** (the Phase-1 `--redact-key-source` flag was removed).

## Steps

1. **Choose per-column strategies.** `--redact '[schema.]table.column=STRATEGY[:options]'` (repeatable). The real strategy families (confirm exact option syntax with `sluice migrate --help` and `docs/cookbook/recipe-redaction-keyset.md`):
   - `null` (NULLABLE columns only), `static:<value>`, `truncate:<n>`
   - `hash:sha256` (stateless, deterministic, **no keyset needed**), `hash:hmac-sha256[:<keyname>]` (keyed)
   - format-preserving masks: `mask:inner:<m1>,<m2>[,<char>]`, `mask:outer:…`, and presets `mask:ssn` / `mask:pan` / `mask:pan-relaxed` / `mask:email` / `mask:ca-sin` / `mask:uk-nin` / `mask:iban` / `mask:uuid`
   - `randomize:int:<min>,<max>` / `randomize:email` / `randomize:uuid` / `randomize:ssn` / `randomize:pan[:<brand>]` / `randomize:iban[:<country>]` / `randomize:dict:<name>` (all `randomize:*` require a PK on the source table)
   - `tokenize:dict:<name>[:<keyname>]` (keyed; the dictionary content must be declared in YAML under `dictionaries:`)

2a. **Plain (non-deterministic) redaction** needs no keyset — e.g. `--redact users.email=hash:sha256 --redact users.ssn=mask:ssn`.

2b. **Keyed (deterministic) redaction** — provision a keyset and reference it. `--keyset-source` forms: `file:PATH` (a keyset YAML), `env:PREFIX` (keys in env vars under a prefix), or `db:DSN` (the `sluice_keysets` control table on the target — shared across streams for cross-stream surrogate stability). It is resolved once at startup (rotation needs a restart). A keyed rule with no `--keyset-source` is **refused loudly at preflight** — there is no built-in fallback key. Two streams produce identical surrogates only when they load the **same** keyset.

3. **Preview.** `sluice schema preview --format json …` annotates each redacted column in the DDL. Confirm the intended set is in effect before a production run. YAML equivalents live under `redactions:` (and `dictionaries:`) in `sluice.yaml`; when both CLI and YAML declare a rule for one column, **CLI wins** with a loud WARN.

4. **Run.** The same `sluice migrate` / `sluice sync start` command — redaction composes into the IR pipeline on BOTH the bulk-copy and the CDC-apply paths. At start, sluice logs one audit line: `redaction configured … columns=N strategies=[…]` (key names elided). An empty redaction set does NOT refuse — a run with no rules produces a fully-plaintext target, so check that line.

5. **Confirm redaction on the target by inspecting real rows.** `verify` has **no** redaction awareness — do not expect a verify flag to prove masking. Instead, query the actual target rows for a known-PII column (e.g. `SELECT email FROM users LIMIT 20`) and confirm the values are the surrogate shape (hash hex / masked pattern / random), not the source plaintext. For row-count fidelity use `sluice verify --depth=count` (counts are unchanged by redaction); `--depth=sample` will flag every redacted row as a mismatch by design, so scope it to the non-redacted tables with `--include-table` (see `fidelity-verify`).

## What you return
- **Redaction plan:** each column → strategy, keyed vs plain, keyset source (if any).
- **Preflight evidence:** the `schema preview` annotations + the startup audit line (column count + strategies) confirming the set is in effect.
- **Target confirmation:** the sampled target rows showing surrogate (not plaintext) values, plus the `--depth=count` verify result.
- **Gaps flagged:** any keyed rule missing a keyset (would refuse), any column you intended but didn't declare.

## References (canonical — don't duplicate)
`docs/cookbook/recipe-redaction-keyset.md` (keyset provisioning, determinism, verifying a redacted target) · `docs/redaction.md` (per-strategy reference) · `AGENTS.md` (envelope, env-first credentials) · `sluice migrate --help` (exact `--redact` / `--keyset-source` syntax).
