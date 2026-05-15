# ADR-0041: operator-keyset persistence for PII redaction (Phase 4)

## Status

**Accepted (2026-05-15).** Implemented in PII Phase 4 (v0.63.0). The keyset type + loader, `--keyset-source` flag (file/env/db schemes), strategy integration, `sluice_keysets` DDL on PG + MySQL, preflight refusal, and audit-log line have shipped. Two design decisions made at implementation time **override the original proposal text below where they conflict** — see "Decisions / deviations from original proposal".

### Decisions / deviations from original proposal

**D1 — Startup-snapshot only, NO hot-reload.** The keyset is resolved ONCE at process startup and is immutable for the run. Sluice does NOT watch the keyset file for atomic-rename updates and does NOT poll the `db:` table. Rotation takes effect on the **next process restart only**. Rationale: a mid-run active-key change would give some rows gen-N and others gen-N+1 surrogates within the same run, breaking within-run referential integrity for `hash`/`tokenize`. Startup-snapshot keeps each run internally consistent, removes the fsnotify dependency, and removes any poll goroutine. This supersedes the original proposal's `file:` "watches for atomic-rename updates", the `db:` "next keyset-watch poll (~30s)", and rotation-flow step 3. Live-watch is explicitly deferred to a future **Phase 4.5**.

**D2 — Clean break, NO backward-compatibility shim.** Sluice is pre-users (zero-users → no-compat tenet), so Phase 4 is a clean break, not an additive layer. Concretely:

- The Phase 1 `--redact-key-source` flag (and its `RedactKeySource` config field / `SLUICE_REDACT_KEY_SOURCE` env mapping / `resolveHMACKey` / `deriveHMACKey` derivation) was **deleted**. `--keyset-source` is the only key path. This supersedes the original proposal's "For backward compatibility, `--redact-key-source=env:VAR` and `--redact-key-source=file:PATH` continue to work as a single-key shim".
- The hardcoded `tokenizeDictHMACKey = []byte("sluice-tokenize-dict-v1")` constant was **deleted**. There is **no synthetic `sluice-tokenize-dict-v1` keyset entry** and **no zero-surrogate-drift commitment**. This supersedes open-question #3's recommendation and the "Compatibility commitment" section: operators upgrading from v0.61.0 WILL see `tokenize:dict` surrogate drift unless they pin the same key material via an explicit keyset.
- **Consequence (implemented as a loud preflight refusal):** any redaction rule using `hash:hmac-sha256` OR `tokenize:dict` REQUIRES `--keyset-source`. If a rule needs a key and none is resolvable, sluice refuses at preflight with an actionable message (e.g. `tokenize:dict on users.email requires --keyset-source; the built-in v0.61.0 key was removed in PII Phase 4 (ADR-0041)`). The CLI/YAML parsers refuse at strategy-construction time; the pipeline preflight (`internal/pipeline/redact_preflight.go`) re-asserts it as defense-in-depth.

Other implementation notes: the keyset YAML shape uses a per-key `name` + `generations` list (each generation carries `bytes`/`created_at`) and an optional top-level `default`, rather than the original single-key sketch — this is what makes per-key independent rotation + the `key:` reference work. The `db:` scheme classifies the DSN to an engine (postgres:// → Postgres, else MySQL) and reads `sluice_keysets` via an engine-registered store opener so the `redact` package never imports an engine package (IR-first tenet). Out of v1 scope (unchanged from below): `sluice keyset rotate`/`list` CLI, KMS/Vault adapters, encryption-at-rest of `bytes`, keyset-integrating `randomize:*`.

---

*Original draft proposal follows (retained for context; D1 + D2 above take precedence on conflict).*

## Context

Three PII redaction surfaces today use HMAC over operator data, and each handles its key independently:

| Surface | Where the key comes from | Cross-stream stability | Operator-controlled? |
|---|---|---|---|
| `hash:hmac-sha256` (v0.53.0) | `--redact-key-source=env:VAR \| file:PATH \| derive:<salt>` | `env:`/`file:` yes; `derive:` no (tied to per-stream-id+salt) | Yes for `env:`/`file:`; partially for `derive:` |
| `tokenize:dict` (v0.61.0) | Fixed constant `"sluice-tokenize-dict-v1"` | Yes (deterministic; same key everywhere) | No |
| `randomize:*` (v0.59.0–v0.60.0) | Per-row SHA-256 over `streamID \| table \| column \| pkCols \| pkVals` | Within the same stream-id; NOT across stream-ids | Indirectly via `--stream-id` |

Each surface works for its primary use case. The gaps:

1. **Cross-stream referential integrity.** An operator running two streams (staging-1 + staging-2) from the same source today gets different surrogates on each side, because each stream's `hash:hmac-sha256 derive:`, `tokenize:dict` HMAC scope, and `randomize:*` seed all incorporate the stream identity. If both targets need to join on the redacted column, the operator has to either share an explicit `env:`/`file:` key across both streams (works for `hash:hmac-sha256` but not for `tokenize:dict`) or live with the inconsistency.
2. **Key rotation policy.** No first-class story for "we changed the key; what happens to historical surrogates." Today the operator's options are: (a) accept that all surrogates change (everything breaks), (b) keep the old key forever, (c) hand-roll a per-column migration. None of these is operator-friendly.
3. **Durable persistence.** `env:` and `file:` make the key the operator's responsibility — they have to ensure the env var or file is present on every host running sluice. Acceptable for single-host deployments; awkward for fleets.
4. **Audit trail.** No record of which key was active when which rows were tokenized. Critical for compliance use cases that have to prove "this surrogate corresponds to that real value at time T" or "the key in use during this run was approved by ticket #1234."

The shared root cause: sluice has no concept of a **keyset** — a durable, versioned, operator-controlled set of named keys that any HMAC-using strategy can reference.

## Goals

Phase 4 should ship a keyset story that:

1. Unifies key sourcing across `hash:hmac-sha256`, `tokenize:dict`, and any future HMAC-using strategy.
2. Supports **cross-stream determinism**: a single keyset shared between two stream-ids produces identical surrogates for identical inputs.
3. Supports **key rotation** with operator-explicit policy: when the active key changes, sluice can either keep the old key for backward-compatibility or migrate to the new key with a documented one-way switch.
4. Persists the keyset in a sluice-controlled store so a host without the keyset file can resume an existing stream by fetching it from the store (analogous to how sluice's `sluice_cdc_state` table persists stream position across restarts).
5. Provides an **audit log** entry per redaction-using run that records the keyset version + key identifier used.

Non-goals (out of scope, deferred):

- Hardware-security-module integration (KMS / Vault). Out of scope for v1; the operator can layer those external systems above sluice's keyset (e.g., `env:VAR` populated by the deployment system from KMS). If a v2 surfaces strong demand, add an `--keyset-source=kms:<arn>` adapter.
- Per-target encryption-at-rest of the keyset. Out of scope; rely on the host's filesystem / DB encryption.
- Encrypted operator-to-sluice keyset transport. Out of scope; the operator-supplied path / env var is assumed trusted.

## Design proposal

### Keyset shape

A keyset is a list of named keys, each with a generation number:

```yaml
# Example keyset.yaml
keyset:
  version: 1                    # the keyset schema version, NOT the key version
  active: 3                     # which generation is the "current" key
  keys:
    - generation: 3             # newest
      created_at: 2026-05-15T00:00:00Z
      bytes: "base64-encoded 32-byte secret"
    - generation: 2             # previous; kept for backward-compat surrogate generation
      created_at: 2026-03-01T00:00:00Z
      bytes: "base64-encoded 32-byte secret"
    - generation: 1             # oldest; can be revoked when no historical surrogates remain
      created_at: 2026-01-01T00:00:00Z
      bytes: "base64-encoded 32-byte secret"
```

- `active` names the generation used for new tokenizations. Existing surrogates produced by an older generation continue to map to the SAME output (as long as that generation's key is still in the keyset).
- `keys` is the full history. Older keys can be revoked once the operator confirms no rows produced under them remain.

### CLI surface

A single new flag replaces (and supersedes) `--redact-key-source`:

```
--keyset-source=<scheme>:<value>
```

Supported schemes:

- `file:<path>` — load keyset from a YAML file at `<path>`. Watches for atomic-rename updates so an operator rotating the key via `mv keyset.new keyset.yaml` is picked up without restart.
- `env:<var>` — load the keyset YAML from an environment variable (useful in containerized deployments where the secret manager writes to env).
- `db:<dsn>` — load the keyset from a sluice-managed table on the named DSN. The table schema is `sluice_keysets` with columns `(name, generation, bytes, created_at, retired_at)`. Multiple sluice streams pointing at the same `--keyset-source=db:...` share the keyset automatically — this is the cross-stream-stability primitive.

For backward compatibility, `--redact-key-source=env:VAR` and `--redact-key-source=file:PATH` continue to work as "single-key shim" — internally sluice constructs an in-memory keyset with `generation: 1, active: 1, bytes: <the supplied value>`. Operators on Phase 1 setups upgrade by changing the flag name and gaining the rotation / cross-stream features; existing usage stays valid.

### Strategy integration

`hash:hmac-sha256` and `tokenize:dict` both take an optional `key: <name>` parameter naming which key from the keyset to use:

```yaml
redactions:
  - table: users.email
    strategy: hash
    algo: hmac-sha256
    key: customer_pii_v2       # names the key entry in the keyset
  - table: users.first_name
    strategy: tokenize
    dict: first_names
    key: customer_pii_v2       # same key → cross-table consistency
```

If `key:` is omitted, sluice uses the keyset's `default` entry (if declared) or the active generation. This gives operators flexibility to scope keys per-column-class (e.g., `key: customer_pii` for customer rows, `key: employee_pii` for employee rows) so different compliance scopes can rotate independently.

### Determinism contract under rotation

When the active key changes from generation N to N+1, sluice's behavior on a re-run depends on which `key:` the strategy references:

- **Default `key:` (no name)** → uses `active`. After rotation, NEW rows produce NEW surrogates under generation N+1. EXISTING rows on the target still hold their generation-N surrogates from before. **Mixed surrogate population on the target**. Operators wanting clean rotation must explicitly migrate the target (drop + re-run cold-start under the new key).

- **Named `key: customer_pii_v2`** → uses that specific generation regardless of `active`. Rotation doesn't affect it. Operators who NEVER want surrogate drift name the key explicitly.

The semantic the operator chooses determines whether rotation is gradual (default key) or pinned (named key). Both have legitimate use cases; the doc should be clear about which.

### Audit log entry

Every command that loads a keyset emits one INFO line at startup:

```
sluice: keyset loaded source=db:postgres://... generations=[1,2,3] active=3 hmac-algo=sha256
```

Per-row surrogate audit (which key was used for which row) is NOT logged — that defeats the purpose of redaction. The startup line is enough for the "which key was approved by ticket #1234" audit case.

## Persistence shape (`db:` scheme)

Schema for the sluice-managed keyset table:

```sql
CREATE TABLE sluice_keysets (
    name        TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    bytes       BYTEA NOT NULL,        -- the raw secret material; encryption-at-rest is the operator's concern
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    retired_at  TIMESTAMPTZ,           -- non-NULL marks the generation as retired; sluice refuses to use retired keys for NEW surrogates but still permits decoding via them
    active      BOOLEAN NOT NULL DEFAULT false,  -- exactly one row per name has active=true
    PRIMARY KEY (name, generation)
);
```

Rotation flow:

1. Operator runs `sluice keyset rotate --name=customer_pii --new-bytes=<base64>` (new CLI command — out of v1 scope; manual SQL works in the interim).
2. sluice inserts the new row with the next generation, marks it `active=true`, marks the prior `active=false`.
3. Existing streams pick up the new active key on next keyset-watch poll (~30s) or restart.
4. Operator monitors the audit log for the `active=N+1` line confirming pickup.

Operators using `file:` or `env:` schemes manage the equivalent state themselves; sluice's keyset-loader normalizes both into the same in-memory shape.

## Open questions

1. **Encryption-at-rest of the `bytes` column.** Sluice doesn't encrypt other sensitive columns (e.g., `--target` DSNs in the state store) and expects the operator's storage layer to handle it. Should the keyset case be an exception? Recommendation: stick with the existing pattern — document that the operator is responsible for storage-layer encryption.

2. **Default keyset name.** If only one named key exists, should `key:` be implicit? Recommendation: yes — `key: <name>` is optional when the keyset has exactly one entry. With multiple entries, omitting `key:` uses the entry named `default` (or refuses if no `default` exists).

3. **Migration from v0.61.0 fixed `tokenize:dict` key.** v0.61.0 hardcodes `"sluice-tokenize-dict-v1"`. Phase 4's keyset story should be backward-compatible: a synthetic keyset named `sluice-tokenize-dict-v1` with `generation: 1` and bytes equal to the v0.61.0 constant should be the default for `tokenize:dict` rules without an explicit `key:`. This means operators on v0.61.0 upgrading to Phase 4 see NO surrogate drift unless they opt in via explicit `key:`. The `-v1` suffix on the v0.61.0 constant was reserved precisely for this transition.

4. **Pre-shared keyset across two sluice installs.** Two operators want their independent sluice deployments to produce IDENTICAL tokenizations for cross-organization data exchange. Today this requires `env:`/`file:` with a shared secret. Phase 4 should support: same keyset YAML installed at both ends → same surrogates. The `file:` scheme handles this naturally; the `db:` scheme would need a secondary export/import path. Recommendation: document the `file:` pattern as canonical for cross-install sharing; the `db:` scheme is for intra-org cross-stream sharing within a single sluice deployment.

5. **Backup/restore semantics under rotation.** If a backup is taken at active=2 and restored at active=3, the restored rows still hold generation-2 surrogates. New rows from CDC apply produce generation-3 surrogates. The target ends up with mixed surrogates. Is this acceptable? Recommendation: yes — it matches the documented "active = used for new surrogates; existing rows retain their generation." Document the consequence in `docs/redaction.md`.

## Sizing

| Component | LOC (impl) | LOC (tests) |
|---|---|---|
| Keyset type + loader (YAML parsing, atomic-watch on file, env decode, db-table reader) | ~250 | ~200 |
| `--keyset-source` CLI flag + `--redact-key-source` backward-compat shim | ~80 | ~60 |
| `hash:hmac-sha256` + `tokenize:dict` integration (extract key from keyset by name) | ~60 | ~80 |
| Audit log entry | ~30 | ~20 |
| `sluice_keysets` table DDL + create-on-init logic | ~80 | ~50 |
| ADR-0041 finalization + docs/redaction.md updates | (docs) | n/a |
| **Phase 4 total** | **~500** | **~410** |

Out of scope for v1 (deferred to Phase 4.5+):

- `sluice keyset rotate` / `sluice keyset list` CLI commands. v1 ships with manual SQL / YAML editing; the CLI helpers are operator-ergonomics improvements that can land after the core lands.
- KMS / Vault adapters. v1 leans on `env:` / `file:` / `db:`; downstream secret managers populate those.

## Decision (when this ADR is accepted)

**Recommended path**:

1. Implement `--keyset-source=<scheme>:<value>` with all three schemes (`file:`, `env:`, `db:`).
2. Refactor `hash:hmac-sha256` and `tokenize:dict` to read from the keyset.
3. Backward-compatibility shim: `--redact-key-source` continues to work; v0.61.0 `tokenize:dict` fixed key remains the default for unnamed `tokenize:dict` rules so no operator surrogate drift on upgrade.
4. Audit log line + `sluice_keysets` DDL.
5. `docs/redaction.md` rewrite covering the keyset model; deprecate the per-strategy key-source docs in favor of unified text.

**Recommended sequencing**: Phase 4 is its own minor-release chunk (~900 LOC including tests). Probably v0.62.0 or later, depending on which non-PII roadmap items take priority first.

**Compatibility commitment**: operators on v0.61.0 should be able to upgrade to Phase 4 with zero surrogate drift unless they explicitly opt in via named `key:` references. The `-v1` suffix on the v0.61.0 tokenize HMAC key reserves the cross-version-compat slot.

## References

- ADR-0039 — Per-row replay-stable seeding for `randomize:*` strategies.
- ADR-0040 — Dictionary-strategy determinism (PK-keyed vs input-value-keyed). Explicitly defers Phase 4's keyset concerns.
- `docs/redaction.md` — Operator-facing reference; will need updates when Phase 4 lands.
- `docs/dev/roadmap.md` §15d — "Phase 4 — deterministic-tokenize keyset persistence."
- `docs/dev/notes/prep-pii-redaction-phase-1.md` §"Determinism across stream restarts" — original Phase 4 scoping note.
