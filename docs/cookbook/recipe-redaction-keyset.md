# Recipe — PII redaction with a persisted keyset

Replicate production data to a staging / analytics / vendor target
with PII redacted, **deterministically across runs** so that the same
input value always produces the same surrogate value on the target.

## When to use this recipe

- You want a copy of production for staging or analytics without
  exposing real users.
- Compliance (GDPR / CCPA / HIPAA) requires PII to stay in the source
  jurisdiction or environment, but the schema + non-PII data needs to
  flow.
- Cross-team data handoffs — a vendor / third-party processor needs
  the schema + the data shape but not the PII.

If you only need PII *removed* (not preserved deterministically across
runs), the `static:redacted` strategy is simpler and doesn't need a
keyset. Reach for this recipe when **CDC determinism** matters: two
streams over the same source must produce the same surrogate for the
same input, or the streams' applied rows would diverge over time.

## The flow at a glance

1. **Provision a keyset** — either a file, env var, or the
   `sluice_keysets` control table on the target.
2. **Declare redactions** — `--redact TABLE.COL=STRATEGY[:arg[:arg]]`
   on the CLI (colon-separated args) or `redactions:` in your `sluice.yaml`.
3. **Run migrate / sync as usual** — the redaction layer is composed
   into the existing IR pipeline.

## Step 1: provision a keyset

A keyset is a small map from `name → key bytes`. Strategies that need
deterministic input — `hash:hmac-sha256` and `tokenize:dict` —
reference a key from the keyset by name via the `key:` rule option.

### Option A: file-backed

```yaml
# /etc/sluice/keyset.yaml — protect with 0600 permissions.
keys:
  email_v1: "base64-of-32-random-bytes-here..."
  pan_v1: "base64-of-32-random-bytes-here..."
```

```sh
sluice migrate ... \
    --keyset-source 'file:/etc/sluice/keyset.yaml' \
    --redact 'public.users.email=hash:hmac-sha256:email_v1'
```

### Option B: env-backed

```sh
export SLUICE_KEYSET_email_v1='base64-of-32-random-bytes-here...'
export SLUICE_KEYSET_pan_v1='base64-of-32-random-bytes-here...'

sluice migrate ... \
    --keyset-source 'env:SLUICE_KEYSET_' \
    --redact 'public.users.email=hash:hmac-sha256:email_v1'
```

### Option C: control-table-backed

The `sluice_keysets` table (created automatically on first use, both
on PG and MySQL targets) stores the keyset rows in the target
database. Sluice startup reads them once into memory.

```sh
sluice migrate ... \
    --keyset-source 'db:public.sluice_keysets' \
    --redact 'public.users.email=hash:hmac-sha256:email_v1'
```

Operator-side management of the table (rotation, lookup, deletion) is
manual SQL / YAML for now — there's no `sluice keyset rotate` CLI in
v1.

## Step 2: declare redactions

```sh
sluice migrate \
    --source-driver postgres --source ... \
    --target-driver postgres --target ... \
    --keyset-source 'file:/etc/sluice/keyset.yaml' \
    --redact 'public.users.email=hash:hmac-sha256:email_v1' \
    --redact 'public.users.ssn=mask:ssn' \
    --redact 'public.payments.pan=mask:pan' \
    --redact 'public.users.name=randomize:dict:fake_names'
```

Or via YAML:

```yaml
# sluice.yaml
keyset_source: file:/etc/sluice/keyset.yaml
redactions:
  - column: public.users.email
    strategy: hash:hmac-sha256
    key: email_v1
  - column: public.users.ssn
    strategy: mask:ssn
  - column: public.payments.pan
    strategy: mask:pan
  - column: public.users.name
    strategy: randomize:dict
    dict: fake_names
dictionaries:
  fake_names:
    - "Alice"
    - "Bob"
    - "Carol"
```

If both YAML and CLI declare a rule for the same column, **CLI wins**
— with a loud `slog.WARN` line naming the column and the YAML strategy
that was skipped. This is documented precedence; if you don't want
the warning, remove the YAML entry.

## Step 3: run

The same `sluice migrate` / `sluice sync start` commands. Redaction
applies on the bulk-copy path AND on the CDC apply path, so a stream
that catches up after the migrate completes continues to redact every
change.

The startup audit log line names how many columns are redacted and
which strategies are in use (with `key=<elided>` so secrets don't leak
into logs):

```
INFO redaction configured stream_id=staging columns=4 strategies=[hash:hmac-sha256, mask:ssn, mask:pan, randomize:dict]
```

## How sluice preserves cross-stream determinism

The HMAC key (for `hash:hmac-sha256`) and the dict seed (for
`tokenize:dict`) come from the keyset. As long as two sluice streams
load **the same keyset**, the same input value produces the same
output:

- `user=alice@example.com` on stream-1 →
  `5a8e91…` on the staging-1 target.
- `user=alice@example.com` on stream-2 →
  `5a8e91…` on the staging-2 target.

This is **load-bearing for CDC**: if a row's email gets hashed on
stream-1's migrate AND later via stream-2's CDC apply, the value must
match exactly or the row-versions diverge.

## Verifying a redacted target

`sluice verify` has **no redaction awareness** — don't expect it to
"see through" redactions. `--depth=sample` (the row-hash verifier)
hashes the full row content, so on a redacted migration it flags every
redacted row as a mismatch *by design*: the source row has
`alice@example.com`, the target row has `5a8e91…`, and the hashes don't
match. (`--depth=sample` is also same-engine only — a cross-engine
verify refuses it loudly.)

So to verify a redacted target:

- Use **`sluice verify --depth=count`** — row counts are unchanged by
  redaction, so a count verify is unaffected and confirms every row
  made it across.
- If you want row-level hashing, scope `--depth=sample` to the
  **non-redacted** tables with `--include-table`.

## Common pitfalls

- **No `--keyset-source`, but `hash:hmac-sha256` declared.** Sluice
  refuses loudly at preflight (since v0.63.0; the prior hardcoded key
  was a deliberate clean break). Provision a keyset; there's no
  fallback.
- **Different keysets on different streams that should be
  cross-consistent.** Cross-stream determinism only holds when both
  streams load the same keyset rows. Operators sometimes
  inadvertently put a per-stream key in the file and then wonder why
  the surrogates differ between staging-1 and staging-2.
- **Forgot to declare any redactions.** sluice does not refuse-to-start
  on an empty redaction set — an operator who declared none gets a
  fully-plaintext target. What sluice *does* give you: an audit line at
  stream start summarizing the redaction scope (column count +
  strategies), and `sluice schema preview` annotates each redacted
  column in the DDL. Check both before a production run to confirm the
  set you intended is actually in effect.

## What's NOT in this recipe

- **JSON-path redaction** (`paths: [$.payment_method: tokenize]`
  inside a JSONB column). Tracked in the roadmap as item 15c; not yet
  shipped.
- **Reversible encryption** — sluice's redactions are one-way by
  design. If you need reversibility, add a reversible-encryption
  middleware outside sluice.
- **Keyset rotation CLI.** Manual SQL / YAML for v1.

## See also

- [`docs/redaction.md`](../redaction.md) — the per-strategy reference,
  every option, every format-preserving mask preset.
- ADR-0041 in [`docs/dev/adr/`](../dev/adr/) — the keyset persistence
  design, the two deviations from the draft, and the v0.63.0 clean
  break.
