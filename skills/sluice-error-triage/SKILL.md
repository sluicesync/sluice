---
name: sluice-error-triage
description: Use when a sluice command failed, to turn its exit code + `SLUICE-E-*` code + envelope `error` object (or a raw error) into a root cause, the recovery command, and whether that recovery needs human approval. Read-only — it diagnoses, it does not re-run the failed command. Trigger when a sluice command errored, exited non-zero, or the user asks "why did this fail / what does SLUICE-E-X mean / how do I recover".
---

# sluice-error-triage

Map a sluice failure to its root cause and the correct next action. Entirely read-only: this skill reads codes and state, it never re-runs the failing command (and never adds a destructive flag on its own).

## When to use
Any `sluice` invocation exited non-zero, printed a `SLUICE-E-*` code, or emitted a `status:"refused"`/`"failed"` JSON envelope — and you need to know the cause, the fix, and whether the fix is safe to apply automatically.

## Inputs you need
- The **exit code** (the single most important branch — see step 1).
- The **`SLUICE-E-*` code** if present (on the envelope `error.code`, or in the `--log-format json` stream's `code` attribute).
- The envelope `error` object (`{"message","code","hint"}`) or the raw stderr text.

## Steps

1. **Branch on the exit code first** (`AGENTS.md` / `docs/operator/error-codes.md`):
   - **0** — success (`verify`/`diff`/`sync health`: success *and* clean). Nothing to triage.
   - **1** — runtime failure, OR a `verify`/`diff`/`sync health` **drift/stale** result. A real failure to fix, or a genuine data/lag difference to report (hand a drift to `fidelity-verify`).
   - **2** — config error (`--config` unloadable/unparsable), or a read-side command that **could not run at all**.
   - **3** — **named refusal (a decision point).** sluice declined by policy and named the remedy — retrying unchanged fails identically. The remedy is often a **destructive** flag needing human approval. Do NOT retry; surface `error.hint` and stop.
   - **80** — kong usage/parse error (unknown flag, missing required arg). Fix the command line; no sluice code ran.

2. **Resolve the `SLUICE-E-*` code** against the table in `docs/operator/error-codes.md` — read off *class* (runtime vs refusal), *cause*, and *remedy*. The `class` drives the exit code (refusal → 3, runtime → 1). Common ones:
   - `SLUICE-E-CONNECT-REFUSED` / `SLUICE-E-CONNECT-AUTH-FAILED` / `SLUICE-E-CONNECT-DATABASE-MISSING` (runtime) — fix DSN host/port, credentials, or db name.
   - `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` (refusal) — target table has data. Remedy names a **destructive** flag (`--reset-target-data --yes` for sync, `--force-cold-start`) or `--resume` for migrate → approval-gated.
   - `SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED` (refusal) — `--enable-pg-extension <ext>`.
   - `SLUICE-E-BULKCOPY-TABLE-FAILED` (runtime) — a **generic wrapper** for any per-table copy failure, and the case where **`code` and `hint` mislead: the real cause and correct remedy are in the `message`, not the `code`.** A value-level failure at copy time — a MySQL zero-date, an out-of-range `BIGINT UNSIGNED`, a NUL byte — surfaces here as `SLUICE-E-BULKCOPY-TABLE-FAILED` with the actual fix in `message` (`--zero-date=null|epoch`, a `--type-override`, clean the data), while the `SLUICE-E-VALUE-*` names below **never appear in the envelope `code`**. So for this code, **match on `message`, not `code`** — and ignore the generic `hint`: `--resume` re-hits the same offending row identically, and `--exclude-table` silently drops the whole table's data. Apply the message's remedy instead.
   - `SLUICE-E-VALUE-ZERO-DATE` → `--zero-date=null|epoch`; `SLUICE-E-VALUE-NUL-BYTE` → clean the data or `--type-override COL=bytea`. **These are the underlying causes carried inside a `SLUICE-E-BULKCOPY-TABLE-FAILED` `message` at migrate time (above), not standalone envelope `code`s — read them out of `message`.**
   - `SLUICE-E-INDEX-STATEMENT-TIME-LIMIT` / `-INDEX-DIRECT-DDL-DISABLED` (runtime, PlanetScale) — `--resume` finishes indexes / disable safe-migrations for the run.
   - `SLUICE-E-CDC-REPLICATION-PERMISSION` (runtime) — `ALTER ROLE x REPLICATION`.

3. **Classify the remedy's blast radius.** If the remedy is a plain re-run, a DSN fix, or a read-only follow-up → safe to propose running. If it names any **destructive flag** (`--reset-target-data`, `--force-cold-start`, `--yes`, `backup prune`/`compact` without `--dry-run`, `slot drop`), it is a decision point — flag it as needing explicit human approval for that specific invocation (`skills/README.md` safety model).

4. **A codeless failure is usually a normal operator state, not a bug — read the `message` before escalating.** An exit-1 `status:"failed"` with **no `error.code` and no `hint`** is most often an ordinary decision point whose fix is stated in the `message`: re-running an already-complete migrate (`migration_id "…" is already complete; drop the target tables to redo, or use a different --migration-id`), a wrong restore/backup passphrase (`unwrap chain cek (wrong passphrase / KMS key?)`), and similar. Read the `message` and hand back its stated fix — do **not** jump to filing an issue. Only when the cause is genuinely unexplained (a real suspected bug, an unexpected mid-run `"failed"`) assemble an operator-bundle: `sluice diagnose --target-driver <drv> --target "$SLUICE_TARGET" --stream-id <id> -o bundle.zip` (add `--source-driver`/`--source`; `--privacy basic|standard|verbose` controls inclusion). Note `diagnose` **requires `--stream-id`**, so it fits a *sync* failure — a failed one-shot `migrate` has no stream id, so triage it from its `message`, not a bundle.

## What you return
- **Root cause:** the `SLUICE-E-*` code (or exit-code class) restated as a one-line cause.
- **Recovery command:** the exact next command from the remedy, DSNs via env (`SLUICE_SOURCE`/`SLUICE_TARGET`), never in argv.
- **Approval needed?** YES if the recovery uses a destructive flag (name it, and stop for human sign-off); NO for a safe re-run/config fix.
- **If unclear:** the `sluice diagnose` line to produce a bundle for an issue.

On exit 3 / `status:"refused"`: surface `error.hint` verbatim and wait — do not retry unchanged, and do not pre-authorize the destructive remedy.

## References (canonical — don't duplicate)
`docs/operator/error-codes.md` (code | class | cause | remedy + the exit-code taxonomy) · `AGENTS.md` (envelope shape, taxonomy, destructive-flags list) · `skills/README.md` (safety model) · `sluice <command> --help`.
