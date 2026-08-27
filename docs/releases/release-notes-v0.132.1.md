# sluice v0.132.1

> **Correction (2026-08-27):** four items originally described in these notes MISSED this tag — a landing step ran against the wrong working tree — and shipped in **v0.132.2** instead: the pgtrigger probe-timeout bounds, the `schema add-table` UNLOGGED census, the G2 environmental-premise pins, and the STATEMENT-DML lead's `=`-cut for unquoted numeric literals (this tag carries the quote/paren cut). Everything else below is in this tag as described. The omission was caught by the post-publish learnings sweep's tag-tree ground-truthing.

**The audit fast-follow: the fresh post-v0.132.0 blind audit found the new replica-source door was itself narrower than its name — blind to MariaDB named-connection (multi-source) replicas — and this patch closes that plus eight hardening findings from the same pass.** Drop-in from v0.132.0 — no schema, format, flag, or command change; no new error codes.

## Fixed

**The `SLUICE-E-CDC-REPLICA-NO-LOG-UPDATES` door now sees MariaDB named-connection replicas.** v0.132.0's replica-source refusal probed only the bare `SHOW REPLICA STATUS` spellings — which return **zero rows** for a `CHANGE MASTER 'name' TO …` named connection, the standard MariaDB multi-source idiom (observed live, twice independently, on real MariaDB 11.4). Combined with MariaDB defaulting `log_slave_updates=0`, the exact silent-loss configuration the door exists to refuse walked straight through it: cold copy complete, CDC tail silently empty for all replicated traffic. The probe now also runs MariaDB's `SHOW ALL REPLICAS STATUS` / `SHOW ALL SLAVES STATUS`, with a deliberately-pinned posture: MySQL's syntax error on the ALL forms is tolerated only after a bare form already succeeded (bare-success + ALL-error identifies a MySQL, whose bare form is channel-complete), so the toleration can never degrade the door on a healthy server. Pinned against a real named-connection MariaDB with an anti-vacuity floor proving the bare-form blindness.

**Value-rewriting `ALTER COLUMN TYPE` shapes that cannot be forwarded now refuse loudly under the DEFAULT `--schema-changes=forward` too.** v0.132.0 made typmod-only rewrites detectable, but two family classes — `interval(p)` and every array-element typmod (e.g. `numeric(10,4)[]→(10,1)[]`, which rounds every stored element) — classify as a change while projecting to an unchanged internal type, so default forward mode emitted no boundary and silently diverged; only refuse mode was loud. The reader now refuses any detected `ALTER COLUMN TYPE` whose changed column's projected type is unchanged (per-column, so a mixed statement can't ride a moved table signature past the check), with the drained-model recovery hint. The same predicate also catches projection-identical type swaps (`time↔timetz`) and dynamic-OID families. A new enumeration gate derives every typmod-carrying family from the Postgres catalog and asserts each either moves the projection or is on the documented refuse list — the class is closed by enumeration, not discovery. Legitimate forwarding is pinned unaffected (identical re-sends unreachable; temporal-precision ALTERs still forward; numeric and varchar shrink convergence integration-pinned end-to-end). A pre-tag value-fidelity review then found (and this release also closes) the gate's own sibling gap: a DROP COLUMN combined with a projection-invisible ALTER in one relation delta classified as the drop alone and bypassed the gate — it now runs for the drop shape too, with a name-keyed column compare so a middle-column drop's ordinal shift cannot hide the surviving column's change (pinned tail-drop, middle-drop, and plain-drop-still-forwards cells, mutation-verified).

**The signature heal now preserves its forensic evidence and leaves a durable record.** The v0.131.3 crash-stale heal re-signed a chain after overwriting the one artifact that proved something had been wrong — the non-verifying signature — and its WARN was the only trace, easily lost in a scheduled job's log. Before re-signing, the old signature is now preserved verbatim beside the chain (`lineage.json.sig.pre-heal-<ts>`) and a durable JSONL record is appended (`maintenance-heal.log`: timestamp, operation, key id, the verify failure that triggered the heal); `backup verify` surfaces every heal record as an informational line. The wrong-key refusal shipped in v0.131.5 is unchanged.

**The `SLUICE-E-CDC-STATEMENT-DML` refusal no longer echoes statement payloads.** The v0.132.0 belt quoted up to 160 bytes of the offending statement — which is binlog text carrying row values, i.e. potentially PII riding a refusal into logs and reports, bypassing `--redact`. The refusal now carries the verb, byte length, a short digest for correlation with the operator's own query log, and a sanitized lead cut before the first quote, paren, **or `=`** — so string *and unquoted numeric* values (`SET ssn=…`) never reach the error. A sibling sweep verified every other new refusal/WARN renders schema-level metadata only.

**Every pgtrigger open-path probe is now bounded at 15 seconds.** The relay-shape probe read a lockable user table, so a queued `ALTER`/`VACUUM FULL` behind a long transaction could park every CDC open indefinitely with no output — a WARN-only detector wedging the stream it protects. All five open-path probes (the audit named three; a sibling sweep found two more) now derive bounded contexts: WARN-probes degrade to their probe-error WARN on timeout, and the fail-closed capture-shape door refuses with a timeout-specific message. A new cross-engine AST roster gate asserts every CDC-open probe in all three engines derives a timeout — the gate caught its own first gap during development.

**`schema add-table` refuses an UNLOGGED table before any side effect.** Adding an unlogged table live to a spanning Postgres sync would have backfilled its rows and then frozen it (the FOR ALL TABLES publication silently excludes it); the previous behavior blocked it late with a misleading message after creating the target table. The registration path now runs the same coded census (`SLUICE-E-CDC-UNLOGGED-TABLE`) before the dry-run report, target DDL, or snapshot.

## Internal

The published remedy SQL for `SLUICE-E-CDC-STATEMENT-DML` (and the `sql_log_bin` session probe) now states its `performance_schema=ON` precondition — MariaDB defaults it OFF, under which the documented query hard-errors (observed). G2's two environmental premises (FOR ALL TABLES silently excludes unlogged tables; `SET UNLOGGED` succeeds under it but is refused for scoped membership) are now pinned directly against real Postgres and ride the version matrix. A latent test-fixture bug (interval's array OID used for the scalar) was found and fixed by the new enumeration gate's development.

## Compatibility

Drop-in from v0.132.0. Two refusals reach configurations they previously missed: a **MariaDB named-connection replica** source without log updates (previously silently lossy — the refusal names a loss that was already happening), and **array/interval typmod ALTERs under default forward mode** (previously silent divergence; the refusal carries the drained-model recovery). No new error codes; the site's error-code table is already current.

## Who needs this — action required

- **Anyone syncing from a MariaDB multi-source (named-connection) replica**: on v0.132.0 and every earlier release, replicated traffic was silently absent from your CDC tail — upgrade, and if the door then refuses, point the sync at the primary (or enable `log_slave_updates`) and run `sluice verify` against your targets.
- **Postgres sources on default `--schema-changes=forward`** that ALTERed an interval precision or an array element's precision/length mid-sync: that rewrite silently diverged the table on every prior release — verify and resnapshot it.
- Everyone else: upgrade normally; no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.132.1
```

Container images: `ghcr.io/sluicesync/sluice:0.132.1` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
