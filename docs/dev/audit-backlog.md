# Audit backlog — the findings that are NOT roadmap items

Findings from the periodic blind audits that are real, confirmed, and **too small or too cross-cutting to earn their own roadmap item** — but not so small that losing them is free.

## Why this file exists

The audit reports live in `workspace/`, which is **gitignored** (`.gitignore:99`). Every audit since 2026-08-01 has recorded its MEDIUMs, LOWs and gate proposals there and nowhere else, so the entire backlog existed on exactly one machine with no copy. The 2026-08-05 audit flagged this as a finding in its own right — and it had already caused two concrete errors:

- the "nine of ten §4 gate proposals are open" figure was repeated as fact in the roadmap **and** in project memory for four days; the true count, re-derived from the code, is **6 built / 2 partial / 2 open**;
- the 08-04 MEDIUMs were attributed to the 08-01 audit in the roadmap paragraph that was supposed to be tracking them.

Both are the doc-lags-code shape the working agreements name. A note *about* backlog durability rots exactly like anything else does, so the durable copy is here, in git.

**Scope rule:** anything with a confirmed silent-loss consequence becomes a numbered roadmap item, not a line in this file. This is for the tier below that.

## Open — 2026-08-05 audit

Full report: `workspace/repo-audit-2026-08-05.md` (untracked). Grade B+; three CRITICALs filed as roadmap items **131** (lane routing / secondary unique), **132** (GTID mid-tx checkpoint), **133** (`--where` inet family), **134** (SQLite index-name scoping). A-3 and B-5 are fixed.

### HIGH, not yet filed as items

| ID | Finding | Location |
|---|---|---|
| B-2 | Raw scan bytes spelling `\x` + valid hex are silently hex-decoded on the cross-engine IR lane. Fix is path-aware decode: hex only in `decodeTuple`, never on the scan path. MySQL sibling at `row_writer.go:1120`. Note B-1's wire pin (shipped) does **not** close this — that fixed the CDC rendering, this is a decode ambiguity. | `postgres/value_decode.go:377` |
| B-4 | RDS/no-FTWRL snapshot fallback opens the snapshot **then** captures the position — the one ordering where a commit in the window lands in neither. Reversing yields duplicates the idempotent apply absorbs; the airtight fix is `LOCK INSTANCE FOR BACKUP` + `performance_schema.log_status`. | `mysql/cdc_snapshot.go:451/:464` |
| B-6 | `verifyLineageNeedsWalk` never checks `Segments[0].Dir`, so a prune-to-floor chain sends `Restore.Run` to the retired root manifest while `backup verify` walks the catalog floor. Compounded by C-8. | `backup/restore.go:317` |
| B-8 | The Bug 226 write-core roster discovers cores by the `bufW` FIELD NAME. **Confirmed by executed mutation:** a fourth core using `out *bufio.Writer` stays GREEN. Needs dual-signal discovery (type-based, or marshal+newline shape). | `blobcodec/backup_chunk_line_limit_test.go` |
| B-9 | ADR-0108's ambiguous-commit proof rests on the 1062 collision, which is inert for truly keyless tables — and the v0.111.1 vtgate 1105 widening arms it on the exact field-report path. Fix: post-table `COUNT(*)` reconciliation when a keyless table's flush retried. | `mysql/row_writer.go:553` |
| B-10 | `schema diff` walks Tables+Views only: `ir.Schema.Sequences` and `Table.RLSEnabled/RLSForced/Policies` are never compared. A target-side `DISABLE ROW LEVEL SECURITY` reads as "in sync", exit 0. | `ir/diff/schema_diff.go:386` |
| CDC-4 | Warm-resume replay decodes old binlog events against the *current* `information_schema`; a same-column-count DDL during downtime silently remaps replayed values. Min gate: compare the TABLE_MAP type vector, refuse loudly. | `mysql/cdc_reader.go:1316` |

### MEDIUM

`C-1` `IsTableEmpty` substring classifiers gate the not-empty refusal (classify by 1146/42P01 instead) · `C-2` `isPGSourceEngine` hand-kept two-name list with no registry gate — **attempted 2026-08-05 and backed out; read this before retrying.** The classification itself genuinely cannot be derived (the engine identity is a lineage-recorded string from a backup manifest, so at restore time the source engine may not even be registered — the names ARE the durable record). So the gate must be a fail-by-default roster that forces a decision when a NEW engine registers, and the hard part is enumerating registered engines. Source-scanning does not work: `engines.Register` takes an `Engine` STRUCT, not a name (`mysql/engine.go:798`), so the names live in `Name()` implementations and flavor tables, and a naive scan finds 2 of 11. Importing `engines/all` from a test would enumerate them exactly but no test in the tree does that, and `migcore` is on the wrong side of the archgate boundary for it. Either put the roster in a package that legitimately links every engine, or derive the names from the flavor tables. **The attempt's anti-vacuity floor is what caught the broken walker** (it failed with "discovered only 2 … expected at least 10" rather than passing on near-zero) — keep that floor in any retry. · `C-3` `backup incremental`'s change-chunk lane has **no byte ceiling** — item 116 P3 reached the data-chunk lane only, and the commit's enumeration claim is false for this lane · ~~`C-4` compaction re-stamps chunk SHA/RowCount from its own writer, never re-reads~~ — CLOSED on branch 2026-08-05 (roadmap item 129) · `C-6` item 118's multi-DB preflight runs per-database inside the copy loop while comments claim the N−1 half is fixed · `C-7` FK drift invisible in the `schema diff` text render and JSON summary counts · `C-8` prune/compact never delete `.sig` siblings, so a retired root still cryptographically verifies · `C-9` appliers diverge on nil-Before Updates · `C-10` update-as-upsert + TOAST-omitted After + absent target row fabricates a partial row · `C-11` unknown-table drift: PG drops-with-WARN forever, MySQL halts loudly, divergence unrecorded (**decision needed**, not a fix) · `C-12` GHCR image not attested while "every release artifact" is claimed · `C-14` per-row geometry SRID silently dropped on both engines.

### Gate proposals not yet built

`G-1`/`G-2`/`G-3` ride items 131/132 (G-3 is **built** — the item-127/A-3 pins). `G-4` bytea family-matrix row ground-truthed with `octet_length()` · `G-5` binlog BIT(N>1) integration column (unit matrix **built**) · `G-6` reflective compared-or-exempt rosters over `ir.Schema`/`ir.Table` and `TableDiff` · `G-7` prune-to-full-only × {plaintext, encrypted, signed} restore cell · `G-8` dual-signal write-core discovery · `G-9` keyless-retry failpoint pin · `G-10` docsync markers (error-codes contiguity **built**; psdb-host×driver, engine-list ×3 sites, README freshness floor outstanding) · `G-11` lock-free-branch ordering pin · `G-12` adopt the invariant sweep as a standing queue.

### Invariant sweep

55 claims: 33 verified, 17 unverified, 5 suspect-false. One suspect-false (the compaction PK-change premise) became CRITICAL A-3 and is fixed; the C-13 binlog-width premise is fixed. Each unverified claim ships with a proposed sub-20-line check in the report.

## Open — 2026-08-01 audit

§4 MEDIUMs: `verifySchemaHashes` never runs for a bare full; the broker hand-rolls a subset of the shared preflights and drops target tables off an UNVERIFIED manifest. §4 gate proposals: **6 built, 2 partial, 2 open** — open are the IR field-classification/DDL property gate and the goroutine global-read analyzer; partial are the bulk-write contract table and the shipped-default gate rule.

## Open — 2026-08-04 audit

MEDIUMs: ~~compaction re-stamps its own verify hashes (= C-4 above)~~ — **CLOSED on branch 2026-08-05 with roadmap item 129**: smart compaction now re-reads every rewritten chunk through the store with the real reader and compares the decoded count against the one it recorded; sibling sweep at the gate (the non-smart path moves chunks verbatim carrying their original hash, `prune` rewrites no bytes). `mergeAfterImage` dead DDL barrier; `mergeAfterImage` dead DDL barrier; `trigger prune` missing peer-stream warning; CI SHA-pinning stops short of the merge/publish workflows; `mergeAfterImage` allocation. Gate repairs: all four mutation-inert gates fixed with mutation proof. Still open: the manifest-consumer gate's one-directory scope, `pgIndexName`'s self-oracle, the curated `canonRoster`, the one-package dispatch pin, archgate's two inert rules, and a cited-but-nonexistent `TestAutoResnapshotReason_*`.
