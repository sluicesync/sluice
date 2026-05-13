# Prep: ADR audit + new ADRs (roadmap §8)

> **Status: SHIPPED** (initial audit). ADR-0006 / ADR-0007 had their "forward-looking" caveats removed; ADRs 0008-0010 landed. Note: ADR-0024 has since regressed and been re-corrected (status: Accepted, with v0.6.0 / v0.39.0 implementation history).

Roadmap reference: [docs/dev/roadmap.md §8](../roadmap.md). Existing ADRs: [docs/adr/](../../adr/).

## Goal

§8 is partly already done — `docs/adr/` already holds the original 7 ADRs the roadmap names (`adr-0001` through `adr-0007`). What's left is:

1. **Audit pass.** Each existing ADR was written when the underlying work was forward-looking. Several reference roadmap sections that have since shipped; their `Status` lines and a few sentences of "this is planned" need to become "this is implemented and stable". A few decisions have evolved (e.g., the three-phase apply is now three-and-a-half phases after §7's identity sync).
2. **New ADRs for decisions accumulated through §2-§7.** Three real architectural choices made during implementation aren't captured anywhere durable yet. Adding them as ADRs prevents relitigation.
3. **Cross-references in `architecture.md`.** The arch doc currently doesn't mention the ADRs; readers learning the project shape should be pointed at them in-line.

Out of scope:

- Any retroactive design changes. ADRs document decisions; they don't relitigate them.
- ADRs for purely tactical choices that already live well in code comments (e.g., the COPY-vs-batched dispatch in §6's `RowWriter` is documented in the package comment and doesn't need a separate ADR).

## Audit findings

| ADR | Status today | Action |
| --- | --- | --- |
| 0001 IR-first translation | Accepted; accurate. References ChangeApplier as if forward-looking but it's now shipped | Minor wording update |
| 0002 Sealed interfaces | Accepted; accurate | None |
| 0003 kong + koanf | Accepted; accurate | None |
| 0004 Three-phase schema apply | Accepted; *slightly stale* — §7 added phase 3.5 (SyncIdentitySequences) between bulk-copy and indexes | Update to "Three-phase schema apply + identity sync"; document phase 3.5 |
| 0005 MySQL flavors | Accepted; accurate | None |
| 0006 pgoutput over wal2json | "Accepted (forward-looking — no CDC reader is implemented yet at the time of this ADR; see roadmap §3)". §3 has shipped | Status → "Accepted"; remove the "no CDC reader yet" caveat |
| 0007 Position persistence | "Accepted (forward-looking — see roadmap §5; the control table is not implemented yet at the time of this ADR)". §5 has shipped | Status → "Accepted"; remove "not implemented yet" caveat; brief note that the implementation matches the ADR |

## New ADRs to add

Three decisions made during §2-§7 implementation are worth pinning:

### ADR-0008: go-mysql for MySQL binlog parsing

Parallels ADR-0006's pgoutput-vs-wal2json analysis but for the MySQL side. The choice was between rolling our own binlog parser, using `github.com/go-mysql-org/go-mysql`, or using `canal` (a higher-level wrapper of go-mysql). go-mysql's lower-level `replication` package won — mature, used by canal and TiDB DM, gives us message-loop control without canal's opinionated schema-cache management. The library is a real dependency (not a sub-package of pgx), so the ADR documents that explicitly.

Outline:

- **Status**: Accepted.
- **Context**: MySQL CDC needs binlog parsing. Three options: (a) own parser, (b) go-mysql's `replication` package directly, (c) `canal` higher-level wrapper.
- **Decision**: go-mysql/replication directly. Library is mature (used by canal, TiDB DM, others). Lower-level interface gives us full message-loop control needed for the IR-typed change events the project produces.
- **Consequences**: Real top-level dependency in `go.mod`. Schema cache, DDL handling, and position bookkeeping are sluice's responsibility (not the library's). Both binlog file/pos and GTID-set positions are supported by the library's API; sluice picks at startup.

### ADR-0009: Streamer as separate orchestrator (not a Mode flag on Migrator)

During §4 (snapshot-to-CDC handoff), the design choice was to add a new `pipeline.Streamer` type for long-running snapshot+CDC, rather than adding a `Mode` field to the existing one-shot `Migrator`. The decision was deliberate; this ADR pins it.

Outline:

- **Status**: Accepted.
- **Context**: Continuous-sync mode has different lifecycle (long-running, ctx-driven shutdown), different required parameters (ChangeApplier), and different failure modes (mid-stream errors that warrant retry rather than abort) than the one-shot `Migrator`.
- **Decision**: Add `pipeline.Streamer` as a separate type. Share the schema-apply + bulk-copy phases via a private `runBulkCopy` helper. Do not extend `Migrator` with a Mode flag.
- **Consequences**: Two orchestrator types instead of one. Each is parameter-light for its actual use case; no "valid only if Mode == X" comments. The shared bulk-copy helper means the duplication isn't real — both paths route through the same schema-apply sequence.

### ADR-0010: Idempotent applier semantics

The applier ships with upsert-on-Insert (using ON DUPLICATE KEY UPDATE on MySQL, ON CONFLICT DO UPDATE on PG) and tolerant Update/Delete (zero-affected-rows is fine). Together, these make any prefix of the change stream replayable, which is the load-bearing property for CDC resume to work without separate dedup.

Outline:

- **Status**: Accepted.
- **Context**: After a process restart, the change stream replays from the last persisted position. If the restart happened *after* a data write but *before* the position update committed, the replayed change must be a no-op rather than a duplicate-key error.
- **Decision**: Upsert-on-Insert (per-engine: ON DUPLICATE KEY UPDATE for MySQL 8.0.20+ row-alias form, ON CONFLICT (pk) DO UPDATE for PG). Tolerant Update/Delete: zero-affected-rows is logged at debug, not raised. Tables without a PRIMARY KEY fall back to plain INSERT (best-effort idempotency, documented).
- **Consequences**: Resume after a partial-apply is safe. Tables without PKs are not safe for continuous-sync (replays produce duplicates) — the applier package comment surfaces this prominently. A future strict-mode flag could error on Update/Delete miss for operators who want loud failure on data drift; punt until a real case appears.

## Architecture cross-references

`docs/architecture.md` gets pointers to ADRs in the relevant sections. The mapping:

| Section in architecture.md | ADR(s) to reference |
| --- | --- |
| IR overview (intro to `internal/ir`) | ADR-0001, ADR-0002 |
| Engine pattern (registry, capabilities, flavors) | ADR-0005 |
| Schema apply phases (Migrator + Streamer) | ADR-0004 (with the §7 update), ADR-0009 |
| CDC overview (when added) | ADR-0006, ADR-0008 |
| Continuous sync (Streamer + applier + position) | ADR-0007, ADR-0009, ADR-0010 |
| CLI / config | ADR-0003 |

Each cross-reference is a short inline link, not a paragraph; the ADR is the authoritative source.

## Files to add / touch

- `docs/adr/adr-0004-three-phase-apply.md` — small update to acknowledge phase 3.5
- `docs/adr/adr-0006-pgoutput.md` — Status update; drop "not implemented yet" wording
- `docs/adr/adr-0007-position-persistence.md` — Status update; drop "not implemented yet" wording
- `docs/adr/adr-0001-ir-first-translation.md` — minor wording on ChangeApplier
- `docs/adr/adr-0008-go-mysql.md` — NEW
- `docs/adr/adr-0009-streamer-vs-mode-flag.md` — NEW
- `docs/adr/adr-0010-idempotent-applier.md` — NEW
- `docs/architecture.md` — inline ADR cross-references in 6 sections

No code changes. No tests. Pre-commit gate is just gofumpt + vet + lint + go test (which all pass on doc-only changes).

## Open questions for the user

1. **ADR-0004 update vs. supersede.** Phase 3.5 is a small addition to a three-phase model; updating ADR-0004 in place keeps it the canonical reference. The alternative — superseding 0004 with a new "Schema apply phases" ADR — feels heavyweight for a one-phase addition. *Recommendation:* update in place. Confirm?
2. **ADR-0008 scope: should it cover the Postgres CDC reader's library choice too?** ADR-0006 already covers pgoutput vs. wal2json, but not pglogrepl as the Go-side library. The pgx ecosystem makes pglogrepl close to the obvious choice; an ADR would mostly document "we use pgx's helper". *Recommendation:* skip a "we use pglogrepl" ADR; the choice is implicit from ADR-0006 (use pgoutput) + the project's existing pgx commitment. Confirm?
3. **Status convention.** The two forward-looking ADRs (0006, 0007) have status `Accepted (forward-looking — ...)`. The new ones can just say `Accepted` since the work is shipped. *Recommendation:* drop the "(forward-looking ...)" qualifiers from 0006/0007 since both have shipped; new ADRs say `Accepted`. Confirm?
4. **Length.** The original 7 ADRs are 23-37 lines. The new ones can match that — Status / Context / Decision / Consequences in 25-40 lines each. Anything longer probably belongs in `docs/architecture.md` or a prep doc. Confirm?

## Suggested first-cut prompt for Claude Code

> "Read CLAUDE.md, docs/dev/notes/prep-adrs.md, and the existing 7 ADRs in docs/adr/. Propose the design before writing: (1) the exact wording changes to the audit-flagged ADRs (0001, 0004, 0006, 0007), (2) the 25-40 line drafts for the three new ADRs (0008, 0009, 0010), (3) the cross-reference layout in docs/architecture.md (which sections, which ADRs each section names). Note any deviation from the prep doc with a why. Stop after the design for review."
