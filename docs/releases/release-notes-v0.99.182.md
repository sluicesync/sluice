# sluice v0.99.182

**An internal-structure release: two structural extractions that pay down maintainability debt from the July repository audit, with zero user-facing or on-the-wire change. A shared trigger-CDC core removes the pgtrigger/sqlite-trigger duplication behind a dialect seam (keeping their divergent snapshot anchors separate, deliberately), and a shared migration-engine core (`migcore`) is carved out of the flat pipeline package to unblock its decomposition. All behavior is byte-identical; every existing pin passes unchanged.**

## Internal

**Shared trigger-CDC core (audit A-2).** The `pgtrigger` and `sqlite-trigger` engines (and, by transport substitution, `d1-trigger`) carried duplicate copies of the trigger-CDC position-token codec and the change-log prune-batching engine. The genuinely-identical protocol now lives once in a new internal package behind a small dialect contract: the `{"last_id":N}` position codec and the keyset-batched prune loop are shared, while the accepted engine-name family is a dialect-provided list — `pgtrigger` accepts `{postgres-trigger}`, `sqlite-trigger` accepts `{sqlite-trigger, d1-trigger}` (preserving the Bug-166 cross-engine resume fix), and a future trigger engine widens only its own list. Crucially, the parts that only *look* alike were kept separate: pgtrigger's contiguous-committed-prefix-plus-xmin-safety-lag snapshot anchor and sqlite's single-writer `MAX(id)` anchor are genuinely different guarantees, and merging them would have been a silent-loss regression rather than a deduplication — so they stay per-engine. Behavior is byte-identical and every pgtrigger/sqlite-trigger/d1-trigger unit and integration test passes unchanged; the shared codec's family acceptance is now additionally pinned as a class (one-, two-, and synthetic three-engine families across round-trip, cross-accept, and foreign-token refusal). This pays the duplication down before a third trigger engine would make it a third copy.

**Shared migration-engine core, `internal/pipeline/migcore` (audit A-1).** The `internal/pipeline` package had grown into a large flat module where backup, restore, and the sync streamer all sat on top of a shared migration engine that migrate also used, while the package's own top layers reached back into backup — a bidirectional coupling that blocked splitting the package along its natural seams. The engine-neutral core has been carved out downstream into `migcore`, which imports the pipeline root nowhere (verified acyclic): the chunk-boundary planners and PK-orderability logic, the copy-parallelism gate with its AIMD backoff and connection-budget resolver, the storage grow-gate coordinator, the schema-diff and cross-engine-supportability checks, the reparent tracker, and the run-summary and error-hint surfaces — about 3,000 lines out of the root package. The copy *orchestration* that is welded to resume state, migration-state persistence, redaction, and shard-stamping deliberately stayed in the root package; only the neutral core moved. This is the load-bearing prerequisite that unblocks carving backup and restore into their own sub-packages in a later change. It is a pure move — the chunking, gate, budget, grow-gate, and reparent behavior is byte-identical, proven across the new package boundary by the same-engine and cross-engine parallel-copy, resume, and backup/restore integration round-trips.

## Compatibility

**No breaking changes; drop-in; nothing to re-verify.** No flags, CLI surface, position-token format, chunk/manifest format, or on-disk layout changed. Both items are pure internal moves validated by the full existing unit and integration suites plus the `-race` gate; there is no operator-visible behavior difference.

## Who needs this — action required

- **No one.** This release contains no bug fixes, no feature changes, and no format changes — only internal refactoring that reduces future maintenance risk. Upgrade at leisure.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.182 · **Container:** ghcr.io/sluicesync/sluice:0.99.182
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
