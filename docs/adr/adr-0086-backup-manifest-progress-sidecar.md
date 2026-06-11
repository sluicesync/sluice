# ADR-0086: O(1) backup checkpoints — the manifest progress sidecar

- **Status:** Accepted (implemented; task #54)
- **Date:** 2026-06-11
- **Relates to:** ADR-0082 (the migrate-state O(1) checkpoint precedent
  and its loud one-way version sentinel — mirrored here), ADR-0084 §3
  (the `manifestCommitter` this changes; the crash/resume contract is
  preserved), ADR-0085 (the early anchor stamp — its pre-sweep manifest
  write IS this ADR's base write; commit timing unchanged), Bug 135
  (table-granular resume — the redo unit a lost checkpoint costs),
  Bug 116 (the proportional-version-bump posture this follows)

## Context

Every per-chunk and per-table checkpoint during `sluice backup full`
re-marshaled the ENTIRE manifest — full embedded schema included — and
re-Put the whole `manifest.json` (`manifestCommitter.commitLocked`).
The manifest grows linearly with table count, so total checkpoint work
was quadratic: the #38 scale probe (2026-06-11) measured
≈ `0.018·N + 2.77e-5·N²` seconds over N tables — **~78 hours of pure
manifest rewriting at 100k tables, ~322 days at 1M**. Everything else
in the backup path is linear; this was the only super-linear wall.

The checkpoints themselves are load-bearing: ADR-0084's crash contract
("a crash leaves at most tableParallelism tables to redo") and the
content-addressed upload skip both ride on durable per-event progress.
They cannot simply be debounced away (ADR-0082 rejected the same
trade: coalescing keeps the O(N) encode and widens the crash-replay
window).

## Decision

Split the in-progress manifest into a **base written once** plus an
**append-only delta sidecar**, and fold everything back into the one
self-contained `manifest.json` at success.

1. **Base manifest, written once (pre-sweep).** Schema, the ADR-0085
   anchor stamp, encryption header, and every pre-staged table entry —
   the heavy immutable parts. This is the SAME pre-sweep write ADR-0085
   introduced; its durability still triggers `BackupSnapshot.Commit`
   (crashed chain slots must survive for adoption — ordering
   unchanged).

2. **`manifest.progress.jsonl` sidecar.** Each checkpoint appends ONE
   compact JSON line — `{"attempt_id", "event": "chunk"|
   "table_complete", "schema", "table", "chunk"|"row_count"}` — via a
   new optional store capability `irbackup.Appender`. O(1) per event,
   independent of table count. `LocalStore` implements it with a
   single `O_APPEND` write + fsync (deliberately NOT tmp+rename —
   append-then-rename re-copies the file per call, the exact quadratic
   shape being removed).

3. **Truth = base + replay.** `irbackup.ReplayProgress` applies the
   sidecar's matching-attempt events onto the base's table entries.
   The replay is folded into the pipeline's two manifest decoders
   (`readManifest` / `readManifestAt`), so EVERY reader — backup
   resume, restore, verify, the broker's chain-root preflight, the
   chain walkers that can meet a crashed full (the ADR-0085 adoption
   surface) — sees one reconstructed truth without knowing the layout
   exists. After replay the DECODED manifest is normalized to the
   self-contained shape (sidecar reference cleared, schema-appropriate
   format version restored): replay is not idempotent, so the
   in-memory view must never be replayable again nor persistable in a
   shape that references a sidecar it already absorbed.

4. **Finalize folds back.** The final write restores the schema's own
   format version (`FormatVersionFor`: 1 or 2), clears the sidecar
   reference, writes the complete self-contained `manifest.json`, and
   deletes the sidecar (best-effort: with the reference cleared the
   file is inert; failing a finished backup over cleanup would be
   disproportionate). **A successful backup's on-disk layout is
   byte-shape-identical to the pre-ADR contract** — restore / verify /
   chain / incremental tooling and older binaries are unaffected on
   the happy path.

5. **Format-version gate (the ADR-0082 sentinel posture).**
   In-progress sidecar-layout bases are stamped
   `FormatVersionProgressSidecar = 3`. An older binary reading one
   would see only the base — completed tables look not-started — and
   would "resume" by redoing work and, worse, finish while leaving the
   sidecar behind; the version gate makes it refuse LOUDLY at the
   existing manifest preflight ("format version 3 is newer than this
   build supports (2); upgrade sluice") instead. The bump is
   proportional (the Bug 116 lesson): finalized manifests never carry
   it, so innocent successful backups keep restoring on older
   binaries.

6. **Stores without append keep the legacy behaviour.** `BlobStore`
   (S3/GCS/Azure) has no append primitive; emulating one via
   read-modify-write would be quadratic again, and per-event object
   sprawl trades one pathology for another. The committer falls back
   to the exact pre-ADR full-rewrite checkpoints (old format versions,
   no sidecar) — a named wart, logged WARN on corpora ≥ 1000 tables.
   The replay READ path needs only Get/Exists, so any store can resume
   a sidecar-layout crash regardless of its own append capability.

7. **Attempt binding.** The base carries a random per-attempt ID; every
   sidecar line repeats it; replay skips (and loudly counts)
   mismatches. This makes the layout immune to stale-sidecar debris
   without ordering tricks (see crash-window analysis).

### Resume semantics: unchanged by construction

Because the replay happens inside the decoder, the resume classifier
(`stageBackupTables` / `tableManifestFullyComplete`), the Bug-135
table-granular re-stream rule, the content-addressed same-path upload
skip, and the ADR-0085 anchor adoption all operate on the same
reconstructed shape they saw before — none of them changed.

## Crash-window analysis

Writer ordering per run: base-manifest Put → sidecar reset (Delete) →
[chunk Put → chunk-event append]\* → table-event append → … → final
manifest Put → sidecar Delete.

| Crash lands… | On-disk state | Outcome on next run |
|---|---|---|
| before the base Put | nothing durable (tmp+rename Put) | fresh start; uncommitted snapshot Close drops the slot (ADR-0085 unchanged) |
| between base Put and sidecar reset | new base (attempt B) + stale sidecar (attempt A lines) | replay skips every mismatched-attempt line, WARN names the count; state = the staged base — sound |
| between sidecar reset and first append | base + no sidecar | replay no-op (DEBUG: base authoritative); staged base — sound |
| between chunk Put and its event append | chunk bytes on store, not in the reconstructed view | table stays Partial → Bug-135 re-stream; the orphan chunk is overwritten index-by-index, the SHA skip avoids re-upload of identical bytes |
| mid-append (torn final line) | base + sidecar with a torn tail | replay tolerates exactly the FINAL torn line (WARN), the event is lost → that table re-streams; any OTHER malformed line is corruption → loud refusal |
| between final manifest Put and sidecar Delete | complete manifest (no reference) + orphan sidecar | sidecar is inert (replay is gated on the manifest reference); a later `--force-overwrite` run mints a new attempt ID and resets it — stale lines can never replay |

The base-manifest Put itself is atomic on `LocalStore` (tmp+rename) —
the same class as before this ADR.

## Version-gating matrix

| Reader ↓ / Artifact → | old finalized (v1/v2) | old in-progress (v1/v2, no sidecar) | new finalized (v1/v2) | new in-progress (v3 + sidecar) |
|---|---|---|---|---|
| **old binary (ceiling 2)** | reads | resumes (legacy layout) | reads — unchanged layout | **refuses loudly** (version gate) |
| **new binary (ceiling 3)** | reads | resumes (replay no-op; no reference) | reads | reconstructs base + replay |

Pinned by: `TestBackupFormatVersion_Bumped` (ladder),
`TestBackup_SidecarCrashResume_EndToEnd` (v3 stamp on the raw base +
reconstruction + finalize shape),
`TestBackup_ResumesOldFormatInProgressManifest` (new-reads-old),
`TestReadManifest_RefusesNewerFormatVersion` (the refusal shape an old
binary produces), `TestBackup_FormatVersion_Bug116` (finalized
manifests keep 1/2).

## Consequences

- Checkpoint cost is O(1) per event; the manifest is marshaled exactly
  twice per run (base + final). Pinned at 10k tables / 20k events by
  `TestManifestCommitter_SidecarCheckpointCost_10kTables` via
  Put/Append byte accounting (manifest Put count == 2, max delta line
  ≤ 512 B) — never wall time.
- A crashed sidecar-mode backup is resumable only by ≥ this version
  (older binaries refuse loudly). Release notes must call this out;
  the failure mode is a refusal naming the remedy, never silence.
- Blob-store backups keep the quadratic legacy checkpoints (named,
  WARN-logged at scale). If object-store-scale corpora materialize,
  the follow-up is segment-rotated delta objects behind the same
  `Appender` seam — the replay format already tolerates it.
- One new file may transiently exist in a backup directory
  (`manifest.progress.jsonl`); operators `jq`-inspecting an
  in-progress backup must consult it for live progress (the base
  under-reports by design).

## Alternatives rejected

- **Per-event delta objects (`manifest.progress/<seq>.json`) on every
  store.** Universal O(1) without a new capability, but 2·N tiny
  objects per run (2M files at 1M tables) — directory-scale pathology
  on local FS, request sprawl on object stores, O(N) deletes at
  finalize. The single-file append covers the store class the probe
  targets; blob stores keep known behaviour instead of gaining a new
  one.
- **Buffered/segmented appends for blob stores.** Amortized O(1), but
  a crash loses the buffered tail — silently widening the ADR-0084
  crash contract on exactly the store class where re-upload is most
  expensive. Deferred until demanded; the fallback is loud.
- **Read-modify-write "append" on blob stores.** O(size) per append —
  quadratic again, just with smaller constants.
- **Debounced full rewrites.** ADR-0082 already rejected this shape:
  keeps the O(N) encode, widens the crash window per checkpoint.
- **Schema-by-reference (split the schema into its own file, keep
  rewriting a slim manifest).** Removes the dominant constant but the
  rewrite stays O(tables + chunks) per checkpoint — still quadratic
  total; and it changes the FINALIZED layout (every reader/tool knows
  `manifest.json` is self-contained), a much bigger compatibility
  surface than an in-progress-only sidecar.
