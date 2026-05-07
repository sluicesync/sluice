# sluice v0.16.1

Two-bug patch from the v0.16.0 test cycle. v0.16.0 shipped logical-backups Phase 2 (cloud backends + resumable writer); both findings are operational papercuts on top of an otherwise-clean cloud-roundtrip surface — round-trips and cross-engine restore worked, but the URL-prefix and resume-granularity gaps broke the workflow shapes the release promised. Fixes are small, targeted, and pinned by new tests.

## Fixed

- **Bug 33 — `--target=s3://bucket/prefix` silently drops the prefix; chunks land at bucket root.** `gocloud.dev/blob.OpenBucket` consumes only the bucket name from the URL — the path-after-bucket component is dropped without warning. v0.16.0's `BlobStore` therefore wrote every key to bucket root regardless of what the operator put in the URL. Side effect: multiple backups in one bucket couldn't coexist — they all collided at root and tripped the "completed backup already exists" guard. Fix: `BlobStore` now extracts the path-after-bucket at construction time and prepends it to every key on Put / Get / List / Exists / Delete; List results are stripped of the prefix so callers see paths relative to it (matching `LocalStore`'s contract). Applies to all URL schemes (`s3://`, `gs://`, `azblob://`); `file://` is exempt because gocloud's fileblob driver treats the whole path as the bucket root.

- **Bug 34a — No "resuming" log line emitted on resume detection.** v0.16.0's resume code ran silently; operators couldn't tell from the log whether a re-run had started fresh or picked up a partial. Fix: explicit `INFO resuming from partial backup` + `INFO resume plan` (per-table fan-out: which tables are already complete, which still need work) + per-table `INFO skipping table — already complete in partial backup` lines.

- **Bug 34b — Resumable writer's per-chunk skip wasn't actually wired up.** The Phase 2 design + v0.16.0 release notes promised "per-chunk skip via `BackupStore.Exists` + manifest's recorded SHA-256" — but the v0.16.0 implementation only checkpointed the manifest at table boundaries. A kill mid-table forced the entire table to be re-bulked from scratch on resume. Fix: per-chunk manifest checkpointing (manifest commits after every chunk, not just every table), plus a pre-write skip path in `backupTable` that consults the prior manifest's `ChunkInfo` for the in-flight chunk index — if `BackupStore.Exists` reports the chunk path is still on the store and the recorded SHA-256 matches, the orchestrator advances the row cursor over that chunk's rows without opening a writer or issuing a Put. Mid-table kills now leave a manifest with `Partial=true` on the in-flight table; the resume picks up at the next un-completed chunk.

## Compatibility

- **No breaking IR / CLI changes.** `--target` / `--from` URL flag shapes are unchanged; pre-existing local-FS path is untouched. A new `Partial bool` field on `ir.TableManifest` is additive (omitted on fully-complete entries; pre-v0.16.1 manifests treated as complete-by-default for backward compat) — older sluice ignores unknown fields.
- **Manifest write count slightly higher** because of per-chunk checkpointing — every chunk write is now followed by a manifest commit. The cost is one extra Put per chunk; for large backups (hundreds of chunks) this is trivial relative to chunk-write cost. The benefit is mid-table resume actually works.
- **Operators with v0.16.0 partial backups** can re-run against the same destination with v0.16.1: any tables fully completed in the v0.16.0 partial run will be skipped (the v0.16.0 manifest's per-table entries, which were only ever persisted at table boundaries, default to `Partial=false` on read and route through the skip-whole-table path); the next un-completed table is re-streamed. Not an in-place upgrade — old partial state moves cleanly forward.

## Who needs this

- **Anyone using `sluice backup full --target=s3://...` (or `gs://`, `azblob://`) with a path-after-bucket prefix** — Bug 33 affects you. v0.16.0's behavior of dropping the prefix means your "many backups, one bucket" workflow doesn't work; v0.16.1 fixes it.
- **Anyone running long backups against large databases who rely on the resumable-writer claim** — Bug 34b affects you. v0.16.0's per-table-only resume means a kill at hour 5 of a 6-hour backup mid-table re-bulks that table from scratch; v0.16.1's per-chunk skip preserves the work that already landed.
- **Operators wanting an explicit "resume happened" signal in their logs** — Bug 34a affects you. v0.16.0 was silent on resume; v0.16.1 emits clear INFO lines so cron / journald / Loki dashboards can show resume events.

## What's next

Phase 3 — incrementals + backup-chain → CDC handoff — is still the next chunk on the backup track. Phase 2 is now complete-as-promised with v0.16.1.
