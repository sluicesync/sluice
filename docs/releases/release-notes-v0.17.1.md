# sluice v0.17.1

Single-bug patch from the v0.17.0 test cycle. v0.17.0 shipped logical-backups Phase 3.1 + 3.2 (incremental backups + chain-aware restore); cycle testing surfaced a writer-side path collision that broke any chain with two or more incrementals into the same destination. Single-incremental chains and the schema-evolution path were unaffected. Fix is small, targeted, and pinned by new unit + integration tests.

## Fixed

- **Bug 35 — Incremental change-chunk filename collision; second incremental clobbers the first's chunk on disk.** v0.17.0's change-chunk writer emitted paths as `chunks/_changes/changes-<idx>.jsonl.gz` with the chunk index reset to 0 per-Run. Two incrementals taken into the same `--output-dir` / `--target` therefore both wrote to `chunks/_changes/changes-0.jsonl.gz` — the second overwrote the first's bytes while each manifest still recorded its own (now-divergent) SHA-256. `backup verify --from-dir=./X` exited 1 with `1 of N chunk(s) failed SHA-256 check`; `restore --from-dir=./X` exited 1 at `chain restore: incremental <id1>: stream chunks: chunk 0 (chunks/_changes/changes-0.jsonl.gz): backup: chunk SHA-256 mismatch`. Engine-agnostic (reproduced on both PG and MySQL in the v0.17.0 cycle) and backend-agnostic (writer is shared between local-FS and the `s3://` / `gs://` / `azblob://` cloud backends). Fix: namespace each incremental's chunks under a per-Run subdirectory derived from the manifest's `CreatedAt` (`chunks/_changes/<unix_millis>/changes-<idx>.jsonl.gz`). `CreatedAt` is the right anchor — `BackupID` would be cleaner conceptually but it depends on `EndPosition`, which is only known after the window closes; chunks need a stable namespace at the first write. The manifest's recorded `change_chunks[].file` is the source of truth for reads, so chain restore + `backup verify` pick up the new shape with no other changes.

## Compatibility

- **No IR / CLI / manifest schema changes.** The fix is entirely in the writer-side path-construction helper. `ir.Manifest` / `ir.ChunkInfo` are unchanged; CLI flags are unchanged; format version stays at 1.
- **Single-incremental chains written by v0.17.0 still restore cleanly post-fix.** The reader follows whatever path the manifest recorded; v0.17.0's flat `chunks/_changes/changes-0.jsonl.gz` is still readable. Only the writer's path shape changed.
- **Multi-incremental chains written by v0.17.0 are unrecoverable** because the second incremental already overwrote the first's bytes on disk before this fix existed. v0.17.0 operators with broken chains: take a fresh full + restart the chain on v0.17.1.

## Who needs this

- **Anyone running `sluice backup incremental` more than once into the same destination** — Bug 35 affects you. v0.17.0's behavior was that the second incremental's chunks silently overwrote the first's, leaving a chain that fails verify + chain restore. v0.17.1 namespaces chunk paths per-Run so multi-incremental chains coexist cleanly. If your workflow takes only a single incremental on top of a full (e.g. nightly full + one daily catch-up incr), v0.17.0 worked — but you still want v0.17.1 because there's no functional cost.
- **Anyone planning to run incremental chains across cloud backends (`s3://`, `gs://`, `azblob://`)** — same writer code path; same bug shape; same fix.

## What's next

Phase 3.3 — `--position-from-manifest` for `sluice sync start`, full-backup `EndPosition` recording, and PG `wal_keep_size` soft-warnings — is the next chunk on the backup track. v0.17.2's subagent picks that up.
