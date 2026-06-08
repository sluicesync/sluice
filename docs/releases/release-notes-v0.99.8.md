# sluice v0.99.8

**Two PlanetScale/VStream warm-resume fixes.** Restarting a sync no longer crashes, and an interrupted cold-start now resumes its bulk COPY at full speed instead of silently crawling. Drop-in upgrade from v0.99.7; a no-op for migrations that never restart a PlanetScale sync. Both were found by post-release validation against a real PlanetScale production branch.

## Fixed

- **Restarting a PlanetScale (VStream) sync no longer crashes the schema-history orderer.** On any warm-resume, sluice orders the persisted position against its retained schema-history (ADR-0049). The MySQL position-orderer assumed the vanilla single-object binlog position, but a VStream position is a **JSON array of per-shard `shardGtid`** — so once any schema-history existed, a restart crashed at startup with `mysql: position-orderer: decode p: json: cannot unmarshal array into Go value of type mysql.binlogPos`. This broke **both** ordinary post-cold-start CDC restarts and interrupted-cold-start resumes on real PlanetScale. The orderer is now VStream-aware: it orders by per-shard GTID superset (the same ADR-0049 partial order, applied per shard), ignoring the COPY-resume `TablePKs` cursor. Pinned across the full VStream-position family (single/multi-shard × with/without cursor × the GTID relations), plus an end-to-end schema-history-prime warm-resume integration test the prior vttestserver pins didn't cover.

- **Resuming an interrupted PlanetScale cold-start now continues the bulk COPY instead of silently crawling.** When a cold-start COPY was interrupted (a process restart with a persisted mid-COPY `TablePKs` cursor), the pipeline routed the resume through the plain CDC reader, which applied the un-copied tail **one INSERT round-trip at a time** (~10 rows/sec against a remote target) — so a large table stalled near where it left off, with only heartbeats and no error (a silent-degrade hazard: an operator could mark a 5%-complete sync "done"). The pipeline now detects a cursor-carrying resume and routes it through the **seeded snapshot stream → batched bulk-COPY writer**, continuing from the cursor (not re-copying from row 0; the idempotent writer absorbs overlap; the partial target copy is preserved). Measured on a real 19M-row PlanetScale branch: resume throughput went from **~514 rows/min to ~5,000 rows/sec**. The completed-cold-start (cursor-less) restart stays on the fast plain-CDC path, unchanged. A cursor-less or non-VStream position is refused loudly rather than silently re-copying from row 0. Pinned by a new process-restart resume integration test (distinct from the in-place reconnect path).

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.7.
- PlanetScale **production** sync restarts (the common case) are fixed by the first item. The second restores the resumable-cold-start guarantee v0.99.5 introduced — which, it turns out, did not hold on real PlanetScale across a full process restart (only the in-place mid-stream reconnect did).

## Who needs this

- **Anyone running a continuous PlanetScale → X sync that may restart** (deploy, crash, operator stop/start) — without this, a restart crashes once schema-history exists.
- **Anyone whose PlanetScale cold-start of a large table gets interrupted** — the resume now finishes at bulk speed instead of crawling indefinitely.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.8`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.8`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
