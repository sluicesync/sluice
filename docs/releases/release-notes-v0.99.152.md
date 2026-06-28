# sluice v0.99.152

**Fixes Bug 167: under sustained `sqlite-trigger` continuous CDC the source SQLite WAL (and the sync's memory) grew without bound — an endurance run saw a 20 GB database's WAL hit 75 GB in ~52 minutes. The poller now keeps the WAL bounded. No data loss in any version (exactly-once always held); this is an operational uptime ceiling fix for long-running SQLite continuous sync.**

## Fixed

**MEDIUM — `sqlite-trigger` source WAL + sync RSS unbounded under sustained CDC (Bug 167).** A ~20 GB endurance run found the source SQLite WAL growing 0 → 75 GB in ~52 minutes (~1.4 GB/min) while the sync's RSS crept ~0.9 MB/min — correlated, loud-eventually (disk-fill / memory pressure), **no data loss** (exactly-once held; the change-log row count stayed bounded by `trigger prune`). Killing the sync collapsed the 75 GB WAL to ~0.6 GB, proving almost all of it was superseded change-log frames the running poller pinned.

**Root cause (ground-truthed with a pure-Go repro + heap/pprof):** the CDC poller's `database/sql` read pool retained an **idle connection** that held a stale SQLite WAL read-mark, pinning the checkpoint — the app's own auto-checkpoint could copy frames into the main DB but never RESET the WAL file, so under sustained change-log churn the WAL grew without bound. The RSS creep is **not a Go heap leak** — the Go heap stayed flat (`HeapInuse`/`Sys` constant, `inuse_space` profiles byte-identical) while the WAL grew; it's modernc's OS-level mmap of the growing `-wal`/`-shm`, a secondary effect that bounding the WAL also bounds.

**Fix (local SQLite path only — `d1-trigger` polls over HTTP, no local pager, unaffected):** the poller's read connection is no longer retained idle (`SetMaxIdleConns(0)`), so its read-mark releases after each poll and the checkpoint can reset the WAL (this alone held the WAL flat at ~8 MB in the repro, vs 69 → 158 MB in 12 s with the default idle pool); and the poll loop additionally issues `PRAGMA wal_checkpoint(TRUNCATE)` on a 30 s cadence (busy-tolerant) so the WAL stays bounded even if the operator's app has disabled `wal_autocheckpoint`. The checkpoint runs in the poll goroutine between polls (never racing the read) and is pure WAL-file management — it does not touch the read/apply path, the watermark, or exactly-once.

## How it was found

The ~20 GB **endurance run** of the large-scale test program — exactly-once, throughput (~182k rows/s migrate, no cliff at 20 GB), lag, and `trigger prune` retention all held over the hour; the long lens exposed this WAL/RSS uptime ceiling that shorter runs couldn't. Pinned by deterministic tests (the checkpoint truncates a churned WAL to ~zero; the poller pool retains zero idle connections; the pump fires the checkpoint on cadence), each verified to fail when its fix piece is reverted.

## Compatibility

Behavior-preserving for correctness (exactly-once and the watermark are untouched — verified by the cold-start + warm-resume + prune integration tests passing with the fix live). The only change is WAL-file management on the local `sqlite-trigger` poller; `d1-trigger`, the migrate path, and all other engines are unaffected. The `-race` integration gate passed before tagging (the checkpoint runs in the existing poll goroutine, no new shared state).

## Who needs this

Anyone running a **long-lived continuous `sqlite-trigger` sync** from a local SQLite source — before this, the source WAL (and process memory) grew without bound over hours, capping uptime; now it stays bounded.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.152 · **Container:** ghcr.io/sluicesync/sluice:0.99.152
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
