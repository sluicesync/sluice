# sluice v0.99.1

**The first sluice release published from the now-public repository.** No new features — a MySQL concurrency correctness fix, a Go toolchain security bump, and routine dependency updates. Drop-in upgrade from v0.99.0; no config changes.

## Fixed

- **MySQL shard-lease deadlock retry (Error 1213 / SQLSTATE 40001).** During multi-shard consolidation, the MySQL shard-lease acquire (`SELECT … FOR UPDATE` on the lease row, then INSERT-if-absent) could deadlock when concurrent shards raced on the gap lock at the INSERT, and the error surfaced to the caller without a retry. It's now wrapped in a bounded deadlock-retry (an `isMySQLDeadlock` classifier; 8 attempts, 5 ms → 200 ms context-aware backoff). Postgres is unaffected (its lease path is an atomic upsert). Pinned by unit tests on both engines plus the Phase-2e 3-shard-contention integration test under `-race`.

## Security

- **Go 1.26.4 toolchain.** Bumps the `go` directive 1.26.2 → 1.26.4, clearing two standard-library advisories present in 1.26.2 — GO-2026-5039 (`net/textproto`) and GO-2026-5037 (`crypto/x509`). Build-only; no API or behavior change.

## Dependencies

- pgx/v5 5.9.2 → 5.10.0, koanf/v2 2.3.4 → 2.3.5, gocloud.dev 0.45.0 → 0.46.0, the aws-sdk-go-v2 group (5 modules), and the `docker/*` CI actions (login / setup-buildx / setup-qemu) v3 → v4.

## Who needs this

- **MySQL → MySQL multi-shard (Vitess / PlanetScale) consolidation users** — the deadlock fix removes a sporadic hard-fail under concurrent shard-lease acquisition. Everyone else gets the Go security bump for free.

---

**Install:** `go install sluicesync.dev/sluice/cmd/sluice@v0.99.1`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.1`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
