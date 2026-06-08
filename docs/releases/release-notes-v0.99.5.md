# sluice v0.99.5

**Resumable PlanetScale cold-start COPY, a memory hard-cap, and a no-PK CDC-resume fix.** Drop-in upgrade from v0.99.4.

## Added

- **Resumable VStream cold-start COPY.** A transient drop or a process crash *mid-COPY* now **resumes from the last-copied primary key** instead of re-copying the table from row 0. sluice checkpoints Vitess's per-table `TablePKs` cursor during the copy (bounded 50k-rows / 10s cadence); on reconnect or restart it replays the cursor so vtgate continues the scan, and the catch-up rows upsert idempotently (zero loss, no `1062`). A fault now costs the in-flight chunk, not the whole table — the payoff for anyone cold-starting large PlanetScale tables over a flaky link. Completes the cold-start-hardening arc from v0.99.4. ([ADR-0072](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0072-resumable-coldstart-copy.md))
- **`--max-memory` flag** — a hard `GOMEMLIMIT`-backed RSS ceiling (e.g. `--max-memory=4GiB`). `--max-buffer-bytes` accounts only raw value bytes, so the real Go-heap footprint of buffered rows runs several times larger and a big buffer cap can push RSS to ~9× the configured value; `--max-memory` gives the GC a real ceiling to defend. Default off; the `GOMEMLIMIT` env var is honored natively too.

## Fixed

- **No-`PRIMARY KEY` CDC apply is now idempotent on a unique key.** The MySQL applier plain-`INSERT`ed no-PK tables, so a CDC warm-resume of a no-PK-but-`UNIQUE` table (the v0.99.4 `connections` shape) hit `1062` and failed. It now emits `ON DUPLICATE KEY UPDATE` (full-row SET) even with no PK — MySQL fires it on any unique index, so re-applied rows upsert idempotently. Truly keyless tables are unchanged best-effort.

## Who needs this

- **Anyone cold-starting large PlanetScale (VStream) tables** — a mid-copy fault now resumes instead of restarting from row 0.
- **Anyone running CDC against a no-`PRIMARY KEY` (but `UNIQUE`-keyed) MySQL table** — warm-resume no longer fails on a duplicate key.
- **Anyone who needs a hard memory ceiling** on a constrained host — `--max-memory`.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.5`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.5`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
