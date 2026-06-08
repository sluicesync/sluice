# sluice v0.99.3

**A CRITICAL correctness fix for large-table PlanetScale cold-start.** No API or CLI changes — drop-in upgrade from v0.99.2.

## Fixed

- **Unbounded memory on PlanetScale (VStream) cold-start → OOM.** The VStream snapshot reader buffered the **entire** COPY phase in RAM before writing a single row to the target — so a large source table could exhaust memory and be OOM-killed mid-cold-start (a ~13 GB / 19M-row table drove RSS to ~41 GB on a 32 GB host, into swap, with **zero target writes** the whole time). The COPY phase now **streams**: a byte-capped, backpressured pump (`--max-buffer-bytes`) feeds rows to the target as they arrive, so large-table cold-start runs at **constant memory** and target writes begin immediately. Multi-table snapshots that would exceed the cap refuse loudly instead of OOM-ing. Multi-shard fan-in, COPY-phase dedup, and the snapshot→CDC position handoff are preserved and validated under `-race`; the `ir.SnapshotStream` contract is unchanged. ([ADR-0071](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0071-vstream-snapshot-bounded-memory.md))

## Who needs this

- **Anyone cold-starting a migration or sync from PlanetScale (Vitess / VStream) with a large table.** Before v0.99.3, a multi-GB table could OOM the process during the snapshot phase; now it streams at constant memory.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.3`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.3`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
