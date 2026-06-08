# sluice v0.99.12

**`--include-table` now scopes the PlanetScale (VStream) cold-start snapshot COPY**, not just the write path — so copying a subset of tables out of a large keyspace no longer streams (and buffers) the excluded tables. Drop-in upgrade from v0.99.11.

## Fixed

- **`--include-table` / `--exclude-table` now scope the PlanetScale (VStream) cold-start snapshot, not just what gets written.** The VStream snapshot COPY used a catch-all filter (`/.*/`) that copied **every** table in the keyspace; `--include-table` only restricted what sluice *wrote*. So copying one small table from a keyspace that also held a large table streamed and buffered the large table too, overflowing `--max-buffer-bytes` (`table "X" would buffer … exceeding the cap … this multi-table interleaving case is not yet disk-spilled`, ADR-0071) — the subset copy could fail outright.

  sluice now passes the filtered table set into the VStream snapshot's **per-table filter rules**, so vtgate's COPY scans only the in-scope tables and a large excluded table in the same keyspace is never streamed. The CDC tail is unchanged (it still streams all tables and filters on dispatch, to keep live add-table working); resume and backup snapshots keep whole-keyspace scope (documented follow-up). New optional engine surface `ir.TableScopedSnapshotOpener`; vanilla MySQL and Postgres snapshots are already per-table and are unaffected.

  Validated on **real PlanetScale** (a 1M-row subset copied cleanly with a coexisting 19M-row table — the exact overflow scenario) plus a vttestserver integration test.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.11. Whole-keyspace syncs (no `--include-table`) are unaffected — the snapshot is scoped to all discovered tables, equivalent to the prior catch-all. Only the PlanetScale/VStream snapshot path changed; vanilla MySQL and Postgres are unchanged.

## Who needs this

- **Anyone migrating a subset of tables from a large PlanetScale/Vitess keyspace.** Before this, `--include-table` couldn't keep the snapshot from streaming a big excluded table, so the COPY could overflow the buffer and fail. Now the snapshot copies only the tables you selected.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.12`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.12`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
