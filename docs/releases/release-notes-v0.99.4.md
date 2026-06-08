# sluice v0.99.4

**A CRITICAL silent-loss fix for PlanetScale cold-start, plus transient-fault auto-retry.** No API or CLI changes — drop-in upgrade from v0.99.3.

## Fixed

- **CRITICAL — silent row loss on a VStream cold-start of a table with no explicit PRIMARY KEY.** The COPY-phase dedup assumed Vitess emits the snapshot scan in ascending order of the column it flags as the primary key. Vitess actually orders the scan by the *cheapest* unique key (its column-type-cost heuristic favors a small-int unique over a `BIGINT` one), so on a table with a `UNIQUE` id but no declared `PRIMARY KEY`, legitimate rows arrived out of that order and were **silently dropped**. A real ~19M-row migration lost ~70% of its rows (13.5M) this way. The order-dependent dedup is removed and the cold-start COPY is now **idempotent** (`ON DUPLICATE KEY UPDATE` on a unique key present during copy) — Vitess's catch-up re-emissions are absorbed, not dropped, regardless of scan order. Truly keyless tables (no PK and no non-null UNIQUE) are now **refused loudly** instead of silently duplicating. Pinned end-to-end against `vttestserver` across the full key-shape family and gated in CI under `-race`. ([ADR-0072](https://github.com/sluicesync/sluice/blob/main/docs/adr/adr-0072-resumable-coldstart-copy.md))

- **VStream cold-start auto-retries a transient connection drop instead of failing.** `Unavailable: connector reset by peer` (and other native gRPC transients) arrives as a gRPC status error, not a MySQL `1105` — the retry classifier missed it, so a large-table cold-start died on a network blip. The classifier now honors gRPC status codes, so the retry policy reconnects and resumes. (Resumable *mid-copy* continuation — so a retry doesn't re-copy from the start — is the designed next step.)

## Changed

- **No-PK VStream→Postgres copies are now refused loudly** rather than silently duplicating catch-up re-emissions, until the Postgres target gains the symmetric upsert treatment. PK tables and MySQL targets are unaffected.

## Who needs this

- **Anyone running a `sync start` / cold-start from PlanetScale (Vitess / VStream) where a source table lacks an explicit `PRIMARY KEY`** (a `UNIQUE` key alone is the trigger). Before v0.99.4 such a cold-start could silently drop the majority of its rows. Upgrade before migrating these tables.
- **Anyone cold-starting large tables over a flaky link** — a transient VStream drop now auto-retries instead of failing the run.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.4`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.4`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
