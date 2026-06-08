# sluice v0.99.23

**`--zero-date` now works on a PlanetScale/Vitess source.** Legacy zero/partial dates on the VStream CDC path were handed downstream as raw bytes — failing confusingly on a Postgres target — instead of applying your `--zero-date` policy. The vanilla MySQL paths have honored `--zero-date` since the original Vector A fix; this brings the VStream path to parity. **Drop-in from v0.99.22.**

## Fixed

- **VStream source: zero/partial dates now honor `--zero-date` (Vector A parity).** The PlanetScale/Vitess CDC decoder parsed `DATE`/`DATETIME`/`TIMESTAMP` cells with a strict layout that rejected a zero or partial date (`'0000-00-00'`, `'YYYY-00-DD'`, `'YYYY-MM-00'`) and then fell back to passing the raw bytes through — so a Postgres target failed with a confusing `expected time.Time, got []byte`, and the operator's `--zero-date` choice was never applied. A zero/partial date is now detected with the same predicate the bulk-copy / binlog paths use and resolved per `--zero-date`:
  - `--zero-date=error` (**default**) — refuse the stream loudly, naming the column.
  - `--zero-date=null` — carry SQL `NULL` (refused loudly on a `NOT NULL` column).
  - `--zero-date=epoch` — substitute `1970-01-01 00:00:01` (the representable floor; see v0.99.20).

  A genuinely malformed but non-zero date (month 13, Feb 30) still fails loudly, unchanged. Coverage spans the live CDC reader and the cold-start COPY + CDC catch-up paths. Pinned by decoder unit tests across the temporal family × every zero shape × each policy (including the `NOT NULL` refusal).

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.22. `--zero-date` now behaves identically on the vanilla MySQL and PlanetScale/Vitess source paths.

## Who needs this — action required

- **Anyone running `migrate` or `sync` from a PlanetScale/Vitess source over a database that may contain zero or partial dates.** Previously such a value errored confusingly mid-stream on a Postgres target; now it follows your `--zero-date` policy (refuse by default). Choose `--zero-date=null` or `=epoch` per your data, the same as on a vanilla MySQL source.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.23`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.23`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
