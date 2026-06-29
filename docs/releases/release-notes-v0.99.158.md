# sluice v0.99.158

**Two observability additions: `sluice schema preview` now tells you where a SQLite target normalizes a column's type-affinity (e.g. decimal → TEXT to stay exact), and MySQL CDC apply now reports whether your workload is actually benefiting from UPDATE/DELETE coalescing. No behavior changes — both are awareness features.**

## Added

- **`sluice schema preview` surfaces SQLite-target type-affinity conversions as advisory notes (text + JSON).** When the target is SQLite, the preview emitted the target DDL but didn't call out where an IR type maps to a different SQLite storage affinity. It now lists those normalizations the same way the existing cross-engine advisories do (unconstrained-numeric widenings, wide-varchar down-maps, etc.). The headline is `DECIMAL`/`NUMERIC` → **TEXT affinity**, with the rationale spelled out — *stored as TEXT to preserve the exact decimal value; SQLite's NUMERIC affinity would coerce it to a lossy 15-digit REAL* (the Bug-162 fidelity feature) — plus `JSON`/`UUID`/`ENUM`/`SET` → TEXT, `CHAR`/`VARCHAR` → TEXT (declared length not enforced), and integer width/sign not preserved. The notes appear in the human-readable preview and as a stable `sqlite_affinity_notes` array under `--format json` for tooling/CI. This makes sluice's automatic affinity conversions visible *before* a migrate/sync runs, so operators can reference them — directly the "generate awareness reports automatically" use case. (Also corrects a stale `Decimal → DECIMAL/NUMERIC` comment in the SQLite DDL emitter to reflect the actual `Decimal → TEXT` mapping.)

- **MySQL CDC apply reports a coalescing-ratio observability line.** The v0.99.154/0.99.157 INSERT/UPDATE/DELETE coalescing helps same-kind *runs* greatly and strictly-alternating workloads little; operators previously had no way to tell which case their sync was in. The applier now tracks coalesced rows and coalesced statements (lock-free atomic counters, safe across the concurrent apply lanes) and emits a rate-limited INFO line — at most once per ~30 s — reporting the running rows-per-coalesced-statement ratio, the totals, and a plain-language assessment (*"good — same-kind runs coalescing well"* vs *"RTT-bound — workload alternates kinds / no same-kind runs"*). Observability only — no change to the apply path or its correctness.

## Compatibility

Purely additive. The preview's existing output is unchanged for non-SQLite targets and for SQLite targets simply gains the new advisory section / JSON field (omitted when empty). The MySQL coalescing line is a new rate-limited INFO log; the apply path, exactly-once, and value fidelity are untouched. The `-race` integration gate passed before tagging (the coalescing counters are mutated from the concurrent apply lanes).

## Who needs this

- Anyone migrating/syncing **to a SQLite target** who wants to see, up front, which columns sluice will normalize (and why decimals become TEXT) — via `sluice schema preview` (human-readable) or `--format json` (tooling/CI gates).
- Anyone running **UPDATE/DELETE-heavy MySQL CDC over a high-latency link** who wants to confirm their workload is actually coalescing rather than running round-trip-bound.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.158 · **Container:** ghcr.io/sluicesync/sluice:0.99.158
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
