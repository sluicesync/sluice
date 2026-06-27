# sluice v0.99.147

**CRITICAL fix. v0.99.146's SQLite target stored DECIMAL/NUMERIC money as a binary float — `19.99` landed on disk as `19.989999999999998`, silently, exit 0. v0.99.147 stores decimals as exact text (byte-exact round-trip). If you produced a SQLite/D1 database from a decimal-bearing source with v0.99.146, re-run with v0.99.147 — those `.db` artifacts hold lossy float money values.**

## Fixed

**CRITICAL — silent decimal corruption on the SQLite target (Bug 162).** Introduced in v0.99.146 (SQLite as a migration target). A `DECIMAL`/`NUMERIC` source column was emitted on the SQLite target with NUMERIC affinity, and SQLite's NUMERIC affinity silently coerces a bound decimal to a binary **REAL**: an ordinary money value like `19.99` was stored as `19.989999999999998`, `5.10` as `5.0999999999999996` — exit 0, no warning. The v0.99.146 writer guarded by *significant-digit count* (>15), but float64 loss is about *dyadic representability*, not digit count — `19.99` has four significant digits yet is not float64-exact, so it slipped the guard. Because the produced `.db` is the deliverable (e.g. `X → SQLite → Cloudflare D1` via `wrangler d1 import`), the artifact handed downstream held money as binary floats, and a re-migrate of the REAL back into a constrained target `NUMERIC` could fail loudly (`98765432.1098` scans back as `9.87654321098e+07`).

**The fix: decimals are emitted with TEXT affinity.** SQLite stores text verbatim with no numeric coercion, so a decimal of any precision round-trips **byte-exact** — `19.99` → `19.99`, `100.00` → `100.00` (scale preserved), `12345678901234567890.1234567890` → exact (TEXT has no precision limit, so the v0.99.146 over-precision *refusal* is gone too). The cost is a documented type downgrade: the column reads back as `ir.Text` rather than `ir.Decimal` — the same value-faithful trade as `JSON`/`UUID` → `TEXT`, and the right call, since a silent value corruption is never acceptable to preserve a type label (SQLite and D1 are dynamically typed, so a decimal-as-text is a faithful decimal value).

## How it was caught (and why it's pinned now)

The v0.99.146 value-fidelity review reported `19.99` as exact — but it read the value back through SQLite's own 15-digit text conversion (which renders the REAL as `19.99`) rather than asserting the on-disk storage class or a byte-exact round-trip through sluice's reader. The post-release regression cycle, running the real released binary end-to-end, caught the corruption. This is the project's recurring lesson (Bug 74): pin the real-path / on-disk behavior, not one representative read. The fix is pinned by a writer→DB→schema-resolved-reader round-trip test asserting **byte-exact** equality across money, preserved scale, big/small magnitude, and far-beyond-float64 values.

## Compatibility

The fix changes only the SQLite-*target* decimal storage (TEXT instead of NUMERIC) and removes the now-unnecessary over-precision refusal. Non-decimal types, all other engines, and the SQLite *source*/reader are unchanged. There is no migration for existing v0.99.146 `.db` files — regenerate them with v0.99.147 to replace the lossy float values with exact text.

## Who needs this

Anyone who ran `migrate --target-driver sqlite` (including the `X → SQLite → D1` path) against a source containing `DECIMAL`/`NUMERIC` columns on v0.99.146. Upgrade and re-run.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.147 · **Container:** ghcr.io/sluicesync/sluice:0.99.147
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
