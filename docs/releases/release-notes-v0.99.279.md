# sluice v0.99.279

> ⚠️ **Correction (2026-07-19):** a fresh independent audit found this remediation fixed one representative of the collation-fidelity class and missed three siblings — additional silent-loss in `sync --where` string/float filters on `_as_cs`/`char(n)`/PAD-SPACE collations and float ordering. **Fixed in v0.99.282.** If you use `sync --where`, upgrade to **≥ v0.99.282**.

**Silent-loss fix for `sync --where` string filters — supersedes v0.99.278.** A post-release audit found the ADR-0174 client-side collation comparator diverges from the source's own `=`, silently dropping or leaking rows in continuous filtered sync. If you use `sync --where` with a **string** filter, upgrade. `migrate --where` was never affected (it evaluates on the source), and nothing that doesn't use `--where` changes.

## Fixed

### The Critical: PAD SPACE collations silently dropped/leaked trailing-whitespace rows

sluice's continuous filtered sync evaluates the `--where` predicate client-side to classify each change. The comparator (Vitess's `evalengine`) applied **NO-PAD-SPACE** comparison regardless of a collation's real `PAD_ATTRIBUTE`. But every *legacy* collation — `utf8mb4_general_ci` (the pre-8.0 / MariaDB default), `utf8mb4_bin`, `latin1_swedish_ci`, … — is **PAD SPACE**, meaning MySQL's `=` ignores trailing spaces. So a stored `'EU '` matched `WHERE region = 'EU'` on the source but *not* in sluice's client-side check — a `sync --where` INSERT of `'EU '` was silently dropped, a DELETE silently swallowed, at exit 0, on the legacy default collation with no operator action. Confirmed against real MySQL 8.0.

The comparator now right-trims ASCII spaces on a PAD SPACE column (on both the case/accent-insensitive and the byte-exact `_bin` paths), reproducing the source's PAD SPACE `=`. NO-PAD collations (`utf8mb4_0900_*` — the MySQL-8 default — and `binary`) are unchanged. Two collation classes the comparator can't faithfully reproduce now **refuse loudly at sync-start** rather than compare wrongly: non-UTF-8 charset collations (`latin1`, `gbk`, …), whose bytes it would mis-decode, and Postgres *named* (possibly non-deterministic ICU) collations, whose determinism it can't prove.

**The real fix is the test that was missing.** The shipped tests verified Vitess's comparator against hand-written booleans — the same library under test, verifying itself, so the whole divergence class was invisible. This release adds a **real-MySQL collation family×shape matrix**: it boots an actual MySQL, and for every collation × every shape (trailing / leading / internal space, case, accent, `ß`/`ss` expansion, distinct value) asserts sluice's classification equals the server's own `WHERE`. It fails on the pre-fix code (20+ diverging cells) and passes on the fix — the independent ground-truth gate that now guards this surface.

### `migrate` / `verify --where`: a typo no longer silently copies the whole table

The plain migrate/verify readers matched a `--where` key by exact table name, so a typo (`--where user=…` missing the `s`) or a case-fold mismatch silently disabled the filter and copied/counted the **whole** table — and `verify`, riding the same lookup, confirmed a false PASS. `--where` keys are now validated against the source schema at migrate/verify start (case-insensitive), refusing an unmatched key loudly (`SLUICE-E-WHERE-UNKNOWN-TABLE`) — matching the continuous-sync leg, which already did.

### Float-equality filters refused

`=` / `!=` / `IN` on a `FLOAT`/`DOUBLE` column is now refused at compile time: the client compares the literal exactly while the source coerces it to a 64-bit double, so a high-precision literal could orphan a row. Ordering (`<`/`>`) on floats is unchanged.

## Compatibility

**No behavior change without `--where`.** For `sync --where` string filters: PAD SPACE collations now evaluate correctly (was silent-loss); non-UTF-8-charset and Postgres *named* collations now refuse loudly (was silently wrong) — if you hit one, use `migrate --where` (source-evaluated, any collation), filter on a NO-PAD `utf8mb4_0900_*` column, or normalize the value on the source. `migrate --where` / `verify --where` now refuse a `--where` key that names no source table (was a silent whole-table copy).

## Who needs this

Anyone running **`sync --where`** (continuous filtered sync) with a **string** predicate — especially on a legacy-default collation (`utf8mb4_general_ci`, `latin1_*`) or with trailing-whitespace values (common from CSV / fixed-width / form / legacy sources). Upgrade from v0.99.278. Users of `migrate --where`, numeric filters, or the NO-PAD `utf8mb4_0900_ai_ci` default are unaffected but should still upgrade for the migrate/verify key-validation fix.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.279
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.279`
