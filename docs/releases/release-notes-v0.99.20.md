# sluice v0.99.20

**Follow-up to v0.99.19's legacy-date handling — `--zero-date=epoch` now lands a real date on a MySQL `TIMESTAMP` target.** If you used `--zero-date=epoch` to carry a zero/partial date into a MySQL **TIMESTAMP** column, the placeholder was silently stored as `0000-00-00` instead of the epoch — re-introducing the exact value you were trying to replace, at exit 0 with no warning. This release fixes it. (`DATE`/`DATETIME` targets and all Postgres targets were already correct.) **Drop-in from v0.99.19.**

## Fixed

- **`--zero-date=epoch` silently stored `0000-00-00` on a MySQL `TIMESTAMP` target (Bug 133, LOW).** The epoch substitute was `1970-01-01 00:00:00` UTC — exactly one second below MySQL's `TIMESTAMP` range floor (`1970-01-01 00:00:01` UTC). Reading a legacy zero-date source requires `--mysql-sql-mode=''` (to get past strict-mode read rejection), and that flag also relaxes sluice's **target/applier** connection — so an out-of-range midnight-epoch write into a MySQL `TIMESTAMP` column was silently **coerced** back to the `0000-00-00` zero sentinel rather than raising `ERROR 1292`. The row landed, but the requested epoch substitution wasn't honored for that one corner (zero/partial **TIMESTAMP** column + **MySQL** target + `--zero-date=epoch`). It was never the CRITICAL silent-corruption class — no value shifted to a wrong-but-plausible neighbouring date, and `DATE`/`DATETIME` columns and every Postgres target were unaffected (the decoder was always correct).

  The epoch sentinel is now **`1970-01-01 00:00:01`**, which sits exactly at MySQL's `TIMESTAMP` floor and is representable by every temporal target (MySQL `TIMESTAMP`/`DATETIME`, Postgres `timestamp`/`date`). A single universal sentinel keeps the resolution target-agnostic in the source reader, and the one-second offset is meaningless on what is by definition a synthetic placeholder for an invalid date. Pinned by a real-MySQL integration test that ground-truths the midnight→`0000-00-00` coercion (the bug mechanism) and proves the new sentinel round-trips as a real non-zero value.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.19.
- `--zero-date=epoch` placeholders are now `1970-01-01 00:00:01` (was `…00:00:00` for date+time types). DATE-only columns are unchanged (`1970-01-01`).

## Who needs this — action required

- **Anyone who ran `sluice migrate --zero-date=epoch` into a MySQL `TIMESTAMP` column on v0.99.19.** Those cells hold `0000-00-00` instead of the epoch; re-run on v0.99.20 (the source was never touched). Postgres targets and `DATE`/`DATETIME` columns need no action.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.20`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.20`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
