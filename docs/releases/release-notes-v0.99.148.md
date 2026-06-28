# sluice v0.99.148

**SQLite gains continuous logical CDC. A local SQLite file now streams its row changes to Postgres or MySQL via the new `sqlite-trigger` source engine — cold-start snapshot handed off gap-free to a trigger-fed change log, exactly-once on resume, with faithful big-integer/BLOB capture. This completes the SQLite/D1 arc (migrate source → schema-feature carry → within-table chunking → SQLite target → trigger CDC).**

## Features

**`sqlite-trigger` CDC source engine (ADR-0135, Phase 1).** SQLite has no logical replication or decodable change stream, so — exactly as `pgtrigger` does for slot-less Postgres (ADR-0066) — `sqlite-trigger` captures changes with triggers. `sluice trigger setup --source-driver sqlite-trigger --dsn ./app.db --tables=t1,t2` installs a `sluice_change_log` table plus per-table AFTER INSERT/UPDATE/DELETE capture triggers; `sluice sync start --source-driver sqlite-trigger --source ./app.db --target-driver postgres --target <dsn>` (or `--target-driver mysql`) does a cold-start snapshot — reusing the validated `sqlite` reader, including within-table chunking and the ADR-0129 date/bool policy — handed off gap-free to a polling CDC reader with a monotonic-id watermark for exactly-once resume. The engine composes `sqlite` by delegation (CDC source only; write/target surfaces stay not-implemented) and self-registers as `sqlite-trigger`; `sluice trigger teardown --source-driver sqlite-trigger` removes every trace.

**Faithful capture — big integers and BLOBs are exact, not silently mangled.** The load-bearing decision: the capture trigger does NOT use SQLite's `json_object()` on raw columns (it serializes an INTEGER as a JSON double — silently rounding any integer > 2^53, e.g. snowflake IDs / nanosecond timestamps — and cannot represent a BLOB). Instead each column is captured as a `(typeof, text/hex)` pair using the SAME proven encoding as the `d1` reader (`typeof(col)` + `CASE typeof(col) WHEN 'blob' THEN hex(col) WHEN 'real' THEN format('%.17g', col) ELSE CAST(col AS TEXT) END`), and the CDC reader reconstructs the exact `int64`/`float64`/text/`[]byte` through the SAME storage-class-faithful `decodeCell` the file/D1 readers use — one decode implementation, so a captured change decodes byte-identically to a cold-start row. Big integers and BLOBs round-trip exact (integration-proven into both PG and MySQL).

**Exactly-once, and loud on schema drift.** Because standard SQLite serializes writers, the change-log id is allocated in commit order, so the reader needs no safety-lag predicate and the snapshot anchor is a simple `MAX(id)` read before the copy (gap-free; over-replay is idempotent on the PK). A source schema change without a re-setup is refused LOUDLY at stream start: a captured-column fingerprint recorded at setup is compared to the live schema, closing the silent `ADD COLUMN` direction (SQLite has no DDL triggers in Phase 1). A table without a PRIMARY KEY is refused loudly at setup.

## Compatibility

Additive: a new `sqlite-trigger` source driver and `trigger setup`/`teardown` wiring; nothing else changes. The change-log/fingerprint/meta tables are auto-skipped by the schema reader and never self-captured. Shipped under a capture-fidelity unit matrix (big-int incl. 2^53+1/max-int64, REAL `%.17g`, BLOB-from-hex, temporal under iso + unixepoch, boolean, NULL), schema-drift refusal pins (ADD COLUMN / dropped table), and cross-engine `SQLite→Postgres` / `SQLite→MySQL` cold-start + CDC + hard-stop/warm-resume exactly-once integration tests; the `-race` integration gate passed before tagging (concurrency chunk); independent value-fidelity review certified the capture fidelity and the exactly-once design. Phase 2 (deferred): the D1-over-HTTP variant, schema-change forwarding, and capture-payload trimming / change-log retention.

## Who needs this

Anyone who wants to continuously sync a local SQLite database into Postgres or MySQL (edge/embedded apps, local-first stores). For Cloudflare D1 continuous sync, the D1-over-HTTP transport is the Phase-2 follow-up; today a D1 database can be migrated one-shot (`--source-driver d1`/`sqlite`) or exported to a local `.db` and streamed with `sqlite-trigger`.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.148 · **Container:** ghcr.io/sluicesync/sluice:0.99.148
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
