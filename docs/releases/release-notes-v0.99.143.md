# sluice v0.99.143

**New: a lossless Cloudflare D1 source reader — `sluice migrate --source-driver d1 --source d1://<account>/<db>` imports a live D1 database into Postgres or MySQL, reading every value via `CAST/typeof` so integers > 2^53 round-trip EXACTLY (ADR-0132). This is the faithful D1 import the `.sql`-export path cannot be: D1's export *and* its default query JSON both silently round large integers. Opt-in, additive; fully drop-in over v0.99.142.**

## Features

**Cloudflare D1 query-API source reader (`--source-driver d1`, ADR-0132).** A new `d1` source engine reads a *live* D1 database over Cloudflare's HTTP query API and imports it through the standard `migrate` pipeline. It exists because of an empirically-verified fidelity problem: **both** of D1's default extraction paths — `wrangler d1 export` and the default query JSON — serialize integers through a JavaScript float64 and **silently round any integer > 2^53** (snowflake IDs, nanosecond timestamps, large counters). The `d1` reader defeats this: for every column it projects `typeof(c)` plus `CASE typeof(c) WHEN 'blob' THEN hex(c) WHEN 'real' THEN format('%.17g', c) ELSE CAST(c AS TEXT) END`, so each value arrives as **exact text** with its true storage class, and the decoder reconstructs the precise `int64` / `float64` / text / bytes. It reuses the SQLite engine's type-resolution, date/bool policy (ADR-0129), and loud storage-class fidelity. Reads do **not** take D1 offline (unlike `export`). Migrate-source only (`Capabilities.CDC = CDCNone`).

```
export CLOUDFLARE_API_TOKEN=...        # token is env-only — never a flag, never logged
sluice migrate --source-driver d1 --source d1://<account_id>/<database_id> \
  --target-driver postgres --target '<pg-dsn>'
# short DSN: --source d1://<database_id>  (account from CLOUDFLARE_ACCOUNT_ID)
```

**Proven on real D1 (before/after).** Live validation against an actual D1 database, migrating the same data both ways into Postgres:

| value | `d1` reader | `wrangler d1 export` → `migrate` |
|---|---|---|
| `9007199254740993` (2^53+1) | **`9007199254740993` — exact** | `9007199254740992` — rounded |
| `9223372036854775807` (max int64) | **exact** | `9223372036854776000` — rounded (overflows int64) |
| `3.141592653589793`, `1.0/3.0` (REAL) | exact | exact |

The exact `9007199254740993` is not even present in the export `.sql` (D1's export serializer rounds it server-side, before sluice runs). DATETIME/boolean/text columns and row counts all migrated correctly and exactly-once via the reader.

## Compatibility

Purely additive and opt-in: a new `d1` source driver; nothing else changes. The API token is **env-only** (`CLOUDFLARE_API_TOKEN`), never a flag and never logged; a missing token/account/database id is refused loudly at startup. Large tables are read in primary-key keyset pages (rowid fallback; a BLOB-only key on a `WITHOUT ROWID` table is refused rather than mis-paginated). It reuses the validated SQLite decode + date/bool policy, shipped under the full value-fidelity matrix (the `typeof` × storage-class decode, big-int-exact, REAL `%.17g` round-trip, blob-from-hex, storage-class-mismatch loud refusal, keyset correctness) and an independent value-fidelity review. Fully drop-in over v0.99.142.

## Who needs this

Anyone importing a **Cloudflare D1** database that contains large integers — snowflake-style IDs (Discord/Twitter), nanosecond timestamps, big counters — where the `.sql`-export path silently rounds values > 2^53. Use `--source-driver d1` for exactness on a live database; the export path (`--source-driver sqlite --source dump.sql`) remains the simple default for D1 databases without large integers and for offline imports. Plain SQLite-file users are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.143 · **Container:** ghcr.io/sluicesync/sluice:0.99.143
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
