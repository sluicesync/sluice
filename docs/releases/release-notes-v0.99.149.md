# sluice v0.99.149

**Cloudflare D1 gains continuous logical CDC. A live D1 database now streams its row changes to Postgres or MySQL over D1's HTTP query API via the new `d1-trigger` source engine — the same trigger + change-log + polling design as the local `sqlite-trigger` engine, run over the lossless D1 transport. Migrate INTO D1 (SQLite target + `wrangler d1 import`), stream OUT of D1 (`d1-trigger`).**

## Features

**`d1-trigger` CDC source engine (ADR-0136, #5 Phase 2).** `sluice trigger setup --source-driver d1-trigger --dsn d1://<account_id>/<database_id> --tables=t1,t2` installs the same `sluice_change_log` table + per-table AFTER INSERT/UPDATE/DELETE capture triggers as `sqlite-trigger` (ADR-0135), and `sluice sync start --source-driver d1-trigger --source d1://<account_id>/<database_id> --target-driver postgres --target <dsn>` (or `--target-driver mysql`) does a cold-start snapshot — reusing the validated lossless `d1` reader (ADR-0132's CAST/typeof projection, so integers > 2^53 survive) — handed off gap-free to a polling CDC reader with a monotonic-id watermark for exactly-once resume. The API token is env-only (`CLOUDFLARE_API_TOKEN`); the account id comes from the DSN or `CLOUDFLARE_ACCOUNT_ID`. `sluice trigger teardown --source-driver d1-trigger` removes every trace.

**Transport substitution, not new CDC logic.** Phase 2 refactored the shipped `sqlite-trigger` setup/CDC/snapshot logic onto a small executor interface with two implementations — the existing local `*sql.DB` path (byte-identical to v0.99.148, re-validated) and a new D1 path over the `d1` reader's `/query` HTTP transport. So the faithful capture/decode is byte-identical to the local engine and the D1 cold-start reader: the `(typeof, text/hex)` pair the triggers capture reconstructs the exact `int64`/`float64`/text/`[]byte` through the SAME storage-class decoder — big integers and blobs round-trip EXACT over HTTP (httptest-pinned: `9007199254740993` and a blob-from-hex survive). The change-log poll binds the resume watermark as a STRING param so a `> 2^53` bound is never JS-rounded (the ADR-0132 discipline).

**Correctness: primary-consistency, not read replicas.** The exactly-once `id > watermark` invariant rests on commit-order = id-order, which holds at D1's write-serialised primary but can wobble against a lagging read replica — so the poll uses D1's default primary (strongly-consistent) query path, NOT Sessions/replica routing (ADR-0136 §4). A schema change without a re-setup is refused loudly at stream start (the captured-column fingerprint), and a failed poll surfaces as a loud error, never a silent empty "no changes" batch.

## Compatibility

Additive: a new `d1-trigger` source driver; the local `sqlite-trigger` path is byte-identical (the executor refactor preserved it — its tests pass unchanged). Pinned by httptest unit tests (no live D1 required) covering the setup DDL, the poll + string-bound watermark, the big-int/REAL/BLOB/NULL fidelity matrix over HTTP, UPDATE/DELETE before-image extraction, the watermark resume, the schema-drift refusal, the transport-error-is-loud contract, and the env-only token/account refusals; live-D1 validation runs separately. The `-race` integration gate passed before tagging. **Operational caveats (documented, not silently handled):** every D1 write fires a trigger that writes a billable change-log row (write amplification; the change-log grows until the Phase-3 retention follow-up), and installing the triggers modifies the operator's D1 database (`trigger teardown` removes every artifact). On a managed Postgres *target* without superuser, note the separate CDC-applier FK caveat (see the operator docs). Deferred to Phase 3: change-log retention/pruning, capture-payload trimming, schema-change forwarding, and a replica-aware poll mode.

## Who needs this

Anyone running a Cloudflare D1 database who wants it continuously replicated into Postgres or MySQL (analytics, reporting, a relational read-replica of an edge store). Combined with the SQLite/D1 target (v0.99.146), sluice now does the full `X → D1` and `D1 → X` round trip.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.149 · **Container:** ghcr.io/sluicesync/sluice:0.99.149
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
