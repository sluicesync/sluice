# ADR-0136: Cloudflare D1 trigger-based CDC over the HTTP query API (`d1-trigger`, #5 Phase 2)

## Status

**Accepted — shipped v0.99.149** (the `d1-trigger` engine, `internal/engines/d1-trigger`,
commit `97adc7b1`; both this header and the index row said Proposed long after the engine
shipped — the doc-vs-code half of the DOC-3 class, fixed alongside the G-17 gate,
audit 2026-07-23). Prior status: Proposed (2026-06-28).

Roadmap item 49 follow-up — Phase 2 of the trigger-CDC work
(ADR-0135 Phase 1 was local-SQLite-file only). Adds continuous logical CDC from a **live
Cloudflare D1** database to Postgres/MySQL by running the same trigger + change-log +
polling design over D1's HTTP query API instead of a local `*sql.DB`.

## Context

ADR-0135 shipped `sqlite-trigger` (v0.99.148): per-table AFTER I/U/D triggers write
faithful `(typeof, text/hex)` row images into a `sluice_change_log` table, a polling
reader emits `ir.Change` with a monotonic-`id` watermark, and a `MAX(id)` snapshot anchor
gives a gap-free cold-start→CDC handoff. It is local-file only. D1 is SQLite and supports
`CREATE TRIGGER`, and sluice already has the two D1 pieces this needs: the lossless `d1`
cold-start reader (ADR-0132 — `CAST`/`typeof` projection so integers > 2^53 survive) and
its `net/http` query-API transport (`d1Client`). Phase 2 is therefore mostly **transport
substitution**: run the Phase-1 setup DDL and the change-log poll over the D1 query API,
reuse the cold-start `d1` reader and the shared faithful capture/decode seam unchanged.

## Decision

1. **New engine `d1-trigger`**, registered alongside `d1`/`sqlite`/`sqlite-trigger`,
   composing the `d1` engine by delegation for `OpenSchemaReader`/`OpenRowReader` (cold-start
   reuses the validated lossless D1 reader) and adding the CDC surfaces. DSN is the `d1`
   form (`d1://<account>/<db>` or `d1://<db>` + `CLOUDFLARE_ACCOUNT_ID`); the API token is
   env-only (`CLOUDFLARE_API_TOKEN`), as for `d1`. `sluice trigger setup/teardown
   --source-driver d1-trigger` and `sluice sync start --source-driver d1-trigger`.

2. **Setup over HTTP.** `d1Client` gains a DDL/exec path (the `/query` endpoint already
   executes arbitrary SQL — `CREATE TABLE`/`CREATE TRIGGER` run there). Setup installs the
   SAME `sluice_change_log` + `sluice_change_log_meta` + `sluice_change_log_columns`
   (fingerprint) tables and the SAME per-table faithful-capture triggers as Phase 1 — the
   trigger body is the shared `CapturedValueExpr` seam, so capture fidelity is byte-identical
   to the file engine and to the D1 cold-start reader. No-PK refusal carries over.

3. **CDC reader polls over HTTP.** The poll is the SAME `SELECT id, op, tbl, before, after
   FROM sluice_change_log WHERE id > ? ORDER BY id LIMIT batch`, executed via `d1Client`,
   bound `id` sent as a string param (so a > 2^53 watermark is not itself JS-rounded — the
   ADR-0132 discipline). Reconstruction is the shared `(typeof, text/hex)` decode. Watermark
   + `MAX(id)` snapshot anchor + schema-drift fingerprint check at stream start are all
   reused from Phase 1.

4. **Consistency — use D1's PRIMARY (strongly-consistent) query path, NOT read replicas.**
   The exactly-once `id > watermark` invariant rests on commit-order = `id`-order (ADR-0135
   §3). D1 serializes writes per database, so that holds at the primary — but D1's Sessions
   API can route reads to **read replicas that lag the primary**. A poll against a lagging
   replica would still never *advance the watermark past an unseen committed row* (it reads
   `id > watermark` and gets whatever the replica has, catching up on a later poll), so it is
   **not a loss** — but it adds latency and, if a later poll hit a *more*-lagged replica, the
   monotonic-read assumption could wobble. Decision: the CDC poll uses the default
   primary-consistent query API (no Sessions/replica routing); a future replica-aware mode
   would have to re-introduce a safety-lag, exactly as the concurrent-writer caveat in
   ADR-0135 §3. Documented as load-bearing.

5. **Operational caveats (documented, not silently handled).** (a) **Write amplification:**
   every D1 write fires a trigger that writes a change-log row — on D1 that is billable rows-
   written and storage; the change-log grows unbounded until the Phase-2 retention/prune
   follow-up. (b) **Polling latency + API limits:** CDC latency is the poll interval + HTTP
   RTT; the cadence must respect D1's request-rate limits (tunable, default 1s as Phase 1).
   (c) Installing triggers + a change-log table MODIFIES the operator's D1 database — use a
   real/test D1, and `trigger teardown` removes every artifact.

## Consequences

- A live D1 database gains continuous logical CDC to PG/MySQL, reusing the lossless D1
  reader + the shared faithful capture/decode (big integers and blobs exact, the ADR-0132
  guarantee carried into CDC) and the Phase-1 exactly-once + schema-drift machinery.
- New concurrency surface is minimal (the poll loop is reused from Phase 1; only the
  transport differs) — the `-race` integration gate still applies before tag.
- D1 CDC is the higher-demand half of the trigger-CDC story (a managed edge DB with no
  other change-stream); Phase 2 makes the `X → D1` story bidirectional (migrate INTO D1 via
  ADR-0134's SQLite target + `wrangler d1 import`; stream OUT of D1 via `d1-trigger`).

## Deferred (Phase 3 / follow-ups)

- Change-log **retention/pruning** (`sluice trigger prune`) — shared with the Phase-1
  local engine; more urgent on D1 (billable writes/storage).
- **Capture-payload trimming** (ADR-0068 full/changed/minimal) to cut D1 write amplification.
- **Schema-change forwarding** (still no DDL triggers in SQLite/D1).
- A **replica-aware** poll mode (Sessions API + safety-lag) if read-replica polling is wanted.

## Alternatives considered

- **Extend `sqlite-trigger` with a transport switch** rather than a new `d1-trigger` engine.
  Rejected for the same reason `d1` is a sibling of `sqlite`: the registry keys on engine
  name, and a separate `d1-trigger` keeps the DSN/token/transport concerns cleanly D1-scoped
  (mirroring the `d1`/`sqlite` split) while still sharing all the logic via composition.
- **D1 Time Travel as the change source.** Rejected: it is point-in-time restore, not a
  consumable change stream.
- **Poll read replicas for lower primary load.** Rejected for Phase 2 (see §4) — breaks the
  no-safety-lag invariant; a deferred replica-aware mode would re-add the lag.
