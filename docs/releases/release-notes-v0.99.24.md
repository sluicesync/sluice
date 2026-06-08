# sluice v0.99.24

**Postgres-source multi-schema continuous sync (ADR-0075 Phase 2b).** `sluice sync start` against a Postgres source now cold-starts **and** continuously replicates multiple schemas in one run — the steady-state CDC counterpart to Phase 2a's multi-schema `migrate`, and the Postgres mirror of the ADR-0074 MySQL multi-database fan-out. **Drop-in from v0.99.23.**

## Added

- **`sync start --include-schema` / `--exclude-schema` / `--all-schemas` for a Postgres source (ADR-0075 Phase 2b).** Each selected source schema is replicated to a same-named target namespace (a Postgres schema, or a database on a MySQL target), for the full cold-start + continuous-CDC lifecycle. A Postgres logical-replication slot is database-wide, so sluice cold-starts the selected schemas under **one spanning exported snapshot**, then routes the single database-wide CDC stream per-change to the matching namespace; warm-resume continues every schema from the one persisted slot/LSN — no per-schema gap or re-copy. The orchestrator is shared with (and unchanged from) the ADR-0074 MySQL multi-database path.

  Previously a multi-schema `sync start` against a Postgres source was refused loudly ("Phase 2b, not in this release") — only the Phase 2a `migrate` snapshot was available. That refusal is now real support.

  Correctness properties (pinned): same-named tables in different schemas are isolated on the target (per-change routing and the per-namespace applier caches are schema-qualified, never keyed by bare table name); a change for an out-of-scope schema is dropped, never misapplied; the spanning snapshot → CDC handoff is gapless by slot construction; and a CDC `TRUNCATE` routes to exactly one namespace without bleeding. Verified by per-drop-site scope unit tests plus PG→PG and PG→MySQL multi-schema integration tests (cold-start + steady-state insert/update/delete/truncate + cross-schema bleed guards + warm-resume parity), all under the CI `-race` Integration gate.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.23. The `--*-schema` flags previously usable only with `migrate` now also work with `sync start` against a Postgres source. Single-schema `sync start` behavior is unchanged (byte-identical back-compat, pinned).

## Who needs this

- **Anyone replicating multiple schemas from one Postgres database in a single continuous sync.** You can now `sync start --all-schemas` (or an explicit `--include-schema a,b,c`) and get a consistent cold-start across all of them plus a routed CDC tail, instead of running one sync per schema. Mirrors `--all-databases` for a MySQL source.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.24`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.24`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
