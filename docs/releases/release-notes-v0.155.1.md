# sluice v0.155.1

**PATCH — one HIGH silent defect from the v0.155.0 regression cycle: on a MariaDB binlog source, a forwarded `ADD COLUMN` landed the WRONG DEFAULT on the target (Bug 286).** Nothing else changes.

<!-- notes-claims-exempt: GC-28, GC-29 -->

## Fixed

**MariaDB binlog `sync`: a forwarded `ADD COLUMN` now lands the declared DEFAULT (Bug 286).** MariaDB's `information_schema.columns` reports defaults differently from MySQL — the literal word `NULL` for "no default", and string defaults pre-quoted — and sluice's cold-start schema reader has translated them through `translateMariaDBDefault` since item 73. The CDC boundary reader `loadTableSchema`, whose projection the ADR-0091 forward intercept re-emits as target DDL, still ran the MySQL-convention `translateDefault` on every flavor, under a comment that said the dispatch was deliberately omitted until a consumer re-emitted the defaults. That consumer had existed since ADR-0091 shipped. Measured on MariaDB 11.4 → Postgres, all silent at exit 0 with the stream running: a nullable no-default column arrived as `DEFAULT 'NULL'` (the four-character string, which a target-side INSERT omitting the column then reads back as the string `NULL`); `DEFAULT 'abc'` as `'''abc'''`; `NOT NULL DEFAULT ''` as `''''''`; `DEFAULT 'it''s'` doubled again; and `DEFAULT CURRENT_TIMESTAMP` killed the stream with a raw `SQLSTATE 22007` where a MySQL source gets the ADR-0058 §2a designed refusal. MySQL 8.4 was correct on every shape, and a MariaDB cold-start `migrate` was correct on every shape — the two readers of one catalog disagreed, the GC-1 shape one field over. `translateColumnDefault` is now a method on `Flavor`, the single dispatcher both readers call, and the comment is replaced by one naming the consumer and the pin. Pinned by `TestLoadTableSchema_DefaultAgreesWithSeed_EveryBinlogFlavor` (the same flavor-shaped catalog rows fed to both readers through a fake driver over every binlog flavor from the registry — seed must equal projection, and both must equal the parity value; red before the fix on exactly the five MariaDB shapes, green on vanilla as the control) and on real MariaDB by `TestStreamer_AddColumnForward_MariaDBToPostgres_DefaultShapes` (indexed table, each shape forwarded, the TARGET's `column_default` graded and a target-side INSERT omitting the column read back — SQL NULL, `abc`, `5`, empty, `it's` — and the `CURRENT_TIMESTAMP` shape now meets the ADR-0058 refusal, not `22007`). Mutation-run: reverting the dispatch turns both pins red, the integration one reproducing the filing byte for byte. Sibling sweep: `populateColumns` already dispatched; `ChangeApplier.colTypesFor` reads types only and never re-emits a default; the VStream lane carries no `COLUMN_DEFAULT` and is never MariaDB; the mydumper reader parses `SHOW CREATE` text, not `information_schema`, and is exempt by construction. **Affected: every release with MariaDB CDC (ADR-0170) through v0.155.0, on MariaDB binlog sources only. Pre-existing — v0.155.0's GC-1 fix is what let INDEXED tables reach the boundary at all, which is how the cycle found it.** Zero loss on replicated rows: CDC rows carry explicit values. The harm is the target's column definition, felt by every target-side write that omits the column after cutover.

## Compatibility

No behaviour change for a MySQL, PlanetScale, Vitess, Postgres, SQLite or D1 source. A MariaDB `sync` that forwarded an `ADD COLUMN` on v0.155.0 or earlier holds a wrong default on the target: check `information_schema.columns.column_default` for every column added mid-sync and `ALTER TABLE … ALTER COLUMN … SET DEFAULT` / `DROP DEFAULT` by hand. Two neighbours found by the sweep and NOT fixed here are filed: a MariaDB dump spelling `DEFAULT current_timestamp()` is refused loudly by the mydumper reader (GC-28), and the MySQL boundary reader does not run the NUL-truncated binary-literal recovery the cold-start reader runs (GC-29, narrow, vanilla MySQL only).

## Who needs this — action required

- **Anyone running `sync` from a MariaDB source whose stream forwarded an `ADD COLUMN`** on v0.155.0 or earlier: upgrade, then audit the target's `column_default` for every column added mid-sync as above. Rows are intact; only defaults for future target-side writes are wrong.
- **Everyone else.** Nothing to do.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.155.1` · **Container:** `ghcr.io/sluicesync/sluice:0.155.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
