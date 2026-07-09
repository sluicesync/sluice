# Stable error codes and exit codes

sluice's error messages have always named the remedy in prose — "pass `--zero-date=null`", "use `--resume`" — but prose is a poor branching surface for scripts, log pipelines, and AI agents driving the CLI. Every error class that carries an operator hint therefore also carries a **stable error code**: a frozen `SLUICE-E-<DOMAIN>-<SLUG>` identifier that machines can match exactly. The human-facing message is unchanged; the code and a concise remedy hint ride along as metadata.

Where the metadata surfaces:

- **Structured logs.** Under the global `--log-format json` flag, a terminal coded error emits one ERROR record with `code`, `hint`, and `err` attributes; a `sync` supervisor marking a stream permanently failed attaches the same `code`/`hint` attributes to its record. Text-format logging shows the identical record in slog's text shape.
- **Exit codes.** Refusal-class codes map to exit code 3 (see the taxonomy below), so a caller can distinguish "sluice refused and named the remedy — retrying won't help" from a generic runtime failure without parsing anything.
- **Go API.** Inside the codebase the metadata is a `sluicecode.CodedError` (`internal/sluicecode`), extractable at any boundary via `errors.As` — the JSON result-envelope work lifts `Code` and `Hint` from the same type.

Codes are minted only for errors that already carry an operator hint or a named remedy, and the registry grows organically as new remedies earn one — it is deliberately not a catalogue of every possible error. Once shipped, a code string is frozen; renaming or removing one is a breaking change. The registry in `internal/sluicecode/sluicecode.go` is the single source of truth, and a unit test enforces that this table and the registry match in both directions.

## Error codes

| Code | Class | Meaning | Remedy |
|---|---|---|---|
| `SLUICE-E-CONNECT-REFUSED` | runtime | The database host/port is unreachable from this machine. | Verify the DSN host/port and network reachability. |
| `SLUICE-E-CONNECT-AUTH-FAILED` | runtime | The database rejected the DSN credentials. | Verify the DSN username and password. |
| `SLUICE-E-CONNECT-DATABASE-MISSING` | runtime | The DSN names a database that does not exist on the server. | Verify the DSN database name. |
| `SLUICE-E-BULKCOPY-TARGET-TABLE-MISSING` | runtime | Bulk-copy hit a missing target table — schema-apply failed or wrote into a different schema. | Check the schema-apply phase's output and the target schema/database the DSN points at. |
| `SLUICE-E-BULKCOPY-TABLE-FAILED` | runtime | A table failed mid-bulk-copy; earlier tables have data but not their declared secondary indexes yet (the indexes phase runs after all tables finish copying). | Fix the offending table and continue with `--resume`, or skip it with `--exclude-table=<name>`. |
| `SLUICE-E-SCHEMA-PERMISSION-DENIED` | runtime | The target role lacks CREATE on the schema. | GRANT the privilege or use a different role. |
| `SLUICE-E-INDEX-STATEMENT-TIME-LIMIT` | runtime | A post-copy index build hit PlanetScale's statement-time limit (MySQL errno 3024); the data is already copied. | `--resume` finishes just the indexes with no re-copy (grow the PlanetScale cluster first for a faster build), or start fresh with `--upfront-indexes`. |
| `SLUICE-E-INDEX-DIRECT-DDL-DISABLED` | runtime | PlanetScale safe-migrations is enabled on the target branch and blocks direct DDL (errno 1105). | Disable safe-migrations on the branch for the migration; sluice does not yet drive PlanetScale deploy requests. |
| `SLUICE-E-CDC-REPLICATION-PERMISSION` | runtime | The connecting role lacks the REPLICATION attribute. | `ALTER ROLE x REPLICATION`; see [postgres-source-prep](../postgres-source-prep.md). |
| `SLUICE-E-COLDSTART-TARGET-NOT-EMPTY` | refusal | Cold-start refused: a target table already contains data (usually a previous run died mid-copy). | Sync: re-run with `--reset-target-data --yes`. Migrate: use `--resume`. Either mode: `--force-cold-start` to copy into the populated table anyway (collides on PRIMARY KEY in most cases). |
| `SLUICE-E-SCHEMA-EXTENSION-NOT-ENABLED` | refusal | A column's type is owned by a PostgreSQL extension the operator has not opted into. | Pass `--enable-pg-extension <ext>`; see [type-mapping](../type-mapping.md). |
| `SLUICE-E-VALUE-ZERO-DATE` | refusal | A MySQL zero/partial date (`0000-00-00 …`) has no valid calendar value the target can hold. | Pass `--zero-date=null` or `--zero-date=epoch` to carry it; see [migrating-legacy-mysql](migrating-legacy-mysql.md). |
| `SLUICE-E-VALUE-NUL-BYTE` | refusal | A string value carries a NUL byte (0x00), which PostgreSQL text types cannot store. | Clean the source data, or map the column to bytea with `--type-override COL=bytea`. |
| `SLUICE-E-EXPR-BACKSLASH-LITERAL` | refusal | A SQLite expression's string literal contains a backslash (or a double-quoted token), which MySQL would silently reinterpret under its default sql_mode. | Rewrite the expression on the SQLite source, or re-create it on the MySQL target post-migration. |
| `SLUICE-E-CONFIRMATION-REQUIRED` | refusal | A destructive command was run without `--yes`. sluice is non-interactive and never prompts, so it refuses loudly instead of blocking on a prompt (`slot drop` is the current caller). | Re-run with `--yes` (or `-y`) to confirm the destructive operation. |
| `SLUICE-E-DRIVER-HOST-MISMATCH` | refusal | The chosen driver cannot drive the DSN's host — today: the vanilla `mysql` driver pointed at a PlanetScale endpoint (`*.connect.psdb.cloud`), whose binlog CDC and `LOAD DATA` cold-copy Vitess blocks. Caught up front, before any connection. | Pass `--source-driver planetscale` / `--target-driver planetscale` for the PlanetScale endpoint. |
| `SLUICE-E-INDEX-MISSING` | refusal | The post-copy verification found the target is missing one or more secondary indexes the migration was expected to build (named as `table.index` in the message) — a loud-failure safety net against a silent index-build no-op. sluice refuses to report a successful migration with an incomplete schema. | Re-run with `--resume` to rebuild the missing indexes; if it recurs, the target rejected the index DDL — check the target's DDL/online-migration policy and the logs for the underlying error. |
| `SLUICE-E-VSTREAM-FLOAT-LOSSY` | refusal | `backup full` on a PlanetScale/Vitess (VStream) source with `--strict-float`, when a single-precision `FLOAT` column cannot be re-read exactly: the table is **keyless / float-PK-only** (no primary key to target the exact re-read — refused upfront) or **larger than `--float-reread-max-rows`** (too large for the bounded-memory exact re-read — refused when reached). vttablet's rowstreamer renders FLOAT at mysqld's 6-significant-digit display precision, and `--strict-float` demands exact-or-fail. | Add a primary key (or exclude the table), raise `--float-reread-max-rows` if it's a size cap and you have the headroom, or drop `--strict-float` (the default archives exact where it can and rounded — with a WARN — elsewhere). A target-side `--type-override` to DOUBLE does NOT help — the source value is already rounded on the wire. |
| `SLUICE-E-BACKUP-SIGNATURE-INVALID` | refusal | A signed (FormatVersion 6, ADR-0154) backup manifest's detached signature failed verification — the manifest was tampered, rolled back to an older version, its change-list was truncated, or the wrong key was supplied. sluice refuses to restore/verify it before any data lands. | Restore from an untampered copy of the backup; if the whole store is suspect, the signature caught exactly the substitution it exists to catch. Verify the passphrase/key matches the chain. |
| `SLUICE-E-BACKUP-SIGNATURE-MISSING` | refusal | A signed (FormatVersion 6) backup manifest asserts a detached signature but none is present (or the lineage catalog's signature is absent) — the tamper signal for a dropped signature or a signed-chain manifest replaced without re-signing. | Restore from a copy whose `.sig` objects are intact; a maintenance run (compact/prune) that could not re-sign must be re-run with the chain's `--encrypt` key material. |

The class column drives the exit code: a terminal `refusal` exits 3, a terminal `runtime` code exits 1 like any other failure — the code is still in the log record either way.

## Exit codes

sluice historically exited 0 on success and 1 on everything else. The taxonomy below keeps those two meanings stable and carves two classes out of the generic-failure bucket, so nothing that checks `!= 0` changes behaviour.

| Exit code | Meaning |
|---|---|
| 0 | Success. For `verify`, `diff`, and `sync-health`: success and clean. |
| 1 | Generic runtime failure. For `verify`/`diff`/`sync-health` this is those commands' long-standing per-command meaning: the check ran and found a mismatch / drift / stale stream. |
| 2 | Config error: the `--config` file could not be loaded or parsed. (The read-side commands `verify`/`diff`/`sync-health`/`metrics-watch` have always used 2 more broadly for "the check could not run at all" — that per-command contract is unchanged.) |
| 3 | Named refusal: sluice declined to proceed (or to silently alter a value) and named the remedy — the refusal-class codes in the table above. Retrying without acting on the hint will fail identically. |
| 80 | Usage error: kong (the CLI parser) exits 80 on unknown flags/commands and missing required arguments before any sluice code runs. sluice adopts this rather than remapping it. |

**Backward compatibility.** Scripts and unit files that check `exit != 0` (including the systemd `Restart=on-failure` pattern in [running-as-a-service](running-as-a-service.md)) are unaffected — every failure class is still non-zero. Scripts that check `exit == 1` specifically should be updated: config errors and named refusals that previously exited 1 now exit 2 and 3 respectively.
