# Schema-change runbook

The operator reference for schema changes with sluice — both halves of the problem:

1. **Executing a schema change safely** on a live database. sluice ships a family of schema-change commands for this: `sluice expand-contract` (the full expand→migrate→contract pattern), `sluice backfill` (the data-migration middle step on its own), and `sluice deploy-ddl` + `sluice control-tables ddl` (the governed DDL channel and bootstrap for PlanetScale safe-migrations branches).
2. **Coordinating a schema change with a running sync stream** so the stream stays continuous through it.

What sluice still deliberately is *not*: a versioned-migration tool. There is no migration history table, no multi-environment promotion, no down-migrations — Atlas / Flyway / sqitch / liquibase own that layer, and sluice's commands compose with them (your tool decides *what* changes; sluice's commands are one safe way to *execute* it).

## Which command do I want?

| You want to… | Run | Engines |
|---|---|---|
| Add a column, backfill it, then drop the old one — online, gated, one command | `sluice expand-contract` | PlanetScale (safe migrations ON) |
| Fill/transform a column's values in place — resumable, keyset-chunked, no long locks | `sluice backfill` | MySQL / PlanetScale / Vitess / Postgres |
| Ship one DDL statement to a PlanetScale safe-migrations branch via a deploy request | `sluice deploy-ddl` | PlanetScale |
| Bootstrap sluice's own control tables on a safe-migrations branch | `sluice control-tables ddl` → `sluice deploy-ddl` per statement | PlanetScale |
| Preview the DDL sluice would emit for a target | `sluice schema preview` | all |
| Check whether source and target schemas have drifted | `sluice schema diff` | all |
| Keep a running sync stream aligned through source DDL | see [Coordinating with a running stream](#coordinating-a-schema-change-with-a-running-stream) | all CDC engines |

## `sluice expand-contract` — the full pattern (PlanetScale)

The classic online schema change is expand→migrate→contract: add the new column (expand), backfill the data (migrate), drop the old column (contract). `sluice expand-contract` (ADR-0162) drives all three against a PlanetScale database — dev branch + deploy request for each DDL leg, the ADR-0159 backfill for the data leg, and a verify gate that must pass before the destructive contract leg is allowed:

```bash
export PLANETSCALE_SERVICE_TOKEN_ID='...'   # service token: branch + deploy-request scopes
export PLANETSCALE_SERVICE_TOKEN='...'      # env, never argv — these never land in shell history

sluice expand-contract \
    --org myorg --database mydb --branch main \
    --dsn "$PROD_BRANCH_DSN" --table users \
    --expand-ddl   'ALTER TABLE users ADD COLUMN full_name VARCHAR(255)' \
    --set          'full_name = CONCAT(first_name, " ", last_name)' \
    --where        'full_name IS NULL' \
    --contract-ddl 'ALTER TABLE users DROP COLUMN first_name' \
    --yes
```

Load-bearing details:

- **`--where` is required and doubles as the verify gate.** Make it self-describing (`new_col IS NULL`): after the backfill completes, sluice counts rows still matching it across the whole table — only a count of 0 authorizes the contract leg. A nonzero count fails with `SLUICE-E-BACKFILL-INCOMPLETE` (re-run to catch the stragglers).
- **`--yes` is the destructive confirmation.** The contract leg is a `DROP COLUMN` deploy request against your production branch; without `--yes` (or without `--contract-ddl`) the run stops after verify as a success and prints the exact resume command. `--dry-run` and `--yes` are mutually exclusive by construction — a plan can never confirm a drop.
- **`--dry-run` prints the full plan** — branches, deploy requests, the rendered backfill statement, the gates — with zero control-plane calls and zero writes.
- **Interrupted runs resume with `--resume-from expand|migrate|contract`.** The backfill leg is natively resumable via its persisted cursor; verify always re-runs before contract.
- **A deploy request that outwaits `--deploy-timeout` un-deployed (your org's review queue) keeps its dev branch.** Deleting the branch would close the still-open deploy request you were just told to approve, so on exactly that timeout the cleanup exempts it (as if `--keep-branches` were set) and the message names the kept branch; delete it yourself once the request closes. Every other failure path still cleans up.
- **Safe migrations must be ON** for the production branch (`SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED` otherwise) — deploy requests are the mechanism the command ships DDL through. sluice **never toggles the setting for you**; see [Safe migrations posture](#planetscale-safe-migrations-posture) below.
- Every dev branch passes the **stale-base freshness gate** (`SLUICE-E-PS-BRANCH-STALE-BASE`): a fresh PlanetScale branch's schema can lag production, and a deploy request from a stale base silently proposes *reverting* the missing schema — on the contract leg that would drop the freshly backfilled column. sluice compares the branch schema against production before any DDL, self-heals once via an on-demand backup rebase, and refuses if still stale. Since v0.99.258 (ADR-0167) the deploy request's computed diff is also fetched and refused if it touches any object the leg never intended, and production's schema is re-verified after a long review wait.

## `sluice backfill` — the migrate step alone (MySQL family + Postgres)

When the schema change itself is already handled (or you're not on PlanetScale), the middle step — a batched, resumable, online-safe in-place data migration — is `sluice backfill` (ADR-0159). It is single-endpoint (reads and updates one database) and walks the table's primary key issuing one bounded `UPDATE` per chunk, so no statement approaches PlanetScale/Vitess's synchronous-transaction-time wall (errno 3024) or holds long locks on any engine:

```bash
sluice backfill \
    --driver postgres --dsn "$DSN" \
    --table users \
    --set   "full_name = first_name || ' ' || last_name" \
    --where 'full_name IS NULL' \
    --verify
```

- **`--driver`** is one of `mysql`, `planetscale`, `vitess`, `postgres` (SQLite/D1 refuse with `SLUICE-E-BACKFILL-UNSUPPORTED-ENGINE`). `--set` / `--where` are native SQL for that engine, emitted verbatim; `--set` splits at the *first* `=`, so CASE arms pass through.
- **Resume is automatic.** The cursor persists in the same database's `sluice_migrate_state` control tables, keyed by a hash of the spec (`--set` + `--where`); a killed run resumes where it stopped, replaying at most one chunk — which is why `--where` should be self-describing (`new_col IS NULL`), so the replay is a no-op. `--restart` discards the cursor and re-walks; `--batch-size` is excluded from the spec hash, so retuning it never orphans a cursor.
- **`--verify`** runs a whole-table remaining-count on `--where` after the walk: 0 prints the safe-to-contract signal; >0 exits with `SLUICE-E-BACKFILL-INCOMPLETE` (rows written behind the walk's cursor during the run — re-run to catch up, then verify again). **`--verify-only`** is the standalone scriptable gate for deploy pipelines: no walk, no UPDATEs, no control-table writes, no PK requirement, `--set` optional.
- **`--dry-run`** prints the exact engine-rendered chunk UPDATE plus a remaining-row estimate without writing anything.
- Refusals are loud and coded: a table with no (orderable) primary key refuses with `SLUICE-E-BACKFILL-NO-PRIMARY-KEY` (there is deliberately no force flag — an unbounded UPDATE is the exact shape the command exists to avoid); a `--set` column that doesn't exist refuses with `SLUICE-E-BACKFILL-UNKNOWN-COLUMN` before any UPDATE runs; a cursor written by an older sluice that provably mangled it refuses with `SLUICE-E-BACKFILL-CORRUPT-CURSOR` (re-run with `--restart`); a spec whose state row shows a heartbeat fresher than 5 minutes while still walking refuses with `SLUICE-E-BACKFILL-CONCURRENT-RUN` (another run — typically an overlapping cron invocation — looks live; wait for it to finish or for its heartbeat to go stale, then re-run).

## `sluice deploy-ddl` + `sluice control-tables ddl` — the safe-migrations bootstrap

A PlanetScale branch with **safe migrations enabled refuses every direct DDL statement** (Error 1105, "direct DDL is disabled") — including sluice's own `CREATE TABLE IF NOT EXISTS` for its control tables, and the user-table CREATEs a fresh `migrate` or `sync` cold-start issues. sluice surfaces this as the coded refusal `SLUICE-E-PS-DIRECT-DDL-BLOCKED`, naming the exact refused statement, and the way through is the governed channel:

```bash
# 1. Print the exact CREATE statements for sluice's control tables (read-only, no credentials)
sluice control-tables ddl

# 2. Ship each statement via a deploy request (dev branch → apply → deploy → cleanup, one command)
sluice deploy-ddl --org myorg --database mydb --ddl '<one statement from step 1>'

# 3a. For sync: pre-create the USER tables the same way, then skip schema-apply
sluice schema preview --source-driver mysql --source "$SRC" --target-driver planetscale --target "$DST"
sluice deploy-ddl --org myorg --database mydb --ddl '<one CREATE from the preview>'   # per table
sluice sync start ... --schema-already-applied

# 3b. For a one-time migrate: pre-create the user tables via deploy-ddl as in 3a, then just run it —
#     no flag needed. migrate's pre-create shape gate (ADR-0166, v0.99.258) detects each
#     pre-created table, verifies its column shape matches (names/types/nullability), and skips
#     the refused CREATE with an INFO. A shape MISMATCH refuses upfront with
#     SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH before any data moves.
sluice migrate --source-driver mysql --source "$SRC" --target-driver planetscale --target "$DST"
```

`deploy-ddl` (ADR-0165) is the full ADR-0162 safety wrapper around ONE verbatim statement: safe-migrations preflight, dev branch with the stale-base freshness gate, apply, deploy request, deploy, skip-revert finalize, always-cleanup. `--dry-run` makes zero control-plane calls. It is also the general escape hatch for any ad-hoc schema change on a safe-migrations branch. The service token rides `PLANETSCALE_SERVICE_TOKEN_ID` / `PLANETSCALE_SERVICE_TOKEN` (env, never argv), same as `expand-contract`.

Indexes: the deferred index build can also hit the safe-migrations block (or the ~900s statement-time wall) *after* the copy — on `migrate`, on `restore`, and on the `sync start` cold-start alike. Arm the automatic deploy-request index-build fallback (ADR-0148) with `--planetscale-org <org>` plus the service-token env vars (optionally `--planetscale-database` / `--planetscale-branch` / `--planetscale-deploy-timeout`) on whichever of those commands you are running, and the still-pending indexes build through a dev branch + deploy request on the already-copied data, no re-copy. Unarmed, the refusal is `SLUICE-E-INDEX-DIRECT-DDL-DISABLED`.

## PlanetScale safe-migrations posture

sluice **never enables or disables safe migrations** on your branch. It is a behavior change on production (direct DDL becomes blocked from then on), and the enable/disable propagation lag makes toggling it around a run unsafe — so:

- `expand-contract` / `deploy-ddl` **require it ON** and refuse with `SLUICE-E-PS-SAFE-MIGRATIONS-DISABLED` when it's off (with it off, direct DDL works and you don't need them).
- `migrate` / `sync` / `backfill` work either way: with it ON, bootstrap via the `control-tables ddl` → `deploy-ddl` flow above; with it OFF they issue DDL directly as on any MySQL.
- If you hit `SLUICE-E-PS-DIRECT-DDL-BLOCKED`, the remedy is the governed channel (or a deliberate operator decision to disable safe migrations for a migration window) — never sluice flipping the toggle for you.

## Coordinating a schema change with a running stream

Separately from executing the change, a schema change on a *source under continuous sync* has to reach the target without breaking the stream.

**Default behavior: unambiguous DDL forwards online.** Since v0.92 (`--schema-changes=forward`, the default), the streamer applies every unambiguous source schema change on the target through the live CDC apply path — `ADD`/`DROP COLUMN`, `ALTER COLUMN TYPE`, `ALTER` nullability, `CREATE`/`DROP INDEX`, `CHECK` changes — logging each applied DDL at INFO. The stream stays online through routine schema evolution; usually there is nothing to coordinate.

**What still refuses loudly:** `RENAME COLUMN` and multi-shape combos (a rename is indistinguishable from drop+add without a stable column id — forwarding the wrong guess risks silent data loss), and an `ADD COLUMN` whose `DEFAULT` is computed/volatile (`NOW()`, `nextval()`, `gen_random_uuid()`, … — forwarding it would silently diverge already-shipped rows). The refusal carries the per-table drift diff (F11) naming every drifted column/index/constraint plus a recovery hint per category.

**`--schema-changes=refuse`** restores the conservative pre-v0.92 posture — every source DDL surfaces loudly — for shops that gate DDL through a separate change-management process.

**The drained-model workflow** (for refused shapes, or under `refuse` mode):

```bash
# 1. Drain and stop; --wait blocks until the streamer confirms clean drain
sluice sync stop --wait \
    --target-driver postgres --target "$TARGET_DSN" \
    --stream-id myapp-prod

# 2. Apply the change on source and target with your SQL client, e.g.:
psql "$TARGET_DSN" -c 'ALTER TABLE accounts RENAME COLUMN old_name TO new_name'
# (and the equivalent ALTER on the source side)

# 3. Restart with the SAME --stream-id — sluice warm-resumes from the persisted CDC position
sluice sync start \
    --source-driver mysql    --source "$SOURCE_DSN" \
    --target-driver postgres --target "$TARGET_DSN" \
    --stream-id myapp-prod
```

`sync stop --wait` guarantees the in-flight batch is committed and the CDC position is persisted past the last applied event. Re-running `sync start` with the same `--stream-id` warm-resumes automatically (there is no `--resume` flag on `sync start`); the post-restart CDC schema cache rebuilds from the first event, so the new shape is recognized on both sides.

**Type widening + a value the target can't hold** stays fail-loud by design: sluice preserves the source value bit-for-bit and lets the target's stricter constraint reject (`SQLSTATE 22001` or analogous) — the alternative is silent truncation. Widen the *target* column first (drained-model above), then restart; `--type-override TABLE.COLUMN=TYPE` is the tool when you deliberately want the target type to differ from sluice's auto-emit choice.

## Planning ahead with `sluice schema diff`

`sluice schema diff` (ADR-0029) runs the source schema through sluice's translation pipeline and compares the result against the target's actual schema. Use it to:

1. **Pre-flight a planned ALTER** — apply it on the source, run the diff, and the missing-on-target columns / type mismatches surface with suggested `ALTER` statements as a starting point.
2. **Verify post-ALTER alignment** — expect "in sync" + exit 0; CI-gateable.
3. **Catch unintended drift** — hand-edits on the target that were never mirrored surface before they break CDC apply.

The suggested ALTER statements are starting points, not verified migration scripts — the diff doesn't know your data volume, lock duration, or downstream consumers.

## When to reach for Atlas / sqitch / liquibase instead

Use a dedicated migration tool when you need version-controlled migration history with an audit log, multi-environment promotion (dev → staging → prod), down-migrations, or schema-as-code in your application's build. sluice runs alongside those: your migration tool decides and versions the change; `sluice backfill` / `expand-contract` / `deploy-ddl` are safe executors for the online-change and safe-migrations cases; and the coordination workflow above keeps a running stream aligned while the change lands.

## See also

- [`docs/operator/error-codes.md`](operator/error-codes.md) — every `SLUICE-E-*` code named above, with remedies
- [`docs/managed-services.md`](managed-services.md) — the PlanetScale safe-migrations bootstrap story in full, plus provider preconditions
- `docs/adr/adr-0159-standalone-backfill-command.md` — backfill design (keyset walk, cursor, verify gate)
- `docs/adr/adr-0162-planetscale-expand-contract-orchestration.md` — expand-contract design + live-validation findings
- `docs/adr/adr-0165-deploy-ddl-and-control-table-bootstrap.md` — deploy-ddl + control-tables ddl
- `docs/adr/adr-0166-migrate-precreate-shape-gate.md` — the pre-create shape gate
- `docs/adr/adr-0167-legrunner-predeploy-gates.md` — the pre-deploy diff + freshness gates
- `docs/adr/adr-0091-default-on-schema-change-forwarding.md` — online schema-change forwarding (`--schema-changes`)
- `docs/adr/adr-0025-graceful-drain-stop.md` — the `sync stop --wait` mechanism
- [`docs/throughput-tuning.md`](throughput-tuning.md) — knobs for a bulk-copy rerun if a change requires `--reset-target-data`
