# Materialized view refresh

`sluice matview refresh` is a PostgreSQL-only operator subcommand (shipped in v0.36.0) that refreshes one or more materialized views on the target. It's a thin, one-shot wrapper around `REFRESH MATERIALIZED VIEW [CONCURRENTLY]` designed for cron / Kubernetes CronJob / Airflow integration — sluice does **not** own the refresh loop.

## Why this exists

Sluice's matview support has two phases:

1. **Cold-start population (Phase 1, v0.35.x).** When the target schema includes a `CREATE MATERIALIZED VIEW`, sluice emits `WITH DATA` and the matview is populated on initial schema apply. This part is automatic.
2. **Ongoing refresh (Phase 2, v0.36.0).** Once CDC traffic is flowing, the matview's underlying base tables change but PostgreSQL does **not** auto-refresh dependent matviews — that's a deliberate PG design choice. Operators who want fresh matview contents on a cadence drive this subcommand from their own scheduler.

The split is deliberate: refresh cadence is a per-workload policy decision (every 5 min? hourly? after a nightly batch load?). Burying that policy inside sluice would surprise operators; surfacing it as an explicit subcommand makes the choice visible.

## Quick start

Refresh every matview in the `public` schema on the target:

```bash
sluice matview refresh \
    --target-driver=postgres \
    --target="postgres://user:pass@host:5432/db?sslmode=disable"
```

Refresh a single matview concurrently (reads keep working during the refresh):

```bash
sluice matview refresh \
    --target-driver=postgres \
    --target="$SLUICE_TARGET" \
    --matview=daily_sales_rollup \
    --concurrently
```

Refresh multiple matviews in JSON format for a metrics scraper:

```bash
sluice matview refresh \
    --target-driver=postgres \
    --target="$SLUICE_TARGET" \
    --matview=daily_sales,user_activity,top_products \
    --concurrently \
    --format=json
```

## Flags

| Flag | Required | Notes |
|---|---|---|
| `--target-driver` | yes | Must be `postgres`. The command refuses with a clear error on any other driver (MySQL has no materialized view concept). |
| `--target` / `SLUICE_TARGET` | yes | PostgreSQL DSN. |
| `--target-schema=NAME` | no (default `public`) | Scope the refresh to one schema. Matches sluice's other `--target-schema` flags (ADR-0031). |
| `--matview=NAME` | no | Refresh only these matview names (comma-separated, repeatable). When empty, every matview in `--target-schema` is refreshed. Names match `pg_matviews.matviewname` **case-sensitively**. |
| `--concurrently` | no | Emit `REFRESH MATERIALIZED VIEW CONCURRENTLY`. Requires a unique index on the matview (PG enforces this). Matviews without a unique index are **skipped with a clear warning** rather than failing the whole run. |
| `--format=text\|json` | no (default `text`) | Output format. `json` is the machine-readable shape for piping through `jq` or a metrics scraper. |

## When to use `--concurrently`

| Situation | Recommendation |
|---|---|
| Matview has a unique index and downstream readers are running. | **Yes** — `CONCURRENTLY` lets reads continue against the old snapshot during the refresh. PG swaps the snapshot atomically at the end. |
| Matview is small (refresh < 1 sec) and reads are infrequent. | Default (no flag) is fine — the locked refresh is simpler and slightly faster. |
| Matview has no unique index. | `--concurrently` will skip it (sluice surfaces this in the output). Either add a `CREATE UNIQUE INDEX` to the schema, or drop the flag for the regular blocking refresh. |
| The matview is recreated on every refresh (some ETL patterns). | Out of scope — this subcommand only issues `REFRESH`, not `DROP / CREATE`. |

## Output

### Text format (default)

```
refreshed: public.daily_sales_rollup (1.247s)
refreshed: public.user_activity (412ms)
skipped:   public.top_products — concurrent refresh requested but no unique index found

matview refresh: 2 refreshed, 1 skipped
```

### JSON format

```json
{
  "refreshed": [
    {"schema": "public", "name": "daily_sales_rollup", "duration_ms": 1247},
    {"schema": "public", "name": "user_activity", "duration_ms": 412}
  ],
  "skipped": [
    {"schema": "public", "name": "top_products", "reason": "concurrent refresh requested but no unique index found"}
  ]
}
```

The JSON shape is stable; tooling can rely on the field names.

## Common error shapes

- **`--target-driver=mysql`** → refuses with `matview refresh is PostgreSQL-only (MySQL has no materialized view concept)`. Use this command only against Postgres targets.
- **Unknown matview name** (`--matview=foo` where `foo` does not exist in `--target-schema`) → the command runs and the requested matview simply doesn't appear in the output. No error. Validate the name list against `\dv+` first if the operator wants strict-error behaviour.
- **Concurrent refresh without unique index** → the matview is **skipped**, not failed. The run continues with the remaining matviews. The skip is surfaced in both text and JSON output.
- **Connection / authentication failure** → exits non-zero with the DSN-level error on stderr.

The exit code is non-zero on any *internal* error (connection, statement-level failure on a non-skipped matview); skipped matviews do **not** flip the exit code. Cron / k8s CronJob exit-status branching gets a clean signal: zero means "refresh attempt completed", non-zero means "operator action needed".

## Scheduling patterns

### cron

```cron
# Refresh hourly during business hours
0 8-18 * * 1-5  sluice matview refresh \
    --target-driver=postgres \
    --target="$SLUICE_TARGET" \
    --matview=hourly_sales \
    --concurrently \
    --format=json \
    >> /var/log/sluice-matview.log 2>&1
```

### Kubernetes CronJob

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sluice-matview-refresh
spec:
  schedule: "*/15 * * * *"   # every 15 min
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
          - name: sluice
            image: ghcr.io/sluicesync/sluice:latest
            env:
            - name: SLUICE_TARGET
              valueFrom:
                secretKeyRef:
                  name: db-creds
                  key: dsn
            args:
            - matview
            - refresh
            - --target-driver=postgres
            - --concurrently
            - --format=json
```

### Airflow

Wrap the binary in a `BashOperator` or `KubernetesPodOperator`; the JSON output streams cleanly into Airflow's task-log capture and downstream XCom.

## Cadence guidance

There's no universally-right answer; cadence is a workload-policy decision. Some reference points:

| Workload pattern | Suggested cadence |
|---|---|
| Real-time-ish dashboards backed by a matview rollup of CDC-streamed source data | Every 1–5 minutes (with `--concurrently`) |
| Hourly reporting / BI extract | Every 15–60 minutes |
| Daily batch / overnight reporting | Once per day (off-peak) |
| Pre-computed analytics for monthly close | Once per day or on-demand |

The refresh cost scales with the matview's underlying query, not with sluice — operators should benchmark against the unrefreshed query and pick a cadence that keeps tail latency tolerable.

## See also

- ADR-0024 (`docs/adr/adr-0024-schema-preview.md`) — how sluice previews target DDL before any data moves.
- `docs/schema-change-runbook.md` — broader operator workflow for ALTERs and schema lifecycle events.
- Roadmap item 13 (`docs/dev/roadmap.md`) — view + matview support phases.
