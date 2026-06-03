# sluice v0.36.0 — `sluice matview refresh` (View support Phase 2)

**View support Phase 2 lands.** Closes Roadmap Item 13 Phase 2 with the operator-cadence-agnostic shape: a one-shot subcommand operators drive from their own scheduler (cron / k8s CronJob / Airflow). Sluice deliberately does NOT own the refresh loop — cadence is operator-policy, and external scheduling brings alerting / backoff / observability the operator already has wired up.

## Added

- **`sluice matview refresh --target=DSN --target-driver=postgres [--matview NAME] [--concurrently] [--target-schema=NAME] [--format=text|json]`** — new top-level subcommand. Drives `REFRESH MATERIALIZED VIEW [CONCURRENTLY] schema.name` against every matview in the target schema, or only those named via repeated `--matview` flags.

- **Concurrent refresh with unique-index preflight.** PG's `REFRESH MATERIALIZED VIEW CONCURRENTLY` requires a unique index on the matview; the path queries `pg_indexes` up-front and falls back to a clear skip-with-reason instead of letting PG return its less-clear error mid-refresh. Matviews without a unique index land in the result's `Skipped` list with reason naming the missing-index requirement.

- **Loud-failure on missing matview filter.** `--matview NAME` where NAME doesn't exist in `pg_matviews` surfaces as a clear error naming every missing matview — better than silently no-op'ing a typo. Validation runs before any REFRESH call.

- **Per-matview timing in output.** Text format renders human-readable rows (`refreshed: schema.name (123ms)` / `skipped: schema.name — reason`); JSON format emits `{"refreshed":[{...}],"skipped":[...]}` for metric-scraper integration.

## Compatibility

- **Drop-in upgrade from v0.35.x.** No format changes, no engine-interface changes. The implementation lives in the postgres package only — MySQL is unaffected. The new subcommand is purely additive.

- **PostgreSQL-only.** `sluice matview refresh --target-driver=mysql` refuses with a clear error naming MySQL's lack of matview concept.

## Phase 3 — deferred

Phase 3 (cross-engine view-body translation via a SELECT-grammar translator) remains deferred. Phase 1's loud-failure-at-apply-time path handles non-portable view definitions today; the `--view-override` escape hatch lands when real operator demand surfaces a specific cross-engine view that hits the loud-failure.

## Who needs this release

- **PG operators with materialized views that should refresh on a cadence:** **upgrade** — wire the subcommand into your scheduler. For matviews with a unique index, prefer `--concurrently` so reads keep working during the refresh.
- **PG operators with existing matview cron pipelines** (pg_cron, manual REFRESH calls): drop-in; sluice doesn't displace your existing setup.
- **MySQL operators**: drop-in; the new command refuses with a clear error if invoked against a MySQL target.

## Example: k8s CronJob refreshing nightly

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: sluice-matview-refresh
spec:
  schedule: "0 2 * * *"  # 02:00 UTC daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: sluice
            image: sluicesync/sluice:0.36.0
            command:
            - sluice
            - matview
            - refresh
            - --target-driver=postgres
            - --target=$(TARGET_DSN)
            - --target-schema=analytics
            - --concurrently
            - --format=json
            envFrom:
            - secretRef:
                name: sluice-target-dsn
          restartPolicy: OnFailure
```

The JSON output pipes cleanly into a Prometheus textfile collector or your existing job-result scraper.

## Verification surface

- **3 unit tests** in `matview_refresh_test.go` covering SQL-statement shape across the four arg permutations, filter-by-name behaviour, and loud-failure-on-typo path.
- **5 integration tests** in `matview_refresh_integration_test.go` (against `postgres:16` testcontainers): plain refresh round-trip (pre/post row counts), concurrent refresh with unique index, concurrent refresh skipped when no unique index, `--matview` filter narrowing, and missing-matview loud-failure. All pass locally; CI runs them under the `integration` build tag.
