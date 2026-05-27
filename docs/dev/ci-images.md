# Pre-baked CI container images

The integration suite's MySQL, Postgres, and PostGIS containers boot from pre-baked images published to GitHub Container Registry, not from the upstream Docker Hub tags. This page explains why, how the images are built, how the weekly cron keeps them current, and what to do when the upstream base version genuinely changes (e.g. MySQL 8.0 → 8.4).

## Why this exists

The integration suite repeatedly hit boot-time flakes on the self-hosted runner pool that were not test bugs and not feature regressions — just the cost of multiple MySQL containers booting concurrently against the same `/var/lib/docker`. Tasks #60, #63, #64, and #69 walked the boot-budget knobs upward:

- **#60** — first-attempt single-shot boot failed all ~62 tests in the engines-mysql shard when `wait until ready` timed out under disk-I/O contention. Added a 3-attempt retry with 30s/60s backoff.
- **#63** — the same flake hit the per-test MySQL containers in `internal/pipeline`. Added the same retry shape there.
- **#64** — bumped the per-attempt timeout from 2 minutes to 4 minutes after CI evidence showed slow attempts stretching past 2min under load.
- **#69** — surfaced that `testcontainers.WithWaitStrategy` hard-wraps the inner strategy with a 60-second outer deadline; needed `WithWaitStrategyAndDeadline` to actually honor the 4-minute budget.

Each round bought headroom without eliminating the root cause. **The root cause is the first-boot init step:** `mysqld --initialize-insecure` writes 50-100 MB of system tables to disk, and `initdb` writes ~40 MB; when multiple containers do that concurrently on a contended runner, individual boots stretch past whatever budget is currently in place.

**Task #68** cuts the root cause by baking the init step into the image. The script in `scripts/build-prebaked-images.sh` runs the init once, then `docker commit`s the resulting filesystem to an image tagged `ghcr.io/orware/sluice-{mysql,postgres,postgis}:*-prebaked`. The integration suite pulls these images instead of upstream, so cold-start drops from 30-60s (or 2-3 minutes under contention) to ~5s.

The retry-with-backoff and 4-minute timeout scaffolding stays in place as defense in depth — if ghcr.io is unreachable or the bake is broken, the retry layer absorbs the boot failure rather than failing all ~62 tests in the shard. A `TODO(#68-follow-up)` near `sharedMySQLBootTimeout` / `mysqlBootTimeout` notes that the timeout can revert to 2 minutes (or even 1 minute) once a few CI cycles confirm the pre-baked image is reliable.

## What gets baked, what doesn't

Pre-baked:

- `ghcr.io/orware/sluice-mysql:8.0-prebaked` — upstream `mysql:8.0` + `mysqld --initialize-insecure` with `--log-bin --binlog-format=ROW --binlog-row-image=FULL` flags matching the shared TestMain's Cmd args.
- `ghcr.io/orware/sluice-postgres:16-prebaked` — upstream `postgres:16` + `initdb`.
- `ghcr.io/orware/sluice-postgis:16-3.4-prebaked` — upstream `postgis/postgis:16-3.4` + `initdb`.

NOT pre-baked:

- `pgvector/pgvector:0.7.4-pg16` — niche extension image used by one test suite. Pre-baking a third artifact for a narrow use case isn't worth the maintenance cost.
- `postgres:17` — used only by the failover-flag tests in `internal/engines/postgres/cdc_reader_integration_test.go`; per-boot cost on a niche path is acceptable.
- `vitess/vttestserver:mysql80`, `vitess/lite`, `quay.io/coreos/etcd` — Vitess test images; behind opt-in `vstream` / `vitessreshard` build tags, not in the default CI gate.

Byte-equivalence is preserved: the pre-baked images are the same base image plus the result of the init step the tests would otherwise run on first boot. No extra layers, no extensions, no config files copied in. The runtime Cmd args (`-c wal_level=logical` etc.) in the TestMains' container customisers are still applied at boot the same way they would be against the upstream image; the pre-bake just removes init from the critical path.

## How to manually rebuild

The script is `scripts/build-prebaked-images.sh`. Run it from anywhere with Docker available:

```bash
# Build + push all three engines (requires GHCR_TOKEN exported).
export GHCR_TOKEN="$(gh auth token)"
./scripts/build-prebaked-images.sh

# Build a single engine.
ENGINES=mysql ./scripts/build-prebaked-images.sh

# Build but don't push (useful for local dev / dry run).
SKIP_PUSH=1 ENGINES=postgres ./scripts/build-prebaked-images.sh

# Override the GHCR namespace (for forks).
GHCR_NAMESPACE="ghcr.io/your-org" GHCR_USER="your-org" ./scripts/build-prebaked-images.sh
```

Idempotency: the script consults `docker manifest inspect` for the target tag and reads the `sluice.basedigest` label off the existing image. If that label matches the digest of the upstream base image you just pulled, the bake is skipped. So the weekly cron is a no-op when upstream hasn't republished its tag.

## Weekly cron

`.github/workflows/build-prebaked-images.yml` runs `scripts/build-prebaked-images.sh` on a weekly cron (Sunday 06:00 UTC) and on `workflow_dispatch`. It uses the workflow's `GITHUB_TOKEN` with `packages: write` to push to `ghcr.io/orware/sluice-*`. On uneventful weeks (upstream digest unchanged), the script's idempotency check makes each job a few-second no-op. When MySQL 8.0 or postgres:16 publish a patch bump, the next Sunday tick rebakes against the new base.

Failure surface: GitHub's default behavior on a workflow failure emails the repo owner. No extra notification plumbing required.

You can also trigger a rebake manually from the Actions UI (`Build pre-baked CI images` workflow → `Run workflow`), e.g. for a base-image security advisory before the next Sunday tick.

## Authentication on self-hosted runners

The integration jobs `docker login` to ghcr.io using the workflow's `GITHUB_TOKEN` (with `packages: read` granted by the job's `permissions:` block). This works without any per-runner secret because the package is hosted under the same org that runs the workflow.

**If you ever fork the repo:** the integration CI on the fork will fail until either (a) the fork publishes its own pre-baked images to its own ghcr namespace and updates the image references in the test files, or (b) you grant the fork's `GITHUB_TOKEN` cross-org read access to `orware/sluice-*` packages (which requires the upstream owner to explicitly grant that access; not the default).

For an isolated dev environment that can't reach ghcr.io: the script supports `SKIP_PUSH=1` so you can build the images locally, and the test image strings can be temporarily pointed at local tags via a one-line `s/ghcr.io\/orware\/sluice-mysql:8.0-prebaked/mysql:8.0/` sed. Don't commit that change.

## Bumping the base version

When upstream MySQL 8.0 → 8.4 (or postgres:16 → 17) is a real, intentional version bump — not just a patch-level rebuild — the change is intrusive and lives in code review, not in the weekly cron:

1. Edit `scripts/build-prebaked-images.sh`. Update the relevant `*_BASE_IMAGE` and `*_TARGET_IMAGE` constants in the `Config` section. For MySQL, the target tag should reflect the new version (e.g. `ghcr.io/orware/sluice-mysql:8.4-prebaked`).
2. Edit the test files that reference the old tag:
   - `internal/engines/mysql/shared_container_integration_test.go` — `sharedMySQLImage`
   - `internal/pipeline/mysql_boot_retry_integration_test.go` — `mysqlBootImage`
   - `internal/engines/postgres/shared_container_integration_test.go` — `sharedPGImage`
   - `internal/pipeline/migrate_postgis_integration_test.go` — `postgisPrebakedImage`
3. Edit `.github/workflows/ci.yml` — update the `docker pull` references in both the `integration` and `integration-postgis` jobs.
4. Run the rebuild workflow (`workflow_dispatch` of `Build pre-baked CI images`) first; verify the new tag is published; only then merge the PR. The integration CI on the PR will exercise the new image end-to-end.

If the new version changes mysqld / postgres semantics in a way that affects load-bearing test invariants, treat the bump as its own task (with a roadmap entry) — pre-baking is the boot-cost optimization, version semantics are the test-shape concern.

## Local development

You don't need to use the pre-baked image locally. `make test-it` works fine against the upstream `mysql:8.0` / `postgres:16` images that testcontainers will pull automatically when the pre-baked tag isn't on the local daemon. The pre-baked image is a CI-runner-pool optimization; the disk-I/O contention it addresses doesn't typically happen on a developer's box with one container booting at a time.

If you want to mirror CI exactly locally: `docker pull ghcr.io/orware/sluice-mysql:8.0-prebaked` etc. (requires being logged into ghcr.io). Then run the integration tests as usual.
