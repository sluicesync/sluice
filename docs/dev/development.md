# Development Workflow

This document describes the local-dev tooling for sluice contributors. The goal is to catch the things CI catches, *before* CI runs — saving the round-trip wait when something trivial is off.

## Tools

The CI gate covers formatting (`gofumpt`), vetting (`go vet`, including every build-tag combination via `scripts/vet-tags.sh`), the golangci-lint set, the test-coverage guards, and the unit tests. To match CI locally you'll want:

- **Go** — version per `go.mod`'s `go` directive (currently 1.26.x).
- **[gofumpt](https://github.com/mvdan/gofumpt)** — stricter cousin of `gofmt`. Catches the formatting nits CI fails on.
- **[golangci-lint](https://golangci-lint.run/welcome/install/)** — runs the rest of the linter set (errcheck, revive, gocritic, etc.).

Install both Go-based tools with `go install`:

```bash
go install mvdan.cc/gofumpt@latest
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

(Note the `/v2/` in the lint path — the project's `.golangci.yml` is v2-schema, and the un-suffixed module path silently installs v1, which rejects the config. CI installs it the same way.)

These land in `$GOBIN` (typically `~/go/bin` or `%USERPROFILE%\go\bin` on Windows). Make sure that directory is on your `PATH`.

## Make targets

The Makefile is the canonical entry point. Run `make help` for the full list; the ones you'll use most:

```
make fmt              # apply gofumpt to every .go file
make fmt-check        # verify formatting without writing changes; CI-shaped
make vet              # go vet ./...
make vet-tags         # type-check every build-tag combination (incl. tagged test files)
make coverage-guards  # CI Lint's test-coverage guards (shard + -run-filter)
make lint             # go vet + golangci-lint run
make test             # unit tests, race detector, no DB
make test-it          # unit + integration; needs Docker for testcontainers
make pre-commit       # the full gate: fmt-check + vet + vet-tags + coverage-guards + lint + test
```

`make pre-commit` is the single command that mirrors what the CI Lint and Test jobs check — formatting, vet (including the build-tag matrix), the test-coverage guards, golangci-lint, and the fast unit tests. Run it before pushing and you'll catch the easy stuff that's been bouncing off CI.

## Git pre-commit hook

For automatic enforcement, install the bundled pre-commit hook:

```bash
git config core.hooksPath .githooks
```

This is a one-time per-clone setup. Once configured, every `git commit` runs `.githooks/pre-commit`, which calls the tools directly (gofumpt, `go vet`, the tags-vet and coverage-guard scripts, golangci-lint, `go test`) rather than going through `make`, so it works even where `make` isn't installed. Commits that don't touch `.go` files skip the Go checks (a conflict-marker check still runs on everything staged).

If a check fails, the commit aborts with the failing command. Fix the issue, `git add` the changes, and re-commit.

To bypass the hook in an emergency (rare — most use cases are better served by amending after the fact):

```bash
git commit --no-verify
```

### Windows specifics

`.githooks/pre-commit` is a POSIX shell script. On Windows it runs through Git for Windows' bundled bash, which handles it transparently — `git config core.hooksPath .githooks` works the same way. PowerShell is not invoked. If you're using WSL, the hook runs in your WSL distro's shell.

**Race detector and `cgo` on Windows.** Go's `-race` flag requires a working C compiler (cgo). Most Windows Go installs have `CGO_ENABLED=0` by default unless you've installed MinGW, MSYS2, or TDM-GCC. The pre-commit hook detects this via `go env CGO_ENABLED` and skips `-race` when cgo is off — your tests still run, just without race detection locally. CI's Linux runners have cgo enabled, so race-condition bugs are still caught before merge. If you want race detection locally, install one of the toolchains above and set `CGO_ENABLED=1`.

## Editor integration

If your editor formats on save, point it at `gofumpt` instead of `gofmt`:

- **VS Code (Go extension)**: in settings, set `"go.formatTool": "gofumpt"` (or `"gofumpt": true` in newer versions).
- **GoLand / IntelliJ**: Preferences → Tools → File Watchers → add a `gofumpt` watcher (or replace the bundled `goimports`/`gofmt` watcher).
- **Vim/Neovim with vim-go**: `let g:go_fmt_command = "gofumpt"`.
- **Emacs go-mode**: `(setq gofmt-command "gofumpt")`.

With editor-on-save formatting plus the pre-commit hook, the gofumpt errors should stop showing up on CI.

## Running integration tests

Integration tests are gated by the `integration` build tag and use [testcontainers-go](https://golang.testcontainers.org/) to boot real database containers. They require Docker (Docker Desktop on Windows/macOS, the Docker engine on Linux).

```bash
make test-it
```

If Docker isn't available, the tests skip cleanly via `testcontainers.SkipIfProviderIsNotHealthy` — they don't fail. CI's Linux runners have Docker installed, so the integration tests execute there even when local devs can't run them.

### Pre-baked CI images

In CI, the MySQL and Postgres containers boot from pre-baked images on GHCR (`ghcr.io/sluicesync/sluice-mysql:8.0-prebaked`, `ghcr.io/sluicesync/sluice-postgres:16-prebaked`, `ghcr.io/sluicesync/sluice-postgis:16-3.4-prebaked`) rather than the upstream Docker Hub tags. The bake step runs `mysqld --initialize-insecure` / `initdb` once and `docker commit`s the result, so containers cold-start in ~5 s instead of 30-60 s (or 2-3 minutes under self-hosted runner disk-I/O contention). See [docs/dev/ci-images.md](ci-images.md) for the full story, the weekly rebuild cron, and how to bump the base version.

Local `make test-it` uses upstream images by default — the pre-baked optimization addresses concurrent-boot contention that doesn't typically happen on a developer's box.

### Build-tag layers and their image/time cost

Integration tests are layered by build tag so the normal pass doesn't pull heavy images:

| Tag | Image(s) | Cost | What it gates |
|-----|----------|------|---------------|
| `integration` | `mysql:8.0`, `postgres:16`, plus extension images | minutes; runs in CI on every PR | the default cross-engine + same-engine suite |
| `integration vstream` | `vitess/vttestserver:mysql80` (~2 GB) | +1–2 min container boot; runs in the vstream pass | VStream CDC reader basics, snapshot→CDC handoff, multi-shard snapshot/CDC, **static-sharded Vitess → sluice Migrate → src==dst** (`internal/pipeline`, `TestMigrate_VStreamShardedSource`) |
| `integration vitessreshard` | `vitess/lite:latest` (~2 GB) + `quay.io/coreos/etcd:v3.5.17` (~70 MB) | cluster bring-up ~40–60 s; each test 80–170 s wall; **heavy, NOT in the normal CI gate** | the Track-1a reshard core: a scripted multi-process Vitess cluster (etcd + vtctld + per-shard primary+replica vttablets + vtgate) that can run `vtctldclient Reshard create` + `SwitchTraffic`. `TestVitessReshard_ProofOfReshardability` (topology feasibility gate) and `TestVitessReshard_ChaosExactlyOnce` (the headline no-gap/no-dup-across-the-journal-cut oracle, `internal/engines/mysql`) |
| `integration vitesscluster` | `vitess/lite:v24.0.1` (~2.6 GB, Vitess 24 — matches the vendored `vitess.io/vitess v0.24.1` client) + `quay.io/coreos/etcd:v3.5.21` (~70 MB) | cluster bring-up ~28–40 s; each test ~90–110 s wall; **heavy, NOT in the normal CI gate** | ADR-0073 (c) FULL online-DDL cutover-survival: a minimal real Vitess cluster (etcd + vtctld + a primary + a replica vttablet + vtgate) that runs the **real online-DDL scheduler** vtcombo stubs out. `TestVitessCluster_OnlineDDL_CutoverSurvivesWithZeroLoss` (snapshot→CDC across a real `ddl_strategy='vitess'` cutover, zero loss, post-cutover schema flows) and `TestVitessCluster_OnlineDDL_ComplexShapesSurviveCutover` (mid-table column drop + ENUM add + ENUM extend each survive a real cutover), `internal/engines/mysql` |

Why `vitessreshard` is a separate tag rather than reusing `vstream`: the `vstream` tag's `vttestserver` image is single-process `vtcombo` — it serves a *statically* sharded keyspace and ships **no `vtctldclient`/`vtctld`/standalone `vttablet`**, so it physically cannot run a shard-count reshard. The reshard correctness core therefore needs the full multi-process cluster (the `examples/local` topology, containerised), which is heavier and slower and must not burden the normal integration pass. Run it explicitly:

```bash
# Windows/Rancher: add docker.exe to PATH + disable ryuk first (see below)
go test -tags='integration vitessreshard' -v -count=1 -timeout=35m \
  -run 'TestVitessReshard' ./internal/engines/mysql/...
```

The cheap CI-smoke half of the Track-1a Vitess validation (VStream basics + static-sharded `src==dst`) stays under `vstream` and runs in the normal vstream pass; only the heavy reshard-chaos core is behind `vitessreshard`.

Why `vitesscluster` is a separate tag (ADR-0073 (c), track item (b2)): vttestserver's `vtcombo` online-DDL scheduler is **stubbed** (`SHOW VITESS_MIGRATIONS` reports `not implemented in vtcombo`), so the `vstream`-tagged online-DDL tests can only prove internal-`_vt_*`-table exclusion + that shadow-table DDL events don't wedge the logical stream — they **cannot** run the genuine VReplication copy + atomic rename cutover. The `vitesscluster` harness boots a real multi-process Vitess cluster (driven via `docker compose`, not testcontainers — the compose file lives at `internal/engines/mysql/testdata/vitesscluster/docker-compose.yml`) whose scheduler reaches `migration_status=complete`, so it validates the full cutover-survival of ADR-0073 (c) end-to-end through sluice's VStream. Run it explicitly:

```bash
# Windows/Rancher: add docker.exe to PATH first (the harness also probes
# the Rancher install path automatically). Ryuk is irrelevant here — the
# harness drives `docker compose` directly, not the testcontainers API.
go test -tags='integration vitesscluster' -v -count=1 -timeout=20m \
  -run 'TestVitessCluster' ./internal/engines/mysql/...
```

Resource needs: ~2 GB free RAM for the 5-container stack (the running footprint is modest, ~500 MB RSS; the cost is the ~2.6 GB `vitess/lite` image on disk). The harness publishes vtgate on fixed host ports (MySQL 15306, gRPC 15991), so it is **not** safe to run two `vitesscluster` stacks at once on one host; the compose file accepts `VTGATE_MYSQL_PORT` / `VTGATE_GRPC_PORT` overrides if you need to relocate them. One subtlety the harness encapsulates: after the replica joins and the primary is reparented, vtgate needs a few seconds before it advertises a healthy `PRIMARY` — seeding before that races the healthcheck and fails with `no healthy tablet available ... tablet_type:PRIMARY`. `startVitessCluster` polls a trivial write through vtgate until it succeeds before returning, so tests never see that race.

### Rancher Desktop on Windows

Rancher Desktop works as a Docker Desktop replacement, with two snags worth knowing:

- **PATH.** Rancher Desktop installs its `docker.exe` shim under `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\`. Add that directory to `PATH` (or use Rancher Desktop's "Add to PATH" toggle on first launch); otherwise `docker` won't be on `PATH` for new shells even though the daemon is running.
- **Disable the testcontainers ryuk reaper.** Testcontainers normally launches a `testcontainers/ryuk` sidecar to garbage-collect leaked containers. Under Rancher Desktop on Windows the ryuk container vanishes from the daemon's view almost immediately, and testcontainers retries it ~10× before failing the test with `No such container: ...`. Set `TESTCONTAINERS_RYUK_DISABLED=true` in your shell:

  ```bash
  export TESTCONTAINERS_RYUK_DISABLED=true   # bash / Git Bash
  $env:TESTCONTAINERS_RYUK_DISABLED='true'   # PowerShell
  ```

  Our tests already terminate their containers via `defer cleanup()` so dropping ryuk doesn't leak anything in practice. CI also disables ryuk on its integration jobs (`TESTCONTAINERS_RYUK_DISABLED=true` in `.github/workflows/ci.yml`): the runners are ephemeral, so there are no orphans to reap, and keeping it on only added a docker.io pull of `testcontainers/ryuk` that intermittently timed out and red-failed a shard. Local Linux/macOS dev can leave ryuk **on** if you like — its orphan-reaping is genuinely useful after a Ctrl-C or panic that skips `defer cleanup()`; this override is only *required* on Windows/Rancher (where ryuk doesn't work) and is what CI does for reliability.

## Branch protection

See [docs/dev/branch-protection.md](branch-protection.md) for the recommended GitHub branch-protection rules. Once those are enabled, CI checks are load-bearing for merges, and the local pre-commit hook catches the same issues earlier.
