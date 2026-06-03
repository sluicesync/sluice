# Development Workflow

This document describes the local-dev tooling for sluice contributors. The goal is to catch the things CI catches, *before* CI runs — saving the round-trip wait when something trivial is off.

## Tools

The CI pipeline runs three checks: `gofumpt` formatting, `go vet`, and `go test`. To match CI locally you'll want:

- **Go** — version per `go.mod`'s `go` directive (currently 1.25.x).
- **[gofumpt](https://github.com/mvdan/gofumpt)** — stricter cousin of `gofmt`. Catches the formatting nits CI fails on.
- **[golangci-lint](https://golangci-lint.run/welcome/install/)** — runs the rest of the linter set (errcheck, revive, gocritic, etc.).

Install both Go-based tools with `go install`:

```bash
go install mvdan.cc/gofumpt@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

These land in `$GOBIN` (typically `~/go/bin` or `%USERPROFILE%\go\bin` on Windows). Make sure that directory is on your `PATH`.

## Make targets

The Makefile is the canonical entry point. Run `make help` for the full list; the ones you'll use most:

```
make fmt          # apply gofumpt to every .go file
make fmt-check    # verify formatting without writing changes; CI-shaped
make vet          # go vet ./...
make lint         # go vet + golangci-lint run
make test         # unit tests, race detector, no DB
make test-it      # unit + integration; needs Docker for testcontainers
make pre-commit   # the bundled fmt-check + vet + test gate
```

`make pre-commit` is the single command that mirrors what the CI lint and Test jobs check. Run it before pushing and you'll catch the easy stuff that's been bouncing off CI.

## Git pre-commit hook

For automatic enforcement, install the bundled pre-commit hook:

```bash
git config core.hooksPath .githooks
```

This is a one-time per-clone setup. Once configured, every `git commit` runs `.githooks/pre-commit`, which executes `make pre-commit` against staged Go files only (commits that don't touch `.go` files skip straight through).

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

Why `vitessreshard` is a separate tag rather than reusing `vstream`: the `vstream` tag's `vttestserver` image is single-process `vtcombo` — it serves a *statically* sharded keyspace and ships **no `vtctldclient`/`vtctld`/standalone `vttablet`**, so it physically cannot run a shard-count reshard. The reshard correctness core therefore needs the full multi-process cluster (the `examples/local` topology, containerised), which is heavier and slower and must not burden the normal integration pass. Run it explicitly:

```bash
# Windows/Rancher: add docker.exe to PATH + disable ryuk first (see below)
go test -tags='integration vitessreshard' -v -count=1 -timeout=35m \
  -run 'TestVitessReshard' ./internal/engines/mysql/...
```

The cheap CI-smoke half of the Track-1a Vitess validation (VStream basics + static-sharded `src==dst`) stays under `vstream` and runs in the normal vstream pass; only the heavy reshard-chaos core is behind `vitessreshard`.

### Rancher Desktop on Windows

Rancher Desktop works as a Docker Desktop replacement, with two snags worth knowing:

- **PATH.** Rancher Desktop installs its `docker.exe` shim under `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\`. Add that directory to `PATH` (or use Rancher Desktop's "Add to PATH" toggle on first launch); otherwise `docker` won't be on `PATH` for new shells even though the daemon is running.
- **Disable the testcontainers ryuk reaper.** Testcontainers normally launches a `testcontainers/ryuk` sidecar to garbage-collect leaked containers. Under Rancher Desktop on Windows the ryuk container vanishes from the daemon's view almost immediately, and testcontainers retries it ~10× before failing the test with `No such container: ...`. Set `TESTCONTAINERS_RYUK_DISABLED=true` in your shell:

  ```bash
  export TESTCONTAINERS_RYUK_DISABLED=true   # bash / Git Bash
  $env:TESTCONTAINERS_RYUK_DISABLED='true'   # PowerShell
  ```

  Our tests already terminate their containers via `defer cleanup()` so dropping ryuk doesn't leak anything in practice. CI runs Docker on Linux where ryuk works fine, so this is a local-only override.

## Branch protection

See [docs/dev/branch-protection.md](branch-protection.md) for the recommended GitHub branch-protection rules. Once those are enabled, CI checks are load-bearing for merges, and the local pre-commit hook catches the same issues earlier.
