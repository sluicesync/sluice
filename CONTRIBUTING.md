# Contributing to sluice

Thank you for your interest in contributing! This document is a quick orientation for new contributors. The deeper material lives in [`docs/dev/`](docs/dev/) — `development.md` for tooling, `branch-protection.md` for the merge gate, `roadmap.md` for what's coming up.

## Project tenets

Before you start, please skim [CLAUDE.md](CLAUDE.md). It's the project's working document for AI-assisted development, and it captures the tenets that take precedence over feature throughput: clean elegant code, IR-first translation, contain Postgres complexity, validate end-to-end before building more. PRs that conflict with these tenets are likely to be reworked, even when the code is otherwise fine.

## Getting set up

You'll need a recent Go toolchain (the version in `go.mod`'s `go` directive — currently 1.26.x), Docker for integration tests, and two small Go-based tools:

```bash
go install mvdan.cc/gofumpt@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

Then enable the pre-commit hook so the local gate matches CI:

```bash
git config core.hooksPath .githooks
```

That's it. Run `make help` to see the targets you'll use day-to-day; `make pre-commit` is the single command that runs everything CI runs.

See [docs/dev/development.md](docs/dev/development.md) for the full workflow, including Windows specifics around CGO and the race detector.

## Picking something to work on

[docs/dev/roadmap.md](docs/dev/roadmap.md) is the living list of upcoming chunks. Each entry has a *why*, a *what*, and *gotchas / open questions*. Some entries are well-bounded (the Postgres→MySQL test, the ADRs, the OSS hygiene items) and good for a first contribution. Others (CDC readers, the snapshot-to-CDC handoff) involve real design decisions and are best discussed in an issue first.

If you have an idea that isn't on the roadmap, open an issue describing the use case and the rough shape of the change before writing code. We'd rather talk for fifteen minutes than rebase you for an hour.

## Pull-request conventions

- **One concern per PR.** Multiple unrelated changes are hard to review and harder to revert.
- **Linear history.** Branch protection on `main` requires it. Use rebase-and-merge or squash-and-merge; merge commits are rejected.
- **Pre-commit hook clean.** If `make pre-commit` doesn't pass locally, CI will fail. Don't surface lint errors that the local hook would have caught.
- **Tests required.** Schema/data/CDC changes need integration tests (`//go:build integration`). Pure orchestration changes get unit tests with the mock engines in `internal/pipeline/migrate_test.go`.
- **Commit messages.** Conventional-style prefixes (`feat:`, `fix:`, `test:`, `ci:`, `docs:`, `chore:`) followed by a short imperative subject. The body explains *why* — the diff already shows *what*.
- **Reviews.** While the project is solo, the bar is "I would have approved this PR if I'd just woken up and forgotten my own context." Once there are more contributors, real reviews kick in.

## Tests

The test suite is layered by infrastructure cost; each layer is opt-in via a build tag:

- **Unit tests** (no tag) run on every push: `go test -race -count=1 ./internal/...`. Mock engines (`stubEngine`, `recordingEngine` in the pipeline package), no Docker.
- **Integration tests** (`integration` tag): testcontainers-go boots `mysql:8.0` and `postgres:16` — `go test -tags=integration -race -count=1 ./internal/...`. Run on Linux in CI; same-engine tests live in each engine package, cross-engine tests in `internal/pipeline`.
- **PostGIS tests** (`integration && postgis` tag): adds the `postgis/postgis:16-3.4` image (~600 MB). Single test (`TestMigrate_PostGIS_MySQLToPG`) gated behind a separate tag so the default integration suite doesn't pull the heavier image.
- **VStream tests** (`integration && vstream` tag): adds the `vitess/vttestserver:mysql80` image (~700 MB). Five tests covering the FlavorPlanetScale CDC + snapshot paths against a vanilla Vitess cluster — same image cost concern as PostGIS, hence the separate tag.
- **PlanetScale verification tests** (`psverify` tag): hits a real PlanetScale account via env vars / a repo-root `PLANETSCALE_CREDENTIALS.env` file. Manual-trigger only via `.github/workflows/psverify.yml`; never runs on push. Use these to validate against actual product quirks the in-container tests can't reach.

Same-engine tests are sanity. Cross-engine tests are validation. Add a cross-engine test before claiming a feature works end-to-end.

Local Windows note: the integration suites need `export TESTCONTAINERS_RYUK_DISABLED=true` because Rancher Desktop's daemon kills the ryuk reaper container; CI on Linux is unaffected. See [docs/dev/development.md](docs/dev/development.md) for the full local setup.

## CI shape

Four jobs run on every PR (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml) and [docs/dev/branch-protection.md](docs/dev/branch-protection.md)): Test (3 OSes, unit only), Integration (Linux only, Docker-backed), Lint (golangci-lint), Build (3 OSes, `go build` smoke test). All four must pass to merge.

## Reporting bugs

Open an issue with: the engine pair (e.g. MySQL 8.0 → Postgres 16), the version of sluice, the smallest reproducible config or schema, and the actual vs expected behavior. If you can include a `--dry-run` plan output, that's gold.

## Reporting security issues

See [SECURITY.md](SECURITY.md). Please do not file public issues for security-sensitive reports.

## License

Contributions are licensed under the project's [Apache License 2.0](LICENSE). By submitting a PR you confirm you have the right to license your contribution under those terms.
