# Working with this codebase

Project orientation and working agreements for AI-assisted development on `sluice`. This file is intentionally compact — code structure should be discovered by reading the code, not duplicated here. What lives here is context that is *not* derivable from the code: tenets, workflow expectations, and lint/format gotchas that have caused friction in past sessions.

## What sluice is

Open-source database migration and continuous-sync tool. Initial release covers MySQL ↔ Postgres in all four directions, but the IR and engine registry are deliberately engine-neutral — additional engines should slot in without touching the orchestrator. Written in Go.

The name is a real piece of canal infrastructure (sluice gate); it regulates flow rather than generating it. The author grew up around the Imperial Valley canal system, which is why the name landed.

## Tenets

These take precedence over feature throughput. Code that violates them is not done.

**Clean, elegant code.** The codebase should read like a story. Composable interfaces, small surface areas, named concepts over scattered conditionals. When pragmatism requires a wart, the wart gets a name, a test, and a comment that explains why it exists. This is non-negotiable.

**IR-first.** All translation passes through the typed IR in `internal/ir`. Source-specific knowledge lives in readers; target-specific knowledge lives in writers; the IR is the only shared contract. No regex over DDL strings, no engine-specific imports leaking into the orchestrator.

**Contain Postgres complexity.** Roles, permissions, extensions, and replication-slot lifecycle are surfaced explicitly (via reports and capability declarations), never silently auto-handled. The Postgres ecosystem's sprawl is a known UX hazard; the tool is opinionated about not propagating it.

**Validate end-to-end before building more.** Same-engine integration tests are sanity. Cross-engine integration tests are validation. Before starting the next vertical chunk, confirm the previous one works against the actual cross-engine product use case — not just same-engine round-trips. Building on unverified ground compounds risk.

## Architecture in one paragraph

`internal/ir` defines a typed schema/value model and the `Engine`, `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier` interfaces. Each engine package (`internal/engines/mysql`, `internal/engines/postgres`) implements those interfaces and self-registers via `init()`. `internal/pipeline.Migrator` is the simple-mode orchestrator: read source schema → optional dry-run plan → create target tables (no constraints) → bulk-copy rows → create indexes → create constraints. `cmd/sluice` is a kong-based CLI; config loading is via koanf YAML+env. Engines are looked up by name from `engines.Get(...)`; the pipeline package never imports specific engine packages.

MySQL has flavors (Vanilla, PlanetScale) — same engine code, different `Capabilities` declarations, registered under different names. Postgres will follow the same pattern when service variants matter.

## Local workflow

A pre-commit hook is set up to mirror CI's lint+vet+test gate. Use it.

- Bash/Linux/macOS: `.githooks/pre-commit` (configure git with `git config core.hooksPath .githooks`)
- Windows: `scripts/pre-commit.ps1` (run manually before committing)
- `make` targets exist for the same checks; `make` is not always present on Windows so the scripts are the canonical entry points

Required to be clean before commit: `gofumpt -l .`, `go vet ./...`, `golangci-lint run`, `go test ./...`. The hook runs all four. Race detector (`-race`) is conditional on `CGO_ENABLED=1` so Windows-with-CGO-off doesn't break.

Integration tests need Docker and the `integration` build tag: `go test -tags=integration ./internal/...`. They take a few minutes (testcontainers boots real MySQL and Postgres). Run them after non-trivial changes to readers/writers/orchestrator.

## CI shape

Four GitHub Actions jobs gate merges (see `.github/workflows/ci.yml` and `docs/dev/branch-protection.md`):

- **Test** on ubuntu/macos/windows — unit tests with `-race`, no integration tag
- **Integration** on ubuntu only — `go test -tags=integration -race ./internal/...`, with `mysql:8.0` and `postgres:16` pre-pulled
- **Lint** on ubuntu — golangci-lint
- **Build** on ubuntu/macos/windows — `go build ./...` smoke test

Branch protection on `main` requires all four to pass. Linear history is enforced (no merge commits).

## Lint and format gotchas (these have bitten us)

`gofumpt` is stricter than `gofmt`; ignoring its complaints fails CI. Common offenders:

- **No leading blank line after an opening `{`**. `switch v := t.(type) {\n\n  case ...` is rejected; remove the blank line.
- **`fmt.Errorf` requires a format verb in the format string**. If the message has no `%`, use `errors.New` instead. This has come up enough times to make it a habit: write `errors.New("foo")` first, only escalate to `fmt.Errorf` when a `%w` or `%v` is genuinely needed.
- **Struct field alignment must be consistent within a block**. If alignment differs between groups of fields, separate them with blank lines so each block aligns internally.

Other recurring lint signals:

- `gocritic paramTypeCombine`: `func f(a string, b string)` → `func f(a, b string)`
- `gocritic commentedOutCode`: don't leave commented-out code in committed files
- `errcheck` / `rowserrcheck` / `sqlclosecheck`: when a `*sql.Rows` crosses a goroutine boundary into a streaming channel, the linter can't track the close path; suppress with a focused `//nolint:rowserrcheck,sqlclosecheck` on the specific line and a comment explaining why

## Testing layout

- **Unit tests** (`*_test.go`, no build tag) — shape, dispatch, error paths, with mocks. Pipeline package has `stubEngine` (panics on unexpected calls — catches bypassed validation) and `recordingEngine` (logs phase calls — asserts ordering).
- **Integration tests** (`//go:build integration`) — testcontainers booting real databases. Same-engine tests live in each engine package; cross-engine tests live in `internal/pipeline` (`migrate_pg_integration_test.go`, `migrate_cross_integration_test.go`).
- **Value contract** — see `docs/value-types.md`. Cross-engine value translation (e.g. MySQL `TINYINT(1)` → Go `bool` → Postgres `BOOLEAN`) is defined there.

## Where to read more

- `docs/architecture.md` — IR, engine pattern, orchestrator, planned roadmap
- `docs/type-mapping.md` — type translation policies, core vs extension types
- `docs/value-types.md` — runtime contract for `ir.Row` values
- `docs/testing.md` — testing strategy and tooling
- `docs/dev/development.md` — local dev environment, hooks, make targets
- `docs/dev/branch-protection.md` — required CI checks and `gh api` example

## External references that informed real decisions

- **PlanetScale's pgcopydb fork** — tactical reference for fast Postgres→Postgres copy. Tactics worth borrowing: parallel `COPY` per table, deferred index/constraint creation, snapshot-based consistency.
- **pscale dumper** — battle-tested batch sizes (1 MB statement, 128 MB chunk) and session variables (`set workload=olap`) for PlanetScale reads. Use these as starting points for any high-throughput MySQL bulk-read code.

## State of play (as of writing)

Done: IR package, both engines (read + write), kong CLI + koanf config, simple-mode orchestrator (`internal/pipeline.Migrator`), unit tests, integration tests for MySQL→MySQL, PG→PG, and MySQL→PG (cross-engine). CI Integration job runs them on every PR.

Open work, in rough priority order: Postgres→MySQL integration test (lower priority, narrower bug surface), MySQL CDC (binlog parsing — likely via `github.com/go-mysql-org/go-mysql`), Postgres CDC (logical replication via pgoutput), Postgres COPY-protocol writer for performance, snapshot-to-CDC handoff (the killer feature for low-downtime migrations), ADRs for the bigger design decisions.

Cross-engine bug surface that hasn't been hit yet but probably will: `BIGINT UNSIGNED` (no Postgres equivalent), `ENUM` translation policy, `JSON`/`JSONB` policy, default-value translation across dialects, identity-sequence sync after manual ID inserts.

## Working agreements with humans on this project

- The repo's owner prefers terse responses over verbose recaps. Don't summarize what was just done; the diff is readable.
- When making a non-trivial design choice, lay out the options and tradeoffs briefly *before* writing code. The "validate end-to-end" tenet exists because of a past instance where this wasn't done.
- Run the pre-commit hook before suggesting a commit. Don't surface lint failures from CI that the local hook would have caught.
- Memory of prior decisions and references lives in this file. If a new convention or hard-won lesson emerges, propose adding it here rather than relying on conversation context.
