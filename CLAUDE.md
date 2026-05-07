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

**Build-tagged files don't compile under bare `go build ./...`.** When changing a package-level symbol's type or signature, also run `go build -tags=integration ./...` (and any other relevant tags like `psverify`) before pushing — otherwise the integration build only fails in CI. This has bitten releases when an `internal/pipeline` symbol got migrated and the integration-tagged tests in the same package missed the rename.

On Windows with Rancher Desktop, two things bite: `docker.exe` lives at `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\` (often missing from `PATH`), and you need `TESTCONTAINERS_RYUK_DISABLED=true` because the ryuk reaper container vanishes immediately under Rancher's daemon. Without that env var the test loops through ~10 retries and fails with `No such container: ...`. See `docs/dev/development.md` for details. CI on Linux is unaffected.

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
- `docs/dev/roadmap.md` — detailed list of upcoming chunks. Each entry is structured (why / what / gotchas) so it can double as a self-contained prompt. Read the relevant section before starting a new chunk.

## External references that informed real decisions

- **PlanetScale's pgcopydb fork** — tactical reference for fast Postgres→Postgres copy. Tactics worth borrowing: parallel `COPY` per table, deferred index/constraint creation, snapshot-based consistency.
- **pscale dumper** — battle-tested batch sizes (1 MB statement, 128 MB chunk) and session variables (`set workload=olap`) for PlanetScale reads. Use these as starting points for any high-throughput MySQL bulk-read code.

## State of play (as of writing)

Done: IR package, both engines (read + write), kong CLI + koanf config, simple-mode orchestrator (`internal/pipeline.Migrator`), unit tests, integration tests for MySQL→MySQL, PG→PG, and MySQL→PG (cross-engine). CI Integration job runs them on every PR.

For the upcoming work, see `docs/dev/roadmap.md` — it has detailed entries for the Postgres→MySQL test, MySQL/Postgres CDC, the snapshot→CDC handoff, the COPY-protocol writer, translation-policy edges, ADRs, and OSS hygiene.

## Release process (autonomous)

The owner has authorized AI-assisted releases end-to-end via the `gh` CLI. This convention has historically lived in auto-memory, but compaction thinned it once and a release got stuck mid-flow — so the canonical version lives here. **Do not wait to be asked at each phase; once a tag is cut, drive it through to publish.**

The flow has six phases for a typical patch release:

1. **Stage + commit** the fix(es) on `main` (run the pre-commit hook locally first; never bypass with `--no-verify`).
2. **Tag** with `git tag -a vX.Y.Z -m "..."` from the commit you intend to ship. Force-moving a tag is acceptable **only while the corresponding GitHub release is still in draft state** (CI failed, fix landing, etc.) — never after publish.
3. **Push** the commit and the tag (`git push origin main && git push origin vX.Y.Z`). The release workflow (`.github/workflows/release.yml`) builds binaries + creates a draft release with auto-generated commit-list notes.
4. **Watch CI** on both the tag and `main` until completion. Both `release.yml` (on tag) and `ci.yml` (on tag, plus the descendant `main` push if the tag points to HEAD~) must finish green. The descendant-commit fallback exists because GitHub doesn't always run `ci.yml` on tag pushes when the tag points to a commit `ci.yml` already ran on; in that case the descendant `main` run is the authoritative signal.
5. **Replace the auto-generated draft notes** with curated release notes (headline + Features / Fixed / Compatibility / Who-needs-this sections, mirroring the style in prior releases). Always include this — feedback memory `feedback_release_notes.md` documents that the owner expects both a CHANGELOG entry **and** a separately formatted GitHub-release block on every release.
6. **Publish via Option B gate.** All five checks must pass before `gh release edit vX.Y.Z --draft=false`:
   1. `release.yml` workflow on the tag → success
   2. `ci.yml` workflow on the tag (or descendant `main` commit, if the on-tag run didn't trigger) → success
   3. Release assets present (`gh release view vX.Y.Z --json assets` returns a non-empty list)
   4. Release notes body present and curated (not the auto-generated commit-list)
   5. Tag uniqueness — `git ls-remote --tags origin vX.Y.Z` returns exactly one ref

If any of the five fails, fix the failure (typically: race conditions caught by `-race`, lint regressions, or missing notes) and either force-move the tag (still-draft case) or cut the next patch version. **Never publish a release with one or more gate checks failing or unverified.**

**Force-moving a tag creates a duplicate draft release.** GoReleaser doesn't update the existing draft when the tag's SHA changes — it creates a new one. After publishing, list `gh api repos/owner/repo/releases --jq '.[] | select(.tag_name=="vX.Y.Z")'` and delete any leftover `draft: true` entries via `gh api -X DELETE repos/owner/repo/releases/<id>`. Pre-tagging cleanup (deleting the existing draft before the force-push) prevents the dup; cleanup after is fine too.

7. **Auto-spawn the next test cycle after publish.** Per the autonomous-loop convention (see auto-memory `feedback_automation_loop.md`), every release publish triggers the next regression cycle in `C:\code\sluice-testing` *without waiting to be asked*. Update `sluice-testing/NEXT-CYCLE.md` to point at the just-shipped version's focus areas, spawn a `general-purpose` subagent in the background to download the new release, exercise the focus scenarios + the standard `RUNBOOK.md` baseline, and write a `session-reports/vX.Y.Z.md` report. Stop conditions: subagent reports clean (cycle done), or files a new bug entry in `BUG-CATALOG.md` (loop into next fix), or the operator says stop. The subagent runs in the testing repo's working directory; its own `CLAUDE.md` (if present there) governs the testing workflow.

The session-local `.claude/settings.local.json` should pre-authorize `Bash(git push origin main:*)`, `Bash(git push origin v*:*)`, and `Bash(gh release edit:*)` so the autonomous flow doesn't trip the deny-by-default hook on every release.

## Working agreements with humans on this project

- The repo's owner prefers terse responses over verbose recaps. Don't summarize what was just done; the diff is readable.
- When making a non-trivial design choice, lay out the options and tradeoffs briefly *before* writing code. The "validate end-to-end" tenet exists because of a past instance where this wasn't done.
- Run the pre-commit hook before suggesting a commit. Don't surface lint failures from CI that the local hook would have caught.
- Memory of prior decisions and references lives in this file. If a new convention or hard-won lesson emerges, propose adding it here rather than relying on conversation context.
- After a release tag is cut, **don't pause for re-approval at each phase** — drive through Phase 4–6 of the Release process autonomously. The owner has explicitly authorized end-to-end automation; pausing mid-flow has stranded releases in draft state in past sessions.
