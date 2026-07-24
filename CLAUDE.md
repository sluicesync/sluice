# Working with this codebase

Project orientation and working agreements for development on `sluice`. This file is intentionally compact — code structure should be discovered by reading the code, not duplicated here. What lives here is context that is *not* derivable from the code: tenets, workflow expectations, and lint/format gotchas that have caused friction in past sessions.

## What sluice is

Open-source database migration and continuous-sync tool. Initial release covers MySQL ↔ Postgres in all four directions, but the IR and engine registry are deliberately engine-neutral — additional engines should slot in without touching the orchestrator. Written in Go.

The name is a real piece of canal infrastructure (sluice gate); it regulates flow rather than generating it. The author grew up around the Imperial Valley canal system, which is why the name landed.

## Tenets

These take precedence over feature throughput. Code that violates them is not done.

**Zero users is the current reality, not a problem to rush past.** sluice has no production users yet. That is *why* correctness and trust gate throughput, not the reverse: there is no install base to be impressed by breadth, but the first real migration that silently corrupts data ends the project's credibility permanently. Every silent-loss class the fuzz/battle-test investment catches is worth more than the next engine or feature. This is the *why* behind "Validate end-to-end before building more" and the loud-failure discipline; when either conflicts with feature velocity, user-trust wins.

**Clean, elegant code.** The codebase should read like a story. Composable interfaces, small surface areas, named concepts over scattered conditionals. When pragmatism requires a wart, the wart gets a name, a test, and a comment that explains why it exists. This is non-negotiable.

**IR-first.** All translation passes through the typed IR in `internal/ir`. Source-specific knowledge lives in readers; target-specific knowledge lives in writers; the IR is the only shared contract. No regex over DDL strings, no engine-specific imports leaking into the orchestrator.

**Contain Postgres complexity.** Roles, permissions, extensions, and replication-slot lifecycle are surfaced explicitly (via reports and capability declarations), never silently auto-handled. The Postgres ecosystem's sprawl is a known UX hazard; the tool is opinionated about not propagating it.

**Validate end-to-end before building more.** Same-engine integration tests are sanity. Cross-engine integration tests are validation. Before starting the next vertical chunk, confirm the previous one works against the actual cross-engine product use case — not just same-engine round-trips. Building on unverified ground compounds risk.

## Architecture in one paragraph

`internal/ir` defines a typed schema/value model and the `Engine`, `SchemaReader`, `SchemaWriter`, `RowReader`, `RowWriter`, `CDCReader`, `ChangeApplier` interfaces; pure-function helpers and feature-scoped contracts over the IR live in sub-packages (`internal/ir/diff` — schema diff + drift reports; `internal/ir/backup` — the logical-backup manifest/chain/snapshot contract) that depend on core `ir`, never the reverse. Each engine package (`internal/engines/mysql`, `internal/engines/postgres`) implements those interfaces and self-registers via `init()`. `internal/pipeline.Migrator` is the simple-mode orchestrator: read source schema → optional dry-run plan → create target tables (no constraints) → bulk-copy rows → create indexes → create constraints. `cmd/sluice` is a kong-based CLI; config loading is via koanf YAML+env. Engines are looked up by name from `engines.Get(...)`; the pipeline package never imports specific engine packages.

MySQL has flavors (Vanilla, PlanetScale, Vitess) — same engine code, different `Capabilities` declarations, registered under different names. Postgres will follow the same pattern when service variants matter.

## Local workflow

A pre-commit hook is set up to mirror CI's lint+vet+test gate. Use it.

- Bash/Linux/macOS: `.githooks/pre-commit` (configure git with `git config core.hooksPath .githooks`)
- Windows: `scripts/pre-commit.ps1` (run manually before committing)
- `make` targets exist for the same checks; `make` is not always present on Windows so the scripts are the canonical entry points

Required to be clean before commit: `gofumpt -l .`, `go vet ./...`, `golangci-lint run`, `go test ./...`. The hook runs all four. Race detector (`-race`) is conditional on `CGO_ENABLED=1` so Windows-with-CGO-off doesn't break.

Integration tests need Docker and the `integration` build tag: `go test -tags=integration ./internal/...`. They take a few minutes (testcontainers boots real MySQL and Postgres). Run them after non-trivial changes to readers/writers/orchestrator.

**Build-tagged files don't compile under bare `go build ./...`.** When changing a package-level symbol's type or signature, before pushing also type-check the build-tagged files — including tagged *test* files. `go build -tags=integration ./...` is **insufficient**: `go build` skips `_test.go`, so a rename that an integration-tagged test still references compiles clean locally and only fails in CI. Use `go vet -tags=integration ./...` (and any other relevant tags like `psverify`) — or `go test -tags=integration -run NoMatch ./...` — which type-check the test files without running them. This has bitten releases when an `internal/pipeline` symbol got migrated and the integration-tagged tests missed the rename; trusting `go build -tags=integration` here cost the v0.58.1 retag.

On Windows with Rancher Desktop, two things bite: `docker.exe` lives at `C:\Program Files\Rancher Desktop\resources\resources\win32\bin\` (often missing from `PATH`), and you need `TESTCONTAINERS_RYUK_DISABLED=true` because the ryuk reaper container vanishes immediately under Rancher's daemon. Without that env var the test loops through ~10 retries and fails with `No such container: ...`. See `docs/dev/development.md` for details. CI on Linux is unaffected.

## CI shape

Six required checks gate merges (see `.github/workflows/ci.yml` — heavily commented — and `docs/dev/branch-protection.md`). Routine PR/push runs are **Linux-only**; the Windows matrix entries join on tag pushes and workflow_dispatch. Docs-only diffs (`**.md`, `docs/`) skip CI on branch pushes; tag pushes always run everything.

- **Test (ubuntu-latest)** — unit tests with `-race` + `go vet`
- **Integration** — rollup of a 5-shard `-tags=integration -race` matrix on real DB containers (pipeline ×3 by test-name regex; mysql engine; postgres + pgtrigger + small packages). The shard package list is hand-maintained; a Lint-job guard (`scripts/check-shard-coverage.sh`) fails CI if a package with integration-tagged tests falls outside it.
- **Integration (PostGIS)**, **Integration (vstream)** — heavier-image suites as separate required jobs
- **Lint** — golangci-lint + the tags-vet matrix (`scripts/vet-tags.sh`: type-checks every `//go:build` combo incl. tagged test files) + the shard-coverage guard
- **Build (ubuntu-latest)** — `go build ./...` smoke test

Non-required but running: **govulncheck** (reachability-based vuln scan), scheduled fuzz (`fuzz-roundtrip.yml`, weekly fresh-seed deep run), the Vitess version matrix (weekly), prebaked-image builds, and **DuckDB parquet compat** (`duckdb-verify.yml`: a family×shape matrix export read back with real DuckDB — the independent-reader gate for the parquet boundary; path-triggered on parquet-touching changes + weekly). Branch protection requires the six named checks; linear history is enforced (no merge commits).

## Lint and format gotchas (these have bitten us)

`gofumpt` is stricter than `gofmt`; ignoring its complaints fails CI. Common offenders:

- **No leading blank line after an opening `{`**. `switch v := t.(type) {\n\n  case ...` is rejected; remove the blank line.
- **`fmt.Errorf` requires a format verb in the format string**. If the message has no `%`, use `errors.New` instead. This has come up enough times to make it a habit: write `errors.New("foo")` first, only escalate to `fmt.Errorf` when a `%w` or `%v` is genuinely needed.
- **Struct field alignment must be consistent within a block**. If alignment differs between groups of fields, separate them with blank lines so each block aligns internally.

Other recurring lint signals:

- `gocritic paramTypeCombine`: `func f(a string, b string)` → `func f(a, b string)`
- `gocritic commentedOutCode`: don't leave commented-out code in committed files
- `errcheck` / `rowserrcheck` / `sqlclosecheck`: when a `*sql.Rows` crosses a goroutine boundary into a streaming channel, the linter can't track the close path; suppress with a focused `//nolint:rowserrcheck,sqlclosecheck` on the specific line and a comment explaining why
- **Stale-cache phantom failures after deleting a git worktree** (typecheck/unused findings pointing at files that no longer exist — hit 4× with the worktree-agent flow): `golangci-lint cache clean` fixes it. Both pre-commit hooks self-heal (on failure they clean the cache and retry once), so a failure that survives the retry is genuine.

**Zero-value-safe config defaults (the v0.99.51 trap).** A `Streamer`/config `bool` that must default *on* needs **opt-out** (`SuppressX` / `NoX`) semantics, never `EnableX`-defaulting-true-by-intent. The CLI sets the field, but **every other construction — all tests, broker/chain paths, future callers — gets the Go zero value (`false`)**. A field named for the on-behavior silently inverts to off for all of them. ADR-0093's first cut shipped `AutoResnapshotOnInvalidPosition` (intended default true); the `-race` integration job caught it as a nil-deref panic because every test Streamer got `false` and took the suppressed branch. Make the zero value the safe/common default.

## Testing layout

- **Unit tests** (`*_test.go`, no build tag) — shape, dispatch, error paths, with mocks. Pipeline package has `stubEngine` (panics on unexpected calls — catches bypassed validation) and `recordingEngine` (logs phase calls — asserts ordering).
- **Integration tests** (`//go:build integration`) — testcontainers booting real databases. Same-engine tests live in each engine package; cross-engine tests live in `internal/pipeline` (`migrate_pg_integration_test.go`, `migrate_cross_integration_test.go`).
- **Value contract** — see `docs/value-types.md`. Cross-engine value translation (e.g. MySQL `TINYINT(1)` → Go `bool` → Postgres `BOOLEAN`) is defined there.

### Pin the class, not the representative (the Bug 74 lesson)

When a change touches an encoder / decoder / codec / serializer that **dispatches on a type *family*** (array/collection elements, numeric vs string vs temporal leaves, per-OID driver codecs, …), the pin must exercise **every family — and every shape variant — not one representative type**. The underlying driver/wire path can differ by the *target* type/OID even when sluice's own code path is byte-identical, so a green test on one family does **not** cover the others.

For array/collection-element work specifically, the pin matrix is **each element family** — native (int/float/bool), string-leaf (text/varchar/char/uuid/inet/cidr/macaddr/decimal), temporal (time/timestamp/timestamptz/date) — **× {scalar/1-D, multi-dim ≥2-D, NULL-element}**, src==dst ground-truthed on the real target (e.g. PG `array_dims` + element `::text`).

**Why this exists (the cost):** v0.69.3's array fix used `pgtype.Array[*string]`; it was pinned green for `int[][]`/`text[][]` and passed independent review — but `numeric[][]` (identical sluice code path, *different pgx target-OID codec*) **silently flattened** to 1-D. That was Bug 74, a CRITICAL silent-loss regression, missed by both the per-representative pin and the reviewer, caught only by the post-release battle-test — costing an extra release and a public correction banner. **Reviewer corollary:** when reviewing a family-dispatched change, re-derive the family matrix yourself and verify the pin covers it; "the integration test is green" is insufficient if the test exercises one family of a family-dispatched path. This is the test-coverage counterpart to the three-phase protocol's "fix the class, not the instance."

### New-surface pre-land checklist (the audit-2026-07-15 lesson)

The Bug 74 discipline above was applied rigorously to *value* codecs and still missed two CRITICALs, because the new surfaces had serialization boundaries that weren't recognized as codecs. Before landing any new surface, walk this list:

- **Anything that round-trips through a store is a codec.** Resume/cursor state, positions, manifests, progress rows — if it's persisted and read back, it gets the full family-matrix treatment (every value family × shape the surface can carry, round-trip byte/value-exact or refuse loudly). The resume-cursor CRITICAL shipped because `encoding/json` silently replaces invalid UTF-8 with U+FFFD and silently routes int64 > 2⁵³ through float64 — both invisible to tests that only pinned INT cursors.
- **No skip-branch without proof.** A lexer/parser branch that skips content must prove the content is skippable; non-empty-but-unrecognized input refuses loudly. The mydumper CRITICAL was a `case "":` that assumed "keyword didn't lex" meant "comment" — a BOM or severed fragment silently vanished.
- **Every new file format gets an independent reader in a test.** Writer-verifying-writer passes symmetric bugs; at least one pin must read the output with a reader that is not the writer's own library (DuckDB/pyarrow for parquet, the real target DB for dumps).
- **Verification paths must not ride the reader under test.** `verify --depth count` shared the mydumper copy path, so it confirmed the loss instead of catching it. If verify and copy share a code path, that shared path is a single point of silent failure — note it and add one check that doesn't.

## Pre-release QA triggers (delta-scoped review)

The periodic full audit (`C:\code\REPO_AUDIT_PROMPT.md` — a blind multi-agent fan-out) is a heavy backstop; running it every release wastes tokens re-deriving unchanged code. Instead, QA layers in three cheapest-first tiers so most of the audit's silent-loss-catching value lands per-release at a fraction of the cost.

**Tier 1 — ratchet each finding into a permanent gate (the highest-leverage move).** The best outcome of an audit is not "we found the bug," it's a *deterministic gate that never needs re-finding*: a family-matrix test, a doc-sync test, a shard-coverage guard, a capability assertion. The 07-18 PAD-SPACE Critical's durable fix was the **real-MySQL/PG collation matrices now in the per-PR shard** — permanent, zero-marginal-cost, and stronger than any review because it ground-truths against a real server every PR. So: **every audit exits by asking "what gate would have caught this class automatically?" and building it.** Prefer a gate over "review more often" — reviews are expensive and forgettable; gates are free and permanent. (Operationalized in the audit prompt's §4 "Gate proposals" required output.)

**Tier 2 — delta-triggered specialist review (run per-release, only when the surface is touched).** `scripts/prerelease-triggers.sh [BASE_REF]` inspects `BASE_REF..HEAD` (default: latest tag) and prints which scoped, read-only specialist agents the changed surface warrants — `value-fidelity-reviewer` (value/collation/codec paths), `perf-parity-checker` (perf techniques), `docs-drift-detector` (flags/capabilities/ADRs/error-codes), plus `-race`-before-tag for concurrency paths and the new-surface codec checklist for persisted-state changes. Most releases (docs-only, non-surface bugfixes) trigger nothing; a risky delta triggers one or two cheap agents. This generalizes the pre-existing "run `perf-parity-checker` after perf chunks" agreement into one deterministic step. Run it before cutting a release tag; act on the hits.

**Tier 3 — the full blind audit stays periodic, not per-release.** Its irreplaceable parts — independent blind re-derivation, the reconciler scorecard, promoting a light area to full depth — only pay off against *accumulated* delta with fresh eyes. Cadence: every ~10–15 releases, or event-triggered when a release introduces a genuinely **new surface** (new file format, new serialization boundary, new engine) — exactly when the "reader ≠ writer" independent pass earns its cost.

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

## State of play

The core product surface is shipped and in continuous release: the engine matrix (MySQL/PlanetScale/Vitess ↔ Postgres ↔ SQLite/D1, plus the trigger-CDC variants), continuous CDC sync with the snapshot→CDC handoff, encrypted backup/restore chains, and the simple-mode migrator — all gated by the CI integration matrix on every PR.

Don't enumerate feature state here — snapshots rot (an earlier version of this section still listed CDC as upcoming long after it shipped). `docs/dev/roadmap.md` is the single source of truth for what's next; `CHANGELOG.md` for what landed when.

## Release process

Releases are cut from `main` and published via GoReleaser behind a draft-review gate.

**Versioning cadence (conventional semver, from v0.100.0 onward).** Patch bumps (`v0.100.1`, `.2`, …) for fixes and small changes; minor bumps (`v0.101.0`, `v0.102.0`, …) for meaningful feature batches or milestones. The `v0.99.x` run all the way to `v0.99.292` was a deliberate **one-time exception** — the minor was held at 99 to reserve `v0.100.0` for a "feels complete" confidence milestone (shipped 2026-07-24) — NOT the ongoing pattern. Prefer the minor/patch distinction so a version communicates change magnitude.

**WinGet submission is manual and selective** — milestones + genuinely notable versions, not every release (microsoft/winget-pkgs reviews each by hand, slowly; historically ~3 of the 292 v0.99.x versions went to WinGet). Use the machine-local `winget-release` skill (`/winget-release`) to produce the fork-branch + upstream PR in one pass. It is deliberately not automated: the CI `TAP_GITHUB_TOKEN` is fine-grained and can't PR against microsoft/winget-pkgs, and per-release auto-PRs would be noise for their manual review. Promotable to an every-X-releases or automatic cadence later (a classic `public_repo`-scope PAT can open the upstream PR).

The flow for a typical release:

1. **Stage + commit** the fix(es) on `main` (run the pre-commit hook locally first; never bypass with `--no-verify`).
2. **Tag** with `git tag -a vX.Y.Z -m "..."` from the commit you intend to ship. Force-moving a tag is acceptable **only while the corresponding GitHub release is still in draft state** (CI failed, fix landing, etc.) — never after publish.
3. **Push** the commit and the tag (`git push origin main && git push origin vX.Y.Z`). `release.yml` builds the cross-platform binaries + the multi-arch GHCR runtime image and creates a draft release with auto-generated commit-list notes.
4. **Watch CI** on both the tag and `main` until completion. Both `release.yml` (on tag) and `ci.yml` (on tag, plus the descendant `main` push if the tag points to HEAD~) must finish green. The descendant-commit fallback exists because GitHub doesn't always run `ci.yml` on tag pushes when the tag points to a commit `ci.yml` already ran on; in that case the descendant `main` run is the authoritative signal.
5. **Replace the auto-generated draft notes** with curated release notes (headline + Features / Fixed / Compatibility / Who-needs-this sections, mirroring prior releases). Every release gets both a CHANGELOG entry **and** a separately formatted GitHub-release block. **Archive that GitHub-release block to `docs/releases/release-notes-vX.Y.Z.md`** (prefixed with an `# sluice vX.Y.Z` H1) and commit it — that directory is the tracked, browsable mirror of every published release. Any release-notes draft written under `workspace/` or `tmp/` (or passed via `--notes-file`) is ephemeral scratch and gitignored; the `docs/releases/` copy is the durable one. **Prose style: release notes and CHANGELOG entries use FLOWING prose — one soft-wrapped line per paragraph/bullet; do not hard-wrap at a fixed column.** (Hard-wrapping crept in during the v0.99.46–v0.99.59 era and was reflowed back to the v0.99.45 house style; keep new entries flowing.)
6. **Publish via Option B gate.** All six checks must pass before `gh release edit vX.Y.Z --draft=false`:
   1. `release.yml` workflow on the tag → success
   2. `ci.yml` workflow on the tag (or descendant `main` commit, if the on-tag run didn't trigger) → success
   3. Release assets present (`gh release view vX.Y.Z --json assets` returns a non-empty list)
   4. Release notes body present and curated (not the auto-generated commit-list)
   5. Tag uniqueness — `git ls-remote --tags origin vX.Y.Z` returns exactly one ref
   6. **Filtered move-OUT cluster gate** — the `filtered-tag-gate` job in `vitess-version-matrix.yml` (check name `Filtered move-OUT gate (tag)`) on the tag → success. It boots a real Vitess cluster and runs the `TestVitessClusterFilteredSync` crux family (cold-start move-OUT, warm-resume, and the `...PadSpaceFallback` A0 client-side-COPY test) — real wire behavior with no unit pin, plus a vacuous-green guard that fails if any crux test didn't run+PASS. It is deliberately a **tag-time publish gate, NOT a branch-protection PR check**: tag pushes aren't PRs, and booting a Vitess cluster on every PR is prohibitive, so the enforcement lives here in the publish checklist rather than in the six required PR-merge checks. This is the audit F-T2 fix — a filtered-sync Critical must not be publishable before its only end-to-end gate reports (v0.99.278 was published ~2 days before the then-weekly-only cluster test could run, and a Critical shipped). It runs on every tag; watch it alongside `release.yml`/`ci.yml` and budget ~60–85m for it (three cluster boots under `-race`). `gh run list --workflow=vitess-version-matrix.yml --event=push` finds the run for the tag; require its `Filtered move-OUT gate (tag)` job green.

If any of the six fails, fix the failure (typically: race conditions caught by `-race`, lint regressions, missing notes, or a filtered-sync regression the cluster gate catches) and either force-move the tag (still-draft case) or cut the next patch version. **Never publish a release with one or more gate checks failing or unverified.**

**`TAP_GITHUB_TOKEN` expires and takes the tap publishers with it.** The winget/homebrew/scoop legs of `release.yml` push via the `TAP_GITHUB_TOKEN` repo secret (a fine-grained PAT with Contents:write on `sluicesync/{homebrew-tap,scoop-bucket,winget-pkgs}`). When it expires, goreleaser fails with three `401 Bad credentials` errors AFTER the binaries, checksums, and GHCR image have already uploaded — the draft is intact, only the taps failed (this burned the first v0.99.175 run, 2026-07-04). Recovery: regenerate the PAT (browser-only; GitHub has no token-mint API), `gh secret set TAP_GITHUB_TOKEN`, delete the stale draft, `gh run rerun <run-id>` — the tag SHA is unchanged so no force-move is involved. Prevention: note the token's expiry date when minting it and renew ahead of it.

**Force-moving a tag creates a duplicate draft release.** GoReleaser doesn't update the existing draft when the tag's SHA changes — it creates a new one. After publishing, list `gh api repos/owner/repo/releases --jq '.[] | select(.tag_name=="vX.Y.Z")'` and delete any leftover `draft: true` entries via `gh api -X DELETE repos/owner/repo/releases/<id>`. Pre-tagging cleanup (deleting the existing draft before the force-push) prevents the dup; cleanup after is fine too.

**Force-move + binaries:** each `release.yml` run builds the binaries at *that* tag-SHA, so after a force-move the draft you publish as "Latest" must be the one whose binaries reflect the SHA you intend to ship (especially when the force-move added a runtime-affecting change, not just test-only fixes). Verify via `gh release view vX.Y.Z --json assets` that the `apiUrl` paths reflect the draft you intend, then `gh api -X DELETE` the others and `gh release edit --draft=false` the right one.

### Concurrency chunks: the `-race` integration gate runs BEFORE the tag

`-race` needs a CGO/TSan runtime + a Linux runner + Docker, so the "integration **+** `-race`" job exists only on CI. For changes touching **concurrency** (goroutines, channels, shared state, rotation/FSM, crash-recovery, failpoints), that gate MUST pass *before* the tag is cut — never cut or force-move a tag ahead of the first `-race` run for such a chunk. Cutting first and watching after is the v0.20.x/v0.67.0 trap: it turns a found race/mis-stitch into a force-tag-move + duplicate-draft + ~50–70-min retag loop.

Two ways to satisfy it, in preference order:

1. **Local Docker (`scripts/race-integration.ps1`).** Runs `go test -tags=integration -race ./internal/...` inside a `golang` Linux container with `gcc`/`CGO_ENABLED=1` and the host Docker socket bind-mounted so testcontainers spawns sibling DB containers (DooD). ~1-minute-to-start local pre-tag gate that mirrors CI exactly. Rancher-Desktop socket caveats are documented in the script; if DooD proves flaky on the local Rancher setup, fall through to (2).
2. **Push-first, tag-after (zero-infra, always applies).** Push the work to `main` (or a branch) and wait for the **Integration** job green *before* cutting the tag. One CI cycle either way — but it eliminates the tag-force-move / duplicate-draft churn entirely. This alone would have prevented the v0.67.0 retag loop.

Non-concurrency chunks keep the existing tag-then-watch flow (CI is almost always green there; the `-race`-before-tag rule is specifically for the chunk class where a race/ordering bug is plausible). When in doubt, treat it as a concurrency chunk.

## Debugging non-obvious failures (the three-phase protocol)

When a CI failure or test regression doesn't match an obvious hypothesis — or when the first speculative patch doesn't fix it — **stop speculating and run the three-phase protocol.** This pattern has closed Bug 37, the v0.20.0 broker false-failures, the v0.20.1 stream regression, and Bug 41 cleanly; speculative patching ahead of ground truth has burned multi-cycle retag loops in the same session. The discipline is non-negotiable when:

- A test fails deterministically but the obvious fix candidates don't match the failure shape
- Multiple plausible hypotheses exist (e.g. timing pressure vs structural bug vs serialization race)
- Local repro isn't easy (e.g. Windows + CGO=0 can't run `-race`; Mac doesn't have testcontainers; the failure only fires under CI's specific scheduler)

### Phase A — instrument and gather ground truth

- Add **temporary DEBUG-level slog instrumentation** at every hypothesized failure path: log entry/exit, log the actual values at decision points, log timing if relevant.
- Push the instrumentation commit to trigger CI; **read CI logs** for ground truth. Do NOT skip this step "because I think I know what the bug is" — that's exactly the speculate-and-patch trap. (Exception: if the bug is fully diagnosable by code-reading + the BUG-CATALOG repro is unambiguous, code-reading can substitute. Document why.)
- The hypothesis you're confirming should be specific enough that the log output tells you "yes" or "no" — not vague.

### Phase B — implement the fix based on Phase A findings

- Only after Phase A's logs (or code-reading proof) confirm a specific hypothesis, write the fix.
- Don't hedge: if Phase A says hypothesis (a), fix (a). Don't bundle in speculative patches for (b) and (c) "just in case" — they make the diff harder to review and obscure the actual fix.
- Add unit tests + integration tests that pin the fix shape so the bug can't regress silently.

### Phase C — cleanup and verify

- Remove or gate Phase A's debug logs behind `--log-level=debug` (don't ship verbose INFO+ noise).
- Re-run all related tests; verify no regressions in adjacent code.
- **Watch for the unused-helper / dead-code lint trap** — when Phase B replaces or removes helpers, remove their supporting code too. golangci-lint's `unused` checker has bitten this multiple times in the v0.19.x → v0.21.x cycles.

### Why this works (and why the alternative fails)

Speculative patching looks faster ("just try fix X, push, see if CI's green") but compounds failure modes — each retag costs ~50-70 min of CI minutes + adds confusion to the release flow (force-tag-moves, duplicate drafts, stale checkpoint state). The three-phase protocol takes one extra CI roundtrip (Phase A) but cuts retry cycles to one. Bug 37 was speculatively patched four times before the heartbeat-clobber ground truth surfaced via instrumentation; v0.20.0 broker "failures" were actually two test-side issues that code-reading identified in 5 minutes once the agent looked at *what the test was actually doing* instead of patching imagined production bugs.

When delegating this protocol (e.g. in a task prompt), make Phase A non-negotiable explicit ("if hypotheses change, adjust before writing the fix") and require the Phase C instrumentation cleanup so verbose debug logs don't ship.

## Working agreements with humans on this project

- The repo's owner prefers terse responses over verbose recaps. Don't summarize what was just done; the diff is readable.
- When making a non-trivial design choice, lay out the options and tradeoffs briefly *before* writing code. The "validate end-to-end" tenet exists because of a past instance where this wasn't done.
- Run the pre-commit hook before suggesting a commit. Don't surface lint failures from CI that the local hook would have caught.
- When a new convention or hard-won lesson emerges, propose documenting it (here, in an ADR, or the relevant doc) rather than relying on conversation context.
- Verify every push to `main` with `git rev-parse origin/main` (or `git log origin/main`), not `git log -1` — `git log -1` shows `HEAD`, which isn't always `main`, and that has given false confidence that a commit reached `origin/main` when it hadn't.
- **Performance chunks must state their engine × mode coverage explicitly.** Any chunk that adds or extends a throughput/latency technique declares, in its ADR or roadmap entry, which cells of `docs/dev/perf-parity-matrix.md` it reaches (engines × {migrate, sync cold-start per flavor, backup, restore, chain-replay, broker, CDC apply}) — and updates the matrix in the same PR. Unreached cells are filed as explicit gaps, never implied. Run the `perf-parity-checker` agent after perf chunks and before releases. This exists because optimizations repeatedly landed in one engine/mode and silently missed siblings (2026-07-04 inventory found 11 such gaps, incl. backup never getting migrate's within-table chunking).
- **Never report a feature/bug/item as "not implemented / remaining / deferred / demand-gated" on the strength of a roadmap header, doc status line, or memory note alone — those LAG the code.** Verify against ground truth first: grep for the symbol/flag, `git log --oneline -- <path>`, `git tag --contains <sha>`. This has bitten repeatedly (mis-reported as unbuilt when already shipped: the `vitess` flavor `476b349`; ADR-0044 Tier 3 uuid-ossp/pgcrypto defaults v0.65.0; `--poll-interval` v0.91.0). When you discover such drift, fix the stale doc in the same pass (roadmap entry → SHIPPED, ADR status line, "Recently landed"), and prefer running the `roadmap-staleness-checker` agent periodically (after releases, before answering "what's next") to sweep for more.
