# Branch Protection

Branch protection rules live in GitHub's repository settings, not in the repository itself, so they have to be applied through the web UI (or the GitHub CLI). This document records the recommended settings so they can be applied consistently.

## Recommended rules for `main`

Navigate to **Settings → Branches → Add rule** (or **Branch protection rules → Add rule** on newer UIs) and apply the following to the `main` branch:

### Require pull requests

- **Require a pull request before merging:** on
  - **Require approvals:** 1 (when the project has more contributors; while it's solo, this can be off)
  - **Dismiss stale pull request approvals when new commits are pushed:** on
  - **Require review from Code Owners:** off (no `CODEOWNERS` file yet)

### Require status checks

- **Require status checks to pass before merging:** on
- **Require branches to be up to date before merging:** on
- Required status checks (these are the GitHub Actions job names from `.github/workflows/ci.yml`):
  - `Test (ubuntu-latest)`
  - `Integration`
  - `Integration (PostGIS)` — added v0.29.0; gates the cross-engine geometry round-trip suite
  - `Integration (vstream)` — added after Bug 125 (CRITICAL silent-loss) shipped without VStream coverage; see the warning in `ci.yml` before touching this job
  - `Lint`
  - `Build (ubuntu-latest)`

GitHub will only suggest these in the search box once they've successfully run at least once on a PR. If they don't appear, push a no-op PR first to trigger CI, then add them.

> **Note (v0.10.4 / v0.20.1 cost optimization):** Cross-platform test/build jobs no longer run on every push/PR — they run only on tag pushes (release verification) and manual `workflow_dispatch` from the GitHub UI. **As of v0.20.1, macOS is dropped entirely** (cost shape: macOS runners cost ~10× Linux per minute and weren't catching anything Linux+Windows didn't). **Do NOT** add `Test (macos-latest)` / `Test (windows-latest)` / `Build (macos-latest)` / `Build (windows-latest)` to the required-checks list — `macos-latest` jobs no longer exist; `windows-latest` jobs only run on tag/dispatch and would never appear on PRs. If you previously configured branch protection per the older shape of this doc, remove those four checks from the required list before pushing further changes.
>
> Additionally (v0.20.1): `paths-ignore` skips this workflow for docs-only branch pushes (markdown, design docs, tmp release-notes staging). **Tag pushes are not affected** by paths-ignore — Release + CI always run on tags. So the publish gate's CI requirement remains satisfiable on every tag, even ones that point at CHANGELOG-only commits.
>
> **The filtered move-OUT cluster gate is a PUBLISH-gate check, NOT a branch-protection PR check (audit 2026-07-19 M2-1).** The `Filtered move-OUT gate (tag)` job (`vitess-version-matrix.yml`) boots a real Vitess cluster and runs the `TestVitessClusterFilteredSync[WarmResume]` move-OUT crux on **tag pushes only** — it is the required 6th check of the CLAUDE.md publish gate (release-process §6), watched before `gh release edit --draft=false`. **Do NOT add it to the required-status-checks list above:** it never runs on PRs (a required check that never reports would deadlock every PR), and booting a Vitess cluster on every PR is prohibitive. Tag-time enforcement in the publish checklist is the deliberate design.

If you want a "platform check" gate before merging a particular PR (e.g. a PR that touches OS-specific code paths), trigger the full matrix manually: GitHub Actions tab → CI workflow → "Run workflow" → pick the PR's branch → click. The Linux+Windows matrix runs and surfaces under the workflow's runs list. The PR's own status checks won't include the dispatched run, but the operator can verify success before merging.

### Other

- **Require conversation resolution before merging:** on
- **Require linear history:** on (recommended) — nominally disallows merge commits, forcing rebase-and-merge or squash-and-merge. Keeps `git log --oneline` readable. **In practice this rule is BYPASSABLE on push** and has been bypassed every time it came up: a `git merge` pushed to `main` reports `Bypassed rule violations … This branch must not contain merge commits` and lands anyway. History carries 7 merge commits as of 2026-08-04. Treat it as a preference to rebase, not a guarantee that `main` is linear.
- **Require signed commits:** off (worth turning on later if you adopt signed commits across the project)
- **Include administrators:** **OFF** (`enforce_admins: false`), and this is a deliberate choice, not drift. This doc previously claimed it was on and that it "prevents accidental direct pushes to `main`" — it does not, and never has here. Every direct push to `main` reports `Bypassed rule violations … 6 of 6 required status checks are expected` and lands. Verify with `gh api repos/sluicesync/sluice/branches/main/protection --jq .enforce_admins`.

  **Why it stays off.** Turning it on would deadlock docs-only commits. `ci.yml` uses `paths-ignore` for `**.md` and `docs/**`, so on a docs-only change the six required checks never report at all — and GitHub treats a never-reported required check as *pending*, not as passed. A typo fix would be unmergeable without an empty code commit to wake CI. It would also end the autonomous release flow, since every fix would become branch → PR → wait for six checks.

  **Prerequisite if you ever do want it on:** convert `paths-ignore` into always-report stub jobs (a job that always runs, does nothing on docs-only diffs, and reports the required check name). Do that *first*; flipping the flag before it is what produces the deadlock.

  **What it does not buy you:** it would not have caught the defects that actually shipped. The deleted v0.109.1 CHANGELOG heading and three wrong scope claims in the v0.110.0 notes all pass CI cleanly (audit 2026-08-04).
- **Allow force pushes:** off — and unlike the linear-history rule, this one is **absolute**: a force-push to `main` is refused outright (`Cannot force-push to this branch … protected branch hook declined`), with no bypass. The consequence worth knowing before you need it: **a merge commit that lands on `main` cannot be rewritten away.** The rule that would have stopped it is soft; the rule that would let you undo it is hard. Accept it, or relax the protection in the UI deliberately — do not burn time on a repair the server will not accept.
- **Allow deletions:** off

## Recommended rules for tags

For release-tag protection (so an accidental tag delete doesn't drop a release):

- Tag rule pattern: `v*`
- **Restrict who can create matching tags** — leave to maintainers or the workflows themselves.

## Setting via the GitHub CLI

If you prefer code over clicks, the same rules can be applied with `gh api`. Example for the basic ruleset:

```bash
gh api -X PUT repos/sluicesync/sluice/branches/main/protection \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "Test (ubuntu-latest)",
      "Integration",
      "Integration (PostGIS)",
      "Integration (vstream)",
      "Lint",
      "Build (ubuntu-latest)"
    ]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": false,
    "required_approving_review_count": 0
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
JSON
```

Set `required_approving_review_count` to `1` once the project has reviewers other than you.

> **`enforce_admins` is `false` in this payload on purpose** — it matches the live configuration and the reasoning above. It said `true` until 2026-08-04, which made this block a copy-pasteable way to deadlock every docs-only commit. If you change it, read the `paths-ignore` prerequisite first.

## Why this matters

Branch protection makes the CI signal *load-bearing*: a failing test or lint job actually blocks a merge. Without it, CI is advisory and tends to drift over time as people merge "just this once" past failing checks. Turning protection on early — even on a solo project — sets the right habit and ensures the `main` branch is always green.
