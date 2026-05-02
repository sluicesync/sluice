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
  - `Test (macos-latest)`
  - `Test (windows-latest)`
  - `Integration`
  - `Lint`
  - `Build (ubuntu-latest)`
  - `Build (macos-latest)`
  - `Build (windows-latest)`

GitHub will only suggest these in the search box once they've successfully run at least once on a PR. If they don't appear, push a no-op PR first to trigger CI, then add them.

### Other

- **Require conversation resolution before merging:** on
- **Require linear history:** on (recommended) — disallows merge commits, forcing rebase-and-merge or squash-and-merge. Keeps `git log --oneline` readable.
- **Require signed commits:** off (worth turning on later if you adopt signed commits across the project)
- **Include administrators:** on — applies the same rules to repo admins, which prevents accidental direct pushes to `main`.
- **Allow force pushes:** off
- **Allow deletions:** off

## Recommended rules for tags

For release-tag protection (so an accidental tag delete doesn't drop a release):

- Tag rule pattern: `v*`
- **Restrict who can create matching tags** — leave to maintainers or the workflows themselves.

## Setting via the GitHub CLI

If you prefer code over clicks, the same rules can be applied with `gh api`. Example for the basic ruleset:

```bash
gh api -X PUT repos/orware/sluice/branches/main/protection \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "Test (ubuntu-latest)",
      "Test (macos-latest)",
      "Test (windows-latest)",
      "Integration",
      "Lint",
      "Build (ubuntu-latest)",
      "Build (macos-latest)",
      "Build (windows-latest)"
    ]
  },
  "enforce_admins": true,
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

## Why this matters

Branch protection makes the CI signal *load-bearing*: a failing test or lint job actually blocks a merge. Without it, CI is advisory and tends to drift over time as people merge "just this once" past failing checks. Turning protection on early — even on a solo project — sets the right habit and ensures the `main` branch is always green.
