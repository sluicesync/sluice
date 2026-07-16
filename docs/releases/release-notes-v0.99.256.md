# sluice v0.99.256

Closes the two operational findings from v0.99.255's live PlanetScale validation runs (roadmap item 71): the branch-cleanup race that stranded a dev branch after every successful deploy, and the wrong-flag hint on a fresh migrate into a safe-migrations branch.

## Fixed

- **PlanetScale dev-branch cleanup no longer strands its branch after every successful deploy (item 71a).** The post-deploy branch delete races the deploy/skip-revert settling window (~1 min), where the control plane refuses with HTTP 422 "cannot be deleted while a deployment is in progress" — so every live `sluice deploy-ddl`, `expand-contract`, and index-fallback run left its `sluice-*` dev branch behind with a WARN (4/4 in the v0.99.255 live validation). Cleanup now retries exactly that 422 class (bounded: 6 attempts, 20s apart, context-aware); any other delete error still WARNs immediately, and exhausted retries keep the WARN naming the branch. All three legRunner consumers benefit.
- **A fresh `migrate` into a safe-migrations PlanetScale branch now refuses loudly and correctly at user-table creation (item 71c).** The user-table `CREATE TABLE` 1105 was an uncoded exit-1 error whose hint named `--schema-already-applied` — a sync-only flag `migrate` doesn't have. It is now the coded `SLUICE-E-PS-DIRECT-DDL-BLOCKED` refusal (exit 3, the v0.99.254 control-table treatment) naming the refused table and both real recovery paths: disable safe migrations for the migration window, or pre-create the schema via deploy requests (`sluice schema preview` prints the target DDL, `sluice deploy-ddl` ships each statement — a sync stream then skips schema-apply with `sluice sync start --schema-already-applied`; `migrate` cannot skip pre-created tables yet, tracked as roadmap item 71b).
- **Docs: the `SLUICE-E-INDEX-DIRECT-DDL-DISABLED` row no longer oversells the ADR-0148 index fallback.** The fallback engages at the deferred index phase (resume/restore flows, or safe migrations enabled mid-run); a fresh migrate into a safe-migrations branch refuses earlier, at table creation. The error-codes and managed-services docs now state the honest reach.

## Compatibility

- **No breaking changes.** No new flags, codes, or surfaces — one retry loop in a best-effort cleanup path (worst case for a persistent non-race 422: the same WARN ~100s later), one previously-uncoded error class now coded (scripts keying on exit codes see exit 3 instead of 1 for the safe-migrations user-table refusal, consistent with every other `SLUICE-E-PS-DIRECT-DDL-BLOCKED` site), and docs corrections.

## Who needs this

Anyone running the PlanetScale deploy-request workflows shipped in v0.99.248–255 (`expand-contract`, `deploy-ddl`, the index-build fallback): cleanup now converges instead of leaving a branch per run. Anyone scripting a fresh `migrate` into a safe-migrations branch gets a coded, correctly-hinted refusal instead of an uncoded error with a flag that doesn't exist on `migrate`.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.256
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.256`
