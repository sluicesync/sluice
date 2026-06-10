# sluice v0.99.33

**Silent-loss fix — restoring a full-only PG backup (a `backup full` with no incrementals) to ANY MySQL-family target (`mysql`, `planetscale`, `vitess`) on any prior version skipped the cross-engine unsupportability gate entirely, so a schema carrying an `EXCLUDE` constraint restored with exit 0 and the constraint silently downgraded to a plain non-unique `KEY`. This is the broader sibling of v0.99.32's chain-path fix — same class, one branch over — found by the v0.99.32 post-release regression cycle within hours of that fix shipping. If you ever restored a full-only PG backup to a MySQL-family target, re-check that schema's constraints (see "Who needs this"); chain restores with ≥1 incremental and PG→PG restores were never on this path. Drop-in from v0.99.32; everything else in this release is internal refactoring and CI-only.**

## Fixed

- **Single-manifest (full-only) cross-engine restores now run the same unsupportability gate as chain restores (Bug 134).** v0.99.32 fixed the PG→`vitess` *chain*-restore refusal skip — but `Restore.Run`'s single-manifest branch (taken when the backup has no incrementals) never called `checkCrossEngineSupportable` at all, on **any** MySQL-family target: a full-only PG backup carrying an `EXCLUDE` constraint restored to `mysql`, `planetscale`, or `vitess` with exit 0 and the constraint silently downgraded to a plain non-unique `KEY` — semantic-invariant loss with every row present, which is exactly why nothing else caught it. The same gap covered the gate's other refusal families (extension opclasses, PostGIS metadata). The gate now runs before the type retarget (so the refusal names the source-true schema) and before the table filter, mirroring the chain path's placement. **Affected releases: v0.15.0 through v0.99.32** — every published version with cross-engine restore; the single-manifest path shipped cross-engine-capable and ungated in v0.15.0. (Exposure per construct tracks when each entered the backup format: `EXCLUDE` carry since v0.72.0, PostGIS/opclass families earlier.) Found by the sluice-testing v0.99.32 regression cycle — the post-release loop catching the adjacent instance of the class it had just verified. Pinned across all three MySQL-family targets (refusal naming the constraint, zero schema-write phases reaching the target) plus PG→PG and clean-schema controls, with a revert-test proving the pin catches the bug.

## Internal

- **The applier batch loop now lives once (ADR-0081, extraction tier b).** Both engines' ~500-line mirrored AIMD/flush/idle-grace state machines collapsed into one shared loop in `internal/appliershared` behind a closure seam; the 69 divergent lines reduce to five named config fields. Behavior-identical — the item-18 timing pins and the ADR-0010 idempotency pin passed unchanged on both engines, with zero test edits. The next batch-loop fix lands in one file instead of two. No user action.

## CI

- Weekly Postgres version matrix (`pg-version-matrix.yml`): the postgres engine integration suite now runs against stock `postgres:17`, `:18`, and a `:latest` canary (PG19-beta drift signal) on a Saturday schedule + dispatch; PR CI stays on the prebaked PG16. `funlen`/`gocyclo` hold-the-line lint ceilings (210 lines / complexity 60) so the orchestrator mega-function class can't regrow silently. No product behavior change.

## Compatibility

- **No breaking changes.** Drop-in from v0.99.32 for all engines and flavors — no flag, default, or invocation changes.
- **Full-only PG → MySQL-family restores that previously "succeeded" by silently downgrading unsupportable constructs will now refuse loudly** — that is the refuse-loudly contract engaging, not a regression. Full-only backups without PG-native unsupportable constructs restore exactly as before; the refusal includes the `--exclude-table` recovery hint.
- **Unaffected:** PG→PG restores (same-engine path), restores from MySQL-family sources, chain restores with ≥1 incremental (gated on the chain path since v0.21.0 for `mysql`/`planetscale`, since v0.99.32 for `vitess`), and all migrate/sync/CDC paths.

## Who needs this — action required

- **Anyone who restored a full-only PG backup (no incrementals) to a `mysql`, `planetscale`, or `vitess` target on any prior version (v0.15.0–v0.99.32)** — that restore may have silently downgraded `EXCLUDE` constraints to plain keys and silently dropped extension-opclass- or PostGIS-dependent fidelity. **Action: re-check that schema's constraints against the source**, or re-run the restore on v0.99.33, which now refuses loudly if the backup carries PG-native constructs the MySQL family can't represent. If the source PG schema carried none of those constructs, nothing was lost.
- **Chain restores (≥1 incremental):** already covered by the chain-path gate — since v0.21.0 for `mysql`/`planetscale` targets, since v0.99.32 for `vitess` targets (see the v0.99.32 notes if you restored a chain to `vitess` before that). Nothing new to do here.
- **Everyone else** — PG→PG restores, MySQL-family-source restores, and all non-restore paths were never affected; the ADR-0081 extraction and CI changes require no action.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.33`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.33`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
