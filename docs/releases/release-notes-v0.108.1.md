# sluice v0.108.1

**Two defects the v0.108.0 regression cycle found in v0.108.0 itself: a false refusal that blocked `backfill --verify` re-runs, and the missing warning that let every pre-v0.108.0 signed chain report `signature valid` while staying schema-forgeable.**

Both were filed against the release that shipped two days ago, and both are the release's own doing — one a regression it introduced, one a silence it left behind. The v0.108.0 notes promised the second of these "in the next patch"; this is it.

## Fixed

**`sluice backfill --verify` refused a plain re-run of a spec it had just completed, and told the operator the backfill had never run.** A genuine backfill moved 500 rows and printed "safe to run the contract step"; the identical command run again exited 3 with `SLUICE-E-BACKFILL-VERIFY-NO-EVIDENCE` and the message *"the run that completed it recorded 0 rows updated, so no run of this spec has ever moved a row"* — which was false. It reproduced on Postgres and on MySQL, on resumed specs as well as completed ones, `--restart` was no help, and only `--verify-only` escaped it. That made the v0.108.0 gate's own third authorizing case — the completed run a re-invocation no-ops over — unreachable, which breaks `expand-contract` re-runs; those live in cron and CI, where re-running a finished step is the correct thing to do.

The cause was not in the gate, and the cycle's suggested cause was falsified before the fix was written. `sluice backfill` records its finished walk as `{state: complete, rows_copied: n}`, and the migrate-state store's JSON codec compacts a *terminal* progress entry to the bare string `"complete"` — a form that carries the state label and nothing else. The count was discarded before it reached the database and read back as zero. Ground-truthed on a real Postgres: the persisted row was the bare string and nothing else, while the header phase and the row itself were intact, so the `MigrationStateStore.Write` contract ("a header-only write never touches per-table progress") held exactly as documented — the loss was a layer above the store.

**Fixed as a class, not as an instance.** The compaction is now a compaction rather than a lossy one: a terminal entry carrying anything beyond its label — a row count, a PK cursor, chunk progress, the indexes-built flag — is written in the object form, and an entry with nothing but its label keeps the compact bare string, so `migrate`'s state rows are byte-identical to before. The mutation run showed the bare string was also dropping `LastPK` and `Chunks` on a terminal state, not only `RowsCopied`, and the reflection-derived gate now fails the build if a field is added to the progress struct without being enumerated.

The refusal's own message no longer asserts something it cannot know. A stored count of zero means either that the completing run genuinely moved nothing **or** that it was written by a release that dropped the count, and sluice will not authorize a `DROP` on that guess — it names `--verify-only` as the escape hatch for entries a pre-fix binary already marked complete. The three genuine refusals v0.108.0 added are unchanged, and are pinned in one table alongside the two authorizing re-run shapes so the boundary cannot be widened by accident.

## Security

**Every backup chain signed before v0.108.1 stays schema-forgeable, and sluice now says so at every signature check.**

v0.108.0 closed the manifest-schema forgery by folding the schema's raw bytes into a new canonical serialization (`sluice-manifest-canon/v5`) — for chains signed from v5 onward, and only those. A signature cannot be strengthened retroactively; ADR-0183 deliberately preserves every retired canon rendering byte-for-byte so existing chains keep verifying, and no re-seal migration exists.

The consequence, which the v0.108.0 notes left to be inferred: an operator who read a headline about a schema-forgery fix, upgraded, and pointed the new binary at the chains they already had got **`signature valid`, exit 0, under `--require-signature`** — over a manifest forged on disk with no key material — and then restored it at exit 0 with CHECK and UNIQUE constraints stripped and row-level security turned off, after which the target accepted a negative amount and a duplicate key the source forbade. Confirmed end to end, not reasoned about.

The design is correct and is unchanged. The defect was the silence. A signature that verifies under a canon version predating v5 now emits a warning naming the recorded version, what it does not authenticate, what that concretely permits, and the only remedy — **a fresh full backup taken with this release**, because an existing chain cannot be re-signed into coverage. It is emitted from inside the single verification routine all three entry points share (`restore` on a bare full, chain restore, and `backup verify`), so no verification path can miss it, and the predicate is derived from the canon-version table rather than a version-string compare, so an unknown newer version reports "not covered" rather than guessing reassurance.

**Refusal is deliberately not the default, and no `--min-canon-version` is wired.** Refusing a pre-v5 canon would break every existing chain on upgrade day — the trade ADR-0181 got right by warning — and a policy knob of that kind has to reach `restore`, `verify`, `export-as-parquet` and the broker together, or it becomes a flag that silently applies to one entry point. That absence is now recorded in ADR-0183 and `SECURITY.md`, alongside the unsigned-chain residual, which is where a reader will look for it.

## Compatibility

**No new refusals, and no format change.** This release removes one refusal that should never have fired and adds one warning. Chains, signatures, and state rows written by any earlier release are read exactly as before.

**Migrate state rows are byte-identical.** The progress-codec change only affects terminal entries that carry detail beyond their label — which, before this release, is precisely the set whose detail was being lost.

**A pre-v0.108.1 `backfill` state row still reads back a zero count.** The codec fix applies from the write side onward; an entry a previous binary already compacted has no count to recover. `--verify-only` is the documented path for those, and the refusal message now names it.

## Who needs this

- **Anyone with a signed backup chain taken before v0.108.1** — you have the old exposure, not the new protection, and you will now be told so. Take a fresh full.
- **`expand-contract` / `backfill` users on v0.108.0** — the `--verify` step is blocked on re-run; this unblocks it.
- **Anyone whose migrate or backfill state rows carry a cursor or a count on a terminal entry** — that detail was being dropped at the JSON boundary.
