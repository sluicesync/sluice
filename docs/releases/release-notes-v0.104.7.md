# sluice v0.104.7

**Four defects found by driving a real 122 GB migration into real PlanetScale, and real encrypted backup chains into real S3 — not by constructed tests.** Every one of them had a comment, a hint, or an ADR asserting the behaviour that was in fact broken, and in each case there was no test that failed when it broke. The most consequential: the canonical PlanetScale reparent error aborted cold copies, and two concurrent `backup incremental` runs forked a chain into permanent unrestorability while both reported success.

**If you run large migrations into PlanetScale, or take incremental backups on a schedule, this is a recommended upgrade.**

### Fixed

**The canonical PlanetScale reparent error now rides out instead of aborting the copy.** vtgate answers a query for a shard whose primary is mid-reparent with `Error 1105 (HY000): … primary is not serving, there may be a reparent operation in progress`. sluice classified that TERMINAL, ending a 122 GB copy after 38 seconds — while its own documented retry bounds (30 minutes wall clock, 100000 attempts) went nowhere near exhausted, and while two *other* errors from the same reparent window were retried correctly. The sentence is emitted by **vtgate itself, not by a tablet**, so it carries none of the `vttablet: rpc error` framing the Vitess-transient branch keys on, and it reached a terminal-code shield that returns *before* the "not serving" text fallback ADR-0108 added for exactly this shape. That fallback had been unreachable for every server-returned error since the shield landed in v0.99.290. Nine vtgate sentences are now matched explicitly, derived from vtgate's **raise sites** rather than from its own constant list — because the first cut of this fix used the constant list, and the scale rig found a missing sentence within hours (`inconsistent state detected, primary is serving but initially found no available tablet`, raised a few lines from the reparent under a different code and therefore absent from that list). Rejected sentences are written down with reasons and negative tests. The same classifier serves **CDC apply and the index/constraint DDL phase**, so both had the identical hole and both are fixed.

**Two concurrent `backup incremental` runs no longer fork a chain into permanent unrestorability.** Both used to exit **0** logging success, both recorded the same parent, and the lineage committed them as if sequential — after which `backup verify` and `restore` refused that chain forever with `branching/mis-stitched lineage`. Worse, the broken chain **kept accepting work**: a later incremental exited 0 and chained off the surviving sibling, so overlapping schedules bank days of "successful" backups on a chain that died at the fork. Postgres slot exclusivity does not save you — the loser waits for the slot, re-reads the same changes, and commits a sibling. Reproduced on local disk *and* real AWS S3. sluice now refuses an incremental whose parent is no longer the chain tip, with `SLUICE-E-BACKUP-CHAIN-CONFLICT`. **An existing forked chain is repairable without a re-backup** — `docs/operator/error-codes.md` documents the procedure, and it was verified by restoring a repaired chain row-exact.

**The errno-3024 index-wall hint now actually reaches you.** On a large PlanetScale-MySQL target the deferred secondary-index build can exceed the statement-time limit. sluice has long carried the hint that matters most at that moment — *"the data is already copied, so `--resume` finishes just the indexes with NO re-copy"* — and it never fired. A ~106 GB index build walled at exactly 15 minutes **after the bulk copy had already succeeded**, and the output was the bare MySQL timeout: no hint, no error code, while a bulk-copy failure in the same run emitted both. The hint table was right; the wiring was not. `migrate` always takes the overlapped copy-and-index path, and that path attributed both of its concurrent axes to the bulk-copy phase, whose entries the walled text matches none of. **The remedy was verified, not assumed:** `--resume` on that chain issued zero bulk-copy operations and zero re-copied rows before going straight to the index phase.

**A cleanly-stopped `backup stream` can be restarted immediately.** `backup stream stop` exits 0, but a restart seconds later was refused as though a stream were still running, succeeding only once the liveness record aged out about two minutes later — and the refusal advised running `backup stream stop`, which the operator had just run and which does nothing to an exited process. This is the supervisor and Kubernetes restart shape sluice documents for rotation-born resume. A stream now records its exit, and a recorded exit permits immediate takeover. A recorded stop *request* deliberately does not: a request is also outstanding during the drain window, so honouring it would put two writers on one chain — the corruption class above.

**sluice no longer calls its own recommended environment variable a typo.** Naming a `--encryption-passphrase-env` variable with a `SLUICE_` prefix made every backup warn that it matched no config key. Variables named by a flag are now claimed and skipped, covering `--sign-key`, `--verify-key`, the keyset-source fields and the config-file `env:VAR` form. Genuine typos still warn.

### Changed

**Concurrent `backup incremental` runs against one chain now refuse instead of both succeeding.** This is the one item that can turn a previously "working" setup into a loud failure — but it was never working: it produced a chain no `restore` would read, while reporting success. If you have overlapping schedules against a single chain, the losing run now exits 3 with `SLUICE-E-BACKUP-CHAIN-CONFLICT` and writes nothing. Serialise the schedules, or give each its own chain.

### Compatibility

Drop-in. No backup format version change, no new fingerprint epoch, no flag removals or renames. Chains written by earlier releases read exactly as before, and chains written by this release read on earlier releases within the same epoch as before.

**No new error codes** — both codes involved already existed. What changed is when you see them, which matters only if you branch on `SLUICE-E-*`. `SLUICE-E-BACKUP-CHAIN-CONFLICT` (exit 3) shipped with the conditional-catalog-write guard and is now *also* raised when a run's parent is no longer the chain's tip, so it covers the fork case as well as the interleaved-write case. `SLUICE-E-INDEX-STATEMENT-TIME-LIMIT` was registered but unreachable on the `migrate` path, and now actually reaches you — so a script matching on the *absence* of a code at the index wall will see a change.

### Who needs this

**Anyone migrating a large dataset into PlanetScale.** The reparent fix is load-bearing at scale and gets more so the bigger the load — a fresh database growing from zero reparents repeatedly, and each one used to be fatal. Measured across three runs of the same 122 GB load: the unfixed binary aborted at 8.4M rows, an intermediate fix at 7.1M, and the shipped version rode 392 transients and carried the copy to completion.

**Anyone taking scheduled incremental backups.** If two runs can ever overlap on one chain — a cron that occasionally runs long, a manual incremental beside a scheduled one — you could already have a forked chain that reports healthy and will refuse at recovery. Run `backup verify` against your chains after upgrading; this release's verify walks the lineage and will tell you.

**Anyone running `backup stream` under a supervisor** that stops and immediately restarts it.

**Not urgent if** you migrate to non-PlanetScale targets, take only full backups, and never run concurrent incrementals.

### Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.7
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.7`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
