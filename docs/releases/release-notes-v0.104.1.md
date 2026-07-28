# sluice v0.104.1

Three fixes, and a correction to what the last release's notes told you.

v0.104.0 said existing backup chains were unaffected in both directions by the new encrypted-backup format version. That is true only of a chain nobody extends — and taking an incremental is the most ordinary thing anyone does to an existing chain. Extending one with the new binary silently made the whole chain unreadable by older ones. The claim has been corrected everywhere it appeared, and this release makes sluice say so itself, at the moment it happens.

## Fixed

**Extending an existing backup chain with a newer sluice now says so, instead of silently locking older binaries out of the entire chain.** Every manifest records the format version its own data was written under, and a reader always recomputes at the version the artifact records — that is what lets a new sluice open an old chain forever. But a restore walks the *whole* chain, so a binary that refuses any single link restores nothing from that chain, including links it wrote itself. One `backup incremental` from a newer binary was enough to make an older one unable to read a chain it had been reading fine, with no warning and exit code 0.

sluice now warns at the moment the chain's version floor rises, naming the previous and new versions, the earliest release that can now read the chain, and the consequence in as many words:

```
WARN backup: this segment raises the chain's format version, so sluice
     releases older than v0.104.0 can no longer restore ANY part of this
     chain — including the segments they wrote themselves. If a binary
     that must be able to restore is still on an older release, upgrade
     it before taking further backups here
     chain=backup-2f9c… previous_format_version=7 new_format_version=9
     minimum_sluice_version=v0.104.0
```

A warning rather than a refusal, deliberately: refusing would break the ordinary incremental cadence for everyone on upgrade day — including the majority who have no older binary anywhere — to guard a condition sluice has no way to detect, since it cannot know what other binaries exist. You have that information; the warning is what gets it to you. All three paths that can raise a floor warn: `backup incremental`, a `backup stream` rollover, and the segment full a stream rotation creates. Chain compaction cannot raise one — it rewrites manifests in place and each keeps its own recorded version. [Backup format versioning](https://github.com/sluicesync/sluice/blob/main/docs/backup-format-versioning.md#a-chains-floor-is-its-newest-link) has the full contract and what it means for a staged fleet upgrade.

This class was invisible to every test sluice had, all of which run one binary against its own output — which is how it reached a release. CI now runs the previous release's binary and the current one against a single backup chain on every scheduled sweep, asserting what each can read after an extension.

**If you run PlanetScale or Vitess, expect to see this once per chain, including on brand-new ones.** A chain's root full is stamped at the version its schema needs, while a CDC segment from a VStream source is stamped 8 — so the first `backup incremental` on any VStream chain raises the floor from the root's version to 8 and says so, even when a single binary wrote both. That is accurate rather than alarming: the chain does require v0.99.228 or newer to restore, and it always did, from its first segment onward. Nothing has been broken and nothing needs fixing. It fires once, because every later incremental's parent is already the raised segment. This also means the situation predates the encrypted-format change entirely — the warning is version-agnostic and covers it rather than being specific to v9.

**Stopping `sluice backup stream` while it is still starting up now exits cleanly instead of reporting a failure.** Every startup step reads the backup destination, so a SIGTERM (or Ctrl-C, or `q` on the live panel) arriving during startup surfaced as an error naming whichever step it interrupted — the signed-chain probe was the shape a CI run caught, but the concurrent-writer preflight, the parent resolution and the initial state write could each produce their own wording. The process exited 1, which a supervisor or an orchestrator reasonably reads as a crash. sluice now distinguishes your own cancellation from a startup fault: the former exits 0 with a log line saying so, nothing having been in flight to lose, and a genuine startup failure — including one that happens to race the shutdown — still fails loudly and exits non-zero as before.

**Refusing to extend a signed backup chain no longer leaks the replication connection it had already opened.** `backup stream` cannot yet sign its rollover manifests, so it refuses a signed chain up front; that refusal returned before arming the teardown for the source replication slot it had opened a moment earlier.

**A refusal that travelled through a phase wrapper reported the wrong error code and the wrong exit status.** The deferrable-key refusal reached `schema add-table` as `SLUICE-E-BULKCOPY-TABLE-FAILED` with exit 1, where `sync start` and chain restore reported the documented `SLUICE-E-TARGET-DEFERRABLE-KEY` with exit 3 — so an operator matching on the documented code, which is what the error-codes table tells you to do, would not have matched it. The cause was general rather than specific to that refusal: the hint layer that attaches operator guidance to engine errors matches on message substrings and then rebuilt the error's code unconditionally, discarding whatever code it already carried, and the exit status is derived from that code. Any precisely-coded refusal passing through a phase wrapper was affected the same way. An error that already carries a code now keeps it, along with its own remedy — which also removes a misleading hint, since the bulk-copy guidance tells you earlier tables are missing their secondary indexes and a refusal copied nothing.

## Compatibility

**Two exit statuses change, both toward being more accurate.** Stopping `backup stream` during startup now exits 0 instead of 1 — this affects non-TTY invocations, which is to say systemd units, Kubernetes probes and CI; under the interactive panel it already exited cleanly. And a coded refusal that reaches you through a phase wrapper now exits 3 (sluice's refusal status) rather than 1, and reports its own error code rather than the phase's. If you have automation branching on either, it is branching on the corrected value now.

No on-disk format changes in this release. This version and v0.104.0 read each other's backups and stamp the same format versions; the contract is exactly as v0.104.0 established it. What changed is that sluice now tells you when extending a chain moves its floor.

## Who needs this

- **Anyone running backup chains across more than one sluice version** — this is the release to have. It will not undo a floor that has already been raised, but it will stop the next one from being silent.
- **Anyone running `backup stream` under systemd, Kubernetes, or any supervisor** — a stop during startup no longer looks like a crash.
- **Anyone whose automation matches on sluice error codes or exit statuses** — read the Compatibility note above.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.1
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
