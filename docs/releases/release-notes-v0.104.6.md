# sluice v0.104.6

**`backup verify` was still green over chains `restore` refuses — one preflight further along than the last release fixed.** v0.104.5 gave verify the schema-fingerprint check restore runs, and shared the rule that decides which backup shapes get it. It did not share the *list* of checks, and that is where this defect lived: the recomputed `BackupID` — which catches a manifest whose `created_at`, `source_engine`, `kind`, or end-position was edited without recomputing its id — was still restore-only. Such a manifest verified `all chunks OK` with exit 0 while `restore` refused it with exit 3 and zero rows.

**It was found by testing the previous release's own sentence.** v0.104.5's notes said the fingerprint was *"the only restore preflight it skipped"*. That is a claim about a list, and the regression cycle checked it the only way such a claim can be checked — by enumerating the list against the code rather than against the shape of the fix. One item did not match. That sentence has been corrected in place on the v0.104.5 notes.

### Fixed

**The `BackupID` recompute now runs at verify time, on the same shapes restore runs it on.** A recorded id that does not match its manifest's content is refused with the same `SLUICE-E-BACKUP-MANIFEST-INVALID` and exit status restore gives, so the two commands answer alike.

**This one reaches a bare full**, which is the mirror image of the fingerprint check's scope and worth stating plainly because the two rules look similar and are not. A bare full — one segment, no incrementals — is exempt from the *fingerprint* check because restore's single-manifest path does not run that one there. Restore does check its id. Verify now matches on both counts: never stricter than restore, never laxer.

**The structural half, which is the reason this is a release and not a one-line patch.** Both checks now come from one function that defines the preflight list, used by chain restore, by the single-manifest restore path, and by verify. The previous fix shared the predicate and left the list duplicated, so closing one instance left the class open. A preflight added to that list now reaches both commands or neither.

### Compatibility

- **`backup verify` may start refusing chains it passed in v0.104.5**, specifically ones carrying a manifest whose recorded `BackupID` does not match its content. As with the previous release, that refusal is the answer `restore` was already giving.
- Chains written by any release in the current fingerprint epoch are unaffected in every case — an id written by sluice matches its content by construction.
- No format-version bump, no flag changes, no new error codes.

### Who needs this

- **Anyone whose DR trust signal is a scheduled `backup verify`.** This is the second of two checks that made verify report healthier than restore. Re-run it against the chains you rely on; a new refusal was already true, only unreported.
- **Anyone who read v0.104.5's "the only restore preflight it skipped".** It was not. The corrected wording is on that release's notes now, and this release is the fix behind the correction.
- Everyone else: no action needed beyond upgrading.

### Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.6
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.6`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
