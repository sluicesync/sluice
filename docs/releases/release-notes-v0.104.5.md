# sluice v0.104.5

**`backup verify` returned a clean result on chains `restore` refuses.** Verify already re-checksums every chunk, authenticates every encrypted one under the key restore would use, and refuses a chain whose links no longer stitch together. It never recomputed each manifest's recorded schema fingerprint — a check `restore` runs on every link of a chain and refuses the whole chain on. So a chain written by a release with a different internal schema field set verified `all chunks OK`, exit 0, and then failed to restore with exit 3 and zero rows.

**It was not only a release-skew story, and that is what makes it worth a release of its own.** Changing a single hex digit of a manifest's own `schema_hash` was invisible to verify on every released binary, while every one of them refused to restore that chain. Verify is the command operators are told to run on a schedule precisely so they never learn about a bad backup during a recovery, and on this one class of manifest damage it was answering a narrower question than the one being asked. Pre-existing back to v0.103.2. Found by the v0.104.4 regression cycle asking the question v0.104.3's own headline had turned into a house rule: does verify agree with restore?

### Fixed

**`backup verify` now runs the schema-fingerprint preflight `restore` runs.** Same function, same links, same `SLUICE-E-BACKUP-MANIFEST-INVALID` refusal with the same two-cause wording — a manifest written by a release whose internal schema field set differs, or genuine corruption — so a script branching on the code gets one answer from both commands rather than a green from one and an exit 3 from the other.

**Scoped to exactly what restore does, deliberately.** Restore runs this check on the chain path only: a one-segment-no-incrementals lineage takes the single-manifest path, which never walks links and never fingerprints them. That is why a **bare full still verifies green** even when its recorded fingerprint does not reproduce — such a backup genuinely restores across a fingerprint-epoch boundary, and a verify that refused it would report a healthy backup broken, which is the same defect inverted. The predicate deciding which shape gets checked is now a single function with two callers, so verify and restore cannot drift apart on it.

**What verify was already catching, for calibration:** a corrupted chunk, a deleted chunk, a severed lineage link, an encrypted chain that claims to be plaintext, and — with `--verify-key` — signature failures. The manifest fingerprint was the only restore preflight it skipped. Signing a chain (`--sign` / `--sign-key` + `--require-signature`) already covered this class and still does; it is opt-in, and the check it backstops should not have been.

### Compatibility

- **`backup verify` will start refusing chains it used to pass**, specifically chains written by a release in a different fingerprint epoch than the binary running verify. That refusal is the truth `restore` was already telling you: those chains were already unrestorable by that binary. Nothing about which chains restore changed in this release.
- A **bare full** is unaffected in every case.
- No format-version bump, no flag changes, no new error codes.

### Who needs this

- **Anyone running `backup verify` on a schedule as their DR trust signal.** Re-run it against the chains you rely on. If one starts refusing, it was already unrestorable by that binary — read the writing release the refusal prints, and either restore with a binary from that release's epoch or re-take the backup with this one. See `docs/operator/error-codes.md` under "Schema-fingerprint epochs".
- **Anyone who scripts on exit codes.** `backup verify` can now exit 3 with `SLUICE-E-BACKUP-MANIFEST-INVALID` where it previously exited 0. That is the same code and status `restore` gives for the same chain.
- Everyone else: no action needed beyond upgrading.

### Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.5
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.5`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
