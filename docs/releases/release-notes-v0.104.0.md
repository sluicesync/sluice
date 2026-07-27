# sluice v0.104.0

A backup-format change that closes a chunk-relocation forgery, and a fix for an availability defect the previous release's own fix created. Minor rather than patch because encrypted backups written from this version record a new format version and a different backup identifier; older chains keep opening exactly as before.

### Fixed

**An encrypted backup can no longer have someone else's rows decrypt into one of your tables.** Each encrypted chunk is sealed under data binding it to its manifest and its parent table, so a chunk cannot be silently relocated — but that binding was assembled by joining fields with newlines, so a value containing a newline and the next field's name could forge a boundary. Two different parent tables could produce byte-identical binding data, and a chunk sealed under one then opened cleanly under the other. Reproduced end to end: seal under a crafted parent, alter the manifest's file reference, and attacker-controlled rows restore into a legitimate table.

The reachable direction was always inward — rows *in*, never rows *out*, because a path sluice generates cannot contain the delimiter — and it required control of the source schema, write access to the backup store, and an unsigned chain, since `--sign` closes the class outright. That is why this was scheduled rather than rushed. Backups written from this version stamp format version 9 and use a length-prefixed encoding that cannot be forged regardless of what a table name or path contains. Chains written earlier keep their recorded version and open unchanged; no re-seal or migration is needed.

**Continuous Postgres→Postgres sync of a table with a `DEFERRABLE` primary key works again.** v0.103.1 fixed a genuine fidelity gap by making a Postgres target carry the `DEFERRABLE` attribute, and that correct fix made the apply path illegal: Postgres refuses a deferrable constraint as an `ON CONFLICT` arbiter, so the stream failed on the first change to such a table, with no retries, and every warm resume failed identically. Other tables in the same stream stalled behind it.

The attribute keeps being carried — reverting that would trade an availability defect for a silent fidelity one. Instead sluice checks the target's keys once the stream opens and, before applying any change, refuses with `SLUICE-E-TARGET-DEFERRABLE-KEY` naming every affected table and the remedy. A table with a deferrable primary key *and* a usable immediate unique index streams normally. Worth knowing if you are reading catalogs yourself: `DEFERRABLE INITIALLY IMMEDIATE` is refused as an arbiter too, so the distinction that matters is deferrable-versus-immediate, not when the check is deferred to. The same defect existed on the bulk-copy writer behind `restore --data-only`, chain replay and `schema add-table`, and is fixed there as well.

### Compatibility

**Encrypted backups only:** a backup written by this version records format version 9 and cannot be read by an older sluice. Reading in the other direction is unaffected — every chain written before this opens exactly as before, because the format version recorded in the manifest decides which encoding is used, never the version of the binary reading it. A resumed backup keeps the version its existing chunks were written under.

A fresh encrypted backup's identifier now differs from the one v0.103.x would have produced for identical input, because the identifier derivation folds in a field that only applies from format version 8 onward and encrypted manifests previously recorded 5. This matters only if you record those identifiers outside sluice; chain linkage is unaffected, since every identifier is derived at the version its own manifest records.

**One narrow behaviour change:** a table with a deferrable primary key that is in a sync's scope but never receives a change used to stream indefinitely, and now refuses at startup. The refusal names `--exclude-table` as the escape. This is deliberate — the alternative is failing later, mid-stream, with the stream unable to resume.

## Who needs this

- **Anyone taking encrypted backups** — upgrade, and note that a backup written from this version cannot be read by an older sluice. Existing chains are unaffected in both directions. If you already run with `--sign`, the forgery this closes was already closed for you.
- **Anyone syncing a Postgres table with a `DEFERRABLE` primary key** — v0.103.1 and v0.103.2 could not stream it, and the failure took the whole stream with it. Upgrade; you will now be told at startup if a table cannot be applied, and why.
- **Anyone who records backup identifiers outside sluice** — a fresh encrypted backup's identifier changes. Chain linkage is unaffected.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.0
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.0`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
