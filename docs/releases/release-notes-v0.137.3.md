# sluice v0.137.3

**Two diagnostics that follow v0.137.2.** Nothing changes what resumes or what refuses — both changes make a situation that was already happening *visible*. If you run MySQL sync from a backup chain, or your source has `gtid_mode=OFF`, you will see new log lines explaining where you stand. Drop-in from v0.137.2.

## Changed

**A resume that cannot verify the source's identity now says so.**

v0.137.2 taught backups to stamp the source's `@@server_uuid` onto binlog file/pos positions, so a resume against a replaced or rebuilt server refuses rather than streaming from an unrelated binlog lineage. That check quietly does nothing in two cases — and until now, both were silent.

*A position captured before v0.137.2 carries no identity*, so there is nothing to check it against. Those positions are still accepted. That population cannot grow, because every capture door now stamps, so it drains on its own; and refusing would force a full re-copy on chains that are almost certainly fine, which is a loud failure on a working configuration traded against a hazard already stopped at the source. What is new is that the resume warns, naming that the binlog *filename* check is all that protects it. That is what makes "take one fresh full backup" actionable for a specific chain instead of blanket advice.

*The second case is different, and is now treated as such.* The position carries an identity, but the **source's** `@@server_uuid` could not be read, so the check could not run. That is a probe failure on a check whose entire job is to refuse — not an old position. It is still allowed through, so a transient read cannot force a re-snapshot, but it warns; a recurring one should be treated as a real finding rather than noise.

Both carry the marker `UNVERIFIED-INSTANCE-IDENTITY`, so every chain currently resuming without the identity check can be found in a single grep.

**A source with `gtid_mode=OFF` is told which resume mode it is on.**

sluice has never required GTID either way and still doesn't — it detects the setting and picks an arm. That was reasonable while the two arms looked equivalent. It stopped being reasonable once file/pos turned out to be the arm carrying an instance-identity hazard: binlog filenames and offsets are instance-local, so a resume against a replaced source is caught only by the `@@server_uuid` stamp, whereas a GTID set is instance-bound by construction and never needed one.

Choosing the weaker arm silently, on MySQL 8's default, left operators with no way to know they were on it. There is now one INFO per CDC open that says so, and says plainly that file/pos is supported and correct.

It is an INFO and not a warning on purpose. A warning on a working configuration is what teaches people to ignore warnings, and plenty of sources run this way deliberately. MariaDB and the Vitess/PlanetScale flavors are excluded, because neither has a choice to advise about — MariaDB is always in GTID mode, and the vtgate flavors resume on VStream positions and reach neither binlog arm.

## Who needs this

- **Anyone resuming MySQL sync from a backup chain created before v0.137.2**: you will now see `UNVERIFIED-INSTANCE-IDENTITY` on those resumes. Nothing is broken — it is telling you which chains would benefit from one fresh full backup.
- **Anyone running a MySQL source at the 8.0 default of `gtid_mode=OFF`**: one new INFO per CDC open, explaining the trade. No action required.
- Everyone else: upgrade normally; no action, and no new output.

## Compatibility

Drop-in from v0.137.2 — no schema, format, or flag change, and no change to which positions resume or refuse. The only difference is log output.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.137.3
```

Container images: `ghcr.io/sluicesync/sluice:0.137.3` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
