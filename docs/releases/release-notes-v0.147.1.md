# sluice v0.147.1

One fix, for PlanetScale and Vitess sources: an **errant GTID** is no longer reported as a replaced keyspace, and now names the remedy that avoids the full re-copy entirely.

Take this one if you sync from PlanetScale or self-hosted Vitess. Nothing else is affected.

## Fixed

**An errant GTID made sluice call your keyspace "a different lineage" and re-copy the whole target.** An errant GTID is a transaction executed directly on a replica, so that tablet's `gtid_executed` carries a source UUID the primary never executed — a well-known Vitess operational condition. sluice records Vitess's VGTID verbatim, and on PlanetScale the CDC tail streams from a `REPLICA` by default, so the errant UUID lands in the persisted resume position. On the next resume the lineage pre-flight probes through vtgate, which may pick a *different* tablet with no trace of that UUID — and the check, which required every UUID in the position to be present, concluded the source had been replaced.

It had not. Measured on a real 3-tablet Vitess cluster: the errant transaction was in a database **outside the synced keyspace entirely**, nothing sluice replicates was touched, and the target had not diverged by one byte. The position was still refused. The control is what settles it — the *same* position probed at the tablet that holds the errant UUID resumed cleanly. Same database, same instant, opposite verdict, decided purely by which tablet vtgate happened to pick. Left alone the condition becomes permanent: once the errant-carrying replica is replaced, no tablet holds that UUID and every future resume refuses.

The verdict is now three-way (`classifyLineage`). A position that shares UUIDs with the shard *and* names one it has never executed is an errant GTID, not a replaced keyspace, and it says so — under `SLUICE-E-CDC-LINEAGE-MISMATCH`, naming the remedy that costs nothing: reconcile the errant GTID on the primary (the usual fix is injecting an empty transaction with that GTID), after which the resume succeeds with no re-copy at all. A position that shares *no* UUIDs is still a genuinely foreign lineage and still refuses as before. Replica lag is unchanged.

**The scope, stated rather than implied:** this separates an errant GTID riding a UUID the shard has **never executed** — the shape a write to a replica that was never a primary makes. An errant transaction on a *demoted* primary rides a UUID already present in every tablet's `gtid_executed`, so it is still classified as replica lag and still proceeds. That shape is unchanged by this release and pinned as a known gap; closing it needs sequence-range reasoning rather than the UUID-set test used here.

## What this deliberately does not do

**It still refuses on the errant shape rather than resuming through it**, and that was measured rather than assumed. With the pre-flight bypassed on a purpose-built 3-tablet cluster:

- **While the errant-carrying tablet is still in the pool**, vtgate routes the stream to the tablet that can serve it and everything works. Resuming would succeed here, so this release's refusal is knowingly conservative in that case.
- **Once that tablet has been replaced** — routine on PlanetScale — every tablet answers `GTIDSet Mismatch`, vtgate spends ~90 seconds cycling through them without surfacing anything to the client, and then gives up. No routing can rescue it: the UUID exists nowhere in the shard.

Refusing at the door costs one re-copy; resuming through it would cost that ~90-second discovery on every future resume, forever. The refusal is kept for the half that cannot be rescued, and the false refusal in the other half is a known, accepted cost rather than an unexamined one.

## Compatibility

No behaviour change for any source that was working. The only outcome that changed is the *diagnosis and message* of a refusal that already fired — an errant GTID previously produced a "different lineage" refusal and now produces an errant-GTID one with an actionable remedy. Vanilla MySQL, MariaDB, Postgres, SQLite and the trigger-CDC engines are untouched: this is the VStream lane only. No flag added, renamed or removed; backup format version unchanged at 10.

## Who needs this

Upgrade if you sync from PlanetScale or Vitess and have ever seen a resume refused as a replaced keyspace — particularly if your fleet sees errant GTIDs. Everyone else can skip this one.

## Install

```sh
# Homebrew
brew install sluicesync/tap/sluice

# Scoop (Windows)
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket
scoop install sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.147.1
```

Container image: `ghcr.io/sluicesync/sluice:v0.147.1`
