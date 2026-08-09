# sluice v0.118.1

**A regression v0.118.0 shipped, caught by its own post-release regression cycle.** A MySQL source column that DECLARES a spatial reference — `POINT SRID 4326` — had every CDC row refused, on a configuration that worked fine on v0.117.0. If you sync spatial data out of MySQL or MariaDB, upgrade.

Worth saying plainly, because it is the more useful half: the finding is that v0.118.0's own new guard fired on correct data. A refusal that trips on a working configuration is a worse outcome than the silent class it was added to catch, and this one shipped because a sibling sweep stopped one engine short.

## Fixed

**A MySQL source column declaring an SRID was refused on the first CDC row (Bug 236).** `loadTableSchema` — the CDC lane's schema source, reached by the binlog reader through its schema loader and by the change applier directly — never selected `srs_id`, so every geometry column it produced carried SRID 0 regardless of what the column declared. v0.118.0's new per-row guard then compared the row's 4326 against a number nothing had read, and refused with `value carries SRID 4326, column declares SRID 0`. sluice's full schema reader DOES read `srs_id`, which is why `migrate` was correct in every direction and the asymmetry stayed hidden.

**It is the same defect, for the same reason, as the one fixed on the Postgres CDC lane pre-tag in the same release.** Both lanes could not tell "this column declares 0" from "nothing ever established what it declares", and 0 is a legitimate value for the first. The Postgres half was caught by CI before v0.118.0 was tagged; the MySQL half was not swept, and shipped.

The SRID is now READ rather than defaulted — from `st_geometry_columns` on MySQL 8 and `geometry_columns` on MariaDB, whose `information_schema.columns` has no `srs_id` column at all — with a table carrying no geometry paying no extra round-trip. **The write half moves with it:** the change applier caches its target shape through the same loader, so a target column declaring an SRID was being written SRID 0, which MySQL rejects with Error 3643. That arm is closed by the same change.

**The Vitess / PlanetScale path is deliberately unchanged**, and it is worth stating why, because it looks identical: there the VStream wire carries no per-column SRID *and* the target column is therefore created without one, so stripping the value's SRID loses it at both levels and lands valid geometry naming the wrong place. Refusing is the correct answer on that path. Treating it as merely "unknown" would have traded a loud over-refusal for silent data loss — the rule now written at both sites is that unknown-and-recoverable resolves, while unknown-and-unrecoverable refuses.

## Compatibility

No flags, formats or on-disk state changed. This release only widens what v0.118.0 accepts; it refuses nothing that v0.118.0 accepted.

The guard itself is unchanged in intent, and the anti-vacuity pin that ships with the fix is what holds it there: an UNDECLARED spatial column holding a row written at a different SRID must still be refused, because "resolve nothing" and "mark everything unknown" would both make the regression test pass while switching the protection off wholesale.

## Who needs this

- **Anyone running `sluice sync` with MySQL or MariaDB as the SOURCE, where any table has a geometry column declaring an SRID. Upgrade — on v0.118.0 that stream refuses immediately.** No data is at risk; the stream does not start.
- **Anyone applying CDC changes INTO a MySQL target whose geometry columns declare an SRID.** The applier was writing SRID 0 at them and MySQL was rejecting the write with Error 3643.
- **Anyone on the Vitess / PlanetScale VStream path: nothing changed, deliberately.** A per-row SRID that differs from the column is still refused there, and still cannot be carried.
- **Everyone else, including all PostgreSQL-source users: nothing to do.** v0.118.0's Postgres lane already had this fix.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.118.1_Linux_x86_64.tar.gz --repo sluicesync/sluice
gh attestation verify oci://ghcr.io/sluicesync/sluice:0.118.1 --repo sluicesync/sluice
```

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.118.1 · **Container:** ghcr.io/sluicesync/sluice:0.118.1

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
