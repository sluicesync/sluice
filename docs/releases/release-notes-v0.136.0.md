# sluice v0.136.0

The M1 audit-remediation batch: the `postgres-trigger` DDL tier learns to see table drops, its replicated-capture opt-in becomes one coherent posture, and the backup store's orphan sweep stops being quadratic. **If you use `trigger setup --capture-replicated-writes`, this release requires one re-run — see Compatibility.**

## Added

**A `DROP` of a synced table is now captured.** The DDL tier listened only on `ddl_command_end`, and PostgreSQL returns **zero** rows from `pg_event_trigger_ddl_commands()` for drops — dropped-object information is only reachable from the `sql_drop` event. So the event trigger fired on `DROP TABLE`, recorded nothing, and the stream carried on as though the table still existed, while ADR-0066 §7 implied coverage (observed on PostgreSQL 13, 16 and 17). A dedicated `sql_drop` capture function now records the drop, and the next resume refuses with a drop-specific remedy — `sluice migrate` reads the *source* schema and cannot land a drop, so the old hint would have been useless. The filter is the dropped-object set rather than a command-tag list: sluice records a drop when the cascade also removed one of its own capture triggers, which means `DROP SCHEMA … CASCADE` over a synced table is caught (a tag list would have re-created the original blind spot), while an unrelated table's drop, a `DROP INDEX`, and a bare `DROP TRIGGER` record nothing. `DROP INDEX` remains deliberately uncaptured — now as a written exemption the roster grades, not an implied one.

## Changed

**`--capture-replicated-writes` is one posture for the whole install.** The opt-in made the row and truncate triggers `ENABLE ALWAYS` but left the DDL event triggers plain, so replica-role DML was captured while replica-role DDL silently was not — the two capture tiers disagreed under exactly the topology the flag exists to support. All of an install's triggers now share the posture, and the capture-shape door grades every one of them.

## Fixed

**The backup store's orphan sweep no longer scans a directory on every write.** v0.134.0's sweep ran a glob per `Put`, and Go's `Glob` enumerates the whole directory — so writing N chunks into one directory cost roughly N²/2 name comparisons and N directory reads. It now sweeps each directory once per store instance. Fixing it surfaced a second defect underneath: because chunk keys are written exactly once, a per-*key* sweep could never fire for them, so chunk orphans were both permanently on disk and permanently hidden from `List`. The scan also moved from `filepath.Glob` to `os.ReadDir`, removing a pattern hazard — a table named `order*` or `t[a-z]` was interpolated straight into a glob, and an unbalanced bracket skipped the sweep entirely.

**Maintenance-heal provenance reaches every signature-verification path**, not just `backup verify` — chain restore, single-manifest restore, parquet export and the broker gate all surface it now, so an operator restoring a healed chain sees that it was healed. **One junk byte no longer hides every heal record:** the JSONL reader skips and *counts* a malformed line with its position instead of refusing the whole file, because indivisibility handed an adversary a one-byte lever to erase evidence. And the pre-heal signature copy now claims its path with `PutIfAbsent`, so its no-overwrite promise is enforced by the store rather than by a probe-then-write race.

## Internal

`TestCaptureTierRoster_EveryEmittedTrigger` renders the setup plan in both postures and requires every emitted trigger to be classified and — under the opt-in — `ENABLE ALWAYS`; `TestLocalStorePut_CostIsIndependentOfDirectorySize` counts directory reads rather than wall-clock, so it measures the regressed quantity directly and cannot flake; `irbackup.Store.List`'s temp-file exclusion is now part of the documented contract with the local-vs-cloud asymmetry named and the cloud driver's staging behavior ground-truthed against the real driver.

## Compatibility

**Action required if you use `trigger setup --capture-replicated-writes`:** an install created by v0.133.x or v0.134.x has the old split posture, and the capture-shape door refuses at its next CDC open until you re-run `sluice trigger setup --capture-replicated-writes`. This is deliberate — a half-applied posture silently loses replica-role DDL, and refusing is how that stops being invisible. Installs *without* the opt-in are unaffected by that refusal; they instead get a `DROP-CAPTURE-ABSENT` warning at open until a re-run adds the new `sql_drop` trigger, because an install predating this release never claimed drop coverage and stranding a running sync over it would be the wrong trade. Everything else is drop-in from v0.135.0.

## Who needs this — action required

- **`trigger setup --capture-replicated-writes` users: re-run it once per install after upgrading.** The capture-shape door refuses at the next CDC open until you do; the re-run takes seconds and preserves the change log, its watermark and the consumer registry.
- **Other `postgres-trigger` users:** a `DROP-CAPTURE-ABSENT` warning appears at each open until you re-run `sluice trigger setup` to install the new drop-capture trigger. Your stream keeps running in the meantime.
- **Backup users on the local store:** nothing to do — the sweep fix is transparent, and chunk orphans that were previously invisible are now both listed and reclaimed.
- Everyone else: upgrade normally; no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.136.0
```

Container images: `ghcr.io/sluicesync/sluice:0.136.0` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
