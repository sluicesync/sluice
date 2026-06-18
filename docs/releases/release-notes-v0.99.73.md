# sluice v0.99.73

**Fix (HIGH): the ADR-0093 auto-resnapshot now actually recovers a GTID/binlog source whose resume position was purged, instead of dead-ending.** When a Vitess/PlanetScale or vanilla-MySQL cold-copy runs long enough that the source's binlog/GTID retention window advances past the snapshot position, the snapshot→CDC handoff surfaces `ir.ErrPositionInvalid`. The auto-resnapshot recovery is supposed to re-snapshot — but the proactive warm-resume fall-through refused on the populated target (a resnapshot of a live stream is populated by definition), so it could never succeed and the stream dead-ended needing a manual `--reset-target-data`. Found live during a large-scale Vitess→PlanetScale-MySQL run whose 300 GB cold-copy outran the source binlog window.

## Fixed

- **Auto-resnapshot recovers a purged GTID/binlog source instead of dead-ending on the populated-target refusal (HIGH; live finding).** A long cold-copy can outrun the source's binlog/GTID retention window, so the snapshot→CDC handoff (or a warm-resume) hits `ir.ErrPositionInvalid` (`gtid_purged advanced past the saved position`). ADR-0093 auto-resnapshot should re-copy and recover, but the **proactive** warm-resume fall-through called the cold-start path with `force=false`, so the populated-target preflight (Bug 9) refused (`cold-start refused: target already contains data`) — and a resnapshot target is populated by definition, so recovery was impossible without a manual `--reset-target-data`. (The "no cdc-state row exists" line in that error is a static recovery hint; here it was misleading — the row existed, and the refusal fired purely on the non-empty target.) The `#52` work gave `--restart-from-scratch` the populated-target handling but the separate auto-resnapshot fall-through never got it; the reactive (mid-stream) path was already correct (it sets `RestartFromScratch` and re-copies). The fix threads a `forceFresh` flag through the cold-start gate (single- and multi-database), set by both `--restart-from-scratch` and the automatic fall-through, so a populated target is handled identically: an idempotent reader (VStream/PlanetScale) re-copies with UPSERT (absorbs the overlap, no drop); a non-idempotent reader (native MySQL binlog) drops + recreates the in-scope tables first so the plain-INSERT re-copy can't dup-key; the `sluice_cdc_state` row is preserved.

## Compatibility

- **The automatic recovery is scoped by source CDC method — no change to PostgreSQL behavior.** `CDCBinlog` (vanilla MySQL) and `CDCVStream` (Vitess/PlanetScale), where a purged position is a routine binlog/GTID-retention event, auto-resnapshot. `CDCLogicalReplication` (PostgreSQL logical replication) and `CDCTriggers` keep the deliberate **loud refusal**: a lost PG replication slot is an abnormal failover/config event whose data-preserving recovery (`--reset-target-data` / `--force-cold-start`) must stay an explicit operator choice (the ADR-0075 Phase 2b contract, unchanged). A genuine fresh cold-start onto a populated target is still refused (Bug 9 preserved). The operator's explicit `--restart-from-scratch` is unchanged for every engine; this fix governs only the *automatic* fall-through.
- **No flags, config, or position/state formats change.** This is purely a recovery-path behavior fix on the sync cold-start gate. `--no-auto-resnapshot` still opts out of the automatic recovery entirely (loud terminal error) for the engines that do auto-resnapshot.

## Who needs this — action required

- **Anyone running `sluice sync` from a Vitess/PlanetScale or vanilla-MySQL source over a database large enough that the cold-copy can approach the source's binlog/GTID retention window:** upgrade. Before this fix, if the snapshot position was purged before CDC consumed it, the automatic re-snapshot dead-ended on the populated-target refusal and the sync stopped until a manual `--reset-target-data`. Now it re-copies and recovers automatically. Best practice still applies: keep the source's `binlog_expire_logs_seconds` comfortably larger than the cold-copy duration (PlanetScale's import guidance requires `> 172800`, i.e. 48 h) so the position never gets purged in the first place — a source binlog-retention preflight is a planned follow-up.
- **PostgreSQL users: no behavior change, no action.** PG logical-slot loss continues to refuse loudly (deliberate-recovery contract).

---

## Install

```
brew install sluicesync/tap/sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.99.73
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.73
```

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
