# sluice v0.141.3

**`sluice sync decommission` did nothing on the default configuration, and said it had succeeded.** If you run Postgres CDC and have ever decommissioned a stream you started without `--slot-name`, check `sluice slot list` for a leftover slot. Nothing was lost, but a slot may still be pinning WAL.

## Fixed

**A stream started without `--slot-name` was reported as "a legacy row from an older sluice" (Bug 271).** Such a stream records an *empty* `slot_name` on its control row — the convention is that empty means the engine default, and the fallback was left to each consumer. `sluice schema add-table` implements it. `sluice sync decommission` did not: it read the empty value as a row from a sluice too old to record one, dropped no slot, dropped no publication, cleared the control row, printed `stream "…" decommissioned` and exited 0. The slot it left behind then blocks the next cold start with `replication slot "sluice_slot" already exists` — the exact failure the command exists to prevent.

**The same gap meant it did not refuse a running stream.** The empty-name case was the first arm of a switch that sat ahead of the active-slot refusal, so on the default configuration `decommission` skipped that check along with the drop and deleted a live stream's control row underneath it. This is why v0.141.2's notes were wrong to say the command "already enforces" that the stream be stopped first; that release carries a correction banner.

**How it is fixed, and what deliberately did not change.** The old code refused to guess, and its reason was good: dropping the engine default slot on a hunch could take out a different stream, and a genuinely legacy row cannot say whether its stream used a custom one. So sluice still does not guess. It now *recovers* the name the stream itself recorded — a Postgres position is `{"slot":…,"lsn":…}` and its encoder refuses an empty slot, so any decodable position names the slot authoritatively. When no name can be recovered, the conservative skip stands, and its message no longer claims the row is legacy. The report also names which slot it acted on and whether that name was recorded or recovered.

## Compatibility

Drop-in from v0.141.2. No flag change, no format change, no new error codes.

This behaviour is **older than v0.141.2** — v0.141.1 and earlier are identical — so the exposure is not new, only newly described. If you decommissioned a Postgres stream that you started without `--slot-name`, the slot was not dropped. `sluice slot list` will show it; `sluice slot drop <name>` removes it. An inactive `sluice_*` slot left from a finished stream is the shape to look for, and it pins WAL until removed.

Streams started **with** `--slot-name` were never affected: their control row records the name, and every check reached it.

## Who needs this

Anyone running Postgres CDC who has used `sluice sync decommission`, and anyone who read v0.141.2's warnings — they advertise that command at three doors.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.3
docker pull ghcr.io/sluicesync/sluice:0.141.3
```
