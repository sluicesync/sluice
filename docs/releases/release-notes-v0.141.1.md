# sluice v0.141.1

**A `d1-trigger` stream could silently skip changes, and could apply the ones it did deliver in the wrong order — on every version since v0.99.175. Upgrade if you run one.** Everything else here is a correction to something v0.141.0 said — including a remedy it printed that would have left your stream unable to resume.

## Fixed

**The `d1-trigger` change-log poll sorted lexicographically.** The poll selects the change id cast to text, under the column's own name, and SQLite resolves `ORDER BY` against the output aliases first — so a page of change ids 1 to 42 came back as `1, 10, 11, … 19, 2, 20, … 42, 5, 6, 7, 8, 9` rather than numerically. That has two separate consequences, and the first draft of these notes described only one of them.

**Changes can be skipped entirely.** Pages are capped at 1,000 rows, and when one truncates the stream advances its position to the highest id it received — so every change below that id which sorted later is captured, never delivered, and never read again. The stream stays alive and reports no error, and restarting does not recover it. Measured on a live database: a backlog drained with 50 of 53 rows on the target.

**Changes that are all delivered can still be applied in the wrong order.** This one needs no truncation, and no backlog larger than a single page. A DELETE whose change id sorts early replays before the INSERT it was supposed to follow, and the row comes back. Measured: a 42-change backlog — 16 INSERTs, then 16 UPDATEs, then 10 DELETEs — left three rows on the target that the source had deleted, because their INSERTs (change ids 7, 8 and 9) sort last and replayed after their own DELETEs (ids 33, 34 and 35). Nothing was lost, nothing truncated, no error was reported, and the stream then went idle believing it was caught up. A restart converges, because the persisted position stays honest — but nothing tells you a restart is needed. This is the shape filed as Bug 268 by the v0.141.0 regression cycle, where it was read as a stall; it is not a separate defect, and the same fix closes it.

As well as fixing the ordering, sluice now checks it. The poller refuses a page that is not in ascending order rather than advancing past it, under the grep-stable marker `CHANGE-LOG-PAGE-UNORDERED` — so an ordering fault from any future cause fails loudly instead of losing or reordering rows quietly. It is the guard that would have caught both arms of this on the first poll.

Only the Cloudflare D1 lane was affected — a local SQLite file selects the id directly and always ordered correctly.

**This is older than v0.141.0, and the first draft of these notes said otherwise.** The D1 poll has been clamped to 1,000 rows a page since v0.99.175, so any backlog above one page truncated the same way, on the ordinary catch-up path, with no adaptive page involved. Simulated against the real pump loop: a 1,100-row backlog delivers 1,000 and loses 100; a 2,000-row backlog loses 899. v0.141.0's adaptive page lowered the threshold and is how this was finally found, but it did not create the exposure. The reordering arm is older still and has no page-size threshold at all: it needs only a poll returning change ids of differing digit length, which is every poll spanning 9 to 10, 99 to 100, and so on. If you have run a `d1-trigger` stream on any version from v0.99.175 onward, assume you are affected.

**The publication-exposure warning told you to do something that leaves the stream unable to resume.** It suggested dropping the publication to restore writes it had named as broken. Doing that on a running stream pins the replication slot's restart position behind the drop, and the stream then fails to resume — reporting that the publication does not exist while it demonstrably does. Recovering costs dropping the slot and re-copying from scratch, and the publication that recreates is database-wide again. Both warnings now say not to, and point at `sluice sync decommission` instead, which retires the slot and the publication together.

**Both `UNSELECTED-NAMESPACE-EXPOSURE` warnings described sets they do not report.** The one raised by `backup full --chain-slot` said it named tables the run does not read; it names every at-risk table in the database, including ones it does. The one raised by a multi-schema `sync start` said "outside the schemas you selected", while its set deliberately also includes a table you removed with `--exclude-table` from a schema you did select — which is the case it was built for. Both now say what they report.

## Compatibility

Drop-in from v0.141.0. No flag change, no format change, no new error codes.

If you have run a `d1-trigger` stream against Cloudflare D1 on any version from v0.99.175 onward, the target may diverge from the source in two ways. Changes may be **missing**, if the change log ever exceeded one page — look at how large `sluice_change_log` grows between polls; a stream restarted after any pause is the usual way it passes 1,000 rows. And rows the source **deleted may still be present**, or a row may carry a stale value, if any single poll returned change ids of differing digit length — which needs no backlog at all. sluice cannot tell you which rows either way: the position advanced past them, so they are indistinguishable from changes applied correctly. Re-snapshotting the affected tables is the only way to be certain.

## Who needs this

Anyone running a `d1-trigger` stream — this is the reason for the release, and the reordering arm reaches streams whose change log never grew past a single page, so a quiet low-volume stream is not exempt. Anyone who saw the new publication-exposure warning in v0.141.0 and acted on its remedy. And anyone reading those warnings, which now describe the right set of tables.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.1
docker pull ghcr.io/sluicesync/sluice:0.141.1
```
