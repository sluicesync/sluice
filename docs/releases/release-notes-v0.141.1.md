# sluice v0.141.1

**A `d1-trigger` stream could silently skip changes, on every version since v0.99.175. Upgrade if you run one.** Everything else here is a correction to something v0.141.0 said — including a remedy it printed that would have left your stream unable to resume.

## Fixed

**The `d1-trigger` change-log poll sorted lexicographically, and skipped every change that fell the wrong side of a page boundary.** The poll selects the change id cast to text, under the column's own name, and SQLite resolves `ORDER BY` against the output aliases first — so it ordered `1, 10, 11, 12, 2, 3, 9` rather than numerically. That is harmless while one page holds the whole backlog, and lossy the moment a page truncates: the stream advances its position to the highest id it received, so every change below that id which sorted later is captured, never delivered, and never read again. The stream stays alive and reports no error, and restarting does not recover it. Pages have been capped at 1,000 rows since v0.99.175, so any backlog above that has always truncated.

As well as fixing the ordering, sluice now checks it. The poller refuses a page that is not in ascending order rather than advancing past it, under the grep-stable marker `CHANGE-LOG-PAGE-UNORDERED` — so an ordering fault from any future cause fails loudly instead of losing rows quietly.

Measured on a live database: a backlog drained with 50 of 53 rows on the target. Only the Cloudflare D1 lane was affected — a local SQLite file selects the id directly and always ordered correctly.

**This is older than v0.141.0, and the first draft of these notes said otherwise.** The D1 poll has been clamped to 1,000 rows a page since v0.99.175, so any backlog above one page truncated the same way, on the ordinary catch-up path, with no adaptive page involved. Simulated against the real pump loop: a 1,100-row backlog delivers 1,000 and loses 100; a 2,000-row backlog loses 899. v0.141.0's adaptive page lowered the threshold and is how this was finally found, but it did not create the exposure. If you have run a `d1-trigger` stream on any version from v0.99.175 onward and its change log has ever exceeded a page — which a stream restarted after any pause will do — assume you are affected.

**The publication-exposure warning told you to do something that leaves the stream unable to resume.** It suggested dropping the publication to restore writes it had named as broken. Doing that on a running stream pins the replication slot's restart position behind the drop, and the stream then fails to resume — reporting that the publication does not exist while it demonstrably does. Recovering costs dropping the slot and re-copying from scratch, and the publication that recreates is database-wide again. Both warnings now say not to, and point at `sluice sync decommission` instead, which retires the slot and the publication together.

**Both `UNSELECTED-NAMESPACE-EXPOSURE` warnings described sets they do not report.** The one raised by `backup full --chain-slot` said it named tables the run does not read; it names every at-risk table in the database, including ones it does. The one raised by a multi-schema `sync start` said "outside the schemas you selected", while its set deliberately also includes a table you removed with `--exclude-table` from a schema you did select — which is the case it was built for. Both now say what they report.

## Compatibility

Drop-in from v0.141.0. No flag change, no format change, no new error codes.

If you have run a `d1-trigger` stream against Cloudflare D1 on any version from v0.99.175 onward, and its change log has ever exceeded one page, changes may be missing on the target. sluice cannot tell you which: the position advanced past them, so they are indistinguishable from changes that were delivered. Re-snapshotting the affected tables is the only way to be certain. To judge whether you were ever exposed, look at how large `sluice_change_log` has grown between polls — a stream restarted after any pause is the usual way it passes 1,000 rows.

## Who needs this

Anyone running a `d1-trigger` stream — this is the reason for the release. Anyone who saw the new publication-exposure warning in v0.141.0 and acted on its remedy. And anyone reading those warnings, which now describe the right set of tables.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.1
docker pull ghcr.io/sluicesync/sluice:0.141.1
```
