# sluice v0.141.1

**A `d1-trigger` stream could silently skip changes, and v0.141.0 is what made it reachable. Upgrade if you run one.** Everything else here is a correction to something v0.141.0 said — including a remedy it printed that would have left your stream unable to resume.

## Fixed

**The `d1-trigger` change-log poll sorted lexicographically, and skipped every change that fell the wrong side of a page boundary.** The poll selects the change id cast to text, under the column's own name, and SQLite resolves `ORDER BY` against the output aliases first — so it ordered `1, 10, 11, 12, 2, 3, 9` rather than numerically. That is harmless while the page holds the whole backlog. v0.141.0 added an adaptive page that halves an oversized batch, and a truncated page turns it into loss: the stream advances its position to the highest id it received, so every change below that id which sorted later is captured, never delivered, and never read again. The stream stays alive and reports no error, and restarting does not recover it.

Measured on a live database: a backlog drained with 50 of 53 rows on the target. Only the Cloudflare D1 lane was affected — a local SQLite file selects the id directly and always ordered correctly. Before v0.141.0 the same input produced a loud stall instead, which is why this surfaces now.

**The publication-exposure warning told you to do something that leaves the stream unable to resume.** It suggested dropping the publication to restore writes it had named as broken. Doing that on a running stream pins the replication slot's restart position behind the drop, and the stream then fails to resume — reporting that the publication does not exist while it demonstrably does. Recovering costs dropping the slot and re-copying from scratch, and the publication that recreates is database-wide again. Both warnings now say not to, and point at `sluice sync decommission` instead, which retires the slot and the publication together.

**Both exposure warnings described sets they do not report.** The one raised by `backup full --chain-slot` said it named tables the run does not read; it names every at-risk table in the database, including ones it does. The one raised by a multi-schema `sync start` said "outside the schemas you selected", while its set deliberately also includes a table you removed with `--exclude-table` from a schema you did select — which is the case it was built for. Both now say what they report.

## Compatibility

Drop-in from v0.141.0. No flag change, no format change, no new error codes.

If you run a `d1-trigger` stream against Cloudflare D1 and your change log has ever exceeded one page, changes may be missing on the target. sluice cannot tell you which: the position advanced past them, so they are indistinguishable from changes that were delivered. Re-snapshotting the affected tables is the only way to be certain.

## Who needs this

Anyone running a `d1-trigger` stream — this is the reason for the release. Anyone who saw the new publication-exposure warning in v0.141.0 and acted on its remedy. And anyone reading those warnings, which now describe the right set of tables.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
go install sluicesync.dev/sluice/cmd/sluice@v0.141.1
docker pull ghcr.io/sluicesync/sluice:0.141.1
```
