# sluice v0.99.101

**A transiently read-only target during a PlanetScale storage-grow window is now retried — the face the ADR-0110 live validation surfaced.** v0.99.100 shipped the coordinated cold-copy grow-window pause (ADR-0110). Its first live validation — a fresh PS-320 cold-copy — immediately found a grow-window face the entire v0.99.92–v0.99.99 reactive arc had never hit, and this release closes it.

## Fixed

**A transiently read-only target (`Error 1290`, `ER_OPTION_PREVENTS_STATEMENT` — "The MySQL server is running with the --read-only option so it cannot execute this statement") is now classified retriable.** During a PlanetScale non-Metal storage auto-grow, the grow's serving transition briefly leaves the target tablet running with `--read-only` before the new primary is promoted; an in-flight cold-copy write then fails with errno 1290 (vttablet frames it as `code = Code(17)`, but the driver still parses `Number==1290`). Because 1290 was not in the retry classifier, the cold-copy went terminal immediately — and the ADR-0110 coordinated grow-gate never engaged, because the gate only quiesces the lanes on a *classified* transient. The write is transient (the retry succeeds once the new primary serves), so 1290-read-only now joins the same bounded-retry class as the reparent / disk-full (1021/1114) / lock-wait (1205) faces, and the grow-gate then coordinates the lane quiesce for the window.

The match is deliberately specific: 1290 is a generic code ("running with the %s option"), so only the read-only variant is treated as the grow transient — a 1290 for any other server option stays terminal (no over-match) — and a genuinely read-only target (a replica endpoint, a misconfigured DSN) exhausts the bounded retry budget and fails loudly rather than waiting forever. Pinned in the classifier test set alongside the disk-full and lock-wait cases, with a negative pin proving a non-read-only 1290 stays terminal.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only difference is that a transient read-only target during a storage-grow window is now ridden out (bounded + loud on genuine exhaustion) instead of aborting the cold-copy. No resume-format, wire, or result-state changes.

## Who needs this

Anyone running a large cold-copy into a **non-Metal PlanetScale MySQL** target across storage auto-grow steps — this was the last unhandled face of the grow window, observed live on a fresh PS-320 validation. Together with v0.99.92–v0.99.100 (every other transient face retriable + the coordinated grow-gate), the cold-copy now rides a storage grow end to end. Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.101
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.101
```
