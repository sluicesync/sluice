# sluice v0.99.97

**InnoDB lock-wait-timeout (`Error 1205`) is now retriable — the sibling of deadlock.** A small, general correctness fix (sluice already retried deadlocks but not lock-wait-timeouts), surfaced by the PlanetScale storage-grow validation where the concurrent cold-copy writers contend during a prolonged auto-grow.

## Fixed

**`Error 1205` / `ER_LOCK_WAIT_TIMEOUT` classified retriable.** A lock-wait-timeout is the textbook "retry the transaction" InnoDB transient — the timed-out statement is rolled back and a retry succeeds once the contending lock releases. The retry classifier already treated InnoDB deadlock (`Error 1213`) as retriable but not its sibling lock-wait-timeout (1205), so a 1205 aborted a cold-copy / CDC apply where a 1213 would have ridden through — an inconsistency independent of any particular target. It surfaces heavily under a prolonged PlanetScale storage-auto-grow stall, where the concurrent cold-copy writers contend on row locks while the volume grows: the v0.99.96 PS-320 validation rode roughly 13 minutes of disk-full / query-killer retries (those fixes working as designed) and then died on this single unretried lock-wait-timeout. vttablet wraps it as `code = DeadlineExceeded desc = Lock wait timeout exceeded`; the fix matches on the MySQL error number 1205, so the wrapping is irrelevant. It is bounded by the existing retry budgets (a persistent, non-clearing lock contention exhausts the budget and fails loudly). Pinned alongside the 1213 deadlock case in the classifier test set.

This completes the standard set of "retry the transaction" InnoDB transients (deadlock + lock-wait-timeout) and, together with v0.99.92–v0.99.96, the set of transient faces a PlanetScale non-Metal storage-auto-grow presents to a large cold-copy.

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. A lock-wait-timeout that previously aborted is now retried (bounded, loud on exhaustion). No resume-format, wire, or result-state changes.

## Who needs this

Anyone whose cold-copy or CDC apply can hit InnoDB lock contention — most visibly a large concurrent cold-copy into a **non-Metal PlanetScale MySQL** target during a storage auto-grow, but the fix is general (it is target-agnostic). Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.97
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.97
```
