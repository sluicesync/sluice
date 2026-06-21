# sluice v0.99.94

**The PlanetScale storage-auto-grow stall has a third face — the target query-killer — and sluice now rides through it too.** This is a small, targeted follow-up to v0.99.92 (ADR-0108) and v0.99.93 (ADR-0109): the same transient storage-grow stall can surface as a vttablet query-kill, which the retry classifier didn't recognize, so a cold-copy still aborted. v0.99.94 closes that gap.

## Fixed

**vttablet query-killer (`code = Canceled` / `QueryList.TerminateAll`) is now retriable.** When a PlanetScale non-Metal target auto-grows its storage mid-copy, the in-flight write can hang past vttablet's query timeout, and vttablet terminates the connection with:

```
Error 1105 (HY000): target: <db>.-.primary: vttablet: rpc error: code = Canceled
  desc = QueryList.TerminateAll(), elapsed time: 1m1s, killing connection ID N
```

This is the same transient stall v0.99.92 handled as a primary-reparent *error* (ADR-0108, target-write) and v0.99.93 handled as a source-read *timeout* (ADR-0109, source side) — but it appeared as a *third* face, the target's query-killer, which the ADR-0038 retry classifier did not mark retriable (it recognized the tx-killer `code = Aborted "tx killer"` plus `Unknown` / `Unavailable` / `ResourceExhausted`, but not `Canceled`). With it unclassified, neither ADR-0108's cold-copy write-retry nor ADR-0109's auto-restart engaged, and the copy exited fatally.

The fix adds the **specific** server-side reason `QueryList.TerminateAll` to the retriable Vitess-message set, so the cold-copy write-retry and the sync auto-restart both ride it out — retrying after the stall clears (the storage grow completes). It is deliberately matched on the precise `QueryList.TerminateAll` reason and **not** on a blanket `code = Canceled`: a bare Canceled also covers a client-side context cancel (a clean operator shutdown), which must stay terminal. It is treated as a plain transient, not a transaction-killer, so it retries at the same batch size rather than forcing an AIMD shrink (a storage stall is not an oversized transaction).

Found live on the v0.99.93 PS-320 storage-grow validation: v0.99.93's source-read-timeout fix correctly held the source side through the stall, which exposed the next layer — the target query-killer. Pinned in the ADR-0038 classifier test set (the retriable shape, the change-detector pin, and a client-cancel negative that must stay terminal).

## Compatibility

No configuration changes and no behaviour change for an untroubled migration. The only difference engages on a transient target query-kill that previously aborted the copy: it is now retried (bounded, loud) instead of being fatal. A genuine client-cancel / clean shutdown is unaffected (stays terminal). No resume-format, wire, or result-state changes.

## Who needs this

Anyone migrating or running CDC into a **non-Metal PlanetScale MySQL** target whose storage auto-grows mid-copy — together with v0.99.92 and v0.99.93, the copy now rides through all three observed faces of a storage-grow stall (reparent error, source-read timeout, target query-kill). Automatic; no action required.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.94
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.94
```
