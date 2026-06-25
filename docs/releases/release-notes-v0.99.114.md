# sluice v0.99.114

**A PlanetScale Postgres storage-grow reparent that severs the connection mid-COPY (`unexpected EOF`) is now retriable on the cold-copy path — the third and final observed reparent face, completing the item-38 grow ride-through.**

## Fixed

The live item-38 re-validation (full MySQL → fresh PlanetScale-Postgres cold-copy) showed the non-Metal storage auto-grow reparent surfaces as a **cluster of transient faces**, hit in different rounds depending on exactly when the new-primary takeover lands relative to the in-flight `COPY`:

1. `53100` could-not-extend (disk-full) — classified retriable in **v0.99.111**.
2. `pg_readonly` read-only window (`XX000`) — classified retriable in **v0.99.113**.
3. **A raw connection severance** — the primary takeover drops the in-flight chunk `COPY` connection, which pgx surfaces as `unexpected EOF` (no SQLSTATE) rather than a server error. This wasn't in the retriable set, so the cold-copy died terminal on it (no data loss — each chunk `COPY` is atomic, so a severed chunk wrote nothing).

This release adds the connection-severance forms to the retriable connection-transient class: the `io.ErrUnexpectedEOF` sentinel and the `unexpected EOF` / `use of closed network connection` message forms now join the existing `io.EOF`, `driver.ErrBadConn`, connection-reset / connection-refused / broken-pipe / i-o-timeout, and class-`08` connection-exception faces. So when a reparent severs the COPY connection, the grow-gate + chunked-COPY retry reconnects on a fresh connection and replays the (atomic, rolled-back) chunk — bounded (~30 min wall-clock) and loud on genuine exhaustion.

With all three faces classified, a full MySQL → PlanetScale-Postgres cold-copy rides the storage auto-grow reparent to completion regardless of which face the timing produces.

Pinned in the classifier test set: both the `io.ErrUnexpectedEOF` sentinel and the wrapped `unexpected EOF` string form classify retriable; the over-match guards (a generic `XX000`, an unrelated error) stay terminal.

## Compatibility

Additive — three more connection-transient faces on the retriable classifier (used by the PG cold-copy chunk retry and the CDC apply reconnect), all unambiguously connection-severance signals. No effect on non-PlanetScale targets; the exactly-once contract is unchanged (a severed atomic chunk wrote nothing, so the replay neither dups nor drops).

## Who needs this

Anyone running `sluice sync` / `migrate` into a **PlanetScale Postgres** target whose cold-copy crosses a storage auto-grow — the reparent is now ridden through all three ways it can surface. Automatic.

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.114
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.114
```
