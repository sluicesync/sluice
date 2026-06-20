# sluice v0.99.85

**Diagnosability: a timed-out MySQL warm-resume now tells you *why* the source is unresponsive — out of disk, globally wedged, or a slow binlog subsystem — instead of a generic "source unresponsive".** Builds directly on the v0.99.84 verify-timeout fix.

## Added

**Source-unresponsive diagnosis on a warm-resume verify timeout.** When sluice resumes a MySQL/Vitess CDC stream it first confirms the persisted position is still resumable (`SHOW BINARY LOGS` in file/pos mode, a `GTID_SUBSET` check in GTID mode). v0.99.84 bounded that check with a timeout so a wedged source can't hang the stream; v0.99.85 makes the timeout *informative*. On expiry, sluice runs a bounded `SELECT 1` liveness probe on a fresh connection and folds the result into the error message. Because the verify query touches the binlog subsystem and a plain `SELECT 1` does not, the differential isolates which layer is actually stuck:

- **The probe returns a disk-full signal** (`Error 1021`, `No space left on device`, `errno 28`, or MySQL's "waiting for someone to free some space" block-message) → the source host is named as **out of disk space**, with the remedy (`PURGE BINARY LOGS` / lower `binlog_expire_logs_seconds`).
- **The probe also times out** → the source server is **globally unresponsive** — a full datadir blocks MySQL writes server-wide, severe overload, or the server is down.
- **The probe succeeds** → the server is up but the **binlog subsystem specifically** is slow — most often an over-large binlog *file count* or a slow/full binlog volume; check `binlog_expire_logs_seconds`.

The diagnosis is honest about its limits. The MySQL wire protocol exposes no datadir free-space surface, and a full disk frequently makes MySQL *block* ("waiting for someone to free some space") rather than return an error — so the verify timeout itself is often the only hard signal. This feature **narrows** the likely cause and points at the remedy; it does not claim a definitive "disk is full" verdict. The probe is itself bounded so it can never become a second hang, and it is pure diagnosis: it changes only the error text — never the retry decision, never the resume position (the error remains retriable, so the stream reconnects, and is never mistaken for an invalid/purged position that would trigger a cold-start).

This was motivated by a live large-scale finding: a source had accumulated 2,585 binlog files on a 100%-full disk, and `SHOW BINARY LOGS` blocked — the old behavior was an indefinite hang (fixed in v0.99.84), and now the operator gets a message that points straight at the disk and binlog retention instead of a bare "unresponsive".

## Compatibility

No interface, flag, or default-behavior changes. The liveness probe runs only on a verify *timeout* (a healthy source never reaches it), is bounded, and affects only the error text. No effect on Postgres sources, the cold-start path, or the steady-state CDC tail.

## Who needs this

Operators running `sluice sync` against MySQL/Vitess/PlanetScale sources, especially long-lived streams against sources that can fill their binlog disk under load. If a resume ever stalls, the error now tells you whether to look at the source's disk, its overall health, or its binlog retention — rather than just that it was "unresponsive".

## Install

```
go install sluicesync.dev/sluice/cmd/sluice@v0.99.85
```

Container image:

```
docker pull ghcr.io/sluicesync/sluice:0.99.85
```
