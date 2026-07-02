# sluice v0.99.170

**A cold-copy `migrate` into a non-Metal PlanetScale-MySQL target now rides a storage-grow reparent that surfaces as `Error 2013 … Canceled desc = EOF`, instead of aborting loudly mid-copy — closing a reparent-retry classifier gap found during live reparent testing.**

## Fixed

**errno 2013 / 2006 (connection-lost) are now reparent-retriable on the cold copy.** When a non-Metal PlanetScale-MySQL target auto-grows its storage volume during a high-parallelism bulk copy, the primary reparent can drop the in-flight connection, and vtgate surfaces it as a MySQL `errno 2013` (CR_SERVER_LOST) packet carrying `vttablet: rpc error: code = Canceled desc = EOF`. sluice's error classifier treated that as terminal and aborted the migrate (`rc=1`) mid-copy — because the `desc = EOF` is text inside the error message (not the `io.EOF` sentinel `errors.Is` looks for), the error number is 2013 (so the vttablet `1105` gRPC-code branch never runs), and the reparent text-fallback ("not serving" / "reparent") didn't match that wording.

errno `2013` (CR_SERVER_LOST) and its sibling `2006` (CR_SERVER_GONE_ERROR) are transport-loss shapes — the same class as `driver.ErrBadConn` / `io.EOF` / "connection reset by peer", all already retriable — and the cold-copy reparent-retry (ADR-0108) re-acquires a fresh connection on the next attempt, so retrying is exactly the right recovery. They now classify **retriable**, keyed on the structured error *number* so the change is orthogonal to the deliberate bare-`code = Canceled` client-cancel exclusion (v0.99.94, a `1105` message-text branch that is untouched): a real client cancel surfaces as `context.Canceled` or `1105 … desc = context canceled`, never as errno 2013, so a clean shutdown still fails terminally.

The classifier is shared, so the same fix also covers the ADR-0109 source-read reconnect and ADR-0114 DDL-phase retry paths. Pinned by `TestClassifyApplierError_ConnectionLostErrno2013`, which also asserts the still-terminal shapes (`context.Canceled`, `1105` bare-cancel) stay terminal.

## Compatibility

No behavior change to any happy path or to any other error class. This only converts one specific **loud abort** (`rc=1` mid-copy, when a reparent dropped the connection as errno 2013) into a **bounded retry** that rides the reparent and continues. It stays loud-safe either way: there was never silent loss (the pre-fix path aborted loudly), and the retry is bounded by ADR-0108's ~30-minute wall-clock — a tablet that genuinely never recovers still fails loudly after the envelope.

## Who needs this

Operators running `sluice migrate` / `restore` into a **non-Metal PlanetScale-MySQL** target large enough to cross a storage auto-grow (around the 12 GB / 39 GB / 62 GB volume boundaries) at a high `--bulk-parallelism`, where the grow's primary reparent can drop the in-flight connection as errno 2013. Everyone else is unaffected.

This pairs with the ADR-0141 migrate reparent-reconciliation, which was **live-validated end-to-end** in the same cycle: a migrate crossed a real storage grow (volume 12 → 65 GB), rode 136 reparent signals to completion, the reconcile re-derived the reparent-touched table from source, and the final row count matched the source **exactly**.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.170 · **Container:** ghcr.io/sluicesync/sluice:0.99.170
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
