# sluice v0.99.181

**Fix: a MySQL-target migrate over the LOAD DATA fast path no longer hangs indefinitely when `NO_BACKSLASH_ESCAPES` is set on the target connection (Bug 178). The LOAD DATA statement's framing was spelled with backslash-escaped SQL literals that `NO_BACKSLASH_ESCAPES` mis-parses, so the server rejected the statement before the data transfer and the writer deadlocked — zero rows, no error, a wedged process. Liveness only, no silent data loss; latent since v0.10.0.**

## Fixed

**MySQL LOAD DATA bulk copy hung under `NO_BACKSLASH_ESCAPES` (Bug 178; liveness, zero silent-loss).** On the LOAD DATA LOCAL INFILE fast path (`local_infile=ON`, the default), a MySQL-target migrate could hang forever — rows streamed, then no completion, no error, zero rows committed — when `NO_BACKSLASH_ESCAPES` (NBE) was active on the target connection, set via `--mysql-sql-mode` or a DSN `sql_mode` param. The bulk writer spelled the LOAD DATA field/escape/line framing as backslash-escaped SQL string literals (`'\t'`, `'\\'`, `'\n'`), which the server parses under the session sql_mode; under NBE the `'\\'` literal is two backslash bytes, which trips MySQL's "ESCAPED BY must be a single character" check and aborts the statement with Error 1083 *before* it requests the infile stream. The driver therefore never invoked the writer's registered reader, nobody drained the internal pipe, and the row-encoder goroutine deadlocked on its pipe write — turning a fast statement error into an indefinite hang. The framing is now emitted as sql_mode-invariant hex literals (`X'09'`/`X'5C'`/`X'0A'` — the same bytes, parsed identically regardless of sql_mode), and the writer now closes its pipe reader before awaiting the encoder so any pre-transfer statement error surfaces loudly instead of deadlocking. The batched-INSERT path (`local_infile=OFF`) was never affected. Pinned by a real-MySQL integration test that runs the writer with NBE active across the escape-framing torture class (literal backslash, backslash adjacent to a field or line terminator, `\N`-looking and `\0`-looking values, a real NUL byte, a real NULL) under a hard deadline, so any regression re-hangs as a test failure rather than a hung process.

## Internal

**Pipeline decomposition, first slice: `internal/pipeline/blobcodec`.** The backup wire-format and storage leaf — chunk codec, fast-JSON chunk writer, change-chunk reader/writer, blob store, compression codec, and local-FS store (~3.3k LOC) — moved out of the flat `internal/pipeline` package into a new cycle-free sub-package that imports only the IR backup contract. This is a down-payment on the pipeline-flatness finding from the July repository audit; it is a pure move with byte-identical chunk/manifest/codec formats and an unchanged backup→restore round-trip (verified across the new package boundary by the existing local-FS and MinIO integration round-trips).

## Compatibility

**No breaking changes; drop-in.** No flags, formats, or on-disk layouts changed. Backups written by any version restore identically. The only behavior change is that a MySQL LOAD DATA copy which previously hung under `NO_BACKSLASH_ESCAPES` now completes normally.

## Who needs this — action required

- **No one must re-verify data.** Bug 178 was a liveness hang with zero silent-loss risk — it never reported success, so nothing incorrect was ever committed.
- **Anyone running MySQL-target migrates with `NO_BACKSLASH_ESCAPES`** (via `--mysql-sql-mode` or a DSN `sql_mode`): the copy that previously hung now succeeds on the default fast path — the `local_infile=OFF` workaround is no longer needed.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.181 · **Container:** ghcr.io/sluicesync/sluice:0.99.181
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
