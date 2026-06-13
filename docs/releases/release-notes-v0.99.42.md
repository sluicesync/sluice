# sluice v0.99.42

**One MySQL-CDC correctness fix — a source-side `TRUNCATE TABLE` carrying a *leading SQL comment* was silently not propagated to the target (Bug 140).** MySQL keeps a statement's leading comment in the binlog, the reader's truncate detection required the statement to start with `TRUNCATE`, so a commented truncate (a hand-written migration note, an APM/ORM query tag) was dropped — and on a live MySQL → {MySQL, Postgres} sync the target silently kept the rows the source truncated, with no error and no lag signal. Postgres sources were never affected. Drop-in from v0.99.41 — no flag, default, format, or invocation changes; no re-verification of any prior migration or backup needed.

## Fixed

- **MySQL CDC now applies a `TRUNCATE TABLE` even when the statement carries a leading SQL comment (Bug 140).** MySQL preserves a statement's leading comment verbatim in the binlog `QUERY_EVENT` (only the trailing delimiter is stripped). The reader's `parseTruncateTable` required the body to *start* with `TRUNCATE`, so a commented truncate — `-- clear staging\nTRUNCATE TABLE t`, or a query tag like `/* trace=abc */ TRUNCATE …` (sqlcommenter / ORM query-log tags / hand-written migrations) — returned "not a truncate", fell through to generic DDL handling (schema-cache invalidation only), and **never emitted a typed truncate event**. On a live MySQL-source continuous sync the target therefore **silently retained every row the source truncated**, the stream never converged, and nothing surfaced an error or lag — a HIGH silent-divergence class on a routine operation. The reader now strips leading SQL comments (`--`, `#`, `/* */`) and whitespace before recognising the command; executable comments (`/*! version-gated */`, `/*+ optimizer hint */`) are deliberately left in place — stripping them could discard conditionally-executed SQL, and a statement led by one simply falls through to generic DDL handling exactly as before (no typed event, but no incorrectness). Trailing comments remain out of scope (they fail the table-name parse into a *loud* apply-side error, not silent loss). **Found by** the deep sync-convergence property hunt (its harness renders every transaction with a `-- tx N` comment, which is exactly the real-world trigger); confirmed via instrumented binlog-event replay (the reader logged the truncate `QUERY_EVENT` with the comment attached and "parse = false"), and a minimal no-comment repro propagated cleanly, isolating the leading comment as the cause. **Affected releases:** every release with MySQL trigger-less binlog CDC — but only for truncates whose statement text carries a leading comment; a bare `TRUNCATE TABLE t` was always propagated correctly. Pinned by unit comment-variant cases (line / hash / block / stacked / CRLF / `--`-without-space non-comment / executable-comment pass-through) and a new MySQL-source integration test (`TestBug140_MySQLToMySQL_CommentedTruncatePropagates` — the only prior truncate integration test was Postgres-only).

## Compatibility

- **No breaking changes.** Drop-in from v0.99.41 — no flag, default, on-disk format, or invocation changes. `migrate`, `sync`, `backup`, and the Postgres CDC path are entirely unchanged. The only behavioral change is that a MySQL-source `TRUNCATE` with a leading comment now reaches the target (as a bare `TRUNCATE` always did).
- **Postgres sources unaffected.** pgoutput emits a typed truncate message; sluice never parsed a query string there, so PG → {PG, MySQL} truncate propagation was always correct.
- **No re-verification required.** This fix changes what a *future* commented truncate does; it does not alter any data already migrated. A target that diverged under the bug is corrected the moment the truncate is re-applied (or by re-running the sync) — but nothing about prior, non-truncate data was wrong.

## Who needs this — action required

- **Anyone running a live MySQL-source continuous sync (`sluice sync`, MySQL → MySQL or MySQL → Postgres) whose source issues `TRUNCATE TABLE` statements that may carry a leading comment** — i.e. truncates run through tools that prepend query tags (APM/sqlcommenter, ORM query-log tags) or hand-written migration scripts with comments. Before this release such a truncate left phantom rows on the target. Upgrade; the next commented truncate propagates. If you suspect a past commented truncate diverged a target, re-run the affected table's sync (or re-issue the truncate) to reconcile.
- **Everyone else: a routine upgrade.** A bare `TRUNCATE TABLE t` (no comment) was always handled correctly, and Postgres sources were never affected.

## Also in this release (internal / test-only)

- The random-op **sync-convergence property** now also covers the cross-engine directions (PG↔MySQL) with a value-semantic canonical compare, hardening the cross-engine CDC apply path against silent divergence. Building that out fixed two harness-side cross-engine canonicalisation edges. Test-only — no runtime change.
- A flaky AIMD-controller integration test was stabilised (dynamic metrics port). Test-only.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.42`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.42`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
