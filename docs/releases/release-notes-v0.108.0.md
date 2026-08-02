# sluice v0.108.0

**Three critical silent-failure fixes from an independent audit — including one that let anyone with write access to your backup store execute arbitrary SQL on a restore target, with `backup verify --require-signature` reporting green.**

This is the second release in a two-part remediation. v0.107.0 fixed what a systematic sweep found; this one fixes what a six-agent blind audit found afterwards, on ground the sweep had already cleared. Every finding below was independently re-derived and verified at the code before it was fixed, and every fix ships with a gate that was watched to fail first.

If you take signed or encrypted backups, sync from a Postgres trigger-CDC source, or migrate into Postgres, there is something here you want.

## Fixed

**A signed backup's manifest signature covered zero bytes of its schema.** The signature folded only the *recorded* schema fingerprint — a string — so schema authenticity was transitive on a recomputation that is deliberately skipped for a bare full, which is the ordinary product of `backup full --encrypt --sign`. An adversary with write access to the backup store and **no key material, no database credentials and no source access** could rewrite the embedded schema, leave the recorded fingerprint untouched, and the signature still verified. Demonstrated, not theorised: a planted index access method survived the round trip with the canonical signed bytes unchanged and the MAC still valid.

The exposure was the whole schema, not one field, and the worst tier needed no injection at all — flipping the row-level-security flags meant the restored target came up with RLS silently off; dropped CHECK, EXCLUDE, foreign-key and uniqueness constraints meant integrity controls silently absent. The restore's own column check compares name *sets* only, so preserving the names left everything else free. And because Postgres DDL executes with no bound arguments, the driver uses the simple protocol there, which permits `;`-separated statements — so escaping one of the bare-identifier positions yielded arbitrary statement execution rather than confined corruption.

Manifests are now signed under a new canonicalization (`sluice-manifest-canon/v5`) that folds the schema's **bytes as written to disk**. Signatures written by every earlier version keep verifying byte-identically under their own recorded version, so no chain is re-signed and there is no migration. A cross-version gate — the only place in the test suite where two sluice binaries meet one backup store — confirms a chain written by an older release still restores here. See [ADR-0183](docs/adr/adr-0183-sign-the-manifest-schema-bytes.md).

**Stated plainly, because a reader should not have to infer it: an unsigned chain remains fully forgeable, on every shape.** The schema fingerprint is a keyless public hash, so an adversary edits the schema and recomputes it. Nothing in this release changes that and nothing can — integrity without a secret is not achievable. `--sign` is the answer.

**A Postgres trigger-CDC sync silently dropped committed changes whenever two transactions overlapped.** The cold-start anchor and the steady-state poll implemented two different correctness arguments, and only the anchor's was sound. The change-log id is assigned when a row is written; the transaction id is assigned at the transaction's first write. So an ordinary multi-statement transaction can commit a *higher* id while a *lower* id is still in flight — and the poll, which held back per row and then advanced its watermark to the highest id it had fetched, stepped straight over the lower one. It was never emitted again, on any poll or any restart, because the durable position was already past it. Exit 0, `sync status` green, one committed change gone.

The poll now consumes only the contiguous run of ids from its watermark, which is the anchor's argument restated: an id missing from the poll's own snapshot *is* evidence that its transaction has not committed. A hole left by a rolled-back transaction is skipped only once it is **proven** permanent, so an abort cannot wedge the stream and an in-flight transaction cannot be mistaken for one.

**Two source indexes whose names collapse to one Postgres name silently lost one of them.** sluice prefixes an index name with its table unless it already starts with that prefix — which is not injective, so a table's `user_id` index and its `posts_user_id` index both render `posts_user_id`. Combined with `CREATE INDEX IF NOT EXISTS`, the second build was a silent no-op. When the loser was the UNIQUE one, the target admitted duplicate rows the source's constraint forbade, permanently, at exit 0. Both are now refused up front, naming both source indexes, across tables as well as within one — because the transform can collapse a name across two different tables too.

**Postgres had no post-index-build verification, on the engine whose index build is the one that no-ops silently.** The verification surface exists precisely because a silently skipped index build is this project's worst failure mode, and its documentation said it runs for all targets that implement it — only MySQL did. Postgres now implements it: after the build, the catalog is read back and a missing index, or one that exists but is not unique where a unique index was requested, is a loud refusal. The engine list is now held to the code by a doc-sync gate rather than by prose.

**Postgres identifier-length validation covered index names only.** Sources with no identifier-length limit of their own — SQLite, D1 — can carry names longer than Postgres's 63 bytes. Two table names sharing their first 63 bytes meant the second `CREATE TABLE IF NOT EXISTS` truncated onto the first and silently did nothing, after which that table's rows were copied **into the first table**. Every emitter that names an object now validates, including synthesized enum type names, which are built from two names that individually fit.

**A column retyped mid-window was silently discarded by chain restore and the broker.** A pure type change produces a schema delta with no added columns, and both replay paths skipped exactly that shape — so a column widened at the source stayed narrow on the target, and Postgres **rounds on insert and returns success**, turning a skipped schema change into silently rounded data. Every delta shape now has a written verdict: applied, or refused, or proven not to need DDL. A new comparator cannot be added without one.

**`sluice backfill --verify` authorized dropping the old column after a backfill that never ran.** The completion check ran the same predicate as the backfill itself, so a `--where` that matched nothing reported zero rows remaining — indistinguishable from finishing — and that verdict is what authorizes the contract step that drops or renames the old column. Verification now requires evidence that work actually happened.

**`export-as-parquet` skipped the shared manifest-integrity preflights** on every chain shape, and **a rolled-back write no longer wedges a trigger-CDC stream**.

## Changed

**`sync start` against a Postgres trigger source now refuses a change-log sequence whose cache size is not 1.** The gap-freedom argument depends on it and nothing checked it; `BIGSERIAL` defaults to 1, so a correct source pays nothing.

## Compatibility

**New refusals, all replacing a silent wrong answer.** Colliding index names, over-length identifiers, a schema value that cannot be safely interpolated, a non-unit sequence cache, a backfill verify with no evidence, and an unresolvable schema delta. Each fires where sluice previously produced a wrong result quietly, so a run that starts failing was already failing — it just was not saying so.

**Signature compatibility is preserved in both directions.** Older signatures verify unchanged; a chain written by an older release restores here, confirmed by a cross-version test that builds both binaries. Note the existing rule still applies: a chain extended by a newer binary raises its format floor, and older releases can then no longer read it.

**One behaviour change if you script against exit codes.** The over-length-identifier refusal previously had no `SLUICE-E-*` code; it now carries one and exits 3.

## Who needs this

- **Anyone who signs backups** — and anyone who does not, who should read the unsigned-chain note above.
- **Postgres trigger-CDC sources** (`postgres-trigger`) under any concurrent write load.
- **Migrations into Postgres**, particularly from MySQL sources where index names are table-scoped, or from SQLite/D1 where identifiers can exceed 63 bytes.
- **Chain restores and `sync --from-backup`** where the source schema changes mid-window.
- **`expand-contract` / `backfill` users** relying on `--verify` before the contract step.
