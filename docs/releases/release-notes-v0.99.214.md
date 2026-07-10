# sluice v0.99.214

**Security hardening + a silent-loss fix, from a fresh full-codebase audit. Signed backup manifests now bind every row chunk to its parent table — closing a cross-table corruption a store-write adversary could otherwise slip past a valid signature — and a single-precision FLOAT in a primary key no longer silently defeats the VStream exact-FLOAT repair (or slips a rounded value past `--strict-float`). Signing customers should read Compatibility: new signed *encrypted* backups are stamped a new format version and need v0.99.214+ to restore; every existing backup verifies and restores byte-identically.**

## Security

- **Signed manifests bind row chunks to their parent table (audit SEC-F1; canonicalization v4 + backup FormatVersion 7).** Before this release the manifest signature flattened every table's row chunks into one globally file-sorted list with no parent-table token, and the encrypted-chunk GCM AAD bound only `(manifest identity, chunk path)`. Swapping the row-chunk lists of two tables with the SAME column set — same-schema shards, multi-tenant clones, an `orders_2023`/`orders_2024` pair — produced byte-identical signed bytes and decrypted cleanly, so both the manifest and lineage signatures verified GREEN and table B's rows restored into table A: silent cross-table corruption surviving the exact tamper-evidence signing ships to provide. (Change chunks bind a replay ordinal and schema deltas bind the table already; row chunks were the gap. Table *renames* were always caught — the name is a signed token — but chunk *reassignment* between existing same-column-set tables was not.) The fix binds the parent on both layers, each independently versioned and fail-closed: the signature canonicalization (v3→v4) folds each row chunk's `(schema, name)` into its signed token, and a signed encrypted backup's row-chunk GCM AAD (FormatVersion 7) appends `\nschema=…\ntable=…` so a ciphertext moved between tables fails its GCM tag even if the signature were absent. The dual-version verifier is unchanged: every v2/v3 signature any prior release wrote still verifies byte-for-byte, and a v4 signature relabeled down to v3 to strip the parent tokens fails the MAC — the path is not a downgrade oracle. Validated end-to-end on real containers across HMAC-off-KEK, Ed25519, and AWS KMS (localstack) signing: the signed-encrypted DR round-trip, the chunk-reassignment refusal, and the whole-manifest-rollback and signed→v5-downgrade tamper cases all behave correctly. Companion hardening (audit M0.4): a tampered manifest with a null table/chunk entry now surfaces the coded `SLUICE-E-BACKUP-SIGNATURE-INVALID` instead of panicking.

## Fixed

- **A single-precision FLOAT in a PRIMARY KEY no longer silently defeats the VStream FLOAT repair, and `--strict-float` refuses it upfront instead of exiting 0 with a rounded archive (audit SL-F1; silent data-loss class).** A PlanetScale/Vitess VStream cold-start COPY renders single-precision FLOAT display-rounded (`8388608` → `8388610`); sluice repairs it by re-reading the column exactly (`(col * 1E0)`) and matching rows by primary key. When the FLOAT is itself a PK member, the re-read scans it exactly while the COPY wrote it rounded, so the PK-keyed match never lands — a table like `PRIMARY KEY (id, f)` with a non-PK FLOAT `g` had `g` silently keep its rounding, and `backup full --strict-float` (whose contract is "exact, or fail") exited 0 with the rounded value. Such a table is now classified non-repairable on both the sync and backup paths through one shared predicate, routing it to the honest "cannot be repaired" warning and, under `--strict-float`, an upfront coded refusal. Tables with no FLOAT in the primary key are unaffected and still repaired exactly.

- **Backup blob-store URL parse errors no longer echo the raw `--backup-target` query string (audit SEC′-1).** The two `url.Parse`-failure sites now redact through a query-stripping helper hardened to survive an unparseable input. No registered gocloud driver carries secrets in the query string today; this is least-information hygiene in error lines.

## Changed

- **`google.golang.org/protobuf` is now a direct dependency** (imported directly by the GCP KMS signer — a `go mod tidy` fix, no functional change), and the MySQL/Postgres FLOAT-repair writers carry compile-time interface pins so a signature drift becomes a build break, not a silently-skipped repair.

## Compatibility

- **Signing customers, read this:** a signed **encrypted** backup written by v0.99.214+ is stamped manifest **FormatVersion 7** and requires v0.99.214+ to restore. Following the project's proportional-versioning discipline, an older sluice refuses a v7 manifest **loudly at preflight** — it never silently mis-restores it. FormatVersion 7 is stamped **only** on a signed encrypted full; unsigned encrypted backups stay on FormatVersion 5, and plaintext-signed backups stay on FormatVersion 6, so neither is affected. A resumed pre-v7 encrypted run keeps its prior version.
- **Every existing backup verifies and restores byte-identically.** Manifest signatures are now produced at canonicalization v4, and the dual-version verifier authenticates every v2 (Phase 1) and v3 (Phases 2–3b) signature any prior release wrote, unchanged. A v4 signature presented to an older binary refuses as a version gap ("upgrade sluice"), never as tamper.
- **No migrate, sync, plaintext-backup, or unsigned-backup behavior changes.** The FLOAT-in-PK fix changes only whether such a table is repaired-or-refused versus silently rounded; no other table's handling changes.

## Who needs this — action required

- **If you sign encrypted backups** (`--sign` with a `--sign-key`): upgrade to gain the cross-table binding, and note that new signed-encrypted backups need v0.99.214+ to restore (older sluice refuses them loudly, never mis-restores). Nothing to change in your workflow.
- **If you back up a PlanetScale/Vitess source that has a single-precision FLOAT in a primary key** and rely on exact FLOAT values or use `--strict-float`: this fixes a silent rounding path — such a table is now refused/warned instead of archived with a rounded (and unmatchable) value.
- **Everyone else: no action.** Plaintext backups, unsigned backups, migrate, and sync are unchanged.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.214 · **Container:** ghcr.io/sluicesync/sluice:0.99.214
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
