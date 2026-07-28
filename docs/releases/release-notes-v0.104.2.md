# sluice v0.104.2

Three fixes for defects that each stayed hidden the same way: an operation reported success, and the damage only surfaced somewhere the operator had no reason to look — in their own application, or at the next restore.

### Fixed

**Stopping a Postgres cold start at the wrong moment no longer throws away the copy you just paid for.** Between "the bulk copy has committed on the target" and "CDC has started", sluice writes the CDC anchor — the record that lets the stream resume from where the copy ended. That write rode the caller's cancellable context, so a Ctrl-C or a SIGTERM arriving in that window failed it with `context canceled`, and the error path dropped the freshly-created replication slot and publication. The result was a target holding every row but no resume position: warm resume had nothing to resume from and a fresh cold start refused on a populated target, leaving `--reset-target-data` and a full re-copy as the only way forward. The window is as long as the anchor write takes, which on a loaded or distant target is seconds rather than microseconds. The anchor write now completes even when the run is being cancelled, on the same uncancellable-with-timeout footing sluice already gives the snapshot teardown's commit and the stop-drain flush — the reason you are stopping must not be the reason the durability record is missing. A genuine anchor-write failure still abandons the stream, since without an anchor the slot really would be orphaned WAL.

This was found by root-causing a test that had been dismissed as flaky and "fixed" twice by raising its timeout. The test was not waiting on a slow walsender at all — it was waiting for a slot that had already been deleted, and its wait loop could not tell "absent" from "still held". That is why it always consumed its entire budget and why no timeout would ever have been long enough. The same test now passes in 1.53s where it had been failing at 181.25s.

**Starting a Postgres sync no longer breaks your own application's writes on a table sluice can't actually replicate.** Postgres refuses to use a `DEFERRABLE` primary key's index as a replica identity, so a table whose only key is one has no usable replica identity. sluice added it to the publication anyway, and from that moment Postgres rejected the **source application's own** `UPDATE` and `DELETE` on that table — `cannot update table "…" because it does not have a replica identity and publishes updates`. `INSERT` kept working, so the table looked healthy until the first update, and the failure surfaced in the application rather than anywhere near sluice, even though sluice's publication was what triggered it. sluice now checks every in-scope table's replica identity **before** creating or extending the publication and refuses with `SLUICE-E-SOURCE-REPLICA-IDENTITY`, naming every affected table and the remedies. `sluice schema add-table` gets the same guard, since it is the other door into the publication.

Worth knowing if you reason about this yourself: a deferrable primary key *plus* a perfectly good immediate unique index is **still** refused, because `REPLICA IDENTITY DEFAULT` resolves to the primary key and nothing else — the extra index is not consulted. The fix is to point Postgres at it explicitly with `ALTER TABLE … REPLICA IDENTITY USING INDEX <index>`, which the refusal names. This is the opposite of the target-side rule for the same catalog bit, where an immediate unique index *is* a usable `ON CONFLICT` arbiter; the asymmetry is Postgres's, not sluice's.

**`backup compact` on an encrypted chain no longer reports success over a chain it has made unrestorable.** Compaction merges segments and then deletes the superseded files, and it treated the chain-root `manifest.json` as one of them — reasonable for a plaintext chain, where the merged segment holds an identical copy, and wrong for an encrypted one. For a passphrase-encrypted chain that file is where the restore side reads the Argon2id salt to re-derive your key; without it sluice silently falls back to a fresh salt, builds the wrong key, and fails at `unwrap chain cek` as though the passphrase were wrong. The compaction exited 0 and logged success, so the damage only surfaced at the next restore — which for a backup tool means during an actual recovery. The root manifest is now kept unconditionally, and compaction proves the chain still reads both before it swaps the catalog and after it deletes anything: a pre-swap refusal has removed nothing, and the post-sweep check exists so a run can never again report success over a chain it just broke.

**If you have already compacted an encrypted chain with an earlier version, that chain is currently unrestorable and can be repaired without a re-backup.** Compaction copies the root manifest into the merged segment before deleting the original, so `cp <chain>/seg-merged-*/manifest.json <chain>/manifest.json` restores the chain's identity byte-exactly, in both `per-chain` and `per-chunk` modes. From this version on, compaction refuses such a chain up front and names that recovery in the error rather than proceeding.

### Changed

**A Postgres source table with no key and no `REPLICA IDENTITY FULL` is now refused at sync start, where it previously synced.** This is the same defect as the deferrable-key refusal above and the same Postgres rule — such a table has no replica identity, so your own `UPDATE`/`DELETE` on it are rejected once sluice publishes it — but it is the one part of this release that can stop a sync that was working. If yours is an append-only table you never update, it really was replicating fine, and `ALTER TABLE … REPLICA IDENTITY FULL` is an instant metadata-only change that costs nothing on a table that takes no updates. `--exclude-table` is the other escape, and the refusal names both. The refusal is deliberate rather than a warning: the alternative is leaving a table in the publication whose first `UPDATE` fails inside your application, with nothing pointing back at sluice.

**Compaction now refuses a passphrase-encrypted chain whose root manifest is already missing.** If a chain was compacted or pruned past its root by an earlier version, it is already unrestorable, and compaction will now say so up front and name the `cp` recovery rather than merging further into a chain nobody can read.

## Who needs this

- **Anyone who has run `backup compact` on an encrypted chain** — that chain is very likely unrestorable right now, and it exited 0 when you ran it, so nothing told you. It is repairable without a re-backup: `cp <chain>/seg-merged-*/manifest.json <chain>/manifest.json`. Check before you need it, not during a recovery.
- **Anyone syncing from Postgres** — read the Changed section before upgrading. Two new refusals fire at sync start, and one of them can stop a sync that works today.
- **Anyone running `backup stream` or `sync start` under systemd, Kubernetes, or any supervisor** — a stop during startup no longer discards a completed copy and forces a full re-copy.
- **Anyone running `backup prune` on encrypted chains** — the same root-manifest defect exists there and is *not* fixed in this release. It is tracked as roadmap item 95; until it lands, take the same care after pruning a rotated multi-segment chain, and know that the same `cp` recovery applies.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.104.2
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.104.2`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
