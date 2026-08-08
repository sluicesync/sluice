# sluice v0.118.0

**The 2026-08-05 audit's silent-loss cluster, closed.** A MySQL target could be handed a row that never existed at the source, at exit code 0. Per-row spatial SRIDs were dropped on both engines. Both trigger engines silently adopted a pre-existing user table sitting at their bookkeeping name. And six error classifiers decided what had failed by matching English prose — four of which swallowed the error they misread.

The framing worth repeating from v0.117.0, because this release is the same programme: the goal is that every command works the way an operator expects on whatever database and dataset they point it at, and `sync` gets the same scrutiny `migrate` does. Everything below was found by auditing, not by a user hitting it — and each one was reproduced on a real server, with the target's own value as the oracle, before anything was changed.

## Fixed

**A MySQL target could receive a row the source never had (audit C-9 + C-10).** These were filed as two findings and are one defect. ADR-0140 rewrites a keyed, non-PK-changing `UPDATE` into `INSERT(after-image) … ON DUPLICATE KEY UPDATE`, which is sound only while the after-image is a COMPLETE row — the upsert's INSERT branch writes a whole one. It is not complete on the load-bearing cross-engine path: pgoutput sends an unchanged out-of-line TOASTed column as the `u` datum and the Postgres reader drops it deliberately, because an absent key means "preserve the target's existing value". An `UPDATE` honours that. An upsert honours it only while the target row EXISTS — and a CDC target row can be absent (an out-of-band delete, a resnapshot gap, a partial restore), which is precisely the drift ADR-0140 claims the upsert form corrects. **Measured pre-fix against a real MySQL: an absent target row came back as a fabricated default — a row that never existed at the source, exit 0.** Whether it was even visible depended on the target column: `NOT NULL` without a default failed the batch with Error 1364, a nullable column took it silently. **The divergence C-9 named is not between the two ENGINES; it is between MySQL's coalescing batch path and everything else.** A nil-Before `UPDATE` was silently upserted on MySQL/batched, while MySQL/serial, PG/serial and PG/pipelined each emitted an empty `WHERE` and let the server answer 1064 / 42601 — loud, but naming neither the stream nor the row. **The cost of the fix is throughput, not correctness:** a table whose `UPDATE`s carry partial after-images loses ADR-0140 coalescing and returns to the rate the serial path always had, announced once per table at INFO.

**A geometry value's own SRID was silently dropped on both engines (audit C-14).** One sentence, repeated verbatim in three decoders: per-row SRID is intentionally dropped because SRID is a per-COLUMN property. True of sluice's IR, false of both engines. PostGIS's unconstrained `geometry` type accepts a different SRID per row and reports srid 0 for the column; MySQL's spatial types do the same without the `SRID n` column attribute. So an unconstrained column holding rows written at 4326 decoded to bare WKB, was re-framed with the column's 0, and landed as a coordinate pair that no longer names a place. Valid geometry, wrong geometry, exit 0. Carrying the per-row value faithfully means changing the IR's geometry VALUE contract from WKB to EWKB — every decoder, both writers, the mydumper literal path, the VStream cell decoder and the backup change codec each read and write it — so **the honest answer here is the loud one: refuse the row, name both SRIDs, and name the remedy**, which is to declare the SRID on the COLUMN, where sluice does carry it end to end. The sweep found two more: a `DOMAIN` over `geometry(POINT,4326)` read back as SRID 0, and the mydumper lexer dropped the `SRID` attribute entirely — **where the fixture DDL carried the attribute while the expectation beside it asserted SRID 0, so the dump leg and the live leg agreed on the same wrong answer.**

**Both trigger engines silently ADOPTED a user table sitting at their own bookkeeping name (item 149b).** `sqlite-trigger`, `d1-trigger` and `pgtrigger` create their change log with `CREATE TABLE IF NOT EXISTS`, so a user table already carrying that name was not refused: setup returned success, the capture triggers went in, and the first captured change failed on a missing column — **on the operator's own write path, after the triggers were installed.** The honest check is a SHAPE probe, because existence proves nothing: a legitimate re-`setup` finds the table there every time. Any relation sitting at one of the engine's own names is now graded and refused if its column set is not sluice's, BEFORE any DDL is applied — including under `--dry-run`, which opens read-only. All three transports are covered directly rather than by inference. **The over-refusal direction is pinned harder than the refusal**: a healthy re-setup stays silent, a superset change log from a hypothetical newer release is accepted, and an install written by the ORIGINAL release is accepted — its fixture being that release's DDL verbatim, including two indexes later dropped.

**Six error classifiers decided what had failed by matching English prose, and four of them swallowed the error they misread (audit C-1).** The class is not that text matching is inelegant. It is that the text is unstable and ambiguous, and that the sites which classify errors are disproportionately the sites that swallow them. "does not exist" is PostgreSQL's house phrasing for a whole family of unrelated conditions, and the one that matters is SQLSTATE **3D000** — a vanished DATABASE, which a pooled connection pool surfaces from any statement after a re-dial. Every site read it as its own benign case: the CDC applier's truncate no-op logged a stale-publication skip and **advanced the stream past a `TRUNCATE` that never ran**; a failed `pg_drop_replication_slot` was read as "already gone", so **the slot survived and kept pinning WAL on the source**; `sluice sync decommission` swallowed the same shape into "already absent" **and then cleared the control row — the only record of the slot NAME**, leaving an operator a WAL leak and nothing to find it by. Also in the sweep: the missing-table probe gating the populated-target refusal on both engines, where **both failure directions were real on stock containers** — a PG target whose database had gone away read as an EMPTY target, which is the answer that bypasses the refusal, and a stock `mysql:8` under a French `lc_messages` returned a translated 1146, so the probe errored and refused a migration whose target table merely did not exist yet. **Every operator-facing message is unchanged.**

**`sluice schema diff` was unusable across PG↔MySQL in BOTH directions, on targets `migrate` itself had just created (Bug 234, item 158).** PG→MySQL reported one missing and one extra index, and exit 1, on a table as simple as `(id INT PRIMARY KEY, v TEXT)`: the comparison keyed indexes by NAME, and PostgreSQL calls the key `plain_pkey` while MySQL calls it `PRIMARY`, so a name-keyed match could never succeed. The primary key is now matched by ROLE, which is structural and therefore reaches **all six ordered engine pairs at once**. MySQL→PG reported phantom column-TYPE drift on sixteen of thirty-seven columns. **That rule table was MEASURED rather than mirrored** — "what does a PG `text` become on MySQL" is a different question from "what does a MySQL `TEXT` read back as on PG" — by migrating a fixture carrying every arm of MySQL's type translation onto a real PostgreSQL 16 and diffing the read-back against the source IR. **Row-level security is no longer reported as permanent drift on a MySQL target**, where no target-side action could ever have closed it. Standalone sequences and `EXCLUDE` constraints are deliberately still compared: suppress where `migrate` warns and DROPS, keep reporting where `migrate` REFUSES.

**A refusal's hint told operators to do the exact thing the refusal's own body rejects (Bug 235).** Filed by the v0.117.0 regression cycle, and generalised into a gate holding the property that a hint must not prescribe what its own body denies.

**The audit's invariant queue is closed — 14 of 14 nameable claims, and 11 of the 14 verdicts differed from how they were filed.** That ratio is the argument for re-running the sweep rather than trusting the filings. The sharpest of the three defects hiding behind the last four claims was **ADR-0075's exported-snapshot premise**: true of PostgreSQL, but a spanning snapshot only yields a spanning COPY if the reader qualifies by each table's own schema — and the readers minted for the ADR-0079 parallel cold start do not. The pipeline's counter-argument ("holds by construction today") had nothing asserting it. **Mutation-proved on PG 16: without the new refusal, one tenant's table returned another's row.**

## Compatibility

No flags changed and no on-disk format changed. Existing backups, chains and resume state are read exactly as before.

Two behaviour changes are worth knowing about, both in the loud direction:

- **A CDC `UPDATE` whose after-image is partial no longer coalesces on MySQL.** Such tables fall back to the serial apply path — correct, and slower than ADR-0140's batched form. It is announced once per table at INFO so the slowdown is never a mystery.
- **A geometry row whose SRID differs from its column's declared SRID is now refused rather than re-stamped.** If you rely on an unconstrained `geometry` / spatial column holding mixed SRIDs, declare the SRID on the column (`geometry(Point,4326)`, `POINT SRID 4326`) — that is the shape sluice carries end to end. The error names both SRIDs and the remedy.

## Who needs this

- **Anyone running `sluice sync` PostgreSQL→MySQL on tables with large out-of-line (TOASTed) columns. Upgrade.** This is the configuration where a partial after-image met the upsert rewrite, and where a row that never existed at the source could be written to your target silently.
- **Anyone syncing or migrating spatial data.** If your geometry columns declare their SRID, nothing changes. If they do not, this release will tell you loudly instead of quietly landing the wrong coordinates.
- **Anyone who ran `sluice trigger setup` against a source that already had a table at sluice's change-log name.** Setup now refuses before installing triggers rather than after.
- **Anyone who has run `sluice sync decommission` and seen a slot reported "already absent".** Worth confirming with `sluice slot list` that it really is: the report could previously say absent for a slot that survived and kept pinning WAL.
- **Anyone using `sluice schema diff` across PostgreSQL and MySQL.** It was reporting drift that was not there, in both directions, on targets sluice itself created.
- **Everyone else: nothing to do.** A run with no partial after-images, no mixed-SRID spatial columns, no pre-existing change-log table and no cross-engine `schema diff` is unaffected.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.118.0_Linux_x86_64.tar.gz --repo sluicesync/sluice
gh attestation verify oci://ghcr.io/sluicesync/sluice:0.118.0 --repo sluicesync/sluice
```

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.118.0 · **Container:** ghcr.io/sluicesync/sluice:0.118.0

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
