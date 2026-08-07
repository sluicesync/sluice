# sluice v0.110.1

**One silent-loss Critical and its siblings, three more correctness fixes, a credential leak, and signed release artifacts.**

Everything here comes from a blind multi-agent audit of v0.110.0 — run specifically *because* that release was itself audit remediation, and the same author checking their own work is exactly when an independent pass earns its cost. It found one Critical. The shape behind most of the rest was a sibling sweep that stopped at the surface a finding named rather than the class it belonged to.

## Fixed

**A `--where` filter on an `inet` or `cidr` column could silently drop every CDC change to the rows it matched.**

sluice decided whether an operator's literal was "the spelling the change stream delivers" by rendering it with Go's `net/netip`. That answers a subtly different question — whether the literal is canonical as a *Go* value — and where the two disagree the predicate compiled cleanly and matched nothing. The cold-start copy still landed the rows, because that leg is evaluated by the server; every subsequent insert, update and delete to them was then dropped at exit 0, with `sync status` green. The filtered subset froze permanently.

Both Postgres and MariaDB render IPv6 through the BSD `inet_ntop6` convention, which prints the trailing 32 bits as a dotted quad when the leading 96 bits are zero. netip does that only for IPv4-*mapped* addresses. So the server delivers `::1.2.3.4` and sluice was writing `::102:304`.

The two servers do not agree with each other either: MariaDB additionally compresses a *single* zero hextet, which RFC 5952 forbids and Postgres obeys. So the rendering is chosen per engine, not once for all of them.

Literals are now rendered the way the source engine renders them. This affects `--where` on network columns only — a sync without a row filter, and every other column type, were never involved. An earlier draft of this note also named `macaddr`; that was wrong. MAC literals never went through netip, so this fix could not have reached them — the separate MAC defect is two entries down.

**That same MariaDB rendering defect could also diverge a plain cold copy from its own CDC tail, with no `--where` anywhere.**

`inet6` values land in a `VARCHAR` on a MySQL-family target, and sluice's binlog decoder rendered IPv6 to the RFC rule while MariaDB compresses that single zero hextet. So a cold copy wrote the server's spelling (`2001:db8::1:2:3:4:5`) and a later CDC `UPDATE` of the same row wrote the decoder's (`2001:db8:0:1:2:3:4:5`). The target held a different string for the same address depending on whether the row had ever been updated, at exit 0.

The decoder now matches the server, verified against MariaDB 10.11, 11.4, 11.8 and 12.3. The test that claimed to have verified this "byte-exact across the family × shape matrix" contained no address whose longest zero run was one, so it could not have detected it.

**Two `--where` literal shapes compiled cleanly and could never match anything.**

A zone-scoped IPv6 address (`fe80::1%eth0`) and a MAC literal wider than six bytes (`08:00:2b:01:02:03:04:05`) both render back to themselves, so they passed the canonical-spelling gate. Neither server can store a zone at all, and a Postgres `macaddr` holds six bytes, so in both cases no stored value could ever equal the literal. Both are now refused at sync-start.

> **Correction (2026-08-04, post-publish).** This section first said these two shapes "matched no row for the life of the stream — the same silent-drop shape as the rendering bug". That is **wrong on a Postgres source**, and it is the one claim in these notes written from the shape of the fix rather than from the path a run actually takes. sluice pushes the raw `--where` predicate into the snapshot `SELECT`, so Postgres rejects the literal itself and the run fails **loudly, mid-copy**; the warm-resume path where that silence could have lived is already closed by `SLUICE-E-WHERE-PUSHDOWN-DRIFT`. The fix is still worth having — the refusal now arrives at sync-start and names the cause, instead of surfacing as a driver error partway through a copy — but it is **earlier and more actionable, not loud instead of silent**. Found by the post-release regression cycle.

**The MAC refusal is deliberately wider than the defect, and one related case stays open.** sluice reads `macaddr` and `macaddr8` into the same IR type and cannot tell them apart, so an eight-byte literal is refused even on a `macaddr8` column, where it would have worked. The converse — a six-byte literal on a `macaddr8` column, which Postgres widens to EUI-64 on input and therefore never delivers back — is *still* silently wrong and is not fixed here; closing it needs the IR to carry the width. If that is your shape, filter on a different column.

**A partial `UNIQUE` index could be silently widened on a Postgres target, and the real index never built.** For a table with no primary key, sluice inlines a non-null unique index into `CREATE TABLE` so the cold-start copy has a conflict target. The selection never considered whether the index was partial, and Postgres cannot put a `WHERE` on a table constraint — so the constraint became unconditional, making the target stricter than the source, *and* the index was then skipped by the build phase, so the source's partial index existed on the target in no form at all.

**A MySQL index prefix length was silently dropped against a SQLite or D1 target.** `UNIQUE KEY (email(20))` forbids two rows whose first 20 characters match; the emitted `UNIQUE INDEX (email)` forbids only exact duplicates, so the target silently accepted data the source rejects. Now refused, naming the rewrite that reproduces the semantics: `substr(email, 1, 20)`, since SQLite indexes may be built over expressions. Non-unique prefixed indexes are unchanged and still carried with a warning.

> **Correction (2026-08-04, post-publish).** This section first said "refused **before any data moves**". It is not. SQLite's check lives only in `emitCreateIndex`, which runs in the deferred index phase *after* the bulk copy. The refusal is still loud and zero-loss — nothing is written wrong, and the migration stops rather than producing a weaker constraint — but it arrives later than stated, so a large table is copied first. `--upfront-indexes` moves it earlier today. This is the same timing the v0.110.0 regression cycle reported for the partial-`UNIQUE` refusal, and it was found by looking for siblings once that bug named the shape; a preflight that closes the whole class is filed as roadmap item 118.

> **Correction (2026-08-04, later).** "or D1" over-claims a target that does not exist. sluice has **no D1 target engine** — `d1` is a migrate SOURCE only (`OpenSchemaWriter` returns `ErrD1NotImplemented`), and a D1-bound migration is written through the `sqlite` engine into a file you then import with `wrangler`. Everyone this fix reaches is still reached; what is wrong is that the sentence names an engine you cannot select as a target. Found by the pre-release value-fidelity review for v0.111.0.

**Two cold-copy write lanes ignored the coordinated storage-grow pause.** The Postgres raw-COPY fast path and the float-repair core on both engines were invisible to it, so they kept writing into a grow window every other lane was backing off from — which made the raw fast path *less* resilient than the lane it replaces, not merely unoptimised. Both now participate. The raw path signals its siblings but cannot retry, and says so: it streams its source bytes, so there is no resume point.

**Blob-store credentials survived redaction into a durable, replicated location.** The redactor stripped a URL's query string and left its userinfo intact. What made it silent is that embedding credentials there *works* — the cloud drivers read the host and ignore the userinfo — so an operator got a working backup and no signal. The redacted value reaches a log line, the CDC-state row on the **target database**, and a diagnose bundle collected at the privacy level whose help text promises no DSN.

## Changed

**Release artifacts now carry build provenance.** Every archive, package and `checksums.txt` is attested via GitHub artifact attestations:

```
gh attestation verify sluice_0.110.1_Linux_x86_64.tar.gz --repo sluicesync/sluice
```

Previously the integrity chain ended at an unsigned `checksums.txt`. This is additive and deliberately *not* a publish gate — a release whose attestation step failed still ships working, checksummed binaries.

**`sluice sync run` now says at startup that it will restart a failing sync forever.** That has always been the default (`--max-consecutive-failures` is 0), but nothing said so, and the supervisor's own documentation claimed the opposite. A sync that can never start — an unreachable target, a bad DSN — retries behind backoff while the process looks healthy to systemd, an orchestrator or CI. The behaviour is unchanged; it is now stated, and the escape hatch is named.

## Compatibility

**One behaviour change that can stop a migration that previously ran:** a MySQL index prefix length on a uniqueness-enforcing key is now refused against a SQLite target (see the correction above: there is no D1 target engine). This is the same refusal Postgres targets have had since v0.108.0, finally applied to the engine its original sweep missed.

If you use `--where` on a network column, **check your predicate against what the server actually returns** — `SELECT col FROM t LIMIT 1`, uncast. A predicate that was silently matching nothing will now either work or be refused with a message naming the correct spelling.

No flags were added, renamed or removed. No error codes changed. Backup chain formats and schema fingerprints are unchanged.

## Who needs this

- **Anyone running a filtered sync (`--where`) on an `inet` or `cidr` column.** Upgrade, and verify the target actually received the changes you expect — a predicate on a diverging address has been matching nothing.
- **Anyone syncing a MariaDB source with native `inet6` columns into a MySQL-family target, with or without `--where`.** This is the one entry here that needs no row filter to reach you. Check whether the same address is spelled two ways on the target: `SELECT DISTINCT <col> FROM t` where you expect one value. Rows that were never updated after the cold copy carry the correct spelling; rows that were updated carry the wrong one.
- **Anyone using `--where` on a `macaddr` column, or on an address with an IPv6 zone.** On a Postgres source these already failed loudly mid-copy (see the correction above); what changes is that the refusal now arrives at sync-start and names the cause. Note the `macaddr8` limitation above.
- **Anyone migrating to SQLite from a MySQL source whose schema uses index prefix lengths** — including a `.db` bound for D1; see the correction above, there is no D1 target engine. You will now get a refusal naming the rewrite instead of a silently weaker constraint.
- **Anyone migrating to Postgres with PK-less tables carrying partial unique indexes.**
- **Anyone who has put credentials in a `--backup-target` URL.** They have been written to your target database's CDC-state row and to any diagnose bundle taken since. Rotate them, and prefer the environment for cloud credentials.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`.
