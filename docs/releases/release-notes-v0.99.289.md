# sluice v0.99.289

**Retry + guard hardening — closing the windows the last release's fixes still left open, including one silent-divergence window in the Postgres publication scope guard.** The ADR-0175 scope-conflict guard now protects stopped streams, not just running ones; a managed-Postgres maintenance restart or standby promotion under a `postgres-trigger` source now reconnects instead of exiting; and the v0.99.288 connect-phase retry now actually engages on Windows and for a severed pool connection at a checkpoint boundary. Two of the three fixes were found by sluice's own post-release regression cycle and tag CI within hours of v0.99.288 publishing. Drop-in upgrade, no breaking changes.

## Fixed

**The publication scope-conflict guard now keys on slot EXISTENCE, not activity — closing ADR-0175's documented residual window (affected: v0.99.287–v0.99.288).** As shipped in v0.99.287, the guard refused a narrowing publication rescope only while another *active* `sluice_%` replication slot existed — so a stream stopped mid-migration, crashed, or between retry attempts held an inactive slot and a resumable position, and a cold start timed inside that window could still silently de-scope it: the stopped stream would resume, advance normally, and receive nothing for its tables. A slot's existence is the durable, source-side claim that a stream holds a scope and intends to resume, so the guard now refuses against any other `sluice_%` slot, active or not; the refusal labels each conflicting slot (active vs inactive) and names all three escapes — `--publication-name` per stream, drain the other stream (`sluice sync stop --wait`), or drop the slot if the stream is truly abandoned. The deliberate cost is a conservative refusal against genuinely abandoned slots, which pin WAL and deserve attention anyway. Pinned by `TestPublicationScope_InactiveSlotStillConflicts` in the ADR-0175 integration gate: an inactive slot alone must refuse, and the refused attempt must leave `pg_publication_rel` untouched. Releases before v0.99.287 carried the broader unguarded class this guard was built for — see the v0.99.287 notes.

**A Postgres server restart or standby promotion under a `postgres-trigger` source now reconnects instead of exiting the sync (affected: every release with the trigger engine, v0.85.0 onward).** The v0.99.286 trigger-CDC hardening classified network/transport transients on the change-log poll but explicitly deferred the SQLSTATE level: a poll failing with `57P01` (admin_shutdown), `57P02`/`57P03` (crash shutdown / cannot connect now), or a class-08 connection exception — exactly what a managed-PG maintenance restart or failover produces — still surfaced unclassified and terminated the stream. Those shapes now ride the same bounded retry budget, via a narrow exported predicate on the postgres engine (`postgres.IsReadTransientSQLState`) so the trigger engine and the applier can never drift on what "connection-availability transient" means. Deliberately narrower than the applier's set: a missing change-log table (`42P01`) and auth failures stay terminal — retrying those masks an operator error. The failure was always loud (clean exit, durable resume position), never data loss.

**Two transient shapes the v0.99.288 connect-phase retry missed now retry (Bug 199, found by the post-release regression cycle within hours of publish).** First, the Windows dial-time refusal wording — `connectex: No connection could be made because the target machine actively refused it.` — matched neither the POSIX `connection refused` text nor the structural `ECONNREFUSED` check (pgx v5 flattens multi-host connect errors), and since the refused window is most of any target restart, the v0.99.288 connect-phase retry was effectively inert on Windows for the canonical local transient (affected: v0.99.288 — earlier releases had no connect-phase retry at all). Second, platform-neutral: a severed pool connection picked up at a checkpoint boundary surfaced pgx's `conn closed` unclassified and exited with zero retries; the Postgres applier classifier now honours pgconn's own `SafeToRetry` contract (the error is guaranteed to have occurred before any data reached the server, so retrying is unambiguous by construction) plus the `ErrConnClosed` sentinel (affected: every release with the apply-retry classifier, v0.42.0 onward). Both were loud, zero-loss exits with clean warm resumes — availability gaps, not data risks.

## Compatibility

No breaking changes; no configuration migration required; no flag or default changes.

One behavior change is deliberate and conservative: a Postgres-source cold start that narrows the shared publication now refuses when another `sluice_%` slot merely *exists* (previously only when one was active). If the refusal names an inactive slot from a genuinely abandoned stream, the message tells you exactly how to clear it (`pg_drop_replication_slot`), or use `--publication-name` to give each stream its own publication. Nothing that was previously *safe* now refuses — the newly-refused shape was silently starving the stopped stream.

The retry changes only convert previously-terminal loud exits into retries within the existing `--apply-retry-attempts` budget; every terminal class (auth, DSN, missing change-log table, unknown shapes) stays terminal, and `migrate` is unaffected. MySQL, PlanetScale, and Vitess sources have no publication and are untouched by the guard change.

## Who needs this — action required

- **Anyone who ran multiple Postgres-source streams through the shared default publication on v0.99.287 or v0.99.288** — if any stream was stopped or crashed while another stream cold-started with a narrower scope, the stopped stream may have been silently de-scoped despite the v0.99.287 guard. After upgrading, compare `pg_publication_rel` membership against each stream's table scope, and re-verify (e.g. `sluice verify`) the tables of any stream that resumed after another stream's cold start. Streams using per-stream `--publication-name` were never at risk.
- **`postgres-trigger` source users on managed Postgres** (RDS, Cloud SQL, Supabase, …) — upgrade; maintenance restarts and failovers no longer kill the sync. No action beyond upgrading.
- **Windows operators running continuous sync** — upgrade; the v0.99.288 connect-phase retry now engages for the refused-connection window it was built for. No action beyond upgrading.
- Everyone else: no action needed. The retry fixes carry no silent-loss class, so no re-verification of past runs is needed for them.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.289
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.99.289`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
