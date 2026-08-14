# sluice v0.125.0

**The PlanetScale/Vitess hardening release.** A license-safe survey of a fellow migration tool's engineering history yielded eleven claims about PlanetScale/Vitess behavior; sluice's code was audited against every one, and the five places where the architecture was right but the gates lagged are now closed — two new connect-time refusals whose absence risked *silent* data loss, three new real-cluster test gates, plus the chunked-copy progress fix a real user's MariaDB migration surfaced, a new backup refusal that stops a case-folding store from silently self-clobbering, quiet preview advisories for two semantically-divergent CHECK operators, and the Go 1.26.6 stdlib security patches. Also in this release: the weekly extended test suites now file their own standing issue when red, ending a three-week stretch of unread failures.

## Security

**Go 1.26.6.** The 2026-08-13 stdlib patches — GO-2026-6218 (net/url), GO-2026-6091 (html/template), GO-2026-6090 (crypto/tls). Every binary in this release carries the patched standard library; upgrading is the way to pick them up.

## Fixed

**A plain-`mysql`-flavor connection to a Vitess or PlanetScale server now refuses at connect.** Vitess servers report a `-Vitess` version suffix, but nothing checked for it under the vanilla flavor — and vanilla full scans run under Vitess's default OLTP workload, which **silently truncates result sets at its row cap**: a data-loss configuration, not a cosmetic one. The refusal (`SLUICE-E-DRIVER-HOST-MISMATCH`) steers to the `vitess`/`planetscale` flavors, whose bulk reads lift the cap with a connection-pinned `set workload=olap`. Fittingly, the new refusal's first CI run caught a misconfigured connection inside sluice's own test suite.

**The multi-database fan-out refuses to `CREATE DATABASE` on Vitess/PlanetScale targets.** vtgate does not support `CREATE DATABASE` — keyspaces are provisioned through the platform — but the fan-out's "vanilla-only" restriction was an unenforced comment, so a VStream-flavor multi-database run died on a raw vtgate error mid-run. It now refuses up front, naming the reality and the per-database explicit-target alternative.

**Chunked-parallel copies now show real progress — no more 0–1% forever with row counts jumping around (reported by a real MariaDB 11 migration).** Each parallel chunk's progress ticker compared its own rows against the whole table's total, and the per-table bar was clobbered by whichever chunk reported last. All of a table's chunks now share one aggregate: the bar and ETA render a monotonic table-level pair (resume offsets included), and the per-chunk log lines gain a `table_rows` attribute so percent-deriving consumers have a matched pair.

**`backup full` to a case-folding store no longer silently self-clobbers a case-colliding table's data at exit 0 (`SLUICE-E-BACKUP-STORE-NAME-COLLISION`).** A local backup directory on Windows or macOS folds letter case, and per-table chunk paths derive from table names — so a source legally holding `Orders` and `orders` wrote chunk paths that fold to one host file, leaving an un-restorable backup whose eventual `verify` refusal blamed bit-rot. Both writers that emit table-derived paths (`backup full`, `export-as-parquet`) now refuse before any write when the store *measurably* folds — a two-object probe, run only when the table set actually collides, so Linux stores and cloud object stores never even probe. `--exclude-table` on one colliding twin is a working remedy. **Existing backups taken on a folding store from a case-colliding source are suspect: run `backup verify`; a chunk-corruption refusal on one of the colliding tables means that table's data was never in the archive — re-take it while the source exists.**

## Added

**Three new real-cluster gates.** The OLTP row-cap gate (100,001 rows through the no-PK full scan on a real vttestserver, mutation-proven: deleting the workload SET fails it on the exact silent truncation); the delete-mid-interrupted-COPY convergence gate (a resume from a genuine mid-COPY checkpoint must fold interleaved source deletes into an exactly-converged final state — the silent-loss class a fellow tool hit in production); and the standing-red automation for the weekly extended suites (a scheduled red files/refreshes one GitHub issue naming the failed legs, so it can age loudly instead of silently).

**`schema preview` advises on the CHECK operators PostgreSQL parses with *different semantics*: infix `^` (MySQL bitwise XOR; PG numeric power — PG's exact spelling is `#`) and `/` (integer operands: MySQL keeps fractions, PG truncates).** Quiet advisories, deliberately never refusals — these constructs parse and apply on the target, with shifted meaning — and the never-refuse property is itself pinned so the rejected alternative can't creep in.

**The verified PlanetScale CDC connection path is now documented** in the managed-services guide: sluice dials vtgate's VStream gRPC API directly through PlanetScale's edge at the connect host on :443 (TLS, per-RPC Basic auth), live-verified by the psverify suite, with a 128 MiB receive ceiling (`?vstream_max_recv_bytes`) sized above real COPY-phase batches.

## Compatibility

No flag or on-disk format changed. One error code added (`SLUICE-E-BACKUP-STORE-NAME-COLLISION`) and one gained a trigger arm (`SLUICE-E-DRIVER-HOST-MISMATCH` now also fires for a vanilla-flavor connection to a Vitess-fingerprinting server — previously that configuration proceeded and risked silent truncation; use `--source-driver`/`--target-driver vitess` or `planetscale` for those servers, which was always the correct configuration). `schema preview` may emit new SeveritySilent advisories for `^` and `/` in MySQL-dialect expressions. **Drop-in upgrade from v0.124.2 for correctly-configured setups.**

## Who needs this — action required

**Anyone whose `mysql`-driver source or target is actually a Vitess/PlanetScale endpoint**: the connect-time refusal now tells you; switch to the `vitess`/`planetscale` driver — before this release that configuration could silently truncate large tables at ~100k rows. **Anyone taking local backups on Windows/macOS from sources with case-colliding table names**: verify existing backups (see Fixed). **Anyone migrating multi-million-row tables**: the progress bar is now trustworthy. Everyone else: the security bump alone is worth the upgrade.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.125.0
```

Container images: `ghcr.io/sluicesync/sluice:0.125.0` (multi-arch; the image tag carries no `v` prefix).
