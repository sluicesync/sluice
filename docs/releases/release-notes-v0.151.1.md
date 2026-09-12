# sluice v0.151.1

**A patch for the preflight v0.151.0 added — it was manufacturing the wasted cycle it was built to prevent.** If you are not migrating into a PlanetScale branch with Safe Migrations enabled, nothing here changes how sluice behaves for you.

## Fixed

**The direct-DDL preflight ran on `--resume`, and its remedy could not work.** v0.151.0 added a probe that refuses a `migrate` or sync cold start in ~200 ms when the target branch will not accept direct DDL, instead of discovering it partway through schema-apply. The probe also ran on resumed runs — so an operator who hit the refusal, disabled Safe Migrations, and re-ran the way the message suggested hit the identical refusal again. Worse, the message stated twice that nothing had been created, while the failed attempt had recorded its own `sluice_migrate_state` row; that makes a plain re-run of the same `--migration-id` a refused partial migration, so the operator was signposted into a dead end from both directions.

Both halves are fixed. `PreflightDirectDDL` no longer runs when `--resume` is set — a resumed run's DDL need is decided later, from recorded per-table progress, and the create phase still fails loudly if DDL is genuinely required. And the refusal now names the recovery that works: **a fresh `--migration-id`, or clearing the recorded state — not `--resume`**, because the refusal fires before the schema phase and there is no partial schema to resume onto. Disabling Safe Migrations remains the direct fix, with the caveat the message already carried: propagation is asynchronous, so re-run only once the preflight itself passes, which is what it is for.

Filed as Bug 284 by the v0.151.0 regression cycle — a defect in a release's own new feature, found by the cycle that release triggered. Pinned by `TestDirectDDLPreflightIsNotRunOnResume`.

**`SLUICE-E-PS-DIRECT-DDL-BLOCKED` documents its third arm now, with the right remedy.** The error-code page and the `planetscale-migration` skill described two cases — control-table DDL and the user-table CREATE during schema-apply — both of which echo the exact statement the branch refused. The preflight arm does neither: it names a throwaway probe table, and its recovery is the opposite of what the page offered. No runtime change; the page was simply wrong about a refusal an operator meets at the worst moment.

## Changed

**`--backup-endpoint`'s help no longer reads as a per-provider guarantee.** It names MinIO, Cloudflare R2, Backblaze B2, Wasabi, Tigris and Archil's S3 read API. One generic S3 client serves all of them — there is no per-provider code path in sluice — so support is a claim about a *class*, and CI boots exactly one S3 server. Six names in a help string read as six things somebody tested; they were not, and the help says so now. `docs/testing.md` carries the derived list of which storage backends a real server has actually answered for.

**A PlanetScale Neki sequence position could be invented rather than read — new refusal `SLUICE-E-SEQUENCE-POSITION-UNREADABLE`.** A Neki router refuses to read a sequence as a relation (`NK013`), so sluice falls back to `pg_catalog.pg_sequences`. That view's `last_value` is **privilege-gated in its own definition**, so a connected role without `SELECT`/`USAGE` reads NULL for a sequence at any position — and sluice mapped NULL to the sequence's `start_value`. Measured on real PostgreSQL 18.6: a sequence genuinely at `(6, is_called=true)` came back as `(5, false)` — wrong by however far it had advanced, with `is_called` flipped.

That number is not cosmetic. The source-side capture writes it into the IR and the target is primed from it, so a source sequence at 10⁶ read as `(start, false)` produces a target that re-issues a million values the copied rows already hold, at exit 0 — and standalone sequences are exactly the ones that cannot be re-derived from `MAX(column)` the way a serial/identity sequence can. sluice now refuses instead, naming the sequence and the grant that fixes it. **Vanilla PostgreSQL was never affected**: there the relation read is attempted first and fails loudly with `permission denied`.

Found by this release's own pre-tag value-fidelity review, which also showed the grading in v0.151.0's shipped comment was too kind in a second way — it reasoned about one of the reader's three consumers, and the other two are source reads where an under-report corrupts rather than degrades. The remaining not-called ambiguity is documented honestly now, including that `ALTER SEQUENCE … RESTART WITH` reaches it by an ordinary route, and `TestSequenceCatalogFallbackMatchesTheRelationRead` pins all of it — ascending and descending, where "ahead" inverts numerically.

## Testing

Nothing here changes runtime behaviour. It is listed because it changes what a green CI run means.

- **A MySQL server-version matrix.** MySQL was the only engine family with no version sweep, while the tree carried behaviour measured on 8.4 servers and PlanetScale MySQL runs the 8.4 line. `mysql-version-matrix.yml` sweeps `scripts/mysql-versions.txt` weekly using a new `SLUICE_TEST_MYSQL_IMAGE` override that mirrors the Postgres one; 8.4 is a pinned leg and `latest` is a canary.
- **GCS and Azure blob-storage coverage**, via fake-gcs-server and Azurite. Two of the four registered `gocloud.dev/blob` drivers had never been booted — including the create-only precondition the ADR-0160 chain concurrent-writer guard maps onto each backend. Both enforce it, with Azure answering 409 where S3 and GCS answer 412, and all three classify correctly inside sluice.
- **MinIO removed its Docker Hub repository**, which turned four blob tests red with nothing in sluice having changed. The image is now a pinned `quay.io/minio/minio` release, and all three blob emulators are mirrored to GHCR so the next such decision arrives on our schedule.
- A **data race** in this package's slog-capture tests (global logger + an unjoined goroutine), and the nekiverify build-tag axis registered in the run-filter manifest that had been failing Lint on `main`.

## Who needs this

**Anyone migrating into a PlanetScale branch with Safe Migrations enabled** — that is the Bug 284 path, and before this patch the refusal's own advice led nowhere.

Everyone else can take it or leave it: the remaining changes are documentation accuracy and test coverage.

## Install

```sh
# Homebrew
brew install sluicesync/tap/sluice

# Scoop (Windows)
scoop install sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.151.1
```

Binaries, checksums and the multi-arch container image are attached to this release.
