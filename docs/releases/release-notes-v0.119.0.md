# sluice v0.119.0


A Vitess/PlanetScale source with any spatial column could not `sluice sync` at all, and `sluice schema diff` reported drift on targets `migrate` itself had just created — in both directions, badly enough that a second `migrate` refused outright. Both are fixed. Also here: sluice now asks the target engine whether it can render every column type BEFORE a copy starts rather than finding out from `CREATE TABLE`, and seven capability fields that declared type support nothing ever read were deleted.

## Added

**The target engine is asked whether it can render every column type, before any data moves (item 159).** Previously only PG-source→MySQL-target was checked early, by a hand-coded name-keyed function; every other engine pair discovered an unrenderable column from the target's own `CREATE TABLE` — after the plan printed, and with earlier tables already created. Now each target answers by calling **its own `emitColumnType`**, the same function its `CREATE TABLE` path calls, at all six copy entry points (`migrate`, `sync` cold-start single- and multi-database, `restore`, `chain restore`, `schema add-table`). The refusal names the table, column and type. **The message text is unchanged — the timing and the reach are the delta**, so an operator who already saw a refusal sees the same words, sooner, on more engine pairs. Two Postgres refusals deliberately stay late (PostGIS absent, `--enable-pg-extension`) because they depend on configuration the preflight runs under the most permissive reading of; that is stated at the interface rather than implied. The single source of truth is the emitter, so the preflight's verdict cannot drift from the behaviour it predicts.

## Fixed

**A Vitess/PlanetScale source with ANY geometry column could not `sync`; `migrate` of the same table worked (Bug 239).** The cold-start copy died with PostgreSQL's own `parse error - invalid geometry` (SQLSTATE XX000) and zero rows. Not a VStream defect, which is where it looked: the Postgres **row writer's** connection pool never registered the PostGIS geometry codec that the CDC applier's three pools have had since task #20. COPY ships values in COPY-BINARY, which `geometry_recv` accepts with no codec — so `migrate`, which takes COPY, was fine. The batch and idempotent cores bind geometry as a query PARAMETER, where pgx has no codec for PostGIS's dynamic OID, falls back to TEXT, and `geometry_in` rejects the EWKB bytes. VStream is simply the one source that ALWAYS demands the idempotent core. **Any PG-target write through a batch core had this**, not just Vitess. Confirmed by feeding the exact bytes the VStream decoder produces into all three cores against a real PostGIS: COPY landed the value, both batch cores died with the verbatim reported error. A roster gate now holds every pool in the package to its classification, with anti-vacuity floors on both sites discovered and sites registering.

**`sluice schema diff` reported phantom drift on a target `migrate` had just created, in both directions — and one arm made a second `migrate` refuse (Bug 237, item 156).** Three arms, all reproduced on real servers first. Unspecified-precision PostgreSQL temporals compared as `Time(unspecified)` against the faithful `Time(6)` the target holds; sluice's own `SET`-emulation and generated-enum CHECK constraints reported as extra; and a DOMAIN's translatable CHECKs landed on MySQL as auto-named table-level constraints the expected side could not carry. **The temporal arm was worse than an advisory nuisance:** the ADR-0166 pre-create gate compares the same rendering, so re-running `migrate` over an existing target refused with `column "ts_bare": want DateTime(unspecified), target has DateTime(6)` — a loud failure on the exact operation an operator reaches for when something already went wrong. Emitted CHECKs are now matched by canonicalized EXPRESSION rather than by name, so a constraint the operator WEAKENED or dropped on the target is still reported; that direction is pinned on real servers in both engines. A fourth axis the filing had not named — PostgreSQL `TIME WITH TIME ZONE`, which MySQL cannot hold at any precision — is closed with it.

**A `CREATE DOMAIN` regex containing an apostrophe landed on MySQL requiring two of them, so the target rejected rows PostgreSQL accepts.** A domain over `^o'brien$` reached MySQL as a pattern needing `o''brien`. Present since v0.97.0. Verified against MySQL's own enforcement — an `INSERT` the server accepts or rejects — rather than against the read-back path, because that path has its own separate escaping defect (filed, not patched blind).

**The Vitess/PlanetScale flavor no longer declares spatial types unsupported.** It excluded them "for conservatism; flip the flag if a user reports they work" — an absence of evidence recorded as a capability, and the absence was Bug 239 above, never a Vitess limitation. Measured with a real vtgate through `sync`'s cold start and a live CDC insert into real PostGIS, over all seven WKB geometry families.

## Changed

**Seven `ir.Capabilities` fields were deleted:** `SupportedTypes`, `SupportsCheckConstraint`, `SupportsGeneratedColumns`, `SupportsPartitioning`, `EnumSupport`, `JSONSupport` and `UnsignedIntegers`, along with the `TypeSet`, `EnumSupport` and `JSONSupport` types. All eight engines declared them; **no production code read any of them**, since the IR was scaffolded on 2026-05-01. They were a second copy of a truth the engines already hold structurally — whether an engine can represent a type is decided by its own type dispatch, and a missing arm IS the answer — and the copy had drifted exactly as second copies do: three extension kinds declared by no engine at all. The capability fields that remain are strategy selectors the orchestrator genuinely consults (`BulkLoad`, `CDC`, `SchemaScope`, `DDLDialect` and friends). `docs/architecture.md` had told contributors since 2026-05-01 that enum and array emission were decided from these fields; that was never true and is corrected.

**`sluice schema diff` now opens the target SchemaWriter on every invocation** (previously only when there was drift to render DDL for), to ask the target what CHECK constraints sluice's own emitter synthesizes. A failed open is non-fatal: it warns, and those constraints report as extra. A read-only target credential still works and yields a slightly degraded diff.

## Compatibility

No on-disk format changed; existing backups, chains and resume state read exactly as before. Three behaviour changes worth knowing:

- **An unrenderable column type is now refused before the copy starts**, on every engine pair rather than one. The message is unchanged — you see the same words sooner, and on pairs that previously failed at `CREATE TABLE` with earlier tables already created.
- **`sluice schema diff` opens the target SchemaWriter on every run.** Non-fatal on failure; a read-only credential still works with a slightly degraded diff.
- **A PostgreSQL `time with time zone` column against a pre-existing MySQL `TIME(6)` target now compares equal** and no longer refuses a `migrate` re-run. The refusal was protecting nothing — the offset is dropped on the first migrate too — but it is worth naming, because it is a loosened comparison sitting upstream of a lossy value path.

Known and filed, not fixed here: a PostGIS `geography` column written through a batch or idempotent core still fails with PostgreSQL's raw `parse error - invalid geometry` rather than a named sluice refusal. That is the sibling of the Bug 239 shape and is tracked in the audit backlog.

## Who needs this

- **Anyone running `sluice sync` from Vitess or PlanetScale with spatial columns. Upgrade — it could not work before.**
- **Anyone writing geometry into a PostgreSQL target through anything other than COPY**, which includes every `WriteRowsIdempotent` caller.
- **Anyone who uses `sluice schema diff` across PostgreSQL and MySQL, or who re-runs `migrate` over an existing target with unspecified-precision temporals.** The first was reporting drift that was not there; the second was refusing outright.
- **Anyone migrating a PostgreSQL `CREATE DOMAIN` whose CHECK regex contains an apostrophe.** Your target has been rejecting rows the source accepts.
- **Everyone else: nothing to do.** The capability-field deletion is internal and changes no behaviour.

## Install

```
brew install sluicesync/tap/sluice
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket && scoop install sluice
```

Binaries for Linux, macOS and Windows are attached below; container images are published to `ghcr.io/sluicesync/sluice`. Every artifact carries a build attestation:

```
gh attestation verify sluice_0.119.0_Linux_x86_64.tar.gz --repo sluicesync/sluice
gh attestation verify oci://ghcr.io/sluicesync/sluice:0.119.0 --repo sluicesync/sluice
```

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.119.0 · **Container:** ghcr.io/sluicesync/sluice:0.119.0

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
