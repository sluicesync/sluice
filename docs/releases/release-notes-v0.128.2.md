# sluice v0.128.2

**Two more steps on the domain story, plus an O(n²) removed from backup compaction.** A domain over an ENUM now migrates to a Postgres target; the rekey-dense-chain compaction path that could burn tens of minutes of CPU now finishes in under a second; and the base-OID wire premise the domain work rests on is now pinned. **Drop-in upgrade from v0.128.1 — no schema/format/flag change.**

## Fixed

**A domain over an ENUM now migrates to a Postgres target (Bug 255, HALF 1).** A `CREATE DOMAIN d AS <enum>` column previously died at create-tables with `Enum DDL emission requires column context` — a PostgreSQL enum is a NAMED type, and the per-column enum path names that type from the table+column it has no access to at `CREATE DOMAIN` time. The schema writer now emits the enum's `CREATE TYPE` — named from the enum's own recorded type name (or `<domain>_enum` when the source didn't carry one), deduped against the same registry a plain enum column uses and idempotent across a restarted cold-start — before the `CREATE DOMAIN` that references it, and drops it on `--reset-target-data`. Pinned by a real-Postgres migrate that lands the type + domain + rows with the enum value order and the domain CHECK preserved. (A domain over a MySQL target already worked, and the CDC read path was completed in v0.128.1.) A NESTED domain-over-domain — rarer, and needing multi-level base-type modelling `information_schema` cannot supply in a single read — is still refused, but now loudly by name ("nested domain", naming both levels) rather than a generic user-defined-type error.

## Performance

**Backup compaction of a rekey-dense chain no longer goes O(n²) in CPU (audit P-2).** The smart compactor removed a key from its live-key ordering by an O(n) linear scan + slice splice, twice per primary-key-changing UPDATE; at the 32 MiB accumulator cap (~80–168k live keys) a segment of a few hundred thousand PK-changing UPDATEs paid a full scan each time, so `backup compact` could spend tens of minutes of CPU — memory stayed bounded, only CPU blew up, and correctness was never affected. The ordering is now a `container/list` with an O(1) per-key removal cached on each accumulator. Measured on a 160k-rekey-churn workload: 37.5s → 0.49s. A permanent gate asserts the removal stays sub-linear (a machine-noise-cancelling ratio against an equal-event in-place-UPDATE reference), mutation-verified against the old scan.

## Changed

**The domain base-OID wire premise is now pinned (audit PREM-1).** The `ir.Domain` value-transparency shipped since v0.128.0 is value-safe because PostgreSQL reports a domain column's BASE type OID (not the domain's own) in a regular query's RowDescription — so the COPY and value codecs key on the base type and the wrapper is erased at the wire. That fact was verified by review but unasserted in the suite; a test now binds it against real PostgreSQL over a base-family matrix (the reported OID equals the base OID and is a builtin, never the domain's dynamic OID). Internal ratchet, no runtime effect.

## Compatibility

Drop-in from v0.128.1 — no schema, format, or flag change. Bug 255 adds a capability (domain-over-enum → Postgres) that previously failed loudly at create-tables; the P-2 change is a pure performance fix with no behavior change.

**Anyone migrating a Postgres source whose schema uses a domain over an enum to a Postgres target**: upgrade — this previously failed at create-tables. **Anyone who runs `backup compact` on a chain with heavy primary-key churn**: upgrade for the CPU fix. **Everyone else: no action — this is a drop-in upgrade.**

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.128.2
```

Container images: `ghcr.io/sluicesync/sluice:0.128.2` (multi-arch; the image tag carries no `v` prefix).
