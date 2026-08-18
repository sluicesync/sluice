# sluice v0.128.1

**A fast-follow patch completing v0.128.0's domain-transparency work — continuous CDC sync from a Postgres `CREATE DOMAIN` source now runs end-to-end.** v0.128.0 made domains transparent across the migrate, apply, backup, predicate, and redaction paths, but one path was missed: the Postgres CDC source reader, which dispatches on the pgoutput wire OID rather than the schema-declared type. A domain-typed table cold-copied correctly and then halted on the first CDC change. This closes that path and corrects the v0.128.0 claim that domains were handled across "every path". **Drop-in upgrade from v0.128.0 — no schema/format/flag change.**

## Fixed

**Continuous sync from a Postgres source with `CREATE DOMAIN` columns no longer halts at the first CDC change (Bug 254).** The Postgres CDC source reader dispatches on the pgoutput wire OID, not on the schema-declared `ir.Column.Type` that v0.128.0's `domaingate` walker covers — so it is the wire-side sibling of the Bug 233 class the audit closed. A PostgreSQL domain has a dynamic per-database OID that matched no builtin type, so a domain-typed table cold-copied correctly and then the stream halted on the first row change with `postgres: cdc: unsupported column type OID <n> (typmod -1)`. Loud, no data loss — but a working continuous sync was impossible for any domain-using Postgres source (this was true on v0.127.x too; v0.128.0 made the gap conspicuous by fixing every neighbouring path). The reader now resolves a domain's wire OID to its ultimate base type via `pg_type.typbasetype` — the wire-side analogue of the unwrap the schema reader already performed — carrying the base type's modifier (a `DOMAIN … AS varchar(10)` column decodes as `varchar(10)`, not a length-defaulted fallback, because that modifier lives only in the catalog while the wire reports the domain column's own typmod as -1), flattening domain-over-domain, and resolving domains over an enum or an array through the existing enum/array arms. The decoded value is the base type and is never re-wrapped, matching what the applier resolves from the target's `information_schema`. Pinned by a real-Postgres CDC round-trip (cold-copy then INSERT/UPDATE/DELETE, source==target) over a `{int, varchar(10), uuid, enum, int[], domain-over-domain}` matrix plus a unit pin on the resolver; mutation-verified that disabling the domain arm reproduces the halt.

## Compatibility

Drop-in from v0.128.0 — no schema, format, or flag change. This also corrects the v0.128.0 notes' "every path" phrasing: that release handled the migrate and apply paths, and this completes the CDC source-read path.

**Anyone running — or intending to run — continuous CDC sync from a Postgres source that uses domain types**: upgrade; on v0.128.0 and earlier the stream halted on the first change to a domain-typed table. Migrate-only users (no continuous sync), and syncs from sources without domain types, are unaffected either way. **Everyone else: no action — this is a drop-in upgrade.**

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.128.1
```

Container images: `ghcr.io/sluicesync/sluice:0.128.1` (multi-arch; the image tag carries no `v` prefix).
