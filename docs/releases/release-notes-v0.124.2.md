# sluice v0.124.2

**Patch: the post-v0.124.1 queue close-out — a silent-restore refusal for pre-v0.120.0 backup chains with backslash literals (found wider than filed), restore's `--exclude-table` remedy no longer defeated by the shape preflights it releases, and the D1 invalid-UTF-8 question measured on real D1 and answered in the docs.** All three are pre-existing behaviours, none introduced by a recent release. Upgrade if you restore backup chains recorded before v0.120.0, use `--exclude-table` during restores, or migrate from Cloudflare D1 sources that might hold invalid-UTF-8 TEXT.

## Fixed

**A pre-v0.120.0 backup chain whose recorded expressions carry literal backslashes now refuses to restore instead of silently enforcing a different predicate.** Chains recorded by a MySQL-family reader older than v0.120.0 spell a literal backslash in MySQL's doubled form (`'a\\d'` meaning one backslash) — structurally valid, so the v0.120.1 malformed-schema gate correctly never fired — and restoring one silently enforced a different CHECK, generated-column, default, or index predicate at exit 0. The original filing scoped this to PostgreSQL targets; closing it found the defect wider: the post-v0.120.0 MySQL emit boundary assumes the bare spelling and re-doubles, so the same-engine round trip silently gains a backslash too, and PostgreSQL and SQLite read the doubled form as two characters directly. The recorded-schema gate (`SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED`) grows a second arm keyed on the manifest's recorded sluice version and MySQL-family source, reaching every door — restore, chain-restore schema deltas, `backup verify`, the broker's pre-drop door, and the incremental/compact/prune warnings — and staying filter-aware at the restore doors so the `--exclude-table` salvage keeps working. One stated boundary: a chain written by a from-source build (version `dev`) is not gated, because its printed remedy — a fresh backup, which would also stamp `dev` — could never release the refusal. Every released binary has stamped a parseable version since backup shipped, so every released-binary chain is covered.

**`restore --exclude-table` now gets past a shape refusal on the excluded table itself.** The five pre-DDL "can this target hold this shape?" gates (table-emit, index-emit, view-emit, column-type, and the target-server name-fold) plus the verbatim-extension gate all graded the unfiltered manifest schema, so a backup whose *excluded* table carried an unrepresentable index, column type, name, or verbatim-typed column refused the whole restore — even though `--exclude-table` was the documented route around exactly that table. Both restore paths now grade the filtered view, and the name-fold check grades the kept set only: a fold collision with an excluded table is not a collision on the target.

## Changed

**D1 TEXT with invalid UTF-8 is unrescuable through Cloudflare's API — measured, pinned, and documented.** The v0.122.0 invalid-UTF-8 guards carried an unverified premise for the D1 lane. Measured against real D1: storage holds the bytes intact, but the `/query` API replaces every invalid byte with U+FFFD server-side — on the `d1` query-API reader and the `d1-trigger` change-log poll alike — so sluice's client-side refusal cannot fire there and the mangled value is indistinguishable from a genuine U+FFFD. The operator guidance now lives in `docs/operator/sqlite-d1-import.md`: repair such values at the source, or export the true bytes server-side with `hex(x)`. A new `d1verify` live-credential test suite pins the measured behaviour, so a Cloudflare serialization change fails a named test instead of silently changing the story. File-backed SQLite sources are unaffected — the loud refusal remains in force there.

## Compatibility

No flag, format, or default changed. One refusal surface widened: `SLUICE-E-BACKUP-RECORDED-SCHEMA-MALFORMED` now also fires on pre-v0.120.0 MySQL-family chains whose recorded literals carry backslashes (previously these restored silently wrong — the refusal is the fix). Its documented remedies apply unchanged: a fresh `backup full` with a current binary, or `--exclude-table` on the named table. **Drop-in upgrade from v0.124.1.**

## Who needs this — action required

**Anyone holding backup chains recorded before v0.120.0 from MySQL-family sources**: run `backup verify` with v0.124.2 — a chain that now refuses with the doubled-backslash message was restoring silently wrong on every target; re-take a `backup full` with a current binary while the source still exists. **Anyone whose restore refused on a shape problem in a table they had excluded**: upgrade; the filter now releases those gates. **Anyone migrating from live Cloudflare D1**: if your TEXT columns might hold invalid UTF-8, read the new caveat in the D1 import guide — those values cannot arrive faithfully through the API, and sluice cannot detect the substitution client-side. If none of these describe you, this patch changes nothing you'll observe.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.124.2
```

Container images: `ghcr.io/sluicesync/sluice:0.124.2` (multi-arch; the image tag carries no `v` prefix).
