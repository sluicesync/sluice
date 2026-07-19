# sluice v0.99.283

**Filtered `sync --where` on a legacy PAD-SPACE collation now works on PlanetScale/Vitess — lifting the v0.99.282 refusal.** If you filter a continuous sync on a string column with a PAD-SPACE collation (e.g. `utf8mb4_general_ci`) against a PlanetScale/Vitess source, it now just works instead of being refused. Nothing else changes.

## Fixed / Changed

### PAD-SPACE-collation `--where` on PlanetScale/Vitess is filtered client-side instead of refused

The audit that produced v0.99.282 found that the VStream server-side filter evaluates the pushed `WHERE` **NO-PAD** — it ignores a legacy collation's PAD-SPACE trailing-space semantics regardless of the column's real `PAD_ATTRIBUTE`. So a `region = 'EU'` filter on a `utf8mb4_general_ci` column would have silently dropped a stored `'EU '` (trailing space) that the source's own `=` keeps. v0.99.282 closed that hole by **refusing** such a filter at sync-start; v0.99.283 replaces the refusal with a fallback that makes it **work**:

- For a table whose `--where` touches a PAD-SPACE-collation string column, sluice streams that table **unfiltered** server-side (it can't be reduced faithfully there) and filters it **client-side** with the same PAD-faithful comparator the CDC leg's row-move classification uses.
- The trailing-space `'EU '` a `region = 'EU'` filter should keep **is kept**, on both the cold-start copy and the CDC stream — exactly as the source's own `=` does — while an out-of-scope row is still dropped.

Verified end-to-end on a real Vitess cluster: a filtered open drops the trailing-space row (confirming the server filter really is NO-PAD), and the fallback path keeps it while dropping the out-of-scope row. The only trade is **more wire traffic for that one table** (it isn't reduced at the source). NO-PAD `utf8mb4_0900_*` collations — the MySQL 8.0 default — are reduced server-side as usual and are unaffected.

## Compatibility

**A filter that v0.99.282 refused now runs; everything else is unchanged.** No behavior change for NO-PAD collations, non-string predicates, the non-VStream flavors (vanilla MySQL binlog, Postgres — which push the filter through the source's own PAD-faithful `=`), or any sync without `--where`.

## Who needs this

Anyone running `sync --where` with a string filter on a legacy PAD-SPACE collation (`utf8mb4_general_ci` and similar) against a **PlanetScale/Vitess** source — it now works instead of refusing. Everyone else can upgrade at leisure.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.283
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.283`
