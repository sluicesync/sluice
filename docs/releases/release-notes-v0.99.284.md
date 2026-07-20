# sluice v0.99.284

**Collation and float fidelity fixes for `sync --where` on MariaDB and PlanetScale/Vitess.** Three small, self-contained correctness fixes to continuous filtered sync, from the audit follow-ups after v0.99.283. If you don't use `sync --where`, nothing changes — and `migrate --where` (source-evaluated) is unaffected throughout.

## Fixed / Changed

### A filtered `sync --where` on a MariaDB NO-PAD collation is now classified correctly

`collationNoPad` now recognizes MariaDB's `*_nopad_*` naming, so a `--where` on a NO-PAD-collation column — one whose `=` treats trailing spaces as significant, e.g. `utf8mb4_nopad_bin` — is reduced faithfully instead of being mis-treated as PAD SPACE. Previously such a column could silently mis-classify a trailing-space row-move on continuous filtered sync.

The wrinkle behind the fix: MariaDB's `information_schema.COLLATIONS.PAD_ATTRIBUTE` column — the authoritative PAD-SPACE-vs-NO-PAD signal MySQL has had since 8.0 — is version-dependent on MariaDB. It's absent through the entire 11.x LTS line and 12.0, and only appears in 12.1+. So sluice keys off the version-independent collation name in every version, and the CI parity gate validates that against the real server both where the catalog exists (12.1+, full catalog assertion) and where it doesn't (11.x, behavioral probe).

### A single-precision `FLOAT` ordering `--where` on a PAD-SPACE-forced PlanetScale/Vitess table is now refused

A table whose `--where` touches a PAD-SPACE-collation column on a VStream source takes the client-side COPY fallback introduced in v0.99.283. That fallback's keep predicate runs on the cold-start COPY row — and vttablet's VStream COPY carrier renders single-precision `FLOAT` at display precision (~6 significant digits; the exact re-read repair runs *after* copy), so a boundary comparison like `amount > 0.1` could compare on the rounded value and silently drop a source-in-scope row. sluice now refuses that predicate at sync-start with `SLUICE-E-WHERE-CDC-UNSUPPORTED-PREDICATE` rather than risk the drop. `DOUBLE` transits the carrier at full precision and is unaffected — use a `DOUBLE` column, filter on a non-FLOAT column, or use `sluice migrate --where` (source-evaluated) for a one-shot subset.

### A `FLOAT IS [NOT] NULL` `--where` is no longer wrongly refused

The float guard above initially caught *any* reference to a single-precision FLOAT column, including a `IS [NOT] NULL` presence test whose result cannot depend on the rounded bits. It now restricts to value comparisons, so `IS NULL` / `IS NOT NULL` on a FLOAT column is evaluated faithfully instead of being refused.

## Compatibility

**All three changes only affect `sync --where`.** No behavior change for `migrate --where`, for a continuous sync without `--where`, for `DOUBLE` columns, or for non-string / non-FLOAT predicates. The MariaDB fix only changes classification for a `--where` on a `*_nopad_*` collation column. `migrate --where` is source-evaluated and was never affected by any of this.

## Who needs this

Anyone running continuous `sync --where` against a **MariaDB** source with a filter on a `*_nopad_*` collation column (upgrade — the classification fix prevents a silent trailing-space row-move error), or against a **PlanetScale/Vitess** source with a filter that compares a single-precision `FLOAT` column (you'll now get a loud refusal with a clear remedy instead of a possible silent drop). Everyone else can upgrade at leisure.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.284
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.284`
