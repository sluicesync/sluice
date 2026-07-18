# sluice v0.99.278

**Continuous filtered sync now works everywhere.** `sync --where` (row-level filtering, ADR-0173) previously refused on MySQL-family sources for two reasons that live testing surfaced; this release makes it *work* — faithfully — across the full engine matrix (ADR-0174). Fully additive: without `--where`, nothing changes.

## Added

### `sync --where` on MySQL — faithful case/accent-insensitive filters

A string filter like `--where "users=region = 'EU'"` on a case- or accent-insensitive column (MySQL's default collation) used to be refused for continuous sync, because a naive byte comparison client-side could disagree with the source's collation-aware `=` and silently leak or drop a row. sluice now reproduces the source's own `=` **faithfully** — by reusing the source engine's collation comparator (Vitess's `evalengine` over its `collations.Environment`), the same code MySQL/Vitess evaluate `=` with. So `region = 'EU'` matches `eu`, `Eu`, and accent-folded values byte-identically to what the source itself would match; the client-side CDC classification and the source's evaluation cannot diverge.

A collation sluice cannot reproduce (an unknown or absent one, or a Postgres non-deterministic ICU collation) still refuses loudly rather than guess. And `--where-strict-collation` forces the strict, pre-0174 behavior — refuse any non-byte-exact string comparison — for operators who want the byte-exact guarantee regardless. It defaults off; faithful mode is the common default.

### `sync --where` on PlanetScale MySQL / Vitess

Continuous filtered sync now supports the `planetscale` / `vitess` (VStream) path. The predicate is pushed into the VStream filter rule (`select * from <t> where (<pred>)`), so Vitess evaluates it **server-side with the source's own collation** — filtering both the cold-start COPY and the streaming tail natively — while sluice classifies row-moves client-side (a row updated *into* scope becomes a target INSERT; *out of* scope becomes a target DELETE). It's validated end-to-end on a real Vitess cluster: the filtered COPY excludes out-of-scope rows server-side, and a move-out arrives with both before- and after-images and becomes a DELETE — never a stale, silently-leaked row.

A universal floor backs it: any filtered UPDATE/DELETE whose before-image omits a column the predicate references refuses loudly (`SLUICE-E-WHERE-CDC-BEFORE-IMAGE`, naming the column), so a source not delivering full row before-images (`binlog_row_image` != `FULL` / missing `REPLICA IDENTITY FULL`) can never silently mis-classify a move-out.

## Compatibility

**Additive — no behavior change without `--where`.** `migrate --where` is unchanged (already universal — it evaluates on the source). For `sync --where`: string filters on MySQL now evaluate faithfully under the column's collation (previously refused), and PlanetScale MySQL / Vitess is now supported (previously refused — `migrate --where` only). sluice's collation set is pinned to MySQL 8.0.30; an unrecognized collation refuses safely rather than compare wrongly.

## Who needs this

Anyone maintaining a **continuously-filtered** replica — a per-region / per-tenant / data-residency split kept live, not just a one-shot extract — where the source is MySQL, PlanetScale MySQL, or Vitess. The Postgres path already worked; this closes the MySQL-family gaps. See the [Split rows by region](https://sluicesync.com/docs/split-rows-by-region/) guide.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.278
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.278`
