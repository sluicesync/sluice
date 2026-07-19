# sluice v0.99.282

**Silent-loss fixes for `sync --where` string and float filters — from a fresh blind audit of the v0.99.279–281 remediation.** The 2026-07-18 remediation fixed one PAD-SPACE collation Critical, but a fresh independent audit found it fixed one *representative* and missed three *siblings* of the same collation-fidelity family. All four are confirmed against real MySQL 8.0.46 + Postgres 16.14. If you use `sync --where` with a string or float filter, upgrade. `migrate --where` (source-evaluated) was never affected; nothing that doesn't use `--where` changes.

## Fixed

### MySQL `utf8mb4_0900_as_cs` and every `_as_cs`/`_cs` UCA collation

The client-side comparator routed these case+accent-sensitive collations to a byte-exact compare, but MySQL's `=` under a UCA collation folds Unicode canonical equivalence (NFC/NFD) and ignores UCA-ignorable code points (e.g. a soft hyphen). So a value differing only by normalization form or an ignorable character was classified out of scope client-side while the source kept it — a move-OUT read as a drop → a silent `DELETE` of an in-scope row, with the mirror leaking. Only `_bin`/`binary` is genuinely byte-exact; `_as_cs`/`_cs`/tailored-UCA now route through the same faithful Vitess comparator as the ci/ai collations. Ground-truthed against MySQL 8.0.46 with explicit NFC/NFD and soft-hyphen shapes.

### Postgres `char(n)`/bpchar filters on trailing-space padding

bpchar `=` is PAD SPACE (trailing-space-insensitive) regardless of collation — unlike `text`/`varchar` — and logical decoding delivers a char value space-padded to its declared width. The comparator compared the padded value byte-exact, so a stored `'EU'` (delivered as `'EU  '`) failed to match `region = 'EU'` which Postgres's own `=` matches — a silent drop/orphan on Postgres's **default** char semantics, no legacy collation required. char/bpchar now trims trailing spaces before the compare, while text/varchar stay trailing-space-significant. Ground-truthed against Postgres 16.14 (`char(4)`, padded wire value, against PG's own `WHERE`).

### PAD-SPACE `--where` on PlanetScale/Vitess now refuses loudly instead of dropping rows

Making the client comparator PAD-faithful in v0.99.279 without reconciling the VStream server-side filter left the two legs disagreeing on a PlanetScale/Vitess source: the pushed `WHERE` is evaluated NO-PAD by vttablet regardless of the column's real `PAD_ATTRIBUTE`, so a PAD-SPACE legacy collation (e.g. `utf8mb4_general_ci`) dropped trailing-space rows the client keeps. Until the client-side fallback lands, such a predicate is now refused loudly at sync-start on VStream sources — use a NO-PAD `utf8mb4_0900_*` collation, filter on a different column, or run `migrate --where` for a one-shot copy. NO-PAD collations, non-string predicates, and non-VStream flavors (vanilla MySQL, Postgres) are unaffected: **server-side filtering stays in place everywhere it's faithful.**

### Float/double ordering now matches the source's IEEE-754 coercion

A `sync --where` ordering comparison (`<`, `<=`, `>`, `>=`) on a FLOAT/DOUBLE column compared the literal exactly while the source coerces it to a 64-bit double, so a high-precision literal that rounds to the stored value — e.g. `d >= 0.10000000000000001` against a stored `0.1` — diverged, silently dropping or leaking the boundary row. Ordering now compares as float64, matching the source. Equality on float columns stays refused (unchanged).

## Compatibility

**No behavior change without `--where`.** For `sync --where`: `_as_cs`/`_cs` MySQL and `char(n)` Postgres string filters are now faithful; float ordering matches the source; a PAD-SPACE-collation filter on a VStream/PlanetScale source now refuses loudly (was a silent drop) — the same filter on a NO-PAD collation or a non-VStream source works unchanged. All four are ground-truthed against real servers, and both real-server collation family matrices run in the per-PR CI gate.

## Who needs this

Anyone running `sync --where` (continuous filtered sync) against MySQL/PlanetScale or Postgres with: a string filter on a case/accent-sensitive (`_as_cs`) column, a Postgres `char(n)` column, a float ordering filter, or a legacy PAD-SPACE collation on PlanetScale. `migrate --where` and any sync without `--where` are unaffected.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.282
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.282`
