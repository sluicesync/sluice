# sluice v0.99.58

**MySQL `SET` columns now replicate to a PostgreSQL `text[]` over CDC
(Bug 149).** A small follow-on to the v0.99.56 SET fix: the decoded `SET`
value reached the PG applier in a slice shape its array binding didn't
accept, halting continuous sync at apply. Cold-start migration already
worked; this closes the CDC path.

## Fixed

- **MySQL `SET` → PostgreSQL `text[]` over CDC (Bug 149).** v0.99.56 made the
  MySQL binlog decode a `SET` to its member labels (a Go string slice), and
  sluice maps a `SET` to a PG `TEXT[]` target. But the CDC applier's array
  parameter-binding required its general `[]any` element shape and rejected
  the string slice with a loud `expected []any for Array column, got []string`
  — so a MySQL→PostgreSQL stream with a `SET` column halted at apply (loud, no
  silent loss). The applier's array binding now accepts the string-slice shape
  and routes it through the **identical** conversion path a native `text[]`
  uses, so the `SET` lands as its member labels. (The cold-start COPY path was
  unaffected — it carries the source `SET` type and pgx already encoded the
  string slice as `TEXT[]` directly.)

## Compatibility / notes

- No flag or config change.
- Scope: **MySQL source → PostgreSQL target, `SET` columns, over CDC.** The
  cold-start (initial copy) path already handled `SET` → `text[]`;
  MySQL→MySQL `SET` sync was never affected.
- Completes the `SET` story begun in v0.99.56 (binlog decode) — decode +
  apply now both correct end-to-end for MySQL→PostgreSQL.

## Who needs this

- Anyone running continuous MySQL→PostgreSQL sync with `SET` columns.

## Install

```
# Linux/macOS/Windows binaries + checksums on the release page.
docker pull ghcr.io/sluicesync/sluice:v0.99.58
```
