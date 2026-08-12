# sluice v0.121.0

The first correctness batch from the 2026-08-11 periodic blind audit. Three confirmed silent-loss defects — a value or table silently altered on a run that exits 0 — each observed live against a real database and each closed with a mutation-verified gate.

## Fixed

**A source table folding onto a pre-existing target table no longer silently merges into it.** On a case-folding target — SQLite always, MySQL under `lower_case_table_names != 0` — the pre-create shape gate looked the target catalog up case-sensitively, so a source `Orders` missed a stored `orders`, read as absent, and `CREATE TABLE IF NOT EXISTS` no-opped while the copy landed the `Orders` rows in the unrelated table: exit 0, zero warnings, both datasets merged (reproduced live on two SQLite files). The gate now folds every catalog lookup through the target's own identifier rule (a new `ir.TargetTableNameFolder` surface). An incompatibly-shaped fold-hit takes the existing `SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH` refusal; a matching-shape fold-hit proceeds (the resume/bootstrap case) but WARNs when the spellings differ, so the merge is never silent. Case-sensitive targets (Postgres, MySQL `lower_case_table_names=0`) are byte-exact and unchanged.

**`schema diff` no longer certifies a backslash-weakened CHECK constraint as in sync.** The CHECK canonicalizer dropped backslashes inside string literals as MySQL escapes, but both diff inputs reach it in SQL-standard spelling (backslashes bare — each engine re-escapes only at its own emit boundary). So a weakened regex `'^a\d+$'` (digits required) canonicalized onto `'^ad+$'` (accepts `add`) and the drift-detection tool reported a weakened target constraint as intact. Backslash is now an ordinary value byte; only the SQL-standard `''` doubling is an escape.

**A server-side `max_error_count=0` no longer hides a silent value clamp on the MySQL write path.** sluice's silent-clamp refusal reads the `SHOW WARNINGS` row count, which the server truncates at `@@max_error_count`; `max_error_count=0` — a legal bulk-load memory tuning — leaves `@@warning_count` accurate but `SHOW WARNINGS` empty, so a truncating `LOAD DATA` committed the clamped value and exited 0. sluice now reads the uncapped `@@warning_count` directly as the trigger (which needs no privilege — unlike setting `max_error_count`, which requires `SESSION_VARIABLES_ADMIN` on MySQL 8.0), so a suppressed warning buffer no longer hides a clamp.

## Compatibility

No error code, flag, or on-disk format changed. **Drop-in upgrade from v0.120.2 and older.** Three behaviour notes: a migrate/sync-cold-start onto a folding target whose source table folds onto an incompatibly-shaped pre-existing table now refuses (`SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH`) where it previously merged silently, and a matching-shape fold onto a differently-spelled table emits one WARN; `schema diff` now reports a backslash-differing CHECK as drift; and a MySQL bulk write against a server tuned with `max_error_count=0` now surfaces a value clamp instead of committing it silently.

## Who needs this

Anyone migrating or syncing into **SQLite** or a **case-insensitive MySQL** target (Windows/macOS servers and some managed tiers run `lower_case_table_names != 0`) where a source and a pre-existing target table can collide under folding; anyone relying on **`schema diff`** to detect drift in regex/backslash-bearing CHECK constraints; and anyone running MySQL bulk loads against a server tuned with `max_error_count=0`. If none of these describe your setup, the upgrade is a no-op.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.121.0
```

Container images: `ghcr.io/sluicesync/sluice:v0.121.0` (multi-arch).
