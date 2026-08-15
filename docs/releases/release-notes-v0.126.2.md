# sluice v0.126.2

**A security patch.** The 2026-08-14 repository audit found and reproduced a bypass of the PostgreSQL restore-injection door that v0.124.0 shipped to close exactly this class: on a default plaintext, unsigned backup chain, a tampered or corrupt manifest could execute arbitrary SQL at restore. This release closes both bypasses and pins the door against a real server so the class stays closed. It also carries two correctness fixes from the pre-release measurement pass. **Upgrade recommended for anyone who restores from backup chains they do not fully control the storage of.**

## Security

**The PostgreSQL restore-injection door is no longer bypassable by a comment or a dollar-sign-in-identifier (audit C-1, CRITICAL).** The single-statement validator shipped in v0.124.0 had two lexical divergences from PostgreSQL, each able to hide a top-level `;` from the validator while the server executed past it: it modeled no `--` or `/* … */` comments (so an apostrophe *inside* a comment opened a validator-only string literal that scanned over an injected statement), and its dollar-quote opener checked no preceding byte (so `col$q$ … ; … $q$` — a single identifier token to PostgreSQL — looked dollar-quoted to the validator). On the default `backup full` (plaintext, unsigned), a party able to write to the backup store, or a corrupt manifest, could therefore run `DROP TABLE` or `CREATE ROLE … SUPERUSER` as the restore role, at exit 0, with `backup verify` reporting the chain healthy — the audit reproduced this end-to-end against real PostgreSQL 16.

The validator now models PostgreSQL comments, including their nesting, and treats a `$` that continues an identifier as an ordinary byte rather than a quote opener. The durable protection is a **differential lexer oracle**: a new integration test diffs the door's verdict against a real PostgreSQL server for a corpus of injection attempts spanning every PostgreSQL lexical form, asserting the door never passes a string the server executes an injection for, with a floor that requires the corpus to contain genuine server-side injections. So the next unenumerated lexical construct fails a test against the real server rather than silently reopening the class. Signing a chain (`--sign` with `--require-signature`) already closed this when enabled; the door now holds on unsigned chains too, which are the default.

Who is affected: anyone who restores (or replays via the broker, or runs `sync from-backup`) from a backup chain stored somewhere a third party can write, without chain signing enabled. If that describes you, upgrade before your next restore; and consider enabling chain signing, which defends the manifest as a whole, not only this door. Chains you take and restore yourself on trusted storage were never exposed to a hostile manifest.

## Fixed

**A MySQL `log(x)` CHECK no longer lands on PostgreSQL silently enforcing a different predicate.** The pre-release overload measurement found that MySQL's single-argument `LOG(x)` is the natural logarithm while PostgreSQL's `log(x)` is base-10, and the untranslated expression *lands* on PostgreSQL (the overload exists) — so a migrated CHECK like `log(c) <= 5` bounded the column at e⁵ on the source and 10⁵ on the target, at exit 0. The expression translator now renames single-argument `log` to `ln`; the two-argument form is untouched. A `power(x, y)` CHECK that was wrongly refused at preflight (MySQL's catalog canonicalizes it to `pow`, which PostgreSQL also accepts) now lands.

**`schema diff` no longer phantom-reports the translator-rewritten CHECK families.** A MySQL `json_extract(j,'$.k')` lands on PostgreSQL as `(j -> 'k')` — correctly — and `schema diff` then compared the source spelling against the target's read-back and reported drift on a target `migrate` itself just created. The diff now renders the expected side through the target writer's own dialect translation, so all four rewritten families (`json_extract`, `json_unquote`, `date_format`, `log`) diff clean after a real migrate, while a genuinely changed predicate still reports.

## Compatibility

No flag, error code, or on-disk format changed. The single-statement door refuses strictly more (the two bypass shapes it previously admitted); every legitimately single statement it accepted before, it still accepts. **Drop-in upgrade from v0.126.1.** No past *data* is at risk from these fixes — but if you have restored from a chain on untrusted storage since v0.124.0, treat that target as potentially tampered and re-verify it against a trusted source.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.126.2
```

Container images: `ghcr.io/sluicesync/sluice:0.126.2` (multi-arch; the image tag carries no `v` prefix).
