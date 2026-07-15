# sluice v0.99.252

**Fix: the `--csv-*` flags now refuse loudly on a non-flat-file source instead of being silently ignored (Bug 189, found by the v0.99.250 release's own regression cycle).** One-fix release; drop-in.

## Fixed

- **Bug 189 — `--csv-null` / `--csv-header` / `--csv-no-header` / `--csv-delimiter` on a source that isn't `csv`/`tsv`/`ndjson` now refuse loudly, naming the flat-file drivers (affected: v0.99.250–v0.99.251).** The flags configure the flat-file source drivers only, but on any other source engine they were silently ignored — an invocation like `--source-driver mysql --csv-null=NULL` ran the migration as if the flag weren't there, contradicting the documented refuse-loudly contract (an ignored `--csv-null` is exactly the kind of silently-dropped operator intent sluice refuses everywhere else). Zero data risk — the flags never influenced non-flat-file reads — the gap was purely the loudness contract. The check is source-scoped by construction: a TARGET engine still tolerates the flags, so `csv → mysql` keeps working (the flags belong to the other side of that run). The regression cycle also caught that the feature's own test suite had pinned the buggy "inert" behavior; that pin now asserts the documented refusal, alongside a dedicated source-vs-target pin.

## Compatibility

- **Drop-in; no breaking changes for correct invocations.** The only behavior change is that a previously-ignored `--csv-*` flag on a non-flat-file source is now a loud up-front error — if a script of yours passes such a flag, it was doing nothing; remove it or switch the source driver as the message says.

## Who needs this

Nothing to re-verify — the ignored flags never affected data. Pick it up if you script sluice with mixed source drivers and want misplaced `--csv-*` flags to fail fast instead of being dropped.

---

Install / upgrade: see the [README](https://github.com/sluicesync/sluice#install). Verify downloads against `checksums.txt`.
