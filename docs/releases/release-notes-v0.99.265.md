# sluice v0.99.265

Three defects found by the Supabase IPv4 validation — including a CRITICAL silent-loss class observed live through a real CDC stream, and closed across every face it had.

## ⚠ Action required for postgres-trigger CDC users

**Re-run `sluice trigger setup` on every trigger-CDC source after upgrading.** Part of the float-exactness fix lives in the capture function's own definition; installed triggers keep capturing rounded floats until setup re-runs `CREATE OR REPLACE`.

## Fixed

- **CRITICAL (Bug 194): PG floats rendered as text for transit are now shortest-exact regardless of the source's `extra_float_digits` server default.** Supabase ships 0 — and the validation watched π arrive rounded through a live CDC stream. Four faces pinned: the raw-copy text lane, the pgoutput stream (the walsender renders tuple text under its own session), the trigger capture function, and `verify --depth=sample` hashes — which previously could produce both false mismatches and false *cleans*, blessing exactly the corruption verify exists to catch. The pins are transaction-scoped, so they hold through transaction-mode poolers (validated against a real pgbouncer in transaction mode — the mode where a plain session `SET` silently lands on the wrong backend), and every pgx pool sluice opens carries a per-connection belt, keeping the typed lanes exact even under `simple_protocol` DSNs.
- **Bug 195: array-element type modifiers thread for every parameterized family** — `varchar(n)[]` no longer false-refuses as VARCHAR(0), `numeric(p,s)[]` keeps precision/scale, bare varchar forms map to unbounded TEXT, and PG 15+ negative numeric scale reads correctly (information_schema itself mis-reports it; sluice decodes the catalog typmod directly).
- **Bug 196: the IPv6-only connection hint now fires on IPv4-only Windows hosts** (its target audience) — DNS-direct AAAA probing instead of `getaddrinfow` — and its remediation names the session-mode pooler specifically.

## Compatibility

- **No breaking changes.** The trigger-setup re-run is the one operational step. All session pins are scoped and leak nothing.

## Who needs this

**Anyone moving float data out of a Postgres source whose `extra_float_digits` is below 1 — most prominently every Supabase project — should upgrade before their next run and, on trigger-CDC sources, re-run setup.** Rows already migrated under the old behavior can be re-verified with `verify --depth=sample` (whose own rendering is now pinned) and repaired with touch-updates or a re-copy.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.265
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.265`
