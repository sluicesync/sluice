# sluice v0.132.2

**The correction release.** Four items the v0.132.1 notes described missed that tag — a landing step ran against the wrong working tree, caught by the post-publish learnings sweep's tag-tree ground-truthing — and ship here, verified present at this tag. The v0.132.1 notes now carry a correction banner. No other changes; drop-in from v0.132.1; no new error codes.

## Fixed

**Every pgtrigger open-path probe is now bounded at 15 seconds** (audit A5). The relay-shape probe read a lockable user table, so a queued `ALTER`/`VACUUM FULL` behind a long transaction could park every CDC open indefinitely with no output — a WARN-only detector wedging the stream it protects. All five open-path probes (the audit named three; a sibling sweep found two more) derive bounded contexts: WARN-probes degrade to their probe-error WARN on timeout, and the fail-closed capture-shape door refuses with a timeout-specific message. A cross-engine AST roster gate asserts every CDC-open probe in all three engines derives a timeout.

**`schema add-table` refuses an UNLOGGED table before any side effect** (audit A7). Adding an unlogged table live to a spanning Postgres sync previously blocked late — after creating the target table — with a misleading message; the registration path now runs the coded `SLUICE-E-CDC-UNLOGGED-TABLE` census before the dry-run report, target DDL, or snapshot, with the `SET LOGGED` / `--exclude-table` remedies.

**The `SLUICE-E-CDC-STATEMENT-DML` sanitized lead also cuts at `=`.** v0.132.1's quote/paren-only cut let unquoted numeric literals (`UPDATE t SET ssn=078051120`) survive inside the 80-byte cap; the lead now stops at the first quote, paren, or `=`, so string *and* numeric values never reach the error — pinned and mutation-verified.

## Internal

G2's two environmental premises (a `FOR ALL TABLES` publication silently excludes unlogged tables; `ALTER TABLE … SET UNLOGGED` succeeds under it but is refused for scoped `FOR TABLE` membership) are pinned directly against real Postgres and ride the version matrix (audit A8). The landing-verification gap behind the miss is filed with a gate proposal — a notes-claims-vs-tag-tree check — in the audit backlog.

## Compatibility

Drop-in from v0.132.1 — no schema, format, flag, or command change; no new error codes. Operators who upgraded to v0.132.1 for the probe bounds or the add-table census specifically: those land here.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.132.2
```

Container images: `ghcr.io/sluicesync/sluice:0.132.2` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
