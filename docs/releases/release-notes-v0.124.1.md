# sluice v0.124.1

**Patch: the MySQL-family→PG untranslatable-expression refusals now name a remedy that actually runs at the site they fire on (Bug 247).** Found by the v0.124.0 post-release regression cycle within hours of publish: the refusals — including v0.124.0's new operator-family messages and the older Bug 242 REGEXP/RLIKE advisories — told operators at CHECK-constraint and DEFAULT sites to "use `--expr-override`", but that flag rewrites generated-column bodies only and errors on any other target, so the printed recovery was a dead end (the same unrunnable-hint class as Bugs 245/246). Loud, zero silent loss, refusal-side UX only. Upgrade if one of these refusals fired on a CHECK or DEFAULT expression and you followed its hint into "the column is not a generated column"; everyone else is unaffected.

## Fixed

**Remedies are now a property of the site, not the pattern.** Generated columns keep `--expr-override` (it genuinely runs there — the override rewrites the column's body and the gate then skips it). CHECK-constraint and DEFAULT sites now get the escapes that run: `--exclude-table`, or fixing the expression on the source and re-creating it on the target in PostgreSQL syntax after the run — with an explicit "`--expr-override` does not apply — it rewrites only generated-column bodies" clause, the same wording sluice's extension-function gate has carried since ADR-0044 §2's post-implementation correction, which never got swept to these sibling gates. Both refusal layers branch per site (the curated gap catalog and the general backstop), and `schema preview` gains the same fix on both of its surfaces: the text advisory block prints a per-gap remedy line, and the JSON output adds a `remedy` field alongside each translator gap's `note`.

**The pins now assert the recommendation, not the substring.** The earlier rendering pins checked that refusal messages contain `--expr-override` — a check the new denial clause would also satisfy, so they could green either way. They now pin the phrasing per site in both directions: a CHECK/DEFAULT-site refusal must offer `--exclude-table` and state the override does not apply, and must never recommend it; a generated-site refusal must recommend it and carry no denial. A premise pin binds `ApplyExpressionOverrides`' generated-only scope to the message design, so if overrides ever learn CHECK/DEFAULT targets the wording pins flip with it instead of silently denying a flag that started working. All four message arms were mutation-run in both directions.

**The docs carried the root drift.** The translator catalog's own flag reference claimed `--expr-override` rewrites "a column's DEFAULT or GENERATED expression" — the false sentence the refusal texts were downstream of. Corrected, along with the catalog's per-rule workaround column, production-readiness, and the comparison page. v0.124.0's release-notes sentence "every refusal carrying a runnable remedy" was corrected in place, dated, in all three claim homes.

## Compatibility

No error code, flag, or on-disk format changed — refusal and advisory message text is updated, and `schema preview --format json` adds a `remedy` string to each `translator_gaps` entry (additive; existing fields unchanged). **Drop-in upgrade from v0.124.0.**

## Who needs this — action required

**Anyone whose MySQL/MariaDB→PostgreSQL `migrate` or `schema preview` refused on a CHECK constraint or DEFAULT expression and hit "the column is not a generated column" following the old hint**: upgrade and re-run — the message now names the escapes that work (`--exclude-table`, or fix the expression on the source and re-create it on the target in PG syntax). **Tooling that parses `schema preview` JSON** can start reading the per-gap `remedy` field instead of deriving guidance from `note`. If none of these refusals ever fired for you, this patch is a no-op.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.124.1
```

Container images: `ghcr.io/sluicesync/sluice:0.124.1` (multi-arch; the image tag carries no `v` prefix).
