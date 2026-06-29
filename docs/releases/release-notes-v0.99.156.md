# sluice v0.99.156

**Fixes Bug 171: the `--type-override` remedy that v0.99.155 prints for a TEXT primary key didn't copy-paste, because the override parser rejected the uppercase type name it suggested. Type names are now case-insensitive. No data loss in any version (the override failed loudly); this is a usability fix that makes the suggested command work verbatim.**

## Fixed

**LOW — `--type-override` rejected an uppercase/mixed-case type name, so the v0.99.155 Bug-170 remedy did not copy-paste (Bug 171).** The Bug-170 refusal message suggests `--type-override <table>.<col>=VARCHAR(n)` (uppercase, as SQL conventionally writes type names), but the override parser matched the type name case-sensitively against its lower-case set — so `VARCHAR(255)` failed with `type "VARCHAR" does not take parenthesised arguments`, and an uppercase bare name such as `BIGINT` failed to resolve. The operator had to know to lower-case it. Loud, zero data loss — but the suggested remedy didn't work as printed.

**Fix.** SQL type names are case-insensitive, so the override now canonicalises the type name to lower case — both in the CLI spec parser (so `VARCHAR(255)`, `Decimal(20,2)`, a bare `BIGINT`, and any other casing all parse) and in the shared resolver (so the YAML `mappings:` input path is covered too). Any casing now resolves identically, and the remedy the Bug-170 message prints works verbatim.

## How it was found

The v0.99.155 PlanetScale-MySQL round: migrating the TEXT-primary-key corpus tables, copy-pasting the refusal message's own `--type-override …=VARCHAR(255)` suggestion failed — the fix's remedy contradicted the parser.

## Compatibility

Behavior-preserving: lower-case type names (every spelling that already worked) are unchanged; this only additionally accepts the upper/mixed-case spellings SQL treats as equivalent. Pinned by parser unit cases (uppercase `VARCHAR(n)`, bare `BIGINT`, mixed-case `Decimal(p,s)`) and a resolver case-insensitivity test.

## Who needs this

Anyone using `--type-override` (or a YAML `mappings:` block) who writes type names in upper or mixed case — including anyone following the v0.99.155 TEXT-primary-key remedy verbatim.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.156 · **Container:** ghcr.io/sluicesync/sluice:0.99.156
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
