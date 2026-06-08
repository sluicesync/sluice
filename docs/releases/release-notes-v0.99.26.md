# sluice v0.99.26

**`--type-override` now takes parenthesised precision/length from the CLI** — `decimal(20,0)`, `numeric(20,0)`, `varchar(255)` — and the range-overflow advisories now point at a remediation that actually works from the command line. Previously a precision-bearing decimal override could only be set via the YAML `mappings:` form. **Drop-in from v0.99.25.**

## Added

- **CLI parenthesised type-override forms.** `--type-override TABLE.COL=decimal(20,0)` (and `numeric(20,0)`, `decimal(20)`, `varchar(255)`) now parse correctly, populating the precision/scale/length that previously required the YAML `mappings:` + `target_type_options` form. `numeric` is accepted as an alias for `decimal` (the Postgres spelling). Bare names (`text`, `jsonb`, `smallint`, …) are unchanged. Malformed specs — unbalanced or empty parentheses, non-integer or wrong-arity arguments, parentheses on a type that takes none — are rejected with a clear error rather than silently ignored.

## Fixed

- **Range-overflow advisories pointed at a CLI remediation that never parsed.** The unsigned-bigint range-narrowing notice, the unconstrained-numeric widening notice, the `schema preview` output, and the LOAD-DATA recovery hint all recommended `--type-override COL=decimal:precision=N,scale=M` — but the CLI flag treated that whole string as a bare type name and failed with "unknown target_type". They now recommend the working `--type-override COL=decimal(N,M)` form (e.g. `decimal(20,0)`, which carries a full unsigned-64 value into PG `numeric(20,0)`). The YAML `mappings:` form was always available; this makes the CLI path the docs pointed at actually function.

## Compatibility

- No breaking changes. Drop-in from v0.99.25. Every existing bare-name override keeps working; the paren forms and the `numeric` alias are additive. (Note: the colon form `decimal:precision=N,scale=M` was never parsed by the CLI and is not added — use the paren form or the YAML `mappings:` form.)

## Who needs this

- **Anyone who needs a precision-bearing decimal (or a sized varchar) override from the command line** — most commonly to carry a MySQL `BIGINT UNSIGNED` value above 2^63-1 into PG with `--type-override TABLE.COL=decimal(20,0)`. The advisory messages now name exactly that working command.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.26`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.26`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
