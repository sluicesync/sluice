# sluice v0.96.0

## v0.96.0 — CLI-overrides-YAML on redaction rules

Closes the last compliance-critical silent-loss class in sluice's redaction layer: an operator passing BOTH a YAML `redactions:` block AND a `--redact` CLI flag for the SAME column now gets the documented "CLI overrides YAML" precedence — instead of silently inheriting the YAML strategy.

### Fixed

- **Bug 108 — silent policy substitution when CLI and YAML both target the same column.** Pre-fix the YAML merge step ran AFTER CLI parsing and called `redact.Registry.Set` unconditionally, silently overwriting any CLI rule for the same column. An operator who inherited a weak team-template YAML strategy (e.g. `static:redacted`) and tried to override per-run with a stronger CLI strategy (e.g. `hash:hmac-sha256`) would silently get the team-template's weak strategy. v0.96.0 has `mergeYAMLRedactions` check `reg.Get(schema, table, column)` for each YAML entry; when a CLI rule is already present, the YAML entry is skipped with a loud `slog.Warn` naming the column AND the YAML strategy that was skipped. Operators now get a clear "your YAML rule was overridden by your CLI flag" signal instead of silent substitution. Pinned by `TestMergeYAMLRedactions_CLIOverridesYAML` (2 sub-pins covering BUG-CATALOG.md Bug 108 variant A `YAML hash + CLI static → CLI static wins` and variant B `YAML static + CLI hash → CLI hash wins`).

### Compatibility

- Existing config files and `--redact` flags continue to work; the change is purely the precedence-collision behaviour (was: YAML wins silently; now: CLI wins with a loud `slog.Warn` on the displaced YAML entry).
- No manifest version bump. No CDC or backup format change.
- Manifest `FormatVersion` unchanged from v0.94.1's bump (still v1 for innocent schemas / v2 for security-metadata-bearing schemas).

### Who needs this

- **Operators who layer YAML config + CLI overrides for redaction**, especially in CI / team-template setups where a baseline `redactions:` block lives in a shared YAML and per-run overrides come via CLI. Before v0.96.0 those overrides were silently ignored for any column already in the YAML.
- **Compliance-driven migrations** where the difference between `static:redacted` and `hash:hmac-sha256` matters (cross-run consistency for hashed PII vs. constant placeholder). Bug 108 was caught only by comparing dst output against expected output — silent enough to land in prod.

### Open backlog after this release

Bug 114 + PG→MySQL DOMAIN-CHECK table-level emit follow-up — the only two items remaining on the public backlog post-v0.95.3.
