# Security policy

## Reporting a vulnerability

If you believe you've found a security vulnerability in sluice, please report it privately rather than opening a public GitHub issue.

**Preferred channel:** [GitHub Security Advisories](https://github.com/sluicesync/sluice/security/advisories/new). Click "Report a vulnerability" to start a private disclosure thread visible only to maintainers.

If GitHub Security Advisories is unavailable for any reason, you can email **orware@gmail.com** with the subject line `[sluice security]`. Encrypted reports via the maintainer's public key are welcome but not required.

Please include, at a minimum:

- A description of the vulnerability and the impact you believe it has.
- Steps to reproduce, including the sluice version, engine versions (MySQL/Postgres), and any relevant configuration.
- Any proof-of-concept code or sample data, with sensitive data redacted.

## What to expect

- Acknowledgment within 72 hours of your report.
- An initial assessment within one week, including whether we accept the report as a vulnerability and a rough severity classification.
- If the report is accepted, regular updates as a fix is developed. If it's not accepted (e.g. the behavior is intentional), a clear explanation.
- A coordinated disclosure timeline. We aim for fixes within 30 days for high-severity issues; lower-severity issues may take longer. We'll always communicate the timeline before publishing.

We will credit you in the release notes and the published advisory unless you prefer to remain anonymous.

## Scope

Sluice's threat model assumes a trusted operator: the user running `sluice migrate` or `sluice sync` is granting the tool the privileges of their database credentials, and the source/target databases are within their control. Issues we treat as in-scope include:

- **Credential handling.** DSNs are passed via flags or environment variables. Anything that causes them to leak (logs, error messages, on-disk artifacts) is in scope.
- **Source-data tampering.** Anything that lets a malicious source produce output that compromises the target beyond the expected schema-and-data copy (e.g. SQL injection through a maliciously crafted column name surviving DDL emission).
- **Misuse of replication slots / binlog access.** Sluice asks for elevated privileges; bugs that misuse them are in scope.
- **Memory or filesystem leaks** that expose data across migrations or beyond the lifetime of a single `sluice` invocation.

Out of scope:

- Denial of service against the source or target arising from the user's own configuration choices (e.g. running a migration without a maintenance window).
- Issues in dependencies that are not exploitable through sluice's API surface — please report those upstream.
- Behaviour that requires the operator to already have privileged access they shouldn't have (privilege escalation against the database itself is the database's concern).

## Supported versions

While the project is in `0.x`, only the latest minor release line is supported for security fixes. Once `1.0` ships, we'll publish a longer support window in this document.

## Defensive practices

If you're operating sluice in a sensitive environment, a few hardening notes:

- Run with the least-privileged DB credentials that work for your migration. The CLI honours read-only DSNs for source where the operation allows it.
- Avoid placing DSNs in shell history or repository-tracked files. The `SLUICE_SOURCE` / `SLUICE_TARGET` environment variables are loaded from your environment; combine them with a secret manager.
- The `--config` YAML may contain sensitive overrides; treat it like a secrets file.
