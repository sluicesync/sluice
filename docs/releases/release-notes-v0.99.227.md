# sluice v0.99.227

**Three quality-and-hardening fixes from the v0.99.221→224 confirming audit's carried tail: a mistyped config key is now a loud error instead of silently dropping a PII-redaction rule; the MySQL NaN/±Infinity float refusal is now a registered coded error; and compile-time pins close a class of silent runtime downgrades. No behavior change for a valid run.**

## Fixed

- **A mistyped config key is a loud error, not a silent drop — closing a PII trap (N-10).** The base config loader silently ignored unknown/misspelled YAML keys. The trap: a typo'd `redactions:` block (e.g. `redaction:`), or a misspelled field inside one (`tabel:` for `table:`), left the redaction list empty — the operator believed PII was being masked while the data flowed to the target unredacted, a compliance-grade silent failure. The loader now rejects any unknown key loudly (mirroring the fleet loader, which already did this). Every documented config key is a real field, so only genuinely-unsupported keys are refused; the documented sample config and every existing config still load unchanged. If you deliberately kept extra keys in your `sluice.yaml`, remove them or move them out of the file.

- **The MySQL NaN/±Infinity float refusal is a registered coded error (DEVEX-D2).** The guard that refuses a `NaN`/±`Infinity` float into a MySQL `FLOAT`/`DOUBLE` (which MySQL cannot represent — it would otherwise corrupt the value or retry-loop on the server's misleading error) emitted `SLUICE-E-VALUE-UNREPRESENTABLE` as a raw string in the message, so it was never in the error registry: automation keying on the code missed it, the refusal exit class didn't apply, and it was absent from the operator error-codes reference. It is now a first-class coded refusal, in the registry and documented.

- **Compile-time pins close a class of silent runtime downgrades (ARCH-F1).** Several optional fast paths are dispatched by runtime type-assertion (`x.(ir.FloatRepairWriter)` etc.), so a method-set break — a signature or receiver change — compiled clean and silently downgraded to a fallback. The concrete case: a drift in the Postgres FLOAT-repair writer compiled clean and Postgres silently skipped the cold-start FLOAT re-read repair, shipping display-rounded floats (MySQL had an integration pin; Postgres had none). Added blank-var compile-time assertions for the Postgres/MySQL FLOAT-repair writer, the VStream lossy-float copy-reader that triggers the repair, and the four backup encryption/signing envelope extensions, so a future break is a build error rather than a silent runtime downgrade. Compile-time only; no behavior change.

## Compatibility

**No behavior change for a valid run.** The config loader change only affects a config file that contains an unsupported key (previously silently ignored, now a loud error naming the fix). The coded-error change is machine-readability only — the refusal fired before and fires now. The compile-time pins add no runtime code.

## Who needs this — action required

- **If you use a `sluice.yaml` config, re-run once after upgrading** — a previously-silent typo (especially in a `redactions:` block) will now surface loudly, which is the point: it means a rule you thought was active was not. Otherwise nobody needs to act.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.227 · **Container:** ghcr.io/sluicesync/sluice:0.99.227
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
