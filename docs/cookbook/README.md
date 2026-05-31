# sluice cookbook

Practical recipes for sluice operators. Each recipe is **task-shaped** —
"I want to do X, what's the command sequence?" — not feature-shaped.
For the per-feature reference, see the documents linked from the
project's [main README](../../README.md).

The recipes assume you've worked through [`docs/examples/quickstart.md`](../examples/quickstart.md)
and have a sluice binary on your PATH. They don't reproduce the
quickstart's "install sluice" preamble.

## Recipes

| Recipe | When to use it |
|---|---|
| [Case study — migrating a GitLab-shape schema](case-gitlab.md) | You're evaluating sluice against a real-world schema with DOMAINs, range types, EXCLUDE constraints, and CHECKs. Validation of "does this actually work on a real codebase's DB?" |
| [One-shot migration MySQL → Postgres](recipe-migrate-once.md) | You want to move data once and stop. The simple `sluice migrate` path. |
| [Bidirectional cutover with sequence priming](recipe-bidirectional-cutover.md) | Live migration with low downtime — snapshot, sync, then cutover to the new target without PK collisions. |
| [Backup chain with at-rest encryption](recipe-backup-encrypted.md) | Periodic full + incremental backups with passphrase-based or AWS-KMS-based encryption. |
| [PII redaction with persisted keyset](recipe-redaction-keyset.md) | Replicate to a staging/analytics target with PII redacted but deterministic across runs. |

## What's not in here

The cookbook deliberately doesn't cover:

- **Feature reference** — see the per-feature docs in [`docs/`](../).
- **Tuning knobs that aren't part of a recipe** — see
  [`docs/throughput-tuning.md`](../throughput-tuning.md) for performance
  knobs and [`docs/postgres-source-prep.md`](../postgres-source-prep.md)
  for source-side prerequisites.
- **Internals** — see [`docs/architecture.md`](../architecture.md) and
  the [ADR index](../dev/adr/).

If you want a recipe that isn't here, open an issue describing the task
shape you're after. Recipes get added when the same shape comes up
twice — once is a one-off, twice is a cookbook entry.
