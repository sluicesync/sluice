# sluice v0.143.0

**Two silent-loss fixes, both found by a blind audit and both measured before and after.** A multi-schema Postgres sync could resume from a stopped-stream `ALTER` and apply rows against a target column that no longer matched the source — target reading `12:00:00` where the source read `21:00:00`, at exit 0. And a Vitess/PlanetScale cold copy running with `vstream_copy_table_parallelism >= 2` silently dropped statement-format DML instead of refusing it.

If you run multi-schema Postgres sync, or parallel VStream copy, take this one.

## Fixed

**A multi-schema Postgres sync now refuses a stopped-stream zone swap instead of priming (SLM-1d).** The first-boundary session-zone-cast refusal compares an incoming schema against a *seed* — the shape the stream last committed under. The two single-stream reader-open sites installed that seed; the two multi-database sites wired the scope predicate and nothing else, and the fan-out never set a seed loader at all, so calling the helper there would have been a no-op. The measured harm, reproduced on real postgres:16 both before and after: stop the stream, `ALTER TABLE ... TYPE timestamp` under `Asia/Tokyo`, resume — the row lands, the target column is unchanged, and source and target disagree by the session offset with nothing reported. The fan-out now builds a namespace-partitioned seed on both the cold-start and warm-resume legs. Postgres is covered; a MySQL multi-database stream still cannot fire this refusal at all, which is now stated in the runbook's residual list rather than left out of it.

**The concurrent VStream COPY pump refuses statement-format DML like its three siblings.** `dispatchCopyEvent` had no statement-DML arm and fell through to a silent `default`, so under `vstream_copy_table_parallelism >= 2` — the only condition where that pump is in use — statement-logged writes were ignored rather than refused. Under `binlog_format=STATEMENT` or `MIXED` the server emits no row events at all, which is precisely the loss the refusal exists to catch. The dispatcher universe is now derived from the source rather than hand-listed, and the environmental premise the whole class rests on — that the vendored vstreamer *forwards* these events rather than absorbing them — is pinned against the module's own source instead of living in a comment.

**A table-filter pattern that matches nothing no longer cries wolf.** v0.142.0 added `TABLE-FILTER-PATTERN-UNMATCHED` because an `--exclude-table` matching nothing fails **open** — the table you meant to keep out is copied at exit 0. Three false-fire arms have since been closed: the Postgres scope push-down (v0.142.1), engine-default `_vt_*` exclusions on PlanetScale, and multi-database runs, which called the filter door once per database and so reported a pattern naming a table in database B as dead while database A was being copied. A fan-out now stays quiet per pass and reports **once**, against the union of every selected database's tables.

**Every remedy sluice prints can now be pasted.** A refusal fires mid-incident and its remedy is the whole recovery path; `sluice trigger setup` has two required flags, so "re-run `sluice trigger setup`" produced a second refusal — "--dsn is required" — leaving you two failures deep on sluice's own advice. 51 messages across nine commands were fixed, and where the values are known the remedy now names your actual tables rather than a placeholder. A gate derives each command's required flags from the real CLI model, so this cannot regress quietly.

## Compatibility

Drop-in from v0.142.1. No flag, format or schema change.

**Remedy text changed in many error messages.** If you match sluice's output with scripts or alerts, check anything keyed on the exact wording of a `trigger setup` / `sync start` / `migrate` remedy. Error codes and markers are unchanged.

**One new refusal can fire where nothing fired before:** a multi-schema Postgres sync resuming across a stopped-stream `timestamptz`/`timestamp` swap now refuses instead of priming. That is the fix. Apply the same `ALTER` on the target via the drained model and restart with the same `--stream-id`.

## Who needs this

- **Multi-schema Postgres sync** — the silent-divergence fix.
- **Vitess / PlanetScale with parallel VStream copy** (`vstream_copy_table_parallelism >= 2`) — the silent row-drop fix.
- **Anyone using `--include-table` / `--exclude-table`**, especially on multi-database runs or PlanetScale.
- **Anyone who has ever pasted a remedy out of a sluice refusal.**

## Development notes

This release was shaped by a full blind audit — five independent workers plus a reconciler — and the fixes above are its top findings. Two things are worth recording.

**Two gates certified the defect they existed to prevent.** One counted a comment as evidence, so disabling a warning while leaving its explanation in place kept the gate green. The other used a fixed lookahead window that ran past one call site into the next, so an unclassified site was satisfied by its neighbour's evidence. Both were mutation-proved by the audit and both now catch that exact mutant.

**The release's own pre-tag review found a defect in the release's own new code**, for the seventh time in eight releases — and this time the worst item was a remedy rewrite that made things worse: naming `--dsn` and `--tables` but not `--source-driver` meant a paste on the SQLite/D1 lane silently ran the *Postgres* installer, where before it had failed fast. Fixed at all seven sites before the tag.
