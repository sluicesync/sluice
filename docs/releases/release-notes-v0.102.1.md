# sluice v0.102.1

**Two ways a continuous sync could stop and stay stopped are now self-healing — and one of them is a correction: the schema-change fix announced in v0.101.0 did not work.** If you run continuous sync against PlanetScale or Vitess, take this release. Drop-in upgrade, no breaking changes, no flag changes.

## Corrected

**v0.101.0 said a schema change on the keyspace no longer kills the stream. It did not — the fix was inert, and the stream also wedged.** That release note claimed a `CREATE TABLE` on any table would become a reconnect. Testing against a real vtgate afterwards showed the stream still died with `row event for … without preceding FIELD event`, naming a different long-established table, **with zero retry attempts** — and then could not be resumed at all: three consecutive warm resumes died at the same position while the source moved on. The claim was wrong in both halves, and this release is what makes it true.

Two independent defects were in the way. **The retriable classification was unreachable**: it was applied only to receive failures, while anything raised while *interpreting* a change event was stored unclassified, and the FIELD-cache miss is raised there — so the carve-out shipped in v0.101.0 could never engage. It went out with a passing unit test that exercised the classifier directly, which pinned the function rather than the path it is reached by. **And reachability alone was not enough**: with retries firing, the stream still died at the same position forever, because a warm resume replays the same schema change *after* the fresh column-shape announcements and the blanket cache invalidation wipes them again.

Both are fixed. Errors raised while interpreting an event are now classified, which also restores every other retriable carve-out that was equally unreachable from that path. And the post-DDL invalidation is now scoped to the table the statement actually names — every shard's entry for it — falling back to the previous clear-everything behaviour for statements that cannot be attributed. Scoping is safe because the source announces a table's new column shape before any row of that shape: a stale entry is overwritten before it can be used, so a parse miss degrades to the old behaviour rather than to a wrong decode.

Verified against a deterministic reproduction on a real vtgate: previously the post-DDL row on a long-established table never applied and three warm resumes died at the same position; now the row applies and all three resumes come back live with zero retries.

## Fixed

**A dropped table no longer strands a stream permanently.** If a table is dropped while a sync is stopped, the persisted position can point into a window whose events reference a relation that no longer exists. The source cannot build a replay plan for it, so every resume failed identically and exhausted its retry budget — the stream was **unrecoverable** without manual intervention, and the terminal message named neither the cause nor the way out even though the retry lines had been printing both.

Retrying first is still correct: the same condition also covers a transient window where the source's schema tracking is catching up, and the two are indistinguishable at first failure. What changed is the disposition once the budget is exhausted, which is the point at which the window is demonstrably unreplayable. It is now treated as what it is — a position that cannot advance — and takes the same recovery an expired position already takes: an automatic cold-start re-snapshot, or a loud refusal with the recovery commands under `--no-auto-resnapshot`. The one-shot-per-run bound is unchanged, so an unreplayable window costs at most a single re-copy.

Validated on the stream that produced the bug: after three failed manual relaunches it recovered on its own, re-copied 262,740 rows, and resumed change capture with no operator action.

## Compatibility

No breaking changes, no flag changes, drop-in upgrade. Both fixes only convert previously-fatal conditions into recoveries, so a stream that never met them behaves identically. The scoped invalidation is strictly narrower than before and falls back to the previous behaviour whenever a statement cannot be attributed. The unreplayable-window recovery honours `--no-auto-resnapshot` exactly as the existing expired-position path does — if you have that flag set, you keep getting a loud refusal with recovery instructions rather than an automatic re-copy.

## Who needs this — action required

- **Anyone running continuous sync against PlanetScale or Vitess who read v0.101.0's schema-change note** — that fix did not work; this one is tested against a real server. If you have been restarting a stream after a `CREATE TABLE`, that is why.
- **Anyone whose stream exited `apply retry budget exhausted` after dropping a table** — it will now recover itself. Upgrade and restart; the exit was loud and lost nothing.
- Everyone else: no action needed beyond upgrading.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.102.1
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:0.102.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
