# sluice v0.146.0

A guard that reached one lane of two, a refusal that promised protection while doing the opposite, and a documented recipe that could never have worked. Every item here was found by asking what a check actually covers rather than whether it passes — and two of them were graded lower than they deserved until someone re-derived them.

Take this one if you migrate from Cloudflare D1, run continuous sync from a **non-GTID** MySQL source, or have ever followed the PII-redaction cookbook's env-backed keyset option.

## Fixed

**The Cloudflare D1 reader never checked that the source renders floating-point values losslessly — only the trigger lanes did.** sluice reads a D1 `REAL` through `format('%!.20g', …)`, where the `!` alternate-form-2 flag is load-bearing: an engine that ignores it clamps to 16 significant digits and silently alters every value. That was a CRITICAL in v0.131.2, and the fix for it was deliberately *probed at runtime* rather than assumed, because "no known SQLite release ignores `!`" is a fact about builds sluice cannot enumerate. The probe reached the trigger readers and stopped there. The reason it stopped is worth naming, because nobody was careless: the rationale for that door was written in terms of triggers — "on D1 the probed engine IS the engine that fires the triggers" — and a `migrate` fires no triggers, opens no CDC stream, and so ran no probe, while projecting the identical expression against an engine Cloudflare controls and can change under a running install. The `--source-driver d1` bulk copy and `migrate --stage-local` now both probe at open (`verifyD1RenderFidelity`) and fail closed, matching the trigger lane. Measured against live D1 before shipping the refusal, with a discriminating control rather than a bare green: the production expression renders `0.300000000000000044` and round-trips bit-exact, while the same format without `!` renders `0.3` — so D1 honours the flag today, this does not fire on a working source, and the probe can still tell the two apart.

**A replaced MySQL source instance used to drop your target tables and re-copy from whatever answered the DSN.** When a persisted binlog file/pos position carried a `@@server_uuid` that no longer matched the source, sluice returned an error wrapping its "position is unusable" sentinel — which routes the automatic recovery, and on a binlog source that recovery drops every target table and re-copies. That is exactly right for a routine *purge*, where the same server has simply advanced past the position. It is the wrong answer here, because an identity change means a **different server**, and sluice cannot tell an intended replacement from a stale connection string or a load-balanced endpoint that landed somewhere else. It now refuses, terminally, naming both instance identities and a grep-stable `SOURCE-INSTANCE-IDENTITY-CHANGED`. What decided it was the asymmetry between the two outcomes: if the replacement was intended, refusing costs one `--restart-from-scratch` on an event that already involved a human rebuilding a database server; if it was not, the old path destroyed the target and repopulated it from the wrong database at exit 0, looking like success.

**And the message an operator saw at that moment said sluice was "refusing to resume to avoid a silent data gap"** — while it was about to perform the most destructive action in its repertoire. That is worse than silence, because nobody reading "refusing" has a reason to go and check which instance actually answered. Both the claim and the behaviour are now the same thing.

**The PII-redaction cookbook documented an env-backed keyset that sluice has never supported.** The recipe told you to export one variable per key holding raw base64 bytes and then pass a prefix, `--keyset-source 'env:SLUICE_KEYSET_'`, as though the loader would glob the environment. It does not, and never did: it reads one variable, named exactly, holding the whole keyset YAML — which is what ADR-0041 specifies and what `docs/redaction.md` has always said. Following the page verbatim produced `environment variable is empty or unset`, with nothing on the page to suggest the page was the problem. It fails loudly, so no data was ever at risk; what makes it worth a release note is how it survived. The recipe has an executable pin that runs on every release, and that pin reported PASS — it exercised the `file:` option and never touched the one that was broken. The page is fixed, the pin gained the missing option, and `TestDocumentedEnvKeysetExamplesLoad` now runs every runnable `env:` keyset example in the docs through the real loader so the page cannot drift again silently.

## Compatibility

**One behaviour change, in a narrow and precisely bounded population.** Resuming a **non-GTID** MySQL file/pos position against an instance whose `@@server_uuid` has changed now stops instead of re-copying. Three commands reach that door and the remedy differs for each, which the refusal spells out: a warm `sync start` takes `--restart-from-scratch`; `sync start --position-from-manifest` needs a manifest captured from *this* instance, or a fresh `backup full` to make one (`--restart-from-scratch` is rejected alongside `--position-from-manifest`, so it is not the answer there); `backup incremental` needs a fresh `backup full`.

Nothing else reaches that arm, and this is worth being concrete about rather than reassuring: a MySQL at `gtid_mode=ON` takes the GTID path, where a replaced instance is caught **by construction** — a GTID is `<server_uuid>:<seq>`, so a fresh instance's executed set cannot contain the old UUID's transactions. Vitess and PlanetScale never reach this code at all; both use VStream, which has its own per-shard lineage check. MariaDB has its own lineage path. And `@@server_uuid` is stable across restarts — it changes only on a fresh install, a wiped or restored data directory, or a failover to another host, none of which happen unattended in a non-GTID deployment.

The companion arm is deliberately **unchanged**: when the `@@server_uuid` probe cannot run at all, sluice still proceeds with a warning rather than refusing. That value is read once at stream open, so an empty result is not a transient blip — the realistic cause is a proxy or managed service that does not expose the variable, and failing closed there would hard-block those deployments rather than catch anything.

The new D1 refusal fires only against a source that renders `REAL` values lossily, which live D1 does not. No flag added, renamed or removed; backup format version unchanged at 10.

## Also in this release

`ir.Interval` now records that PostgreSQL's interval typmod — the field range and fractional-seconds precision — does not survive sluice's type model, so a PG→PG migrate lands every declaration as bare `interval`. Measured on real PostgreSQL 16, the **values** are byte-identical (bare `interval` is the widest interval type and PG rounds on store, so the source had already rounded); what is lost is the target's constraint, and the harm arrives at cutover rather than during the copy. Carrying the typmod would move the backup schema fingerprint and repartition every existing chain containing an interval column, so it is a deliberate deferral with a tripwire test rather than an oversight — the argument is recorded where the next person to touch that type will find it.

## Who needs this

Upgrade promptly if you migrate from Cloudflare D1 — the render-fidelity gap is the one item here whose failure mode is silent. Upgrade if you run sync from a non-GTID MySQL source, and read the Compatibility note first. Everyone else can take this at their leisure; the remaining items are a documentation fix and a recorded deferral.

## Install

```sh
# Homebrew
brew install sluicesync/tap/sluice

# Scoop (Windows)
scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket
scoop install sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.146.0
```

Container image: `ghcr.io/sluicesync/sluice:v0.146.0`
