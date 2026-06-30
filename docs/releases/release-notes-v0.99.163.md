# sluice v0.99.163

**Hardening follow-up to the v0.99.162 namespace-rename feature: the continuous `sync` path now runs the same case-fold-collision guard on renamed target names that `migrate` already did. A small, fully backward-compatible release (no behavior change unless you use `--map-database`/`--map-schema` against a folding MySQL target).**

## Fixed

**`sync` multi-namespace rename now guards case-fold collisions on the target (ADR-0142 follow-up).** v0.99.162 added `--map-database`/`--map-schema` to both `migrate` and `sync`, and `migrate` ran a fold-collision preflight on the renamed target names (a folding MySQL target — `lower_case_table_names != 0` — can collapse two distinct names to one identifier, silently merging two namespaces). The `sync` path only carried the engine-agnostic *exact* many-to-one guard, not the case-fold one. v0.99.163 wires `preflightNamespaceFoldCollisions` into the streamer's `resolveStreamDatabases` (covering both cold-start and warm-resume), so a case-fold collision between renamed target namespaces is refused **loudly before any data moves** on the sync path too. No-op on a Postgres target (case-sensitive schemas); the identity map (no rename) is byte-for-byte unchanged.

Also adds a streamer multi-schema rename CDC integration test (PG→PG, `--map-schema a=x,b=y`) that asserts a cold-start plus a full insert/update/delete CDC workload route exactly-once into the renamed target schemas `x`/`y`, with no cross-schema bleed and the source-named schemas never created on the target. The `-race` integration gate passed before tagging.

## Who needs this

Anyone using `--map-database`/`--map-schema` (v0.99.162) on a continuous `sync` into a MySQL target. Migrate users, Postgres targets, and anyone not using the rename map are unaffected.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.163 · **Container:** ghcr.io/sluicesync/sluice:0.99.163
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
