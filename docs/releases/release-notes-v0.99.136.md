# sluice v0.99.136

**Fixes two fleet-observability warts surfaced by the new dashboard, plus a branding pass on the dashboard itself: shared-source PostgreSQL fleets no longer spuriously fail+restart a sync at cold-start (idempotent publication creation), a recovered sync no longer shows a stale error/failure-count, and the `sync run --dashboard-listen` page now carries the sluice logo + brand colors. Fully drop-in over v0.99.135.**

## Fixed

**Shared-source PostgreSQL fleets: publication creation is now idempotent against the concurrent-create race.** When several PostgreSQL-source syncs share one source — the `sync run` command-center's normal case — they ensure the same publication concurrently at cold-start. The prior check-then-create had a TOCTOU window: two sessions both passed the existence check and both ran `CREATE PUBLICATION`, so one hit a unique-violation on `pg_publication` (SQLSTATE 23505), failed, and the supervisor restarted that sync. sluice now treats the duplicate as benign (the publication already exists), re-reads, and reconciles its scope instead of failing — so a shared-source fleet cold-starts cleanly with no spurious failure + restart. No data was ever at risk (the restart self-healed); this removes the noise and the scary boot-time error. Single-sync runs are unaffected. Found via the fleet-dashboard demo.

**Fleet dashboard / `sync status --all`: a recovered sync no longer shows a stale last error and failure count.** The supervisor's consecutive-failure reset is lazy — it fires only when the *next* failure is recorded — so a sync that failed, recovered, and has been running healthily kept reporting its old consecutive-failure count and `last_error` indefinitely: a green/`running` sync displaying a scary, no-longer-relevant error. `Supervisor.Snapshot()` now derives health at read time — a sync that has been `running` past the healthy-run threshold reports zero consecutive failures and an empty last error (the lifetime restart count is preserved). It is a read-only derivation using the exact threshold the lazy reset already uses, so the `MaxConsecutiveFailures` isolation cap is untouched (clearing eagerly on transition-to-running would have defeated it — a crash-loop could evade isolation).

## Changed

**Fleet dashboard: sluice branding.** The `sync run --dashboard-listen` page now carries the sluice gate mark (inline SVG logo + favicon) and the sluicesync brand palette (primary `#F35815`, deep `#C0410A`, flow accent `#FFDCC6`), matching sluicesync.com — a cosmetic refresh of the v0.99.135 dashboard, no behavior change.

## Compatibility

Fully drop-in over v0.99.135. The two fixes are strictly safer (no spurious cold-start restart; honest status for recovered syncs) with no configuration or data-path change; the branding is cosmetic. The dashboard remains opt-in (`--dashboard-listen`) and read-only. No flag defaults move.

## Who needs this

Operators running a `sync run` fleet — especially several PostgreSQL-source syncs sharing one source — get a clean cold-start (no spurious failure+restart) and an honest dashboard (recovered syncs read green with no stale error). Anyone using the dashboard gets the branded look. Single-sync users are unaffected. Upgrading is optional and safe.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.136 · **Container:** ghcr.io/sluicesync/sluice:0.99.136
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
