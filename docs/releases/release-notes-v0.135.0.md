# sluice v0.135.0

The audit-remediation release: the M0 batch from the 2026-08-31 blind audit. One security hardening in the subsystem v0.134.1 just patched, one deliberate behavior change on MySQL sources, one regression repair, and a publish gate that now actually works. Drop-in from v0.134.1 apart from the MySQL refusal below.

## Security

**The `postgres-trigger` DDL-suppression marker is now a privilege boundary, not a convention.** v0.133.1 suppressed sluice's own setup DDL with a session GUC — but a custom GUC is settable by *any* session, so an unprivileged writer could set it and have their DDL go unrecorded by the only DDL-detection tier (observed: three statements recorded zero `op='X'` rows while identical controls before and after recorded normally). Suppression now additionally requires an *armed* row — the firing backend's own PID, a random nonce, and a freshness window — in `sluice_change_log_meta`, a table `trigger setup` grants to nobody (verified: unprivileged roles get `permission denied` on both SELECT and UPDATE, so the nonce is unreadable as well as unguessable). The fail-safe direction is to **record**: an evidence read that raises records the DDL rather than suppressing it. Nothing is baked at render time, so a `--dry-run` plan applied by hand still suppresses correctly in the operator's own session.

## Changed

**Mid-sync `TIMESTAMP ⇄ DATETIME` MODIFY on a MySQL source now refuses under the default `--schema-changes=forward`.** MySQL resolves that conversion using the *executing session's* `time_zone`, which the replication wire does not carry — so a forwarded MODIFY re-casts the target's pre-existing rows against a different zone and silently diverges them (measured: a 9-hour shift on real MySQL 8.0.46). This is the same class v0.134.0 refused for the Postgres scalars, and it is the more commonly hit member. The refusal is wired into all three of the engine's boundary emitters (binlog, VStream, VStream-snapshot) and is armed for both forwarding paths, including Shape A's boundary router, which applies column-type changes regardless of `--schema-changes`. Precision-only changes (`DATETIME(3)→(6)`) forward and converge exactly as before.

## Fixed

**Postgres `time[] ⇄ timetz[]` array swaps refuse again — a regression introduced in v0.134.0.** That release's timetz projection fix made the two array types project differently, which silently disabled the projection-equality path that had been refusing the swap. The refusal now matches on the type **pair** rather than on projection equality, so it covers all four array variants and cannot be re-opened by a future projection change. The plan's DDL is also transaction-scoped now (`BEGIN … SET LOCAL … COMMIT`), so a hand-applied `--dry-run` plan can no longer leak DDL suppression into the rest of an operator's psql session, with an explicit `ROLLBACK` on the error path so a pinned connection is never returned to the pool mid-transaction.

## Internal

**The release-notes claims gate stops grading itself.** `scripts/check-notes-claims.sh` verified claimed identifiers against the whole tag tree — including the notes file it was grading — so every claim satisfied itself and the gate could never fail for the class it was built to catch. Its content check now uses a positive allowlist (`internal cmd scripts .github docs/adr`) rather than excluding prose paths, because the release commit routinely writes several prose homes about the same change. It gained the self-test it shipped without (`check-notes-claims-selftest.sh`, whose fixtures commit and tag the way the real release flow does), a stricter anti-vacuity floor, and real wiring: the Lint job runs the self-test on every PR and the gate itself on every release-tag push. Also new: `TestSessionGUCCastRoster_EveryCDCLane`, a cross-engine roster that reads each CDC lane's declared type pairs out of that lane's own code and fails closed when a new engine or a new schema-boundary emitter appears unclassified.

## Compatibility

Drop-in from v0.134.1 with one deliberate posture change: a mid-sync `TIMESTAMP ⇄ DATETIME` MODIFY on a MySQL source now stops the stream loudly where it previously forwarded. If your ALTER session and sluice's applier both ran UTC the old forwarding converged; if they differed, prior releases silently diverged pre-existing rows — run `sluice verify` on any table that crossed such an ALTER. The remedy for the refusal is the standard drained-model apply. The `postgres-trigger` security hardening bumps the internal change-log meta schema to v4; existing installs migrate on the next `trigger setup` run and are unaffected until then.

## Who needs this

- **MySQL-source syncs on the default `--schema-changes=forward`:** a mid-sync `TIMESTAMP` ⇄ `DATETIME` MODIFY now refuses instead of forwarding. If you ran one on a prior release with the ALTER session in a non-UTC zone, run `sluice verify` on that table.
- **`postgres-trigger` installs:** re-run `sluice trigger setup` to pick up the suppression hardening (and, if you have not yet, the v0.134.1 security fix — the same re-run covers both).
- Everyone else: upgrade normally; no action.

## Install

```
# Homebrew
brew upgrade sluice

# Scoop (Windows)
scoop update sluice

# Go
go install sluicesync.dev/sluice/cmd/sluice@v0.135.0
```

Container images: `ghcr.io/sluicesync/sluice:0.135.0` (multi-arch; the image tag carries no `v` prefix).

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
