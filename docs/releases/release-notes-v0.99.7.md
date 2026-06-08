# sluice v0.99.7

**A Vitess robustness release.** Online-DDL cutovers no longer leak Vitess shadow-table rows into the target, and a primary-only Vitess cluster now works — or fails loudly — instead of wedging silently. Drop-in upgrade from v0.99.6; a no-op for migrations that don't run online DDL on a PlanetScale/Vitess source.

## Fixed

- **Vitess internal / online-DDL shadow tables (`_vt_*`) are excluded from VStream — a silent-loss hazard during online-DDL cutovers.** While an online DDL (`gh-ost`/`vreplication`-backed `ALTER`) is in flight, Vitess materializes transient shadow tables (`_vt_vrp_*`, `_vt_hld/prg/evc/drp/…` — the unified `_vt_<op>_<uuid>_<timestamp>_` form, vitessio/vitess#14582) and emits their rows + DDL on the stream. sluice previously forwarded those to the target, which at cutover could write rows under an internal table name or apply churn that never belonged in the user's schema. sluice now anchors exclusion to Vitess's own `schema.IsInternalOperationTableName()` helper — not a static name list, so it tracks Vitess's evolving naming — and drops both the row events and the shadow-table DDL across every dispatch path. The user's real tables, including the freshly-cut table an online DDL swaps in, flow through untouched. Validated end-to-end against a **full Vitess 24 cluster** (the real online-DDL scheduler) through a completed cutover with zero row loss, including complex shapes (column drop, enum add/extend).

- **A primary-only Vitess cluster no longer wedges silently — it works, or fails loudly.** sluice's pure CDC tail requests a `REPLICA` tablet by default. Against a cluster with **no replica** — a PlanetScale **development** branch, or a minimal self-hosted Vitess — vtgate has no `REPLICA` to serve the stream, yet keeps heart-beating while emitting no data, so the reader hung forever with `Err() == nil`: a silent stall. Two fixes:
  - a **first-event liveness watchdog** turns that wedge into a **loud, actionable error** (`vstream_liveness_timeout`, default 30s; keyed on the absence of any *non-heartbeat* event, so a legitimately idle-but-healthy source never false-trips);
  - a new **`vstream_tablet_type={primary|replica|rdonly}` DSN parameter** (default `replica`, unchanged for PlanetScale production) lets a primary-only cluster stream from the primary via `vstream_tablet_type=primary`.

  A COPY-resume (mid-snapshot cursor) still targets `PRIMARY` regardless. Pinned on a primary-only Vitess-cluster harness: the default tail fails loudly, the `primary` tail delivers with zero loss.

## Compatibility

- No breaking API or CLI changes. Drop-in from v0.99.6. New optional DSN knobs (`vstream_tablet_type`, `vstream_liveness_timeout`) default to the prior behavior.
- PlanetScale **production** branches are unaffected by both fixes (they have replicas; they don't surface `_vt_*` tables to consumers) — this hardens **online-DDL cutovers** and **primary-only / dev-branch / self-hosted** Vitess topologies.

## Who needs this

- **Anyone migrating from or syncing a Vitess/PlanetScale source while online DDL may run** — the `_vt_*` exclusion closes a silent-loss class at cutover.
- **Anyone pointing sluice at a primary-only Vitess** (PlanetScale dev branch, minimal self-hosted) — set `vstream_tablet_type=primary`; a misconfiguration now fails loudly within seconds instead of hanging.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.7`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.7`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
