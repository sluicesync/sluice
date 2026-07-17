# sluice v0.99.266

The last of the provider-campaign CRITICALs: MySQL CDC silently lost every UPDATE on sources with partial binlog row images — Azure Database for MySQL's platform default.

## Fixed

- **CRITICAL (Bug 193): MySQL binlog CDC silently lost every UPDATE when the source ran `binlog_row_image=MINIMAL` or `NOBLOB`.** The partial before-image built zero-match WHERE predicates that the resume-idempotency path absorbed — the stream stayed green while row content diverged, and default-depth verify sampled past it. Live-proven on Azure Database for MySQL Flexible Server, whose platform default is MINIMAL. sluice now reads `@@GLOBAL.binlog_row_image` at every CDC start — sync cold start (before the bulk copy runs), warm resume, and `backup incremental` — and refuses partial images with the new coded `SLUICE-E-CDC-ROW-IMAGE-PARTIAL`, naming the `SET GLOBAL binlog_row_image=FULL` remedy and the Azure parameter recipe. The refusal also covers `binlog_row_value_options=PARTIAL_JSON`, whose partial-update events were previously silently dropped even on FULL-image sources.
- **Defense-in-depth:** a dispatch-time belt keyed on the binlog's present-columns bitmap stops the stream loudly if a partial image slips past the preflight (session-level overrides, or replaying segments recorded before the flip); the UPDATE before-image narrows to PK columns (the Bug-88/Bug-92 parity); and PK-less tables whose identity MySQL keys on a unique index — invisible to a PRIMARY-only lookup — refuse partial DELETE images instead of silently zero-matching.

## Compatibility

- **One deliberate loud behavior change:** MINIMAL/NOBLOB sources that previously "streamed" (losing every UPDATE) now refuse at CDC start with the remedy — dynamically settable on every known managed platform. **Prospective-only caveat:** backup chains recorded under MINIMAL by earlier versions carry the nil-filled images baked in at capture; re-take backups of such sources after setting FULL.

## Who needs this

**Anyone running MySQL CDC against Azure Database for MySQL — upgrade and set `binlog_row_image=FULL` (the refusal walks you through it) before trusting UPDATE replication.** Self-managed MySQL with non-FULL row images or `PARTIAL_JSON` enabled: same. If you synced such a source on any earlier version, compare row *content* (not counts) between source and target — `verify --depth=sample` on v0.99.265+ renders both sides faithfully.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.266
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.266`
