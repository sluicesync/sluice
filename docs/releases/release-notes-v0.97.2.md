# sluice v0.97.2

## v0.97.2 — `sluice sync from-backup run` now follows rotated chains

Closes the Phase 4.5 multi-segment broker deferral identified by the v0.97.1 Round D soak. The broker no longer refuses loudly when the producer rotates the backup chain into a new segment — it follows the lineage across the rotation seam cleanly.

### Added

- **Multi-segment broker following — Phase 4.5 deferral closed.** `sluice sync from-backup run` previously refused loudly on any chain with more than one segment with the documented `Broker following a multi-segment lineage is deferred (ADR-0046 Phase 4.5); point the broker at a single-segment backup, or restore the multi-segment lineage with sluice restore instead` message. v0.97.2 walks the full lineage instead — `buildBrokerChain` now delegates to `buildLineageChain` directly (the same multi-segment walker `sluice restore` has always used). The broker's apply loop already skips full manifests unconditionally, so segment-N+1's rotation snapshot is auto-skipped; ADR-0067's born-contiguous rotation guarantees the new segment's first incremental covers the `(P_N, S]` overlap from the prior segment's end position; ADR-0010's idempotent applier handles the brief re-application of any changes that landed between the broker's last advance and the rotation moment.
- The Round D soak (2026-05-31; see `sluice-testing/session-reports/v0.97.1-roundD-broker-soak.md`) characterized the gap and proved it was tractable with the existing chain walker + idempotent applier infrastructure — the deferral was a conservative scope-narrowing in Phase 4.5, not an architectural limitation.
- Pinned by `TestBuildBrokerChain_MultiSegmentFollows` (3-segment lineage walked end-to-end with chain ordering + per-link Kinds asserted) + `TestBuildBrokerChain_DeferralRemoved` (the literal Phase 4.5 refusal is gone on a 2-segment minimal case). ADR-0046 updated to mark the deferral CLOSED with the resolution path documented inline.

### Compatibility

- Single-segment broker behavior is **byte-identical** to v0.97.1 and earlier — the multi-segment walker handles single-segment chains correctly (it's how `sluice restore` has always worked).
- Operators who'd been working around the constraint by omitting `--retain-rotate-at-chain-length` on their producer can now safely enable rotation; the broker tail follows through the rotation seam without intervention.
- Operators who'd been using `sluice restore` periodically instead of the broker for rotated chains can switch to the broker for steady-state continuous-replication.

### Who needs this

- **Operators running `sluice sync from-backup run` on chains with rotation enabled.** Before v0.97.2 the broker refused at the first rotation transition; the workarounds (disable producer rotation or use `sluice restore` periodically) had operational tradeoffs. v0.97.2 removes the constraint.
- **Long-running continuous-replication workflows on backup-as-broker topology.** Cross-region, air-gapped, compliance-driven backup-as-audit-trail, multi-target fan-out — all of these now work without the "single-segment-or-bust" caveat.

### Open backlog after this release

**Zero numbered bugs. Zero tracked silent-loss-class follow-ups.** v0.97.2 closes the last identified gap from the v0.94.0 → v0.97.x arc plus the post-arc Round D finding. The cookbook entry covering broker-vs-`sync start` decision matrix (no longer needs to document a single-segment constraint; just the architectural framing) is queued separately.
