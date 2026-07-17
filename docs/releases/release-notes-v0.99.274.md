# sluice v0.99.274

Post-audit hardening plus a broadened MariaDB LTS test matrix. This release is mostly internal quality and coverage — there is **no change to any successful path**.

## Added

- **MariaDB test coverage now spans the full LTS spread — 10.11, 11.4, 11.8, and 12.3 — plus the 13.1 preview.** 11.8 and 12.3 run as **required** CI integration legs; the 13.1 preview (pulled from `quay.io/mariadb-foundation/mariadb-devel`) runs as an **informational, non-blocking** leg (a preview must never gate a merge). Every one of these was live-ground-truthed for the native `uuid`/`inet` CDC byte layout, and it is **byte-identical across all of them** — which closes the residual-risk note in ADR-0171 (that a future MariaDB line could silently change the storage byte order and slip past the decoder's width guard) with real evidence and **no codec change**. The binlog-status probe fallback (`SHOW BINARY LOG STATUS` / `SHOW MASTER STATUS` / `SHOW BINLOG STATUS`) was confirmed to cover the whole spread with zero code change — notably, 12.3 still accepts `SHOW MASTER STATUS` (so the "MariaDB 12 removes it" expectation is premature), and 13.1 accepts `SHOW BINARY LOG STATUS`.

## Changed

- **Post-audit hardening (from the 2026-07-17 confirming audit's Medium/Low findings — no user-facing behavior change):**
  - The MariaDB native-type **schema mapping and CDC decoder are now coupled through a single source-of-truth registry**, so a native binary-storage type can no longer be schema-mapped without also gaining a decoder — a mapping-without-decoder would have silently stringified raw binlog bytes (the Bug-74 silent-mis-decode class). This is a behavior-preserving structural refactor with a lockstep test.
  - The MariaDB integration images are now **GHCR-mirrored**, removing a docker.io cold-pull flake class that could red-fail required shards and stall the publish gate.
  - The MariaDB `inet6` rendering is now pinned through the **text protocol** (the actual cold-start bulk-copy path) as well as the binary protocol — confirmed byte-identical on 11.4 and 10.11, so no cross-wire-path divergence existed.
  - MariaDB is now represented in the **perf-parity matrix** (its technique inheritance: native-binlog concurrent cold-start, shared row writers, coalesced CDC apply).
  - The operator **error-code catalog, ADR index, and roadmap** are reconciled with the shipped MariaDB arc — the two retained MariaDB-CDC refusal codes are consistently marked retained-but-unemitted — and a doc-sync test now fails CI on stale active-refusal prose.

## Compatibility

- **No behavior change.** Everything here is added test coverage, CI hardening, a behavior-preserving internal refactor, and documentation reconciliation. The one thing an operator will notice is confidence: if you run MariaDB 11.8 or 12.3, it is now a validated, CI-gated source/target.

## Who needs this

Anyone running (or planning to run) a MariaDB source/target on **11.8 or 12.3** — those are now covered by required CI legs and the same value-fidelity ground-truthing as 10.11/11.4. No action required for existing users.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.274
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.274`
