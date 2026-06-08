# sluice v0.99.6

**A self-hosted-Vitess compatibility fix.** A no-op for PlanetScale users — drop-in upgrade from v0.99.5.

## Fixed

- **`--source-driver=planetscale` no longer leaks `vstream_*` DSN parameters into the MySQL session.** sluice's `vstream_*` DSN extensions (`vstream_endpoint`, `vstream_transport`, `vstream_auth`, `vstream_shards`, …) are consumed only by the gRPC CDC reader, but the schema-reader / row-reader / schema-writer / change-applier paths passed them through to the underlying MySQL connection, which emitted them as `SET vstream_endpoint = …` session vars on connect. **Self-hosted Vitess / vttestserver rejects those** (`Error 1105` for the IP-bearing endpoint, `VT05006 unknown system variable` for the rest), so a VStream-source cold-start failed at "open source schema reader" before any data moved. The params are now stripped centrally before every MySQL connection (one `openDB` choke point, leak-proof against future paths). **Real PlanetScale was unaffected** (its vtgate tolerates the unknown vars), so this is a no-op there and a fix for self-hosted Vitess. Pinned by a new `vttestserver`-backed integration test — the first to exercise the PlanetScale `Open*` (non-CDC) path against a real Vitess.

## Who needs this

- **Anyone running sluice against a self-hosted Vitess (or `vttestserver`) source** via `--source-driver=planetscale`. Before v0.99.6 the cold-start failed before reading any data; now it connects cleanly. PlanetScale users are unaffected either way.

---

**Install:** `brew install sluicesync/tap/sluice`  ·  `go install sluicesync.dev/sluice/cmd/sluice@v0.99.6`  ·  **Container:** `ghcr.io/sluicesync/sluice:0.99.6`
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
