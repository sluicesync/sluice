# sluice v0.99.271

MariaDB flavor Phase 3 — continuous CDC sync from a MariaDB source (roadmap item 73 Phase 3, ADR-0170). This completes the MariaDB engine arc: bulk migrate (Phase 1, v0.99.268), type fidelity (Phase 2, v0.99.270), and now CDC.

## Added

- **CDC from a MariaDB source is now supported.** `sync start` and backup CDC chains stream continuous changes off a MariaDB primary. MariaDB replicates with domain-based GTIDs (`0-100-38`, domain-server-sequence), which the binlog reader now parses, serializes, resumes, and advances per event — flavor-branching the go-mysql GTID parser and the MariaDB-specific position SQL (`@@gtid_binlog_pos` for cold-start; MariaDB has no `GTID_SUBSET`/`@@gtid_purged`), and handling the `MariadbGTIDEvent` that opens each transaction. (MariaDB emits no `BEGIN` QueryEvent, so the vanilla pump would otherwise never advance the position — a silent wrong-resume hazard closed here.) Cold-start snapshot → CDC handoff → convergence → stop → warm-resume-exactly-once is validated end-to-end against a Postgres target, with JSON-identity and temporal values carried verbatim through the CDC path. Version floor MariaDB 10.11 LTS; validated live on 11.4 and 10.11. The binlog-status probe accepts `SHOW BINLOG STATUS` (MariaDB's spelling, working on 10.11+ and forward-compatible with MariaDB 12) alongside the existing forms.
- **A purged-position resume is refused loudly.** MariaDB exposes no honest way to pre-check GTID reachability (`@@gtid_binlog_state` is unchanged across `PURGE BINARY LOGS`), so sluice classifies the server's reactive error 1236 (“Could not find GTID state requested…”) as an invalid-position refusal that routes to a clean cold-start rather than a silent wrong-position stream.

## Fixed

- **Native `uuid`/`inet6`/`inet4` columns are refused loudly, pre-data, when they would enter a CDC stream — closing a silent-corruption path on MySQL-family targets.** These types read correctly under bulk `migrate` (Phase 2, the driver returns text), but their binlog carries raw storage bytes the value-decoder cannot yet interpret; the decoded string is garbage. A Postgres target rejects it (SQLSTATE 22P02), but a MySQL-family target's `CHAR(36)`/`VARCHAR(45)` would silently accept it. sluice now refuses at CDC stream start, at cold-start snapshot open (before any rows copy), and at mid-stream `add-table`, on all targets, with the coded `SLUICE-E-CDC-MARIADB-NATIVE-TYPE-UNSUPPORTED` — steering to bulk `migrate` for those columns. Faithful binlog decode of these types (which first requires ground-truthing MariaDB's uuid byte order — a naive format would be a valid-but-wrong silent corruption) is a filed follow-up.
- **Warm-resume of a MariaDB continuous sync no longer crash-loops.** The streamer stamps a resumed position with the source engine name `mariadb`, which the binlog position-decoder did not accept — the same Bug-142 shape the vitess entry closed. Surfaced only by the full cross-engine sync path (the reader always encodes `mysql`, so only the resume-retag exercises it).

## Compatibility

- **MariaDB CDC is additive.** Bulk migrate, type fidelity, and backup/restore are unchanged; MariaDB now additionally supports continuous CDC. The one deliberate refusal is native `uuid`/`inet` columns through CDC (coded, loud, pre-data) — bulk `migrate` of those columns is unaffected. The MySQL-8 binlog path is byte-identical (every MariaDB branch carries the MySQL behavior as its zero value).

## Who needs this

Anyone who wants continuous replication (not just a one-shot bulk copy) from a MariaDB source onto Postgres, MySQL, PlanetScale, or another supported target. With Phases 1–3 shipped, MariaDB is now a first-class source for both `migrate` and `sync`.

## Install

- Binaries: https://github.com/sluicesync/sluice/releases/tag/v0.99.271
- Homebrew: `brew install sluicesync/tap/sluice`
- Scoop: `scoop bucket add sluicesync https://github.com/sluicesync/scoop-bucket; scoop install sluice`
- winget: `winget install sluicesync.sluice`
- Docker: `docker pull ghcr.io/sluicesync/sluice:v0.99.271`
