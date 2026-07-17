# ADR-0171: MariaDB native uuid / inet6 / inet4 CDC binlog decode

## Status

**Accepted (2026-07-17).** Roadmap item 73 Phase 3 follow-up, building on ADR-0169 (native uuid/inet types for schema + bulk copy) and ADR-0170 (Phase-3 CDC + the loud refusal this ADR lifts). Every byte-order and text-rendering claim below was **ground-truthed live** on `mariadb:11.4` and `mariadb:10.11` (identical on both) — the raw bytes go-mysql's `RowsEvent` delivers were captured for known values and the canonical text was ground-truthed against the server's own `SELECT`/`HEX` rendering. Pinned by a unit byte→text family matrix plus a CDC integration matrix on both LTS lines (same-engine value fidelity + cross-engine `mariadb → postgres` and `mariadb → mysql` convergence through the live CDC stream).

**Concurrency note:** this chunk touches the binlog CDC row-decode path (`decodeBinlogRow` on the pump goroutine). The `-race` Integration job is CI-only (the dev box is CGO=0), so it **must pass `-race` before any tag.**

## Context

ADR-0170 shipped MariaDB CDC but **refused** any table with a native `uuid` / `inet6` / `inet4` column in CDC scope (`SLUICE-E-CDC-MARIADB-NATIVE-TYPE-UNSUPPORTED`, pre-data, on all targets). The reason was loudness asymmetry: the binlog carries these fixed-width types as **raw storage bytes**, which `decodeValue`'s `ir.UUID`/`ir.Inet` handler — written for MySQL, where these live in text-backed `VARCHAR` columns whose binlog bytes ARE the text — would stringify into a wrong-but-valid-looking value. A Postgres target rejects that string loudly (22P02), but a **MySQL-family target (`CHAR(36)`/`VARCHAR(45)`) silently accepts it** — a reachable silent corruption. The bulk-copy path is unaffected: it reads these columns as TEXT via the driver (a plain `SELECT` returns MariaDB's own canonical rendering), so `decodeString` is correct there.

This ADR implements the faithful binlog decode and lifts the refusal.

## The empirical ground truth (this was the whole risk)

The roadmap draft assumed MariaDB reorders the UUID time fields for index locality (analogous to MySQL's `UUID_TO_BIN(x, 1)` swap), and warned that a straight big-endian format would be a valid-but-WRONG uuid — the Bug-74 silent-corruption class. **Both of that premise's parts were tested against live servers and the reordering premise is FALSE.** Two findings are load-bearing:

### Finding 1 — NO byte reordering (uuid is canonical big-endian)

A known uuid `01234567-89ab-cdef-8123-456789abcdef` was inserted and the raw `RowsEvent` bytes captured:

```
canonical text:  01234567-89ab-cdef-8123-456789abcdef
binlog bytes:    01 23 45 67 89 ab cd ef 81 23 45 67 89 ab cd ef
```

The bytes are **identical to the text with the dashes removed** — MariaDB stores UUID in canonical big-endian order, NOT the `UUID_TO_BIN(x,1)` time-field swap. A reordered layout would have delivered `cd ef 89 ab 01 23 45 67 …`. The decode is therefore a straight hex format into `8-4-4-4-12` (lowercase). `HEX(u)` also returns canonical order, and `SELECT` on both LTS lines confirms lowercase-canonical output; a mixed-case insert normalizes to lowercase.

(MariaDB's `UUID` type also validates the RFC-4122 variant nibble on INSERT — e.g. it rejects `…-cdef-0123-…` (variant `0`) and the specific `…-8000-…` group-4 value — so all pinned test values use accepted variants. This is an insert-side quirk, irrelevant to read/decode, but noted so future test-value edits don't trip it.)

### Finding 2 — trailing-zero stripping (the real subtlety)

MariaDB frames these fixed-width types in the binlog as **length-prefixed `CHAR`/`BINARY`** (`MYSQL_TYPE_STRING`, table-map meta length 16 for uuid/inet6, 4 for inet4). go-mysql's `decodeString` honours that 1-byte length prefix, which MariaDB writes as the **significant length with trailing `0x00` bytes REMOVED**. Captured (both LTS lines identical):

| value | source | binlog bytes (hex) | recv len |
|---|---|---|---|
| `00000000-0000-0000-0000-000000000000` | uuid nil | *(empty)* | 0 |
| `01234567-89ab-cdef-8100-000000000000` | uuid | `0123456789abcdef81` | 9 |
| `ffffffff-…-ffffffff0000` | uuid | `ffffffff…ffff` | 14 |
| `::` | inet6 | *(empty)* | 0 |
| `2001:db8::` | inet6 | `20010db8` | 4 |
| `0.0.0.0` | inet4 | *(empty)* | 0 |
| `10.0.0.0` | inet4 | `0a` | 1 |
| `192.168.1.10` | inet4 | `c0a8010a` | 4 |

So the decode **must right-pad the received bytes back to the fixed width** (16 for uuid/inet6, 4 for inet4) before formatting; the significant length is NOT the type width. A value LONGER than the width is a corruption signal and is refused loudly (never truncated).

**Consequence:** the received byte length cannot distinguish inet4 from inet6 (a stripped inet6 can be ≤4 bytes, e.g. `2001:db8::` → 4 bytes, `::` → 0 bytes). The pad width therefore comes from the column's **declared native kind**, captured from `information_schema.data_type` in `loadTableSchema` and threaded (parallel to the columns) into the decode — never inferred from `len(raw)`.

### Finding 3 — inet6 text must match MariaDB's `inet_ntop6`, not Go's `netip`

The bulk-copy (cold-start) path lands MariaDB's own text; the CDC decode must produce the **same** string so a snapshot and the CDC tail converge byte-for-byte on the target. Go's `net/netip` `String()` matches MariaDB across the whole compression/IPv4-mapped matrix **except IPv4-COMPATIBLE addresses** (`::a.b.c.d`, leading zero-run length 6): MariaDB renders those in dotted form (`::1.2.3.4`) where `netip` renders pure hextets (`::102:304`). MariaDB uses the classic BSD `inet_ntop6` algorithm, with one deviation from BIND9: it renders the trailing two words as a dotted quad only when the leading zero run starts at word 0 AND is exactly **6 words** (IPv4-compatible) OR **5 words with word 5 == `0xffff`** (IPv4-mapped) — it does NOT dotted-render a 7-word leading run (so `::2`, `::100`, `::ffff` stay hextets, unlike BIND9). `sluice` reimplements this exactly (`mariadbInet6Text`), verified byte-exact against the server across the family × shape matrix (compressed, IPv4-mapped, IPv4-compatible, NAT64 prefix, leftmost-run-on-tie, leading/trailing runs). inet4 is a plain dotted quad.

## Decision

Add a MariaDB-native CDC decode path, gated by the reader's flavor and the column's declared native kind, and lift the ADR-0170 refusal.

1. **`mariadbNativeKind`** (`value_decode_mariadb.go`) — `none`/`uuid`/`inet4`/`inet6`, derived from `information_schema.data_type` (`mariadbNativeKindOf`). Captured parallel to `Columns` on `tableSchema.NativeKinds` in `loadTableSchema`. `none` for every ordinary column and every non-MariaDB source (only MariaDB reports these `data_type` strings).

2. **`decodeMariaDBNative`** — right-pads the raw bytes to the kind's fixed width (refusing an over-width value) and formats: uuid → lowercase `8-4-4-4-12`; inet6 → `mariadbInet6Text`; inet4 → dotted quad. NULL passes through.

3. **`decodeBinlogRow`** gains `natives []mariadbNativeKind` + `flavor Flavor`. For a MariaDB column with a non-`none` kind it uses `decodeMariaDBNative`; every other column takes the ordinary `decodeValue` path (unchanged, including the zero-date policy). The bulk-copy `decodeValue` path is untouched — `ir.UUID`/`ir.Inet` still `decodeString` there, correct for the driver text and for MySQL's VARCHAR-backed uuid/inet.

4. **Refusal lifted.** The stream-start preflight (`preflightMariaDBNativeUUIDInet`), the two snapshot-opener scans (`scanMariaDBNativeUUIDInet` in `cdc_snapshot.go` / `cdc_snapshot_concurrent.go`), the add-table `Engine.PreflightCDCScope` implementation, and the shared refusal builder are removed. The `ir.CDCScopePreflighter` interface and the pipeline `add_table` hook are **kept** as a generic, engine-neutral extension point (no engine implements it now). `SLUICE-E-CDC-MARIADB-NATIVE-TYPE-UNSUPPORTED` stays **registered** in the `sluicecode` catalog (removing a published code is breaking) but is no longer emitted.

### Why the decode discriminator is the source `data_type`, not the IR type

At the CDC-reader layer `loadTableSchema` builds column types directly from `information_schema` — `--type-override` is a downstream (pipeline/translate) concern that never touches this layer. So a MariaDB native `uuid` is always `ir.UUID` here with `data_type = 'uuid'`, and the binlog always carries its binary bytes. Keying the decode on `data_type` (via `NativeKinds`) is therefore authoritative and immune to the override ambiguity that made the IR-type-based add-table refusal conservative. The bulk-copy path is the other consumer and is deliberately not routed through the native decoder — it reads text, and text is what `decodeString` wants.

## Consequences

- MariaDB native `uuid`/`inet6`/`inet4` columns now stream through CDC to every target, converging byte-for-byte with the cold-start snapshot. The MySQL-family target that would have silently corrupted now holds the exact source text.
- The bulk-copy path, all MySQL flavors, and every non-native column are byte-identical (the native branch is gated on `flavor == FlavorMariaDB && kind != none`).
- **Residual risk (stated explicitly):** the decode was ground-truthed against `mariadb:11.4` and `mariadb:10.11`. If a future MariaDB line changes the binlog storage layout for these types (byte order or the length-prefix framing), the decode would diverge silently. The cross-engine integration pins (src == dst on the real target) are the guard, and they run on both LTS lines; a new LTS line should be added to that matrix. The over-width refusal catches a framing change that produces MORE bytes than the width, but not one that merely reorders within the width — the reorder guard is the empirical pin, not a runtime check.

## Alternatives considered

- **Use `net/netip` for inet6.** Rejected: it diverges from MariaDB on IPv4-compatible addresses (Finding 3), which would silently split cold-start text from the CDC tail on that shape.
- **Infer inet4-vs-inet6 from the received byte length.** Rejected: trailing-zero stripping makes the length ambiguous (Finding 2). The declared `data_type` is authoritative.
- **Keep a narrowed refusal for any shape.** Rejected: at the CDC-reader layer there is no unsupported native shape — every native `uuid`/`inet4`/`inet6` decodes faithfully. An over-width byte string is the only refusal, and it is a corruption signal, not a type-class gap.
