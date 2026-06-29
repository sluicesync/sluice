# sluice v0.99.159

**CRITICAL fix: restoring an incremental backup chain silently corrupted integer values above 2^53 (e.g. large IDs), and could leave deleted rows on the target. If you rely on `sluice restore` / `sync from-backup` of an *incremental* chain, upgrade. Full backups, and live (non-backup) CDC, were never affected. The on-disk backup format is unchanged, so existing backups restore correctly with this build.**

## Fixed

**CRITICAL — incremental backup-chain restore silently corrupted `int64` values above 2^53 (Bug 172).** The incremental change-chunk *decoder* unmarshalled each record into a structure whose row maps were `map[string]any`, so Go's `encoding/json` decoded the tagged `int64` value envelope's number into a **`float64` before sluice's value codec ever ran** — losing precision for any integer beyond 2^53 (≈ 9.0 × 10¹⁵). On a `sluice restore` / `sync from-backup` replay of an incremental chain this caused **silent data corruption**:

- a large id such as `9007199254782995` came back as `…996`; and
- worse, because a corrupted *before-image* no longer matched any target row, replayed `DELETE`s affected zero rows — so **rows that were deleted on the source survived on the target**, with no error.

This affected incremental-chain restore for any source whose changes carry int64 values above 2^53 (large/snowflake IDs, big counters), across all source engines. The **full-snapshot (row-chunk) decoder was always correct** — it decodes each value as a `json.RawMessage` and hands the exact bytes to the codec — so full backups never corrupted, and live (non-backup) CDC apply was never on this path.

**Fix.** The change-chunk decoder now holds each row value as a `json.RawMessage` (matching the always-correct row-chunk path) and hands the exact wire bytes to the value codec, so every type — `int64` above 2^53, and `int64` nested inside array/JSON-column values — round-trips losslessly. The on-wire chunk format is byte-identical (a `map[string]json.RawMessage` marshals exactly as before), so **existing backups now restore correctly with this build** (the corruption was decode-side only; the bytes on disk were always right).

## How it was found

The SQLite backup-roundtrip test in the SQLite/D1 validation program: SQLite *full* backups verified byte-exact, and the *chain* restore surfaced this. It was a textbook value-fidelity coverage gap — the existing change-chunk round-trip test used `int64(1)` (which `float64` represents exactly) and a drift-tolerant comparison, so it never exercised the unsafe range. The new pin uses 2^53+1, int64 min/max, the exact repro values, **and** int64 nested in a list and a JSON-column map, with strict per-column equality across Insert / Update / Delete, so any precision drift fails loudly.

## Compatibility

The backup file format is unchanged; no re-backup is required, and existing incremental chains restore correctly once you're on this build. Independently reviewed for value-fidelity (the fix routes every value family — including nested int64, uint64, float64, bytes, time, NULL — through the exact-bytes codec path, byte-identical to the row-chunk decoder). The `-race` integration gate passed before tagging.

## Who needs this

Anyone who takes **incremental logical backups** and may **restore the chain** (`sluice restore` / `sync from-backup`) where the data contains integers above 2^53 — large primary keys, snowflake IDs, high counters. Upgrade before relying on such a restore. Full-backup-only users and live-CDC users are unaffected, but upgrading is recommended regardless.

---

**Install:** brew install sluicesync/tap/sluice · go install sluicesync.dev/sluice/cmd/sluice@v0.99.159 · **Container:** ghcr.io/sluicesync/sluice:0.99.159
**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
